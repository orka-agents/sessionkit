// Package sessionkit captures and installs byte-identical Codex session rollouts
// for cold handoff into isolated Codex homes. It never accesses SQLite.
package sessionkit

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/orka-agents/sessionkit/harness/codex"
	"github.com/orka-agents/sessionkit/internal/budget"
	bundleio "github.com/orka-agents/sessionkit/internal/bundle"
	"github.com/orka-agents/sessionkit/internal/fsx"
	"github.com/orka-agents/sessionkit/internal/harness"
	"github.com/orka-agents/sessionkit/internal/journal"
	"github.com/orka-agents/sessionkit/internal/jsonl"
)

var adapters = map[Harness]harness.Adapter{Codex: codex.Adapter{}}

func adapterFor(h Harness) (harness.Adapter, error) {
	a, ok := adapters[h]
	if !ok {
		return nil, reject("harness", "unsupported harness")
	}
	return a, nil
}
func reject(code, message string) error {
	return &RejectionError{Rejections: []Rejection{{Code: code, Message: message}}}
}

func Inspect(ctx context.Context, src Source, limits Budget) (Inspection, error) {
	var empty Inspection
	b := budget.New(ctx, limits)
	if err := b.Check(); err != nil {
		return empty, err
	}
	a, err := adapterFor(src.Harness)
	if err != nil {
		return empty, err
	}
	ctx, cancel := operationContext(ctx, b)
	defer cancel()
	root, err := fsx.OpenRoot(src.Root)
	if err != nil {
		return empty, err
	}
	defer func() { _ = root.Close() }()
	lock, err := a.LockSource(ctx, src, root)
	if err != nil {
		return empty, operationError(b, err)
	}
	defer func() { _ = lock.Close() }()
	if err = root.CheckPath(src.Root); err != nil {
		return empty, err
	}
	rel, err := a.Select(ctx, src, root, b)
	if err != nil {
		return empty, operationError(b, err)
	}
	f, err := root.Open(rel)
	if err != nil {
		return empty, err
	}
	defer func() { _ = f.Close() }()
	in, err := a.Inspect(ctx, f, rel, b)
	if err == nil {
		err = root.CheckPath(src.Root)
	}
	return in, operationError(b, err)
}

func operationError(b *budget.Tracker, err error) error {
	if budgetErr := b.Check(); budgetErr != nil {
		return budgetErr
	}
	return err
}

func operationContext(ctx context.Context, b *budget.Tracker) (context.Context, context.CancelFunc) {
	return context.WithDeadline(ctx, b.Deadline())
}

func Capture(ctx context.Context, src Source, o CaptureOptions) (Bundle, error) {
	return capture(ctx, src, o, nil)
}

func capture(ctx context.Context, src Source, o CaptureOptions, hook func(string) error) (Bundle, error) {
	var empty Bundle
	hit := func(phase string) error {
		if hook != nil {
			return hook(phase)
		}
		return nil
	}
	b := budget.New(ctx, o.Budget)
	if err := b.Check(); err != nil {
		return empty, err
	}
	ctx, cancel := operationContext(ctx, b)
	defer cancel()
	a, err := adapterFor(src.Harness)
	if err != nil {
		return empty, err
	}
	if !filepath.IsAbs(o.BundleDir) {
		return empty, reject("bundle_path", "bundle directory must be absolute")
	}
	o.BundleDir = filepath.Clean(o.BundleDir)
	parent, err := fsx.OpenRoot(filepath.Dir(o.BundleDir))
	if err != nil {
		return empty, err
	}
	defer func() { _ = parent.Close() }()
	root, err := fsx.OpenRoot(src.Root)
	if err != nil {
		return empty, err
	}
	defer func() { _ = root.Close() }()
	lock, err := a.LockSource(ctx, src, root)
	if err != nil {
		return empty, operationError(b, err)
	}
	defer func() { _ = lock.Close() }()
	if err = root.CheckPath(src.Root); err != nil {
		return empty, err
	}
	rel, err := a.Select(ctx, src, root, b)
	if err != nil {
		return empty, operationError(b, err)
	}
	before, size, err := digestFile(root, rel, b)
	if err != nil {
		return empty, err
	}
	if err = b.Temp(size); err != nil {
		return empty, err
	}
	if err = parent.Mkdir(filepath.Base(o.BundleDir)); err != nil {
		return empty, err
	}
	var out *fsx.Root
	err = hit("bundle_directory_created")
	if err == nil {
		out, err = parent.Sub(filepath.Base(o.BundleDir))
	}
	if err != nil {
		_ = parent.RemoveDir(filepath.Base(o.BundleDir))
		return empty, err
	}
	defer func() { _ = out.Close() }()
	success := false
	defer func() {
		if !success {
			ownsPath := out.CheckPath(o.BundleDir) == nil
			_ = out.Remove(bundleio.RolloutPath)
			_ = out.Remove("manifest.json")
			_ = out.RemoveDir("components")
			if ownsPath {
				_ = parent.RemoveDir(filepath.Base(o.BundleDir))
			}
		}
	}()
	if err = hit("bundle_created"); err != nil {
		return empty, err
	}
	if err = parent.SyncDir("."); err != nil {
		return empty, err
	}
	if err = out.Mkdir("components"); err != nil {
		return empty, err
	}
	in, err := root.Open(rel)
	if err != nil {
		return empty, err
	}
	target, err := out.Create(bundleio.RolloutPath)
	if err != nil {
		_ = in.Close()
		return empty, err
	}
	copied, n, err := copyDigest(target, io.LimitReader(in, size), b)
	_ = in.Close()
	if err == nil {
		err = target.Sync()
	}
	closeErr := target.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return empty, err
	}
	if err = hit("copied"); err != nil {
		return empty, err
	}
	after, afterSize, err := digestFile(root, rel, b)
	if err != nil {
		return empty, err
	}
	if before != copied || before != after || size != n || size != afterSize {
		return empty, &IntegrityError{Component: "rollout", Reason: "source changed during capture"}
	}
	if err = root.CheckPath(src.Root); err != nil {
		return empty, err
	}
	f, err := out.Open(bundleio.RolloutPath)
	if err != nil {
		return empty, err
	}
	inspection, err := a.Inspect(ctx, f, rel, b)
	_ = f.Close()
	if err != nil {
		return empty, operationError(b, err)
	}
	if inspection.SourceDigest != copied {
		return empty, &IntegrityError{Component: "rollout", Reason: "captured bytes changed during inspection"}
	}
	if inspection.ThreadID != src.ThreadID {
		return empty, &IntegrityError{Component: "rollout", Reason: "source thread ID mismatch"}
	}
	manifest := makeManifest(src.Harness, rel, inspection, time.Now().UTC().Format(time.RFC3339Nano))
	data, err := bundleio.Encode(manifest)
	if err != nil {
		return empty, err
	}
	if err = b.Temp(int64(len(data))); err != nil {
		return empty, err
	}
	if err = out.SyncDir("components"); err != nil {
		return empty, err
	}
	if err = bundleio.Write(out, data); err != nil {
		return empty, err
	}
	if err = out.CheckPath(o.BundleDir); err != nil {
		return empty, err
	}
	if err = root.CheckPath(src.Root); err != nil {
		return empty, err
	}
	if err = b.Check(); err != nil {
		return empty, err
	}
	success = true
	return Bundle{Dir: o.BundleDir, Manifest: manifest}, nil
}

func makeManifest(h Harness, rel string, in Inspection, created string) Manifest {
	return Manifest{BundleFormat: 1, Harness: h, Profile: in.Profile, AdapterVersion: "1", SourceCLIVersion: in.CLIVersion,
		ThreadID: in.ThreadID, SourceRelativePath: rel, RecordedCWD: in.RecordedCWD, LatestCWD: in.LatestCWD,
		RuntimeWorkspaceRoots: in.RuntimeWorkspaceRoots, ModelProvider: in.ModelProvider, Omitted: in.Omitted, CreatedAt: created,
		Components: []Component{{LogicalPath: bundleio.RolloutPath, Role: "rollout", SizeBytes: in.SourceSizeBytes, SHA256: in.SourceDigest}}, Inspection: in}
}

func OpenBundle(ctx context.Context, dir string, limits Budget) (Bundle, error) {
	b := budget.New(ctx, limits)
	if err := b.Check(); err != nil {
		return Bundle{}, err
	}
	result, _, err := openBundle(ctx, dir, b)
	return result, err
}

func openBundle(ctx context.Context, dir string, b *budget.Tracker) (Bundle, string, error) {
	var empty Bundle
	root, err := fsx.OpenRoot(dir)
	if err != nil {
		return empty, "", err
	}
	defer func() { _ = root.Close() }()
	raw, err := bundleio.Read(root, b)
	if err != nil {
		return empty, "", err
	}
	object, err := jsonl.Decode(raw, b)
	if err != nil {
		return empty, "", err
	}
	var m Manifest
	if err = jsonl.CheckFields(object, m, "manifest", b); err != nil {
		return empty, "", err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&m); err != nil {
		return empty, "", &IntegrityError{Component: "manifest", Reason: "invalid schema"}
	}
	if m.BundleFormat != 1 || m.AdapterVersion != "1" || len(m.Components) != 1 || m.Components[0].LogicalPath != bundleio.RolloutPath || m.Components[0].Role != "rollout" {
		return empty, "", &IntegrityError{Component: "manifest", Reason: "unsupported bundle schema"}
	}
	if err = root.WalkFiles(".", func(name string, isDir bool) error {
		if err := b.Node(); err != nil {
			return err
		}
		if isDir && name == "components" || !isDir && (name == "manifest.json" || name == bundleio.RolloutPath) {
			return nil
		}
		return &IntegrityError{Component: "bundle", Reason: "undeclared entry"}
	}); err != nil {
		return empty, "", err
	}
	if _, err = time.Parse(time.RFC3339Nano, m.CreatedAt); err != nil {
		return empty, "", &IntegrityError{Component: "manifest", Reason: "invalid creation time"}
	}
	a, err := adapterFor(m.Harness)
	if err != nil {
		return empty, "", err
	}
	if _, err = a.Layout(m.Inspection, m.SourceRelativePath); err != nil {
		return empty, "", err
	}
	f, err := root.Open(bundleio.RolloutPath)
	if err != nil {
		return empty, "", err
	}
	in, err := a.Inspect(ctx, f, m.SourceRelativePath, b)
	_ = f.Close()
	if err != nil {
		return empty, "", operationError(b, err)
	}
	expected := makeManifest(m.Harness, m.SourceRelativePath, in, m.CreatedAt)
	if !reflect.DeepEqual(m, expected) {
		return empty, "", &IntegrityError{Component: "manifest", Reason: "manifest does not match rollout bytes and inspection"}
	}
	if err = root.CheckPath(dir); err != nil {
		return empty, "", err
	}
	if err = b.Check(); err != nil {
		return empty, "", err
	}
	return Bundle{Dir: filepath.Clean(dir), Manifest: m}, sha(raw), nil
}

func sha(data []byte) string { sum := sha256.Sum256(data); return hex.EncodeToString(sum[:]) }

func digestFile(root *fsx.Root, rel string, b *budget.Tracker) (string, int64, error) {
	f, err := root.Open(rel)
	if err != nil {
		return "", 0, err
	}
	defer func() { _ = f.Close() }()
	return copyDigest(io.Discard, f, b)
}
func copyDigest(dst io.Writer, src io.Reader, b *budget.Tracker) (string, int64, error) {
	h := sha256.New()
	out := io.MultiWriter(dst, h)
	bufferSize := int64(32 * 1024)
	if remaining := b.RemainingBytes(); remaining < bufferSize {
		bufferSize = max(1, remaining+1)
	}
	buf := make([]byte, int(bufferSize))
	var size int64
	for {
		if err := b.Check(); err != nil {
			return "", size, err
		}
		readBuf := buf
		if remaining := b.RemainingBytes(); remaining < int64(len(readBuf)) {
			readBuf = readBuf[:max(0, remaining)+1]
		}
		n, err := src.Read(readBuf)
		if e := b.Bytes(int64(n)); e != nil {
			return "", size, e
		}
		if n > 0 {
			written, e := out.Write(buf[:n])
			size += int64(written)
			if e != nil {
				return "", size, e
			}
			if written != n {
				return "", size, io.ErrShortWrite
			}
		}
		if errors.Is(err, io.EOF) {
			return hex.EncodeToString(h.Sum(nil)), size, nil
		}
		if err != nil {
			return "", size, err
		}
	}
}

func validateDestination(dst Destination) error {
	if _, err := adapterFor(dst.Harness); err != nil {
		return err
	}
	if dst.CLIVersion != "0.160.0" {
		return reject("cli_version", "destination CLI version must be the supported 0.160.0")
	}
	workspace, err := fsx.OpenRoot(dst.WorkingDir)
	if err != nil {
		return err
	}
	_ = workspace.Close()
	root, err := fsx.OpenRoot(dst.Root)
	if err != nil {
		return err
	}
	defer func() { _ = root.Close() }()
	journal, err := fsx.OpenRoot(dst.JournalDir)
	if err != nil {
		return err
	}
	defer func() { _ = journal.Close() }()
	inside, err := root.Contains(journal)
	if err != nil {
		return err
	}
	if inside {
		return reject("journal_path", "journal directory must be outside destination home")
	}
	return nil
}

func collision(root *fsx.Root, id, allowed string, b *budget.Tracker) error {
	id = strings.ToLower(id)
	for _, base := range []string{"sessions", "archived_sessions"} {
		err := root.WalkFiles(base, func(rel string, isDir bool) error {
			if err := b.Node(); err != nil {
				return err
			}
			if !isDir && rel != allowed && strings.Contains(strings.ToLower(path.Base(rel)), id) {
				return &CollisionError{ThreadID: id, TargetPath: rel}
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func validateBundleDestination(root *fsx.Root, bundleDir string) error {
	bundle, err := fsx.OpenRoot(bundleDir)
	if errors.Is(err, os.ErrNotExist) {
		return nil // Published recovery does not require the original bundle.
	}
	if err != nil {
		return err
	}
	defer func() { _ = bundle.Close() }()
	inside, err := bundle.Contains(root)
	if err != nil {
		return err
	}
	if !inside {
		inside, err = root.Contains(bundle)
		if err != nil {
			return err
		}
	}
	if inside {
		return reject("destination_path", "destination home and bundle must not overlap")
	}
	return nil
}

func PlanInstall(ctx context.Context, bundle Bundle, dst Destination) (Plan, error) {
	var empty Plan
	b := budget.New(ctx, Budget{})
	if err := b.Check(); err != nil {
		return empty, err
	}
	checked, digest, err := openBundle(ctx, bundle.Dir, b)
	if err != nil {
		return empty, err
	}
	if dst.Harness != checked.Manifest.Harness {
		return empty, reject("harness", "source and destination harnesses differ")
	}
	if err = validateDestination(dst); err != nil {
		return empty, err
	}
	journalRoot, err := fsx.OpenRoot(dst.JournalDir)
	if err != nil {
		return empty, err
	}
	err = journal.CheckOutside(journalRoot, checked.Dir)
	_ = journalRoot.Close()
	if err != nil {
		return empty, err
	}
	a, err := adapterFor(dst.Harness)
	if err != nil {
		return empty, err
	}
	target, err := a.Layout(checked.Manifest.Inspection, checked.Manifest.SourceRelativePath)
	if err != nil {
		return empty, err
	}
	root, err := fsx.OpenRoot(dst.Root)
	if err != nil {
		return empty, err
	}
	defer func() { _ = root.Close() }()
	if err = validateBundleDestination(root, checked.Dir); err != nil {
		return empty, err
	}
	if err = collision(root, checked.Manifest.ThreadID, "", b); err != nil {
		return empty, err
	}
	id, err := fsx.RandomID()
	if err != nil {
		return empty, err
	}
	roots := []string{dst.WorkingDir}
	p := Plan{OperationID: id, BundleDigest: digest, Profile: checked.Manifest.Profile, ThreadID: checked.Manifest.ThreadID,
		TargetPath: target, Omitted: checked.Manifest.Omitted, Preserved: checked.Manifest.Components,
		ResumeHints: ResumeHints{CodexHome: dst.Root, CWDOverride: dst.WorkingDir, RuntimeWorkspaceRoots: roots,
			RecordedProvider: checked.Manifest.ModelProvider, SQLiteNote: "CODEX_SQLITE_HOME must be unset or point to this isolated home's private database directory.",
			NativeCommand:   []string{"codex", "exec", "resume", checked.Manifest.ThreadID},
			AppServerParams: map[string]any{"threadId": checked.Manifest.ThreadID, "cwd": dst.WorkingDir, "runtimeWorkspaceRoots": roots}},
		bundleDir: checked.Dir, destination: dst}
	p.seal = planSeal(p)
	if err = b.Check(); err != nil {
		return empty, err
	}
	return p, nil
}
func planSeal(p Plan) string {
	raw, err := json.Marshal(planFields(p))
	if err != nil {
		return ""
	}
	return sha(append(raw, []byte(p.bundleDir+"\x00"+p.destination.Root+"\x00"+p.destination.WorkingDir+"\x00"+p.destination.JournalDir+"\x00"+string(p.destination.Harness))...))
}

// Verification compares the rollout against the receipt without starting Codex.
func Verify(ctx context.Context, receipt Receipt, dst Destination) (Verification, error) {
	return verify(ctx, receipt, dst, nil)
}

func verify(ctx context.Context, receipt Receipt, dst Destination, hook func() error) (Verification, error) {
	var empty Verification
	b := budget.New(ctx, Budget{})
	if err := b.Check(); err != nil {
		return empty, err
	}
	ctx, cancel := operationContext(ctx, b)
	defer cancel()
	if err := validateDestination(dst); err != nil {
		return empty, err
	}
	if receipt.Outcome != Installed || receipt.Phase != "verified" {
		return empty, reject("receipt", "receipt does not describe a verified installation")
	}
	threadID, err := codex.ValidatePath(receipt.TargetPath)
	if err != nil {
		return empty, err
	}
	root, err := fsx.OpenRoot(dst.Root)
	if err != nil {
		return empty, err
	}
	defer func() { _ = root.Close() }()
	a, err := adapterFor(dst.Harness)
	if err != nil {
		return empty, err
	}
	lock, err := a.LockPublication(ctx, root, threadID)
	if err != nil {
		return empty, operationError(b, err)
	}
	defer func() { _ = lock.Close() }()
	parent, err := root.Sub(path.Dir(receipt.TargetPath))
	if err != nil {
		return empty, err
	}
	defer func() { _ = parent.Close() }()
	digest, size, err := digestFile(parent, path.Base(receipt.TargetPath), b)
	if err != nil {
		return empty, err
	}
	if digest != receipt.TargetDigest {
		return empty, &IntegrityError{Component: "rollout", Reason: "target digest mismatch"}
	}
	if hook != nil {
		if err = hook(); err != nil {
			return empty, err
		}
	}
	if err = root.CheckPath(dst.Root); err != nil {
		return empty, err
	}
	if err = parent.CheckPath(filepath.Join(dst.Root, filepath.FromSlash(path.Dir(receipt.TargetPath)))); err != nil {
		return empty, err
	}
	if err = b.Check(); err != nil {
		return empty, err
	}
	return Verification{Valid: true, TargetPath: receipt.TargetPath, TargetDigest: digest, SizeBytes: size}, nil
}
