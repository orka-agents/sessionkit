package sessionkit

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"

	"github.com/orka-agents/sessionkit/internal/budget"
	bundleio "github.com/orka-agents/sessionkit/internal/bundle"
	"github.com/orka-agents/sessionkit/internal/fsx"
	"github.com/orka-agents/sessionkit/internal/journal"
)

// Install publishes the captured bytes without replacing any destination file.
// Reuse the same Plan to reconcile an interrupted operation.
func Install(ctx context.Context, plan Plan) (Receipt, error) { return install(ctx, plan, nil) }

// A per-call hook lets tests interrupt actual durability boundaries without
// global state or changing filesystem behavior for other operations.
type installHook func(string) error

func install(ctx context.Context, p Plan, hook installHook) (Receipt, error) {
	receipt := Receipt{OperationID: p.OperationID, Outcome: RejectedBeforeMutation, TargetPath: p.TargetPath, Phase: "planned"}
	hit := func(phase string) error {
		if hook != nil {
			return hook(phase)
		}
		return nil
	}
	unknown := func(err error) (Receipt, error) {
		receipt.Outcome = Unknown
		return receipt, &UnknownOutcomeError{OperationID: p.OperationID, Err: err}
	}
	if p.seal == "" || planSeal(p) != p.seal {
		return receipt, reject("plan", "plan was altered or was not created by PlanInstall")
	}
	b := budget.New(ctx, Budget{})
	// Until the journal is read, a prior attempt may already have published.
	if err := b.Check(); err != nil {
		return unknown(err)
	}
	ctx, cancel := operationContext(ctx, b)
	defer cancel()
	j, err := journal.Open(ctx, p.destination.JournalDir, p.OperationID)
	if err != nil {
		return unknown(operationError(b, err))
	}
	defer func() { _ = j.Close() }()
	if len(p.Preserved) != 1 {
		return unknown(reject("plan", "invalid component inventory"))
	}
	receipt.TargetDigest = p.Preserved[0].SHA256
	expected := journal.State{OperationID: p.OperationID, ThreadID: p.ThreadID, TargetPath: p.TargetPath, BundleDigest: p.BundleDigest,
		TargetDigest: receipt.TargetDigest, Destination: p.destination.Root, Phase: "planned", TempPath: path.Join(path.Dir(p.TargetPath), ".sessionkit-"+p.OperationID+".tmp")}
	state, err := j.Read(b)
	fresh := errors.Is(err, os.ErrNotExist)
	if fresh {
		state = expected
	} else if err != nil {
		return unknown(err)
	}
	receipt.Phase = state.Phase
	if state.OperationID != expected.OperationID || state.ThreadID != expected.ThreadID || state.TargetPath != expected.TargetPath ||
		state.BundleDigest != expected.BundleDigest || state.TargetDigest != expected.TargetDigest || state.Destination != expected.Destination || state.TempPath != expected.TempPath {
		return unknown(&IntegrityError{Component: "journal", Reason: "journal does not match plan"})
	}
	if state.Phase != "planned" && state.Phase != "staged" && state.Phase != "published" && state.Phase != "verified" {
		return unknown(&IntegrityError{Component: "journal", Reason: "unrecognized phase"})
	}
	resumingStaged := state.Phase == "staged"
	failBeforePublication := func(err error) (Receipt, error) {
		if state.Phase == "planned" {
			return receipt, err
		}
		return unknown(err)
	}
	if err = validateDestination(p.destination); err != nil {
		return failBeforePublication(err)
	}
	root, err := fsx.OpenRoot(p.destination.Root)
	if err != nil {
		return failBeforePublication(err)
	}
	defer func() { _ = root.Close() }()
	if state.Phase == "published" || state.Phase == "verified" {
		return finishInstall(ctx, p, receipt, state, j, root, b, hit)
	}
	checked, digest, err := openBundle(ctx, p.bundleDir, b)
	if err != nil {
		return failBeforePublication(err)
	}
	if digest != p.BundleDigest {
		return failBeforePublication(&IntegrityError{Component: "bundle", Reason: "bundle changed since planning"})
	}
	if checked.Manifest.ThreadID != p.ThreadID || checked.Manifest.Profile != p.Profile || checked.Manifest.SourceRelativePath != p.TargetPath || checked.Manifest.Components[0] != p.Preserved[0] {
		return failBeforePublication(&IntegrityError{Component: "plan", Reason: "plan does not match verified bundle"})
	}
	if fresh {
		if err = collision(root, p.ThreadID, "", b); err != nil {
			return receipt, err
		}
		if err = j.Write(state, b); err != nil {
			return receipt, err
		}
	}
	if state.Phase == "planned" {
		if err = collision(root, p.ThreadID, "", b); err != nil {
			return receipt, err
		}
		if err = root.MkdirAll(path.Dir(p.TargetPath)); err != nil {
			return receipt, err
		}
		// The planned phase owns this exact private temp, including a partial file
		// left by a process that stopped before recording staged.
		if err = root.Remove(state.TempPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return receipt, err
		}
		if err = b.Temp(checked.Manifest.Components[0].SizeBytes); err != nil {
			return receipt, err
		}
		source, err := fsx.OpenRoot(checked.Dir)
		if err != nil {
			return receipt, err
		}
		in, err := source.Open(bundleio.RolloutPath)
		_ = source.Close()
		if err != nil {
			return receipt, err
		}
		tmp, err := root.Create(state.TempPath)
		if err != nil {
			_ = in.Close()
			return receipt, err
		}
		copied, size, err := copyDigest(tmp, io.LimitReader(in, checked.Manifest.Components[0].SizeBytes), b)
		_ = in.Close()
		if err == nil {
			err = tmp.Sync()
		}
		closeErr := tmp.Close()
		if err == nil {
			err = closeErr
		}
		if err == nil && (copied != receipt.TargetDigest || size != checked.Manifest.Components[0].SizeBytes) {
			err = &IntegrityError{Component: "rollout", Reason: "bundle changed during staging"}
		}
		if err != nil {
			_ = root.Remove(state.TempPath)
			return receipt, err
		}
		if err = root.SyncDir(path.Dir(state.TempPath)); err != nil {
			return receipt, err
		}
		state.Phase = "staged"
		if err = j.Write(state, b); err != nil {
			return receipt, err
		}
		receipt.Phase = "staged"
	}
	if err = hit("staged"); err != nil {
		return unknown(err)
	}
	a, err := adapterFor(p.destination.Harness)
	if err != nil {
		return failBeforePublication(err)
	}
	lock, err := a.LockPublication(ctx, root, p.ThreadID)
	if err != nil {
		return failBeforePublication(operationError(b, err))
	}
	defer func() { _ = lock.Close() }()
	rejectCollision := func(err error) (Receipt, error) {
		// A staged retry may have linked already. Keep its witness until the
		// caller resolves the competing identity.
		if resumingStaged {
			return unknown(err)
		}
		// This attempt did not publish. Record that it can restage before
		// removing the witness, including if cleanup is interrupted.
		state.Phase = "planned"
		if writeErr := j.Write(state, b); writeErr != nil {
			return unknown(writeErr)
		}
		receipt.Phase = "planned"
		_ = root.Remove(state.TempPath)
		return receipt, err
	}
	// Recheck every matching filename while holding Codex's publication lock.
	if err = collision(root, p.ThreadID, p.TargetPath, b); err != nil {
		return rejectCollision(err)
	}
	if err = root.CheckPath(p.destination.Root); err != nil {
		return unknown(err)
	}
	target, targetErr := root.Open(p.TargetPath)
	if targetErr == nil {
		targetInfo, e := target.Stat()
		_ = target.Close()
		if e != nil {
			return unknown(e)
		}
		temp, e := root.Open(state.TempPath)
		if e != nil {
			return unknown(fmt.Errorf("staged target exists without its publication witness: %w", e))
		}
		tempInfo, e := temp.Stat()
		_ = temp.Close()
		if e != nil {
			return unknown(e)
		}
		if !os.SameFile(targetInfo, tempInfo) {
			return rejectCollision(&CollisionError{ThreadID: p.ThreadID, TargetPath: p.TargetPath})
		}
	} else {
		if !errors.Is(targetErr, os.ErrNotExist) {
			return unknown(targetErr)
		}
		staged, _, e := digestFile(root, state.TempPath, b)
		if e != nil {
			return unknown(e)
		}
		if staged != receipt.TargetDigest {
			return unknown(&IntegrityError{Component: "staged rollout", Reason: "digest mismatch"})
		}
		// From the first publication attempt onward any error other than EEXIST
		// has an uncertain outcome, including directory durability failures.
		if err = hit("link"); err != nil {
			return unknown(err)
		}
		if err = root.CheckPath(p.destination.Root); err != nil {
			return unknown(err)
		}
		if err = root.Link(state.TempPath, p.TargetPath); err != nil {
			if errors.Is(err, os.ErrExist) {
				return rejectCollision(&CollisionError{ThreadID: p.ThreadID, TargetPath: p.TargetPath})
			}
			return unknown(err)
		}
	}
	if err = hit("dir_fsync"); err != nil {
		return unknown(err)
	}
	if err = root.SyncDir(path.Dir(p.TargetPath)); err != nil {
		return unknown(err)
	}
	if err = hit("temp_remove"); err != nil {
		return unknown(err)
	}
	if err = root.Remove(state.TempPath); err != nil {
		return unknown(err)
	}
	if err = hit("after_temp_remove"); err != nil {
		return unknown(err)
	}
	// Persist removal as well as publication before acknowledging this phase.
	if err = root.SyncDir(path.Dir(p.TargetPath)); err != nil {
		return unknown(err)
	}
	state.Phase = "published"
	if err = j.Write(state, b); err != nil {
		return unknown(err)
	}
	receipt.Phase = "published"
	return finishInstall(ctx, p, receipt, state, j, root, b, hit)
}

func finishInstall(ctx context.Context, p Plan, receipt Receipt, state journal.State, j *journal.Journal, root *fsx.Root, b *budget.Tracker, hit installHook) (Receipt, error) {
	fail := func(err error) (Receipt, error) {
		receipt.Outcome = Unknown
		return receipt, &UnknownOutcomeError{OperationID: p.OperationID, Err: err}
	}
	if err := ctx.Err(); err != nil {
		return fail(operationError(b, err))
	}
	if err := collision(root, p.ThreadID, p.TargetPath, b); err != nil {
		return fail(err)
	}
	if err := hit("verify"); err != nil {
		return fail(err)
	}
	digest, _, err := digestFile(root, p.TargetPath, b)
	if err != nil {
		return fail(err)
	}
	if digest != receipt.TargetDigest {
		return fail(&IntegrityError{Component: "rollout", Reason: "installed digest mismatch"})
	}
	if err = root.CheckPath(p.destination.Root); err != nil {
		return fail(err)
	}
	if state.Phase != "verified" {
		state.Phase = "verified"
		if err = j.Write(state, b); err != nil {
			return fail(err)
		}
	}
	if err = j.CheckPath(); err != nil {
		return fail(err)
	}
	if err = b.Check(); err != nil {
		return fail(err)
	}
	receipt.Phase = "verified"
	receipt.Outcome = Installed
	return receipt, nil
}
