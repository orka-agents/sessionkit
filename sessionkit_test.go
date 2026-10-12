package sessionkit

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/orka-agents/sessionkit/harness/codex/writerlock"
	"github.com/orka-agents/sessionkit/internal/budget"
	"github.com/orka-agents/sessionkit/internal/journal"
)

func tempDir(t *testing.T) string {
	t.Helper()
	p, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func testSource(t *testing.T) (Source, string, []byte) {
	return testSourceName(t, "basic")
}

func testSourceName(t *testing.T, name string) (Source, string, []byte) {
	t.Helper()
	files, err := filepath.Glob("harness/codex/testdata/" + name + "/sessions/*/*/*/*.jsonl")
	if err != nil || len(files) != 1 {
		t.Fatalf("fixture: %v %v", files, err)
	}
	raw, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatal(err)
	}
	rel := strings.TrimPrefix(filepath.ToSlash(files[0]), "harness/codex/testdata/"+name+"/")
	base := filepath.Base(rel)
	id := strings.TrimSuffix(base[len("rollout-")+20:], ".jsonl")
	root := tempDir(t)
	file := filepath.Join(root, filepath.FromSlash(rel))
	if err = os.MkdirAll(filepath.Dir(file), 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(file, raw, 0600); err != nil {
		t.Fatal(err)
	}
	return Source{Harness: Codex, Root: root, ThreadID: id}, rel, raw
}

func testBundle(t *testing.T) (Source, Bundle, string, []byte) {
	t.Helper()
	src, rel, raw := testSource(t)
	bundle, err := Capture(context.Background(), src, CaptureOptions{BundleDir: filepath.Join(tempDir(t), "bundle")})
	if err != nil {
		t.Fatal(err)
	}
	return src, bundle, rel, raw
}

func testDestination(t *testing.T) Destination {
	t.Helper()
	return Destination{Harness: Codex, CLIVersion: "0.160.0", Root: tempDir(t), WorkingDir: tempDir(t), JournalDir: tempDir(t)}
}

func mustRead(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestCapturePreservesSourceAndRoundTrip(t *testing.T) {
	ctx := context.Background()
	src, rel, raw := testSource(t)
	file := filepath.Join(src.Root, rel)
	before, err := os.Stat(file)
	if err != nil {
		t.Fatal(err)
	}
	in, err := Inspect(ctx, src, Budget{})
	if err != nil {
		t.Fatal(err)
	}
	bundle, err := Capture(ctx, src, CaptureOptions{BundleDir: filepath.Join(tempDir(t), "bundle")})
	if err != nil {
		t.Fatal(err)
	}
	after, err := os.Stat(file)
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(before, after) || !before.ModTime().Equal(after.ModTime()) || !bytes.Equal(raw, mustRead(t, file)) {
		t.Fatal("source was changed")
	}
	if in.SourceDigest != bundle.Manifest.Components[0].SHA256 || int64(len(raw)) != in.SourceSizeBytes {
		t.Fatal("digest/size summary mismatch")
	}
	opened, err := OpenBundle(ctx, bundle.Dir, Budget{})
	if err != nil {
		t.Fatal(err)
	}
	dst := testDestination(t)
	entriesBefore, err := os.ReadDir(dst.Root)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := PlanInstall(ctx, opened, dst)
	if err != nil {
		t.Fatal(err)
	}
	entriesAfter, err := os.ReadDir(dst.Root)
	if err != nil || len(entriesBefore) != len(entriesAfter) {
		t.Fatal("plan mutated destination")
	}
	if plan.TargetPath != rel || plan.ResumeHints.AppServerParams["cwd"] != dst.WorkingDir || plan.ResumeHints.NativeCommand[3] != src.ThreadID {
		t.Fatal("incorrect plan")
	}
	receipt, err := Install(ctx, plan)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Outcome != Installed || receipt.Phase != "verified" {
		t.Fatalf("receipt: %+v", receipt)
	}
	if !bytes.Equal(raw, mustRead(t, filepath.Join(dst.Root, rel))) {
		t.Fatal("installed bytes changed")
	}
	verification, err := Verify(ctx, receipt, dst)
	if err != nil || !verification.Valid {
		t.Fatalf("verify: %+v %v", verification, err)
	}
	retry, err := Install(ctx, plan)
	if err != nil || retry != receipt {
		t.Fatalf("retry: %+v %v", retry, err)
	}
	for _, name := range []string{"state_5.sqlite", "thread_history_1.sqlite", "session_index.jsonl", "history.jsonl"} {
		if _, err := os.Lstat(filepath.Join(dst.Root, name)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("unexpected %s: %v", name, err)
		}
	}
}

func TestCaptureDetectsMutationAndReleasesLock(t *testing.T) {
	src, rel, raw := testSource(t)
	dst := filepath.Join(tempDir(t), "bundle")
	_, err := capture(context.Background(), src, CaptureOptions{BundleDir: dst}, func(phase string) error {
		if phase == "copied" {
			return os.WriteFile(filepath.Join(src.Root, rel), append(raw, '\n'), 0600)
		}
		return nil
	})
	var integrity *IntegrityError
	if !errors.As(err, &integrity) {
		t.Fatalf("want integrity rejection: %v", err)
	}
	if _, err = os.Stat(dst); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("partial bundle retained: %v", err)
	}
	lock, err := writerlock.Source(context.Background(), src.Root, src.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	_ = lock.Close()
}

func TestActiveWriterBlocksInspectAndCapture(t *testing.T) {
	src, _, _ := testSource(t)
	lock, err := writerlock.Source(context.Background(), src.Root, src.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lock.Close() }()
	for _, fn := range []func() error{
		func() error { _, e := Inspect(context.Background(), src, Budget{}); return e },
		func() error {
			_, e := Capture(context.Background(), src, CaptureOptions{BundleDir: filepath.Join(tempDir(t), "bundle")})
			return e
		},
	} {
		var active *ActiveWriterError
		if err := fn(); !errors.As(err, &active) {
			t.Fatalf("active writer: %v", err)
		}
	}
}

func TestBundleTamperingRejected(t *testing.T) {
	for _, which := range []string{"rollout", "manifest", "duplicate", "component_path"} {
		t.Run(which, func(t *testing.T) {
			_, bundle, _, _ := testBundle(t)
			manifest := filepath.Join(bundle.Dir, "manifest.json")
			switch which {
			case "rollout":
				f, err := os.OpenFile(filepath.Join(bundle.Dir, "components/rollout.jsonl"), os.O_APPEND|os.O_WRONLY, 0600)
				if err != nil {
					t.Fatal(err)
				}
				_, _ = f.WriteString("{}\n")
				_ = f.Close()
			case "manifest":
				data := mustRead(t, manifest)
				data = bytes.Replace(data, []byte(`"modelProvider": "`), []byte(`"modelProvider": "changed-`), 1)
				if err := os.WriteFile(manifest, data, 0600); err != nil {
					t.Fatal(err)
				}
			case "duplicate":
				data := mustRead(t, manifest)
				data = bytes.Replace(data, []byte("{"), []byte("{\"bundleFormat\":1,"), 1)
				if err := os.WriteFile(manifest, data, 0600); err != nil {
					t.Fatal(err)
				}
			case "component_path":
				data := mustRead(t, manifest)
				data = bytes.ReplaceAll(data, []byte("components/rollout.jsonl"), []byte("../rollout.jsonl"))
				if err := os.WriteFile(manifest, data, 0600); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := OpenBundle(context.Background(), bundle.Dir, Budget{}); err == nil {
				t.Fatal("tampered bundle accepted")
			}
		})
	}
}

func TestBundleRejectsUndeclaredEntries(t *testing.T) {
	for _, entry := range []struct {
		name, kind string
	}{
		{"credentials.json", "file"},
		{"components/extra.jsonl", "file"},
		{"extra", "directory"},
		{"credentials-link", "symlink"},
		{"components/extra-link", "symlink"},
	} {
		t.Run(entry.name, func(t *testing.T) {
			_, bundle, _, _ := testBundle(t)
			name := filepath.Join(bundle.Dir, entry.name)
			var err error
			switch entry.kind {
			case "file":
				err = os.WriteFile(name, []byte("undeclared"), 0600)
			case "directory":
				err = os.Mkdir(name, 0700)
			case "symlink":
				err = os.Symlink(filepath.Join(bundle.Dir, "manifest.json"), name)
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := OpenBundle(context.Background(), bundle.Dir, Budget{}); err == nil {
				t.Fatal("bundle with undeclared entry accepted")
			}
		})
	}
}

func TestCaptureBudgetsAndExistingBundle(t *testing.T) {
	src, _, raw := testSource(t)
	for _, limit := range []Budget{{MaxBytes: int64(len(raw)) * 2}, {MaxTempBytes: int64(len(raw)) - 1}, {MaxBytes: -1}, {Timeout: time.Nanosecond}} {
		_, err := Capture(context.Background(), src, CaptureOptions{BundleDir: filepath.Join(tempDir(t), "bundle"), Budget: limit})
		var exceeded *BudgetError
		if !errors.As(err, &exceeded) {
			t.Fatalf("budget %+v: %v", limit, err)
		}
	}
	existing := tempDir(t)
	sentinel := filepath.Join(existing, "keep")
	if err := os.WriteFile(sentinel, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Capture(context.Background(), src, CaptureOptions{BundleDir: existing}); err == nil {
		t.Fatal("overwrote existing bundle")
	}
	if string(mustRead(t, sentinel)) != "keep" {
		t.Fatal("existing bundle changed")
	}
}

func TestPlanCannotBeAlteredAndVerifyDetectsDamage(t *testing.T) {
	_, bundle, rel, _ := testBundle(t)
	dst := testDestination(t)
	plan, err := PlanInstall(context.Background(), bundle, dst)
	if err != nil {
		t.Fatal(err)
	}
	changed := plan
	changed.TargetPath = "../escape"
	if _, err = Install(context.Background(), changed); err == nil {
		t.Fatal("altered plan accepted")
	}
	receipt, err := Install(context.Background(), plan)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(dst.Root, rel), []byte("damage"), 0600); err != nil {
		t.Fatal(err)
	}
	_, err = Verify(context.Background(), receipt, dst)
	var integrity *IntegrityError
	if !errors.As(err, &integrity) {
		t.Fatalf("verify failed to find damage: %v", err)
	}
	dst.JournalDir = dst.Root
	if _, err = PlanInstall(context.Background(), bundle, dst); err == nil {
		t.Fatal("journal inside destination accepted")
	}
}

func TestGoldenInspectionAndPlan(t *testing.T) {
	for _, name := range []string{"basic", "compacted"} {
		t.Run(name, func(t *testing.T) {
			src, _, _ := testSourceName(t, name)
			in, err := Inspect(context.Background(), src, Budget{})
			if err != nil {
				t.Fatal(err)
			}
			checkGolden(t, name+"-inspection", in)
			bundle, err := Capture(context.Background(), src, CaptureOptions{BundleDir: filepath.Join(tempDir(t), "bundle")})
			if err != nil {
				t.Fatal(err)
			}
			plan, err := PlanInstall(context.Background(), bundle, testDestination(t))
			if err != nil {
				t.Fatal(err)
			}
			plan.OperationID = "<operation-id>"
			plan.BundleDigest = "<manifest-sha256>"
			plan.seal = "<plan-sha256>"
			plan.bundleDir = "/sessionkit/bundle"
			plan.destination = Destination{Harness: Codex, CLIVersion: "0.160.0", Root: "/sessionkit/destination-home", WorkingDir: "/sessionkit/destination-workspace", JournalDir: "/sessionkit/journal"}
			plan.ResumeHints.CodexHome = plan.destination.Root
			plan.ResumeHints.CWDOverride = plan.destination.WorkingDir
			plan.ResumeHints.RuntimeWorkspaceRoots = []string{plan.destination.WorkingDir}
			plan.ResumeHints.AppServerParams["cwd"] = plan.destination.WorkingDir
			plan.ResumeHints.AppServerParams["runtimeWorkspaceRoots"] = []string{plan.destination.WorkingDir}
			checkGolden(t, name+"-plan", plan)
		})
	}
}

func TestCaptureCleansUpDuringCreation(t *testing.T) {
	for _, boundary := range []string{"bundle_directory_created", "bundle_created", "copied"} {
		t.Run(boundary, func(t *testing.T) {
			src, _, _ := testSource(t)
			dir := filepath.Join(tempDir(t), "bundle")
			failure := errors.New("bundle creation failed")
			_, err := capture(context.Background(), src, CaptureOptions{BundleDir: dir}, func(phase string) error {
				if phase == boundary {
					return failure
				}
				return nil
			})
			if !errors.Is(err, failure) {
				t.Fatalf("expected creation failure, got %v", err)
			}
			if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("failed capture left a bundle directory: %v", err)
			}
			if _, err := Capture(context.Background(), src, CaptureOptions{BundleDir: dir}); err != nil {
				t.Fatalf("retry after failed creation: %v", err)
			}
		})
	}
}

func TestCancellationAfterInspectionRejectsSuccess(t *testing.T) {
	for _, operation := range []string{"inspect", "open bundle"} {
		t.Run(operation, func(t *testing.T) {
			src, bundle, _, _ := testBundle(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			original := adapters[Codex]
			adapters[Codex] = replacingSourceAdapter{Adapter: original, afterInspect: func() error {
				cancel()
				return nil
			}}
			t.Cleanup(func() { adapters[Codex] = original })
			var err error
			if operation == "inspect" {
				_, err = Inspect(ctx, src, Budget{})
			} else {
				_, err = OpenBundle(ctx, bundle.Dir, Budget{})
			}
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("reported success after cancellation: %v", err)
			}
		})
	}
}

type countingReader struct {
	input io.Reader
	bytes int
}

func (r *countingReader) Read(p []byte) (int, error) {
	n, err := r.input.Read(p)
	r.bytes += n
	return n, err
}

func TestCopyDigestBoundsReadRequest(t *testing.T) {
	input := &countingReader{input: strings.NewReader(strings.Repeat("x", 1<<20))}
	_, _, err := copyDigest(io.Discard, input, budget.New(context.Background(), Budget{MaxBytes: 8}))
	var exceeded *BudgetError
	if !errors.As(err, &exceeded) {
		t.Fatalf("expected byte budget failure, got %v", err)
	}
	if input.bytes > 9 {
		t.Fatalf("8-byte budget read %d bytes; limit reads to the remainder plus one EOF probe", input.bytes)
	}
}

func TestInspectTimeoutUsesBudgetError(t *testing.T) {
	src, _, _ := testSource(t)
	lock, err := writerlock.Publication(context.Background(), src.Root, src.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lock.Close() }()
	_, err = Inspect(context.Background(), src, Budget{Timeout: 20 * time.Millisecond})
	var exceeded *BudgetError
	if !errors.As(err, &exceeded) || exceeded.Limit != "timeout" {
		t.Fatalf("operation timeout must preserve BudgetError, got %T: %v", err, err)
	}
}

func TestDestinationVersionIsRequired(t *testing.T) {
	_, bundle, _, _ := testBundle(t)
	for _, version := range []string{"", "0.159.1", "0.160.1"} {
		t.Run(version, func(t *testing.T) {
			dst := testDestination(t)
			dst.CLIVersion = version
			_, err := PlanInstall(context.Background(), bundle, dst)
			var rejected *RejectionError
			if !errors.As(err, &rejected) || len(rejected.Rejections) != 1 || rejected.Rejections[0].Code != "cli_version" {
				t.Fatalf("unsupported destination version accepted: %v", err)
			}
			for _, dir := range []string{dst.Root, dst.JournalDir} {
				entries, err := os.ReadDir(dir)
				if err != nil || len(entries) != 0 {
					t.Fatalf("planning changed destination: %v %v", entries, err)
				}
			}
		})
	}
}

func TestArtifactSchemasRejectCaseAliases(t *testing.T) {
	_, bundle, _, _ := testBundle(t)
	dst := testDestination(t)
	plan, err := PlanInstall(context.Background(), bundle, dst)
	if err != nil {
		t.Fatal(err)
	}
	manifestPath := filepath.Join(bundle.Dir, "manifest.json")
	manifest := mustRead(t, manifestPath)
	for _, data := range [][]byte{
		bytes.Replace(manifest, []byte(`"bundleFormat": 1`), []byte(`"bundleFormat": 999, "BundleFormat": 1`), 1),
		bytes.Replace(manifest, []byte("{"), []byte(`{"BundleFormat":999,`), 1),
		bytes.Replace(manifest, []byte(`"cliVersion": "0.160.0"`), []byte(`"cliVersion": "0.159.1", "CLIVersion": "0.160.0"`), 1),
	} {
		if err := os.WriteFile(manifestPath, data, 0600); err != nil {
			t.Fatal(err)
		}
		_, err := OpenBundle(context.Background(), bundle.Dir, Budget{})
		var integrity *IntegrityError
		if !errors.As(err, &integrity) {
			t.Fatalf("manifest case alias accepted: %v", err)
		}
	}
	if err := os.WriteFile(manifestPath, manifest, 0600); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	for _, data := range [][]byte{
		bytes.Replace(raw, []byte("{"), []byte(`{"TargetPath":"../escape",`), 1),
		bytes.Replace(raw, []byte(`"destination":{`), []byte(`"destination":{"Root":"/other",`), 1),
	} {
		var decoded Plan
		var integrity *IntegrityError
		if err := json.Unmarshal(data, &decoded); !errors.As(err, &integrity) {
			t.Fatalf("plan case alias accepted: %v", err)
		}
	}
	j, err := journal.Open(context.Background(), dst.JournalDir, plan.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = j.Close() }()
	if err := j.Write(journal.State{Phase: "staged"}, budget.New(context.Background(), Budget{})); err != nil {
		t.Fatal(err)
	}
	journalPath := filepath.Join(dst.JournalDir, plan.OperationID+".json")
	data := bytes.Replace(mustRead(t, journalPath), []byte(`"phase": "staged"`), []byte(`"phase": "staged", "Phase": "planned"`), 1)
	if err := os.WriteFile(journalPath, data, 0600); err != nil {
		t.Fatal(err)
	}
	var integrity *IntegrityError
	if _, err := j.Read(budget.New(context.Background(), Budget{})); !errors.As(err, &integrity) {
		t.Fatalf("journal case alias accepted: %v", err)
	}
}

func TestJournalRejectsDataAfterItsReadLimit(t *testing.T) {
	dir := tempDir(t)
	id := strings.Repeat("a", 32)
	j, err := journal.Open(context.Background(), dir, id)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = j.Close() }()
	if err = j.Write(journal.State{OperationID: id, Phase: "staged"}, budget.New(context.Background(), Budget{})); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(filepath.Join(dir, id+".json"), os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.WriteString(strings.Repeat(" ", 16385) + "trailing garbage")
	closeErr := f.Close()
	if err != nil {
		t.Fatal(err)
	}
	if closeErr != nil {
		t.Fatal(closeErr)
	}
	if _, err = j.Read(budget.New(context.Background(), Budget{})); err == nil {
		t.Fatal("journal parser accepted bytes beyond its truncated view")
	}
}

func checkGolden(t *testing.T, name string, value any) {
	t.Helper()
	raw, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	raw = append(raw, '\n')
	file := filepath.Join("harness/codex/testdata", name+".golden.json")
	if os.Getenv("UPDATE_GOLDEN") == "1" {
		if err = os.WriteFile(file, raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if !bytes.Equal(raw, mustRead(t, file)) {
		t.Fatalf("golden mismatch: %s", file)
	}
}
