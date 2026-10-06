package sessionkit

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orka-agents/sessionkit/harness/codex/writerlock"
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
	return Destination{Harness: Codex, Root: tempDir(t), WorkingDir: tempDir(t), JournalDir: tempDir(t)}
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

func TestCollisionLiveAndArchived(t *testing.T) {
	ctx := context.Background()
	src, bundle, rel, raw := testBundle(t)
	for _, base := range []string{"sessions", "archived_sessions"} {
		t.Run(base, func(t *testing.T) {
			dst := testDestination(t)
			collisionPath := filepath.Join(dst.Root, strings.Replace(rel, "sessions", base, 1))
			if err := os.MkdirAll(filepath.Dir(collisionPath), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(collisionPath, raw, 0600); err != nil {
				t.Fatal(err)
			}
			_, err := PlanInstall(ctx, bundle, dst)
			var collision *CollisionError
			if !errors.As(err, &collision) || collision.ThreadID != src.ThreadID {
				t.Fatalf("collision: %v", err)
			}
			if !bytes.Equal(raw, mustRead(t, collisionPath)) {
				t.Fatal("collision changed file")
			}
			journal, err := os.ReadDir(dst.JournalDir)
			if err != nil || len(journal) != 0 {
				t.Fatal("collision mutated journal")
			}
		})
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

func TestRejectSymlinksAndTraversal(t *testing.T) {
	for _, which := range []string{"root", "rollout", "parent", "thread"} {
		t.Run(which, func(t *testing.T) {
			src, rel, raw := testSource(t)
			switch which {
			case "root":
				link := filepath.Join(tempDir(t), "home")
				if err := os.Symlink(src.Root, link); err != nil {
					t.Fatal(err)
				}
				src.Root = link
			case "rollout":
				outside := filepath.Join(tempDir(t), "rollout")
				if err := os.WriteFile(outside, raw, 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.Remove(filepath.Join(src.Root, rel)); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(outside, filepath.Join(src.Root, rel)); err != nil {
					t.Fatal(err)
				}
			case "parent":
				outside := tempDir(t)
				if err := os.Rename(filepath.Join(src.Root, "sessions"), filepath.Join(outside, "sessions")); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(filepath.Join(outside, "sessions"), filepath.Join(src.Root, "sessions")); err != nil {
					t.Fatal(err)
				}
			case "thread":
				src.ThreadID = "../escape"
			}
			if _, err := Inspect(context.Background(), src, Budget{}); err == nil {
				t.Fatal("unsafe source accepted")
			}
		})
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
	for _, limit := range []Budget{{MaxBytes: int64(len(raw)) * 3}, {MaxTempBytes: int64(len(raw)) - 1}, {MaxBytes: -1}, {Timeout: time.Nanosecond}} {
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

func TestInstallFaultReconciliation(t *testing.T) {
	for _, fault := range []string{"link", "dir_fsync", "temp_remove", "after_temp_remove", "verify"} {
		t.Run(fault, func(t *testing.T) {
			_, bundle, rel, raw := testBundle(t)
			dst := testDestination(t)
			plan, err := PlanInstall(context.Background(), bundle, dst)
			if err != nil {
				t.Fatal(err)
			}
			receipt, err := install(context.Background(), plan, func(phase string) error {
				if phase == fault {
					return fmt.Errorf("injected %s", phase)
				}
				return nil
			})
			var unknown *UnknownOutcomeError
			if !errors.As(err, &unknown) || receipt.Outcome != Unknown {
				t.Fatalf("fault: %+v %v", receipt, err)
			}
			expectedPhase := "staged"
			if fault == "verify" {
				expectedPhase = "published"
			}
			if receipt.Phase != expectedPhase {
				t.Fatalf("phase %s != %s", receipt.Phase, expectedPhase)
			}
			var journal struct {
				Phase string `json:"phase"`
			}
			if err = json.Unmarshal(mustRead(t, filepath.Join(dst.JournalDir, plan.OperationID+".json")), &journal); err != nil {
				t.Fatal(err)
			}
			if journal.Phase != expectedPhase {
				t.Fatalf("journal: %+v", journal)
			}
			retry, err := Install(context.Background(), plan)
			if fault == "after_temp_remove" {
				if !errors.As(err, &unknown) || retry.Outcome != Unknown {
					t.Fatalf("missing witness: %+v %v", retry, err)
				}
				return
			}
			if err != nil || retry.Outcome != Installed {
				t.Fatalf("retry: %+v %v", retry, err)
			}
			if !bytes.Equal(raw, mustRead(t, filepath.Join(dst.Root, rel))) {
				t.Fatal("reconciled bytes changed")
			}
		})
	}
}

func TestConcurrentInstall(t *testing.T) {
	_, bundle, rel, raw := testBundle(t)
	dst := testDestination(t)
	plan, err := PlanInstall(context.Background(), bundle, dst)
	if err != nil {
		t.Fatal(err)
	}
	errs := make(chan error, 8)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			receipt, err := Install(context.Background(), plan)
			if err != nil {
				err = fmt.Errorf("phase %s: %w", receipt.Phase, err)
			}
			if err == nil && receipt.Outcome != Installed {
				err = fmt.Errorf("outcome %s", receipt.Outcome)
			}
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if !bytes.Equal(raw, mustRead(t, filepath.Join(dst.Root, rel))) {
		t.Fatal("concurrent install changed bytes")
	}
	files, err := filepath.Glob(filepath.Join(dst.Root, filepath.Dir(rel), "*"))
	if err != nil || len(files) != 1 {
		t.Fatalf("publication files: %v %v", files, err)
	}
}

func TestCompetingPlansDoNotReplace(t *testing.T) {
	_, bundle, rel, raw := testBundle(t)
	dst := testDestination(t)
	first, err := PlanInstall(context.Background(), bundle, dst)
	if err != nil {
		t.Fatal(err)
	}
	second, err := PlanInstall(context.Background(), bundle, dst)
	if err != nil {
		t.Fatal(err)
	}
	outcomes := make(chan error, 2)
	var wg sync.WaitGroup
	for _, p := range []Plan{first, second} {
		wg.Add(1)
		go func() { defer wg.Done(); _, err := Install(context.Background(), p); outcomes <- err }()
	}
	wg.Wait()
	close(outcomes)
	installed, collided := 0, 0
	for err := range outcomes {
		var collision *CollisionError
		if err == nil {
			installed++
		} else if errors.As(err, &collision) {
			collided++
		} else {
			t.Fatal(err)
		}
	}
	if installed != 1 || collided != 1 {
		t.Fatalf("installed=%d collided=%d", installed, collided)
	}
	if !bytes.Equal(raw, mustRead(t, filepath.Join(dst.Root, rel))) {
		t.Fatal("target replaced")
	}
}

func TestInstallHoldsCoordinationLockAndPreservesModes(t *testing.T) {
	_, bundle, rel, _ := testBundle(t)
	dst := testDestination(t)
	existing := filepath.Join(dst.Root, "sessions")
	if err := os.Mkdir(existing, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(existing, 0755); err != nil {
		t.Fatal(err)
	}
	plan, err := PlanInstall(context.Background(), bundle, dst)
	if err != nil {
		t.Fatal(err)
	}
	_, err = install(context.Background(), plan, func(phase string) error {
		if phase != "link" && phase != "dir_fsync" && phase != "temp_remove" {
			return nil
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		defer cancel()
		lock, e := writerlock.Publication(ctx, dst.Root, plan.ThreadID)
		if e == nil {
			_ = lock.Close()
			return fmt.Errorf("coordination lock absent at %s", phase)
		}
		if !errors.Is(e, context.DeadlineExceeded) {
			return e
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(existing)
	if err != nil || info.Mode().Perm() != 0755 {
		t.Fatalf("existing mode: %v %v", info, err)
	}
	for _, name := range []string{filepath.Join(dst.Root, filepath.Dir(rel)), bundle.Dir, filepath.Join(bundle.Dir, "components")} {
		info, err = os.Stat(name)
		if err != nil || info.Mode().Perm() != 0700 {
			t.Fatalf("private directory %s: %v %v", name, info, err)
		}
	}
	for _, name := range []string{filepath.Join(dst.Root, rel), filepath.Join(bundle.Dir, "manifest.json"), filepath.Join(bundle.Dir, "components/rollout.jsonl")} {
		info, err = os.Stat(name)
		if err != nil || info.Mode().Perm() != 0600 {
			t.Fatalf("private file %s: %v %v", name, info, err)
		}
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

func TestPlanSurvivesProcessRestartSerialization(t *testing.T) {
	_, bundle, _, _ := testBundle(t)
	dst := testDestination(t)
	plan, err := PlanInstall(context.Background(), bundle, dst)
	if err != nil {
		t.Fatal(err)
	}
	_, err = install(context.Background(), plan, func(phase string) error {
		if phase == "dir_fsync" {
			return fmt.Errorf("process interruption")
		}
		return nil
	})
	if err == nil {
		t.Fatal("fault not exercised")
	}
	wire, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	var restored Plan
	if err = json.Unmarshal(wire, &restored); err != nil {
		t.Fatal(err)
	}
	receipt, err := Install(context.Background(), restored)
	if err != nil || receipt.Outcome != Installed {
		t.Fatalf("persisted plan retry: %+v %v", receipt, err)
	}
	wire = bytes.Replace(wire, []byte(plan.ThreadID), []byte("01a10020-1222-76e3-977d-111111111111"), 1)
	if err = json.Unmarshal(wire, &restored); err == nil {
		t.Fatal("tampered persisted plan accepted")
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
			plan.destination = Destination{Harness: Codex, Root: "/sessionkit/destination-home", WorkingDir: "/sessionkit/destination-workspace", JournalDir: "/sessionkit/journal"}
			plan.ResumeHints.CodexHome = plan.destination.Root
			plan.ResumeHints.CWDOverride = plan.destination.WorkingDir
			plan.ResumeHints.RuntimeWorkspaceRoots = []string{plan.destination.WorkingDir}
			plan.ResumeHints.AppServerParams["cwd"] = plan.destination.WorkingDir
			plan.ResumeHints.AppServerParams["runtimeWorkspaceRoots"] = []string{plan.destination.WorkingDir}
			checkGolden(t, name+"-plan", plan)
		})
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
