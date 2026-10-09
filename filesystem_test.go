package sessionkit

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/orka-agents/sessionkit/harness/codex/writerlock"
	"github.com/orka-agents/sessionkit/internal/budget"
	"github.com/orka-agents/sessionkit/internal/fsx"
	"github.com/orka-agents/sessionkit/internal/harness"
	"github.com/orka-agents/sessionkit/internal/journal"
)

type replacingSourceAdapter struct {
	harness.Adapter
	afterLock, afterSelect func() error
	afterInspect           func() error
}

func (a replacingSourceAdapter) Inspect(ctx context.Context, input io.Reader, rel string, b *budget.Tracker) (Inspection, error) {
	in, err := a.Adapter.Inspect(ctx, input, rel, b)
	if err == nil && a.afterInspect != nil {
		err = a.afterInspect()
	}
	return in, err
}

func (a replacingSourceAdapter) LockSource(ctx context.Context, src Source, root *fsx.Root) (io.Closer, error) {
	lock, err := a.Adapter.LockSource(ctx, src, root)
	if err == nil && a.afterLock != nil {
		err = a.afterLock()
		if err != nil {
			_ = lock.Close()
		}
	}
	return lock, err
}

func (a replacingSourceAdapter) Select(ctx context.Context, src Source, root *fsx.Root, b *budget.Tracker) (string, error) {
	rel, err := a.Adapter.Select(ctx, src, root, b)
	if err == nil && a.afterSelect != nil {
		err = a.afterSelect()
	}
	return rel, err
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

func TestSourceReplacementCannotBypassWriterLock(t *testing.T) {
	for _, operation := range []string{"inspect", "capture"} {
		for _, phase := range []string{"after lock", "after selection"} {
			t.Run(operation+"/"+phase, func(t *testing.T) {
				src, rel, raw := testSource(t)
				moved := src.Root + "-moved"
				t.Cleanup(func() { _ = os.RemoveAll(moved) })
				replace := func() error {
					if err := os.Rename(src.Root, moved); err != nil {
						return err
					}
					if err := os.MkdirAll(filepath.Dir(filepath.Join(src.Root, rel)), 0700); err != nil {
						return err
					}
					// Invalid replacement bytes distinguish an accidental pathname read.
					if err := os.WriteFile(filepath.Join(src.Root, rel), []byte("live replacement\n"), 0600); err != nil {
						return err
					}
					writer, err := writerlock.Source(context.Background(), src.Root, src.ThreadID)
					if err == nil {
						t.Cleanup(func() { _ = writer.Close() })
					}
					return err
				}
				original := adapters[Codex]
				a := replacingSourceAdapter{Adapter: original}
				if phase == "after lock" {
					a.afterLock = replace
				} else {
					a.afterSelect = replace
				}
				adapters[Codex] = a
				t.Cleanup(func() { adapters[Codex] = original })
				var err error
				if operation == "inspect" {
					_, err = Inspect(context.Background(), src, Budget{})
				} else {
					_, err = Capture(context.Background(), src, CaptureOptions{BundleDir: filepath.Join(tempDir(t), "bundle")})
				}
				if err == nil || !strings.Contains(err.Error(), "moved or replaced") {
					t.Fatalf("expected source identity rejection, got %v", err)
				}
				if !bytes.Equal(raw, mustRead(t, filepath.Join(moved, rel))) {
					t.Fatal("source bytes changed")
				}
			})
		}
	}
}

func TestCaptureRejectsMovedBundlePath(t *testing.T) {
	src, _, _ := testSource(t)
	dir := filepath.Join(tempDir(t), "bundle")
	moved := dir + "-moved"
	_, err := capture(context.Background(), src, CaptureOptions{BundleDir: dir}, func(phase string) error {
		if phase != "copied" {
			return nil
		}
		if err := os.Rename(dir, moved); err != nil {
			return err
		}
		return os.Mkdir(dir, 0700)
	})
	if err == nil || !strings.Contains(err.Error(), "moved or replaced") {
		t.Fatalf("capture must reject a replaced bundle path, got %v", err)
	}
	if _, err = os.Stat(dir); err != nil {
		t.Fatalf("cleanup removed the replacement directory: %v", err)
	}
}

func TestCaptureRejectsReplacedBundleParent(t *testing.T) {
	src, _, _ := testSource(t)
	parent := tempDir(t)
	dir := filepath.Join(parent, "bundle")
	moved := parent + "-moved"
	t.Cleanup(func() { _ = os.RemoveAll(moved) })
	original := adapters[Codex]
	adapters[Codex] = replacingSourceAdapter{Adapter: original, afterLock: func() error {
		if err := os.Rename(parent, moved); err != nil {
			return err
		}
		return os.MkdirAll(dir, 0700)
	}}
	t.Cleanup(func() { adapters[Codex] = original })
	_, err := Capture(context.Background(), src, CaptureOptions{BundleDir: dir})
	if err == nil || !strings.Contains(err.Error(), "moved or replaced") {
		t.Fatalf("capture accepted an unowned bundle directory: %v", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 0 {
		t.Fatalf("capture wrote into the replacement: %v %v", entries, err)
	}
}

func TestOpenBundleRejectsMovedPath(t *testing.T) {
	_, bundle, _, _ := testBundle(t)
	original := adapters[Codex]
	adapters[Codex] = replacingSourceAdapter{Adapter: original, afterInspect: func() error {
		if err := os.Rename(bundle.Dir, bundle.Dir+"-moved"); err != nil {
			return err
		}
		return os.Mkdir(bundle.Dir, 0700)
	}}
	t.Cleanup(func() { adapters[Codex] = original })
	_, err := OpenBundle(context.Background(), bundle.Dir, Budget{})
	if err == nil || !strings.Contains(err.Error(), "moved or replaced") {
		t.Fatalf("open must reject a replaced bundle path, got %v", err)
	}
}

func TestJournalContainmentUsesDirectoryIdentity(t *testing.T) {
	_, bundle, _, _ := testBundle(t)
	for _, alias := range []bool{false, true} {
		t.Run(map[bool]string{false: "nested", true: "case alias"}[alias], func(t *testing.T) {
			dst := testDestination(t)
			dst.Root = filepath.Join(dst.Root, "Home")
			if err := os.MkdirAll(filepath.Join(dst.Root, "journal"), 0700); err != nil {
				t.Fatal(err)
			}
			dst.JournalDir = filepath.Join(dst.Root, "journal")
			if alias {
				dst.JournalDir = filepath.Join(filepath.Dir(dst.Root), "home", "journal")
				if _, err := os.Stat(dst.JournalDir); errors.Is(err, os.ErrNotExist) {
					t.Skip("filesystem is case-sensitive")
				} else if err != nil {
					t.Fatal(err)
				}
			}
			_, err := PlanInstall(context.Background(), bundle, dst)
			var rejected *RejectionError
			if !errors.As(err, &rejected) || rejected.Rejections[0].Code != "journal_path" {
				t.Fatalf("accepted journal inside destination: %v", err)
			}
			entries, err := os.ReadDir(dst.JournalDir)
			if err != nil || len(entries) != 0 {
				t.Fatalf("planning wrote into journal: %v %v", entries, err)
			}
		})
	}
}

func TestDestinationAndBundleMustNotOverlap(t *testing.T) {
	for _, scope := range []string{"bundle", "components", "parent"} {
		t.Run(scope, func(t *testing.T) {
			_, bundle, _, _ := testBundle(t)
			dst := testDestination(t)
			plan, err := PlanInstall(context.Background(), bundle, dst)
			if err != nil {
				t.Fatal(err)
			}
			switch scope {
			case "bundle":
				dst.Root = bundle.Dir
			case "components":
				dst.Root = filepath.Join(bundle.Dir, "components")
			case "parent":
				dst.Root = filepath.Dir(bundle.Dir)
			}
			_, err = PlanInstall(context.Background(), bundle, dst)
			var rejected *RejectionError
			if !errors.As(err, &rejected) || rejected.Rejections[0].Code != "destination_path" {
				t.Fatalf("planning accepted destination/bundle overlap: %v", err)
			}
			// Simulate a saved plan from before overlap validation was added.
			plan.destination = dst
			plan.ResumeHints.CodexHome = dst.Root
			plan.seal = planSeal(plan)
			receipt, err := Install(context.Background(), plan)
			if receipt.Outcome != RejectedBeforeMutation || !errors.As(err, &rejected) {
				t.Fatalf("installation accepted destination/bundle overlap: %+v %v", receipt, err)
			}
			if _, err := OpenBundle(context.Background(), bundle.Dir, Budget{}); err != nil {
				t.Fatalf("overlap rejection mutated the bundle: %v", err)
			}
		})
	}
}

func TestJournalCannotMutateVerifiedBundle(t *testing.T) {
	for _, inside := range []string{".", "components"} {
		t.Run(inside, func(t *testing.T) {
			_, bundle, _, _ := testBundle(t)
			dst := testDestination(t)
			dst.JournalDir = filepath.Join(bundle.Dir, inside)
			_, err := PlanInstall(context.Background(), bundle, dst)
			var rejected *RejectionError
			if !errors.As(err, &rejected) {
				t.Fatalf("planning accepted journal inside bundle: %v", err)
			}
			opened, err := journal.OpenOutside(context.Background(), dst.JournalDir, strings.Repeat("a", 32), bundle.Dir)
			if opened != nil {
				_ = opened.Close()
			}
			if !errors.As(err, &rejected) {
				t.Fatalf("journal open accepted overlap: %v", err)
			}
			if _, err := OpenBundle(context.Background(), bundle.Dir, Budget{}); err != nil {
				t.Fatalf("overlap rejection mutated the verified bundle: %v", err)
			}
		})
	}
}

func TestInstallRootChangesRetainRetryState(t *testing.T) {
	for _, phase := range []string{"staged", "link"} {
		for _, priorStaged := range []bool{false, true} {
			t.Run(phase+map[bool]string{false: "/unpublished", true: "/staged retry"}[priorStaged], func(t *testing.T) {
				_, bundle, rel, raw := testBundle(t)
				dst := testDestination(t)
				plan, err := PlanInstall(context.Background(), bundle, dst)
				if err != nil {
					t.Fatal(err)
				}
				if priorStaged {
					_, err = install(context.Background(), plan, func(phase string) error {
						if phase == "staged" {
							return fmt.Errorf("interrupted before publication")
						}
						return nil
					})
					var unknown *UnknownOutcomeError
					if !errors.As(err, &unknown) {
						t.Fatalf("expected staged interruption, got %v", err)
					}
				}
				moved := dst.Root + "-moved"
				t.Cleanup(func() { _ = os.RemoveAll(moved) })
				for range 2 {
					injected := false
					receipt, err := install(context.Background(), plan, func(at string) error {
						if at != phase {
							return nil
						}
						injected = true
						if err := os.Rename(dst.Root, moved); err != nil {
							return err
						}
						return os.Mkdir(dst.Root, 0700)
					})
					var unknown *UnknownOutcomeError
					if !injected || err == nil || errors.As(err, &unknown) != priorStaged {
						t.Fatalf("unexpected root-change outcome: %+v %v", receipt, err)
					}
					wantPhase, wantOutcome := "planned", RejectedBeforeMutation
					if priorStaged {
						wantPhase, wantOutcome = "staged", Unknown
					}
					var state struct{ Phase string }
					if err := json.Unmarshal(mustRead(t, filepath.Join(dst.JournalDir, plan.OperationID+".json")), &state); err != nil {
						t.Fatal(err)
					}
					if receipt.Phase != wantPhase || state.Phase != wantPhase || receipt.Outcome != wantOutcome {
						t.Fatalf("root-change receipt/journal mismatch: %+v %+v", receipt, state)
					}
					witness := filepath.Join(moved, filepath.Dir(rel), ".sessionkit-"+plan.OperationID+".tmp")
					_, statErr := os.Stat(witness)
					if priorStaged && statErr != nil || !priorStaged && !errors.Is(statErr, os.ErrNotExist) {
						t.Fatalf("unexpected witness after root change: %v", statErr)
					}
					for _, root := range []string{dst.Root, moved} {
						if _, err := os.Stat(filepath.Join(root, rel)); !errors.Is(err, os.ErrNotExist) {
							t.Fatalf("root-change attempt published a rollout: %v", err)
						}
					}
					if err := os.Remove(dst.Root); err != nil {
						t.Fatal(err)
					}
					if err := os.Rename(moved, dst.Root); err != nil {
						t.Fatal(err)
					}
				}
				if _, err := Install(context.Background(), plan); err != nil {
					t.Fatalf("retry after restoring destination: %v", err)
				}
				if !bytes.Equal(raw, mustRead(t, filepath.Join(dst.Root, rel))) {
					t.Fatal("retry changed the rollout bytes")
				}
			})
		}
	}
}

func TestRootReplacementDuringPublicationDoesNotReportInstalled(t *testing.T) {
	_, bundle, rel, _ := testBundle(t)
	dst := testDestination(t)
	plan, err := PlanInstall(context.Background(), bundle, dst)
	if err != nil {
		t.Fatal(err)
	}
	moved := dst.Root + "-moved"
	t.Cleanup(func() { _ = os.RemoveAll(moved) })
	receipt, err := install(context.Background(), plan, func(phase string) error {
		if phase != "link" {
			return nil
		}
		if err := os.Rename(dst.Root, moved); err != nil {
			return err
		}
		return os.Mkdir(dst.Root, 0700)
	})
	if _, statErr := os.Stat(filepath.Join(dst.Root, rel)); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("expected replaced destination to have no target, got %v", statErr)
	}
	if receipt.Outcome == Installed || err == nil {
		t.Fatalf("installation went to moved root but claimed success: %+v %v", receipt, err)
	}
}

func TestDestinationReplacementAfterJournalVerificationDoesNotReportInstalled(t *testing.T) {
	for _, scope := range []string{"root", "target_parent"} {
		t.Run(scope, func(t *testing.T) {
			_, bundle, rel, raw := testBundle(t)
			dst := testDestination(t)
			plan, err := PlanInstall(context.Background(), bundle, dst)
			if err != nil {
				t.Fatal(err)
			}
			toMove := dst.Root
			if scope == "target_parent" {
				toMove = filepath.Join(dst.Root, filepath.Dir(rel))
			}
			moved := toMove + "-moved"
			t.Cleanup(func() { _ = os.RemoveAll(moved) })
			receipt, err := install(context.Background(), plan, func(phase string) error {
				if phase != "journal_verified" {
					return nil
				}
				data, err := os.ReadFile(filepath.Join(dst.JournalDir, plan.OperationID+".json"))
				if err != nil {
					return err
				}
				var state journal.State
				if err := json.Unmarshal(data, &state); err != nil {
					return err
				}
				if state.Phase != "verified" {
					return fmt.Errorf("journal phase is %q, want verified", state.Phase)
				}
				if err := os.Rename(toMove, moved); err != nil {
					return err
				}
				return os.Mkdir(toMove, 0700)
			})
			var unknown *UnknownOutcomeError
			if receipt.Outcome != Unknown || !errors.As(err, &unknown) {
				t.Fatalf("destination changed after journal write but install claimed success: %+v %v", receipt, err)
			}
			if _, err := os.Stat(filepath.Join(dst.Root, rel)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("replacement destination contains target: %v", err)
			}
			retainedTarget := filepath.Join(moved, rel)
			if scope == "target_parent" {
				retainedTarget = filepath.Join(moved, filepath.Base(rel))
			}
			if got, err := os.ReadFile(retainedTarget); err != nil || string(got) != string(raw) {
				t.Fatalf("published target changed in retained root: %v", err)
			}
			if err := os.Remove(toMove); err != nil {
				t.Fatal(err)
			}
			if err := os.Rename(moved, toMove); err != nil {
				t.Fatal(err)
			}
			receipt, err = Install(context.Background(), plan)
			if err != nil || receipt.Outcome != Installed {
				t.Fatalf("same-plan retry after restoring destination: %+v %v", receipt, err)
			}
		})
	}
}

func TestVerifyRejectsTargetParentReplacement(t *testing.T) {
	_, bundle, rel, _ := testBundle(t)
	dst := testDestination(t)
	plan, err := PlanInstall(context.Background(), bundle, dst)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := Install(context.Background(), plan)
	if err != nil {
		t.Fatal(err)
	}
	parent := filepath.Join(dst.Root, filepath.Dir(rel))
	moved := parent + "-moved"
	result, err := verify(context.Background(), receipt, dst, func() error {
		if err := os.Rename(parent, moved); err != nil {
			return err
		}
		return os.Mkdir(parent, 0700)
	})
	if result.Valid || err == nil {
		t.Fatalf("verification accepted target in moved parent: %+v %v", result, err)
	}
	if err := os.Remove(parent); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(moved, parent); err != nil {
		t.Fatal(err)
	}
	result, err = Verify(context.Background(), receipt, dst)
	if err != nil || !result.Valid {
		t.Fatalf("verification after restoring parent: %+v %v", result, err)
	}
	receipt.TargetPath = "missing/../" + receipt.TargetPath
	result, err = Verify(context.Background(), receipt, dst)
	if result.Valid || err == nil {
		t.Fatalf("verification accepted noncanonical receipt path: %+v %v", result, err)
	}
}

func TestJournalReplacementDuringPublicationDoesNotReportInstalled(t *testing.T) {
	_, bundle, _, _ := testBundle(t)
	dst := testDestination(t)
	plan, err := PlanInstall(context.Background(), bundle, dst)
	if err != nil {
		t.Fatal(err)
	}
	moved := dst.JournalDir + "-moved"
	t.Cleanup(func() { _ = os.RemoveAll(moved) })
	receipt, err := install(context.Background(), plan, func(phase string) error {
		if phase != "link" {
			return nil
		}
		if err := os.Rename(dst.JournalDir, moved); err != nil {
			return err
		}
		return os.Mkdir(dst.JournalDir, 0700)
	})
	if _, statErr := os.Stat(filepath.Join(dst.JournalDir, plan.OperationID+".json")); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("expected replaced journal directory to have no receipt, got %v", statErr)
	}
	if receipt.Outcome == Installed || err == nil {
		t.Fatalf("journal was written into moved directory but install claimed success: %+v %v", receipt, err)
	}
}

func TestVerifiedRetryDetectsJournalReplacement(t *testing.T) {
	_, bundle, _, _ := testBundle(t)
	dst := testDestination(t)
	plan, err := PlanInstall(context.Background(), bundle, dst)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = Install(context.Background(), plan); err != nil {
		t.Fatal(err)
	}
	moved := dst.JournalDir + "-moved"
	t.Cleanup(func() { _ = os.RemoveAll(moved) })
	receipt, err := install(context.Background(), plan, func(phase string) error {
		if phase != "verify" {
			return nil
		}
		if err := os.Rename(dst.JournalDir, moved); err != nil {
			return err
		}
		return os.Mkdir(dst.JournalDir, 0700)
	})
	if receipt.Outcome == Installed || err == nil {
		t.Fatalf("verified retry lost its configured journal but claimed success: %+v %v", receipt, err)
	}
}

func TestMovedJournalBeforeReadDoesNotBecomeFresh(t *testing.T) {
	_, bundle, _, _ := testBundle(t)
	dst := testDestination(t)
	plan, err := PlanInstall(context.Background(), bundle, dst)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = Install(context.Background(), plan); err != nil {
		t.Fatal(err)
	}
	moved := dst.JournalDir + "-moved"
	t.Cleanup(func() { _ = os.RemoveAll(moved) })
	receipt, err := install(context.Background(), plan, func(phase string) error {
		if phase == "journal_opened" {
			return os.Rename(dst.JournalDir, moved)
		}
		return nil
	})
	var unknown *UnknownOutcomeError
	if receipt.Outcome != Unknown || !errors.As(err, &unknown) {
		t.Fatalf("missing journal directory was mistaken for a fresh attempt: %+v %v", receipt, err)
	}
	if err := os.Rename(moved, dst.JournalDir); err != nil {
		t.Fatal(err)
	}
	receipt, err = Install(context.Background(), plan)
	if err != nil || receipt.Outcome != Installed {
		t.Fatalf("same-plan retry after restoring journal: %+v %v", receipt, err)
	}
}
