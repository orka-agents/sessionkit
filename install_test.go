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
	"sync"
	"testing"
	"time"

	"github.com/orka-agents/sessionkit/harness/codex/writerlock"
	"github.com/orka-agents/sessionkit/internal/journal"
)

func TestCollisionLiveAndArchived(t *testing.T) {
	ctx := context.Background()
	src, bundle, rel, raw := testBundle(t)
	for _, entry := range []struct {
		base      string
		uppercase bool
	}{{"sessions", false}, {"sessions", true}, {"archived_sessions", false}, {"archived_sessions", true}} {
		t.Run(fmt.Sprintf("%s/uppercase_%t", entry.base, entry.uppercase), func(t *testing.T) {
			dst := testDestination(t)
			name := strings.Replace(rel, "sessions", entry.base, 1)
			if entry.uppercase {
				name = strings.ReplaceAll(name, src.ThreadID, strings.ToUpper(src.ThreadID))
			}
			collisionPath := filepath.Join(dst.Root, name)
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

func TestInstallCollisionRetainsRetryState(t *testing.T) {
	for _, tc := range []struct {
		name     string
		phase    string
		archived bool
	}{
		{name: "matching filename", phase: "staged", archived: true},
		{name: "existing target", phase: "staged"},
		{name: "link race", phase: "link"},
	} {
		for _, previouslyPublished := range []bool{false, true} {
			attempt := "fresh"
			if previouslyPublished {
				attempt = "published retry"
			}
			t.Run(tc.name+"/"+attempt, func(t *testing.T) {
				_, bundle, rel, raw := testBundle(t)
				dst := testDestination(t)
				plan, err := PlanInstall(context.Background(), bundle, dst)
				if err != nil {
					t.Fatal(err)
				}
				target := filepath.Join(dst.Root, rel)
				witness := filepath.Join(filepath.Dir(target), ".sessionkit-"+plan.OperationID+".tmp")
				var originalWitness os.FileInfo
				if previouslyPublished {
					_, err = install(context.Background(), plan, func(phase string) error {
						if phase == "dir_fsync" {
							return fmt.Errorf("interrupted after publication")
						}
						return nil
					})
					var unknown *UnknownOutcomeError
					if !errors.As(err, &unknown) {
						t.Fatalf("expected interrupted publication, got %v", err)
					}
					originalWitness, err = os.Stat(witness)
					if err != nil {
						t.Fatal(err)
					}
					published, err := os.Stat(target)
					if err != nil || !os.SameFile(originalWitness, published) {
						t.Fatalf("publication lacks its witness: %v", err)
					}
					if !tc.archived {
						if err = os.Remove(target); err != nil {
							t.Fatal(err)
						}
					}
				}
				competingPath := target
				if tc.archived {
					competingPath = filepath.Join(dst.Root, "archived_sessions", "rollout-"+plan.ThreadID+".jsonl")
				}
				competingBytes := []byte("competing rollout")
				for range 2 {
					injected := false
					receipt, err := install(context.Background(), plan, func(phase string) error {
						if phase != tc.phase {
							return nil
						}
						injected = true
						if err := os.MkdirAll(filepath.Dir(competingPath), 0700); err != nil {
							return err
						}
						return os.WriteFile(competingPath, competingBytes, 0600)
					})
					var collision *CollisionError
					if !injected || !errors.As(err, &collision) {
						t.Fatalf("expected injected collision, got %+v %v", receipt, err)
					}
					expectedPhase := "planned"
					if previouslyPublished {
						expectedPhase = "staged"
						var unknown *UnknownOutcomeError
						if receipt.Outcome != Unknown || !errors.As(err, &unknown) {
							t.Fatalf("prior publication must remain uncertain: %+v %v", receipt, err)
						}
						retained, err := os.Stat(witness)
						if err != nil || !os.SameFile(originalWitness, retained) {
							t.Fatalf("collision lost its publication witness: %v", err)
						}
					} else {
						if receipt.Outcome != RejectedBeforeMutation {
							t.Fatalf("unpublished collision must reject publication: %+v %v", receipt, err)
						}
						if _, err = os.Stat(witness); !errors.Is(err, os.ErrNotExist) {
							t.Fatalf("unpublished collision retained its temp: %v", err)
						}
					}
					var state struct {
						Phase string `json:"phase"`
					}
					if err = json.Unmarshal(mustRead(t, filepath.Join(dst.JournalDir, plan.OperationID+".json")), &state); err != nil {
						t.Fatal(err)
					}
					if receipt.Phase != expectedPhase || state.Phase != expectedPhase {
						t.Fatalf("receipt phase %q and journal phase %q, want %q", receipt.Phase, state.Phase, expectedPhase)
					}
					if !bytes.Equal(competingBytes, mustRead(t, competingPath)) {
						t.Fatal("install changed the competing rollout")
					}
					if err = os.Remove(competingPath); err != nil {
						t.Fatal(err)
					}
				}
				receipt, err := Install(context.Background(), plan)
				if err != nil || receipt.Outcome != Installed {
					t.Fatalf("same-plan retry after collision removal: %+v %v", receipt, err)
				}
				if !bytes.Equal(raw, mustRead(t, target)) {
					t.Fatal("retry did not install the captured rollout")
				}
			})
		}
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
			// Publication is recorded before the staged witness is removed, so
			// only faults before the journal write leave the operation staged.
			expectedPhase := "published"
			if fault == "link" || fault == "dir_fsync" {
				expectedPhase = "staged"
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

func TestInstallRejectsActiveDestinationWriter(t *testing.T) {
	_, bundle, rel, _ := testBundle(t)
	dst := testDestination(t)
	plan, err := PlanInstall(context.Background(), bundle, dst)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := writerlock.Source(context.Background(), dst.Root, plan.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = writer.Close() })
	for attempt := range 2 {
		receipt, err := Install(context.Background(), plan)
		var active *ActiveWriterError
		if !errors.As(err, &active) || receipt.Outcome != RejectedBeforeMutation || receipt.Phase != "planned" {
			t.Fatalf("attempt %d must reject the destination writer before publication: %+v %v", attempt, receipt, err)
		}
		if _, err = os.Stat(filepath.Join(dst.Root, rel)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("install published beside an active writer: %v", err)
		}
	}
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = Install(context.Background(), plan); err != nil {
		t.Fatalf("retry after the writer stopped: %v", err)
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

func TestRetryAfterPublicationDoesNotClaimRejectedWhenBundleIsMissing(t *testing.T) {
	_, bundle, rel, _ := testBundle(t)
	dst := testDestination(t)
	plan, err := PlanInstall(context.Background(), bundle, dst)
	if err != nil {
		t.Fatal(err)
	}
	_, err = install(context.Background(), plan, func(phase string) error {
		if phase == "dir_fsync" {
			return fmt.Errorf("interrupted after publication")
		}
		return nil
	})
	var unknown *UnknownOutcomeError
	if !errors.As(err, &unknown) {
		t.Fatalf("expected interruption, got %v", err)
	}
	if _, err = os.Stat(filepath.Join(dst.Root, rel)); err != nil {
		t.Fatal(err)
	}
	if err = os.Remove(filepath.Join(bundle.Dir, "manifest.json")); err != nil {
		t.Fatal(err)
	}
	receipt, err := Install(context.Background(), plan)
	if receipt.Outcome == RejectedBeforeMutation {
		t.Fatalf("published target misreported as rejected: %+v %v", receipt, err)
	}
	if err != nil && !errors.As(err, &unknown) {
		t.Fatalf("retry must report uncertainty: %+v %v", receipt, err)
	}
}

func TestRetryCollisionPreservesPublicationWitness(t *testing.T) {
	_, bundle, rel, raw := testBundle(t)
	dst := testDestination(t)
	plan, err := PlanInstall(context.Background(), bundle, dst)
	if err != nil {
		t.Fatal(err)
	}
	_, err = install(context.Background(), plan, func(phase string) error {
		if phase == "dir_fsync" {
			return fmt.Errorf("interrupted after publication")
		}
		return nil
	})
	var unknown *UnknownOutcomeError
	if !errors.As(err, &unknown) {
		t.Fatalf("expected interruption, got %v", err)
	}
	archived := filepath.Join(dst.Root, "archived_sessions", "rollout-"+plan.ThreadID+".jsonl")
	if err = os.MkdirAll(filepath.Dir(archived), 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(archived, raw, 0600); err != nil {
		t.Fatal(err)
	}
	receipt, err := Install(context.Background(), plan)
	if receipt.Outcome != Unknown || !errors.As(err, &unknown) {
		t.Errorf("retry after prior publication must remain uncertain: %+v %v", receipt, err)
	}
	witness := filepath.Join(dst.Root, filepath.Dir(rel), ".sessionkit-"+plan.OperationID+".tmp")
	if _, err = os.Stat(witness); err != nil {
		t.Fatalf("retry removed its publication witness: %v", err)
	}
}

func TestStagedVerificationFailurePreservesPublicationKnowledge(t *testing.T) {
	for _, failure := range []string{"missing", "digest"} {
		for _, retry := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/retry_%t", failure, retry), func(t *testing.T) {
				_, bundle, _, _ := testBundle(t)
				dst := testDestination(t)
				plan, err := PlanInstall(context.Background(), bundle, dst)
				if err != nil {
					t.Fatal(err)
				}
				witness := filepath.Join(dst.Root, filepath.Dir(plan.TargetPath), ".sessionkit-"+plan.OperationID+".tmp")
				damage := func() error {
					if failure == "missing" {
						return os.Remove(witness)
					}
					return os.WriteFile(witness, []byte("changed"), 0600)
				}
				receipt, err := install(context.Background(), plan, func(phase string) error {
					if phase != "staged" {
						return nil
					}
					if retry {
						return fmt.Errorf("interrupted staged attempt")
					}
					return damage()
				})
				if retry {
					var unknown *UnknownOutcomeError
					if !errors.As(err, &unknown) {
						t.Fatalf("expected interrupted attempt: %v", err)
					}
					if err := damage(); err != nil {
						t.Fatal(err)
					}
					receipt, err = Install(context.Background(), plan)
					if receipt.Outcome != Unknown || !errors.As(err, &unknown) {
						t.Fatalf("staged retry must preserve uncertainty: %+v %v", receipt, err)
					}
					if failure == "digest" {
						if _, err := os.Stat(witness); err != nil {
							t.Fatalf("staged retry removed its witness: %v", err)
						}
					}
					return
				}
				if receipt.Outcome != RejectedBeforeMutation || receipt.Phase != "planned" || err == nil {
					t.Fatalf("unpublished attempt must reject and reset: %+v %v", receipt, err)
				}
				if _, err := os.Stat(witness); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("unpublished attempt left its witness: %v", err)
				}
				receipt, err = Install(context.Background(), plan)
				if err != nil || receipt.Outcome != Installed {
					t.Fatalf("same-plan retry after rejection: %+v %v", receipt, err)
				}
			})
		}
	}
}

func TestTargetOpenFailureRetainsRetryState(t *testing.T) {
	for _, retry := range []bool{false, true} {
		t.Run(fmt.Sprintf("retry_%t", retry), func(t *testing.T) {
			_, bundle, rel, raw := testBundle(t)
			dst := testDestination(t)
			plan, err := PlanInstall(context.Background(), bundle, dst)
			if err != nil {
				t.Fatal(err)
			}
			if retry {
				_, err = install(context.Background(), plan, func(phase string) error {
					if phase == "staged" {
						return fmt.Errorf("interrupted before publication")
					}
					return nil
				})
				var unknown *UnknownOutcomeError
				if !errors.As(err, &unknown) {
					t.Fatalf("expected staged interruption: %v", err)
				}
			}
			target := filepath.Join(dst.Root, rel)
			other := filepath.Join(tempDir(t), "other")
			if err := os.WriteFile(other, []byte("other file"), 0600); err != nil {
				t.Fatal(err)
			}
			injected := false
			receipt, err := install(context.Background(), plan, func(phase string) error {
				if phase == "target_open" {
					injected = true
					return os.Symlink(other, target)
				}
				return nil
			})
			if !injected || err == nil {
				t.Fatalf("expected target-open failure: %+v %v", receipt, err)
			}
			expectedPhase := "planned"
			witness := filepath.Join(filepath.Dir(target), ".sessionkit-"+plan.OperationID+".tmp")
			if retry {
				expectedPhase = "staged"
				var unknown *UnknownOutcomeError
				if receipt.Outcome != Unknown || !errors.As(err, &unknown) {
					t.Fatalf("staged retry lost uncertainty: %+v %v", receipt, err)
				}
				if _, err := os.Stat(witness); err != nil {
					t.Fatalf("staged retry lost its witness: %v", err)
				}
			} else {
				if receipt.Outcome != RejectedBeforeMutation {
					t.Fatalf("unpublished attempt must reject: %+v %v", receipt, err)
				}
				if _, err := os.Stat(witness); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("unpublished attempt left its witness: %v", err)
				}
			}
			var state journal.State
			if err := json.Unmarshal(mustRead(t, filepath.Join(dst.JournalDir, plan.OperationID+".json")), &state); err != nil {
				t.Fatal(err)
			}
			if receipt.Phase != expectedPhase || state.Phase != expectedPhase {
				t.Fatalf("receipt and journal phase must be %s: %+v %+v", expectedPhase, receipt, state)
			}
			if string(mustRead(t, other)) != "other file" {
				t.Fatal("changed the symlink target")
			}
			if err := os.Remove(target); err != nil {
				t.Fatal(err)
			}
			receipt, err = Install(context.Background(), plan)
			if err != nil || receipt.Outcome != Installed || !bytes.Equal(raw, mustRead(t, target)) {
				t.Fatalf("same-plan retry failed: %+v %v", receipt, err)
			}
		})
	}
}

func TestSlowCleanupReportsItsBudget(t *testing.T) {
	_, bundle, _, _ := testBundle(t)
	dst := testDestination(t)
	plan, err := PlanInstall(context.Background(), bundle, dst)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	receipt, err := install(ctx, plan, func(phase string) error {
		switch phase {
		case "staged":
			cancel()
		case "cleanup_journal":
			time.Sleep(1100 * time.Millisecond)
		}
		return nil
	})
	var exceeded *BudgetError
	if !errors.As(err, &exceeded) || exceeded.Limit != "timeout" || receipt.Outcome != RejectedBeforeMutation || receipt.Phase != "planned" {
		t.Fatalf("slow cleanup did not report its budget: %+v %v", receipt, err)
	}
	var state journal.State
	if err := json.Unmarshal(mustRead(t, filepath.Join(dst.JournalDir, plan.OperationID+".json")), &state); err != nil {
		t.Fatal(err)
	}
	if state.Phase != "planned" {
		t.Fatalf("journal did not reset: %+v", state)
	}
	if _, err := os.Stat(filepath.Join(dst.Root, state.TempPath)); err != nil {
		t.Fatalf("cleanup timeout lost staging: %v", err)
	}
	receipt, err = Install(context.Background(), plan)
	if err != nil || receipt.Outcome != Installed {
		t.Fatalf("retry after cleanup timeout: %+v %v", receipt, err)
	}
}

func TestCancellationBeforePublicationUsesCleanupBudget(t *testing.T) {
	_, bundle, rel, raw := testBundle(t)
	dst := testDestination(t)
	plan, err := PlanInstall(context.Background(), bundle, dst)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var held io.Closer
	receipt, err := install(ctx, plan, func(phase string) error {
		if phase != "staged" {
			return nil
		}
		var err error
		held, err = writerlock.Publication(context.Background(), dst.Root, plan.ThreadID)
		if err != nil {
			return err
		}
		t.Cleanup(func() { _ = held.Close() })
		timer := time.AfterFunc(30*time.Millisecond, cancel)
		t.Cleanup(func() { timer.Stop() })
		return nil
	})
	if !errors.Is(err, context.Canceled) || receipt.Outcome != RejectedBeforeMutation || receipt.Phase != "planned" {
		t.Fatalf("unpublished cancellation must reset: %+v %v", receipt, err)
	}
	var state journal.State
	if err := json.Unmarshal(mustRead(t, filepath.Join(dst.JournalDir, plan.OperationID+".json")), &state); err != nil {
		t.Fatal(err)
	}
	if state.Phase != "planned" {
		t.Fatalf("journal did not reset: %+v", state)
	}
	for _, name := range []string{rel, state.TempPath} {
		if _, err := os.Stat(filepath.Join(dst.Root, name)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("unpublished cancellation left %s: %v", name, err)
		}
	}
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	receipt, err = Install(context.Background(), plan)
	if err != nil || receipt.Outcome != Installed || !bytes.Equal(raw, mustRead(t, filepath.Join(dst.Root, rel))) {
		t.Fatalf("same-plan retry after cancellation: %+v %v", receipt, err)
	}
}

func TestVerifyCoordinatesWithNativeWriters(t *testing.T) {
	_, bundle, _, _ := testBundle(t)
	dst := testDestination(t)
	plan, err := PlanInstall(context.Background(), bundle, dst)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := Install(context.Background(), plan)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := writerlock.Source(context.Background(), dst.Root, plan.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	result, err := Verify(context.Background(), receipt, dst)
	var active *ActiveWriterError
	if result.Valid || !errors.As(err, &active) {
		_ = writer.Close()
		t.Fatalf("verification accepted an active writer: %+v %v", result, err)
	}
	retry, err := Install(context.Background(), plan)
	_ = writer.Close()
	if retry.Outcome != Unknown || !errors.As(err, &active) {
		t.Fatalf("installation retry accepted an active writer: %+v %v", retry, err)
	}
	result, err = verify(context.Background(), receipt, dst, func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		defer cancel()
		writer, err := writerlock.Source(ctx, dst.Root, plan.ThreadID)
		if writer != nil {
			_ = writer.Close()
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("writer was not blocked during verification: %v", err)
		}
		return nil
	})
	if err != nil || !result.Valid {
		t.Fatalf("coordinated verification: %+v %v", result, err)
	}
}

func TestVerifiedRetryWithoutBundleDirectory(t *testing.T) {
	_, bundle, _, _ := testBundle(t)
	dst := testDestination(t)
	plan, err := PlanInstall(context.Background(), bundle, dst)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = Install(context.Background(), plan); err != nil {
		t.Fatal(err)
	}
	if err = os.RemoveAll(bundle.Dir); err != nil {
		t.Fatal(err)
	}
	receipt, err := Install(context.Background(), plan)
	if err != nil || receipt.Outcome != Installed {
		t.Fatalf("verified recovery must not require the original bundle directory: %+v %v", receipt, err)
	}
}
