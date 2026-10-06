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
	"time"

	"github.com/orka-agents/sessionkit/harness/codex/writerlock"
	"github.com/orka-agents/sessionkit/internal/budget"
	"github.com/orka-agents/sessionkit/internal/journal"
)

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

type reviewReadCounter struct {
	input io.Reader
	bytes int
}

func (r *reviewReadCounter) Read(p []byte) (int, error) {
	n, err := r.input.Read(p)
	r.bytes += n
	return n, err
}

func TestCopyDigestBoundsReadRequest(t *testing.T) {
	input := &reviewReadCounter{input: strings.NewReader(strings.Repeat("x", 1<<20))}
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
