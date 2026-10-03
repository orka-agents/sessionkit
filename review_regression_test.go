package sessionkit

import (
	"context"
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
	lock, err := writerlock.Publication(context.Background(), src.Root)
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
