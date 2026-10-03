package budget

import (
	"context"
	"errors"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/orka-agents/sessionkit/internal/model"
)

func TestBudgetCountsWholeOperation(t *testing.T) {
	b := New(context.Background(), model.Budget{MaxBytes: 8, MaxRecords: 1, MaxNodes: 2, MaxTempBytes: 4})
	for _, n := range []int64{3, 5} {
		if err := b.Bytes(n); err != nil {
			t.Fatal(err)
		}
	}
	assertBudget(t, b.Bytes(1), "bytes")
	if err := b.Record(); err != nil {
		t.Fatal(err)
	}
	assertBudget(t, b.Record(), "records")
	if err := b.Node(); err != nil {
		t.Fatal(err)
	}
	if err := b.Node(); err != nil {
		t.Fatal(err)
	}
	assertBudget(t, b.Node(), "nodes")
	if err := b.Temp(4); err != nil {
		t.Fatal(err)
	}
	assertBudget(t, b.Temp(1), "temp_bytes")
}

func TestInvalidCancellationTimeoutAndOverflow(t *testing.T) {
	assertBudget(t, New(context.Background(), model.Budget{MaxDepth: -1}).Check(), "depth")
	assertBudget(t, New(context.Background(), model.Budget{MaxBytes: -1}).Check(), "bytes")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := New(ctx, model.Budget{}).Check(); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
	b := New(context.Background(), model.Budget{Timeout: time.Nanosecond})
	b.deadline = time.Now().Add(-time.Second)
	assertBudget(t, b.Check(), "timeout")
	b = New(context.Background(), model.Budget{MaxBytes: math.MaxInt64})
	if err := b.Bytes(math.MaxInt64); err != nil {
		t.Fatal(err)
	}
	assertBudget(t, b.Bytes(1), "bytes")
	assertBudget(t, b.Bytes(-1), "bytes")
}

func TestConcurrentCharges(t *testing.T) {
	b := New(context.Background(), model.Budget{MaxNodes: 100})
	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() {
			for range 10 {
				if err := b.Node(); err != nil {
					t.Error(err)
				}
			}
		})
	}
	wg.Wait()
	assertBudget(t, b.Node(), "nodes")
}

func assertBudget(t *testing.T, err error, limit string) {
	t.Helper()
	var exceeded *model.BudgetError
	if !errors.As(err, &exceeded) || exceeded.Limit != limit {
		t.Fatalf("want budget %s, got %v", limit, err)
	}
}
