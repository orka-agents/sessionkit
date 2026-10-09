// Package budget accounts for resources shared by all work in one operation.
package budget

import (
	"context"
	"sync"
	"time"

	"github.com/orka-agents/sessionkit/internal/model"
)

// Tracker counts repeated reads and temporary writes, rather than distinct files.
type Tracker struct {
	mu                          sync.Mutex
	ctx                         context.Context
	limits                      model.Budget
	deadline                    time.Time
	invalid                     string
	bytes, records, nodes, temp int64
}

func New(ctx context.Context, limits model.Budget) *Tracker {
	if ctx == nil {
		ctx = context.Background()
	}
	t := &Tracker{ctx: ctx, limits: limits}
	values := []struct {
		name     string
		value    *int64
		fallback int64
	}{
		{"bytes", &t.limits.MaxBytes, 1 << 30}, {"line_bytes", &t.limits.MaxLineBytes, 64 << 20},
		{"records", &t.limits.MaxRecords, 100000}, {"nodes", &t.limits.MaxNodes, 10000000},
		{"temp_bytes", &t.limits.MaxTempBytes, 1 << 30},
	}
	for _, v := range values {
		if *v.value < 0 && t.invalid == "" {
			t.invalid = v.name
		}
		if *v.value == 0 {
			*v.value = v.fallback
		}
	}
	if t.limits.MaxDepth < 0 {
		t.invalid = "depth"
	}
	if t.limits.MaxDepth == 0 {
		t.limits.MaxDepth = 128
	}
	if t.limits.Timeout < 0 {
		t.invalid = "timeout"
	}
	if t.limits.Timeout == 0 {
		t.limits.Timeout = 5 * time.Minute
	}
	t.deadline = time.Now().Add(t.limits.Timeout)
	return t
}

func (t *Tracker) Limits() model.Budget { return t.limits }

// Deadline lets callers bound blocking operations without creating a timer for
// every tracker. Check reports an elapsed budget as a BudgetError.
func (t *Tracker) Deadline() time.Time { return t.deadline }

func (t *Tracker) Check() error {
	if t.invalid != "" {
		return &model.BudgetError{Limit: t.invalid}
	}
	if err := t.ctx.Err(); err != nil {
		return err
	}
	if !time.Now().Before(t.deadline) {
		return &model.BudgetError{Limit: "timeout"}
	}
	return nil
}

func (t *Tracker) charge(value *int64, amount, limit int64, name string) error {
	if err := t.Check(); err != nil {
		return err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if amount < 0 || amount > limit-*value {
		return &model.BudgetError{Limit: name}
	}
	*value += amount
	return nil
}

func (t *Tracker) Bytes(n int64) error { return t.charge(&t.bytes, n, t.limits.MaxBytes, "bytes") }
func (t *Tracker) Record() error       { return t.charge(&t.records, 1, t.limits.MaxRecords, "records") }
func (t *Tracker) Node() error         { return t.charge(&t.nodes, 1, t.limits.MaxNodes, "nodes") }
func (t *Tracker) Temp(n int64) error {
	return t.charge(&t.temp, n, t.limits.MaxTempBytes, "temp_bytes")
}

// RemainingBytes bounds an upcoming read before the reader allocates or fills it.
func (t *Tracker) RemainingBytes() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.limits.MaxBytes - t.bytes
}
