// Package harness defines the byte-preserving adapter boundary.
package harness

import (
	"context"
	"github.com/orka-agents/sessionkit/internal/budget"
	"github.com/orka-agents/sessionkit/internal/fsx"
	"github.com/orka-agents/sessionkit/internal/model"
	"io"
)

type Adapter interface {
	Inspect(context.Context, io.Reader, string, *budget.Tracker) (model.Inspection, error)
	Select(context.Context, model.Source, *fsx.Root, *budget.Tracker) (string, error)
	Layout(model.Inspection, string) (string, error)
	LockSource(context.Context, model.Source, *fsx.Root) (io.Closer, error)
	LockPublication(context.Context, *fsx.Root, string) (io.Closer, error)
}
