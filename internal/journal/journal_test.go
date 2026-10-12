package journal

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/orka-agents/sessionkit/internal/budget"
	"github.com/orka-agents/sessionkit/internal/model"
)

func TestWriteReadSizeLimit(t *testing.T) {
	for _, name := range []string{"below", "exact", "above", "escaped path"} {
		for _, existing := range []bool{false, true} {
			t.Run(name+map[bool]string{false: "/new", true: "/existing"}[existing], func(t *testing.T) {
				dir, err := filepath.EvalSymlinks(t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				const id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
				j, err := Open(context.Background(), dir, id)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = j.Close() }()
				tracker := func() *budget.Tracker { return budget.New(context.Background(), model.Budget{}) }
				state := State{OperationID: id, Destination: "/", Phase: "published"}
				var previous []byte
				if existing {
					if err := j.Write(state, tracker()); err != nil {
						t.Fatal(err)
					}
					previous, err = os.ReadFile(filepath.Join(dir, id+".json"))
					if err != nil {
						t.Fatal(err)
					}
				}
				data, err := json.MarshalIndent(state, "", "  ")
				if err != nil {
					t.Fatal(err)
				}
				size := maxBytes
				switch name {
				case "below":
					size--
				case "above":
					size++
				}
				state.Destination += strings.Repeat("a", size-len(data)-1)
				if name == "escaped path" {
					state.Destination = "/" + strings.Repeat("<", 3000)
				}
				err = j.Write(state, tracker())
				if name == "above" || name == "escaped path" {
					var exceeded *model.BudgetError
					if !errors.As(err, &exceeded) || exceeded.Limit != "journal bytes" {
						t.Fatalf("oversized write: %v", err)
					}
					current, readErr := os.ReadFile(filepath.Join(dir, id+".json"))
					if existing {
						if readErr != nil || !bytes.Equal(current, previous) {
							t.Fatal("rejected write changed the existing journal")
						}
						if _, err := j.Read(tracker()); err != nil {
							t.Fatalf("previous journal is unreadable: %v", err)
						}
					} else if !errors.Is(readErr, os.ErrNotExist) {
						t.Fatalf("rejected write created a journal: %v", readErr)
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				got, err := j.Read(tracker())
				if err != nil || got != state {
					t.Fatalf("accepted journal did not round trip: %v", err)
				}
			})
		}
	}
}

func TestWriteReservesPhaseGrowth(t *testing.T) {
	for _, phase := range []string{"planned", "staged"} {
		for _, overflow := range []bool{false, true} {
			t.Run(phase+map[bool]string{false: "/fits", true: "/overflows"}[overflow], func(t *testing.T) {
				dir, err := filepath.EvalSymlinks(t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				const id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
				j, err := Open(context.Background(), dir, id)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = j.Close() }()
				tracker := func() *budget.Tracker { return budget.New(context.Background(), model.Budget{}) }
				state := State{OperationID: id, Destination: "/", Phase: "published"}
				data, err := json.MarshalIndent(state, "", "  ")
				if err != nil {
					t.Fatal(err)
				}
				padding := maxBytes - len(data) - 1
				if overflow {
					padding++
				}
				// JSON escapes each '<' to six bytes. The absolute path itself
				// stays below a typical Linux PATH_MAX while the journal reaches its cap.
				state.Destination += strings.Repeat("<", padding/6) + strings.Repeat("a", padding%6)
				state.Phase = phase
				err = j.Write(state, tracker())
				if overflow {
					var exceeded *model.BudgetError
					if !errors.As(err, &exceeded) || exceeded.Limit != "journal bytes" {
						t.Fatalf("journal without publication space accepted: %v", err)
					}
					if _, err := j.Read(tracker()); !errors.Is(err, ErrNotFound) {
						t.Fatalf("oversized operation persisted its initial journal: %v", err)
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				for _, next := range []string{"staged", "published", "verified"} {
					state.Phase = next
					if err := j.Write(state, tracker()); err != nil {
						t.Fatalf("accepted journal cannot advance to %s: %v", next, err)
					}
					got, err := j.Read(tracker())
					if err != nil || got != state {
						t.Fatalf("advanced journal cannot round trip: %v", err)
					}
				}
			})
		}
	}
}
