package sessionkit

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

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
						t.Fatalf("fresh collision must reject publication: %+v %v", receipt, err)
					}
					if _, err = os.Stat(witness); !errors.Is(err, os.ErrNotExist) {
						t.Fatalf("fresh collision retained its temp: %v", err)
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
				receipt, err = Install(context.Background(), plan)
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
