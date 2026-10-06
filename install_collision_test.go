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
