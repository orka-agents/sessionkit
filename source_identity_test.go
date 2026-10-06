package sessionkit

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/orka-agents/sessionkit/harness/codex/writerlock"
	"github.com/orka-agents/sessionkit/internal/budget"
	"github.com/orka-agents/sessionkit/internal/fsx"
	"github.com/orka-agents/sessionkit/internal/harness"
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

func TestCaptureCleansUpDuringCreation(t *testing.T) {
	for _, boundary := range []string{"bundle_directory_created", "bundle_created", "copied"} {
		t.Run(boundary, func(t *testing.T) {
			src, _, _ := testSource(t)
			dir := filepath.Join(tempDir(t), "bundle")
			failure := errors.New("bundle creation failed")
			_, err := capture(context.Background(), src, CaptureOptions{BundleDir: dir}, func(phase string) error {
				if phase == boundary {
					return failure
				}
				return nil
			})
			if !errors.Is(err, failure) {
				t.Fatalf("expected creation failure, got %v", err)
			}
			if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("failed capture left a bundle directory: %v", err)
			}
			if _, err := Capture(context.Background(), src, CaptureOptions{BundleDir: dir}); err != nil {
				t.Fatalf("retry after failed creation: %v", err)
			}
		})
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

func TestCancellationAfterInspectionRejectsSuccess(t *testing.T) {
	for _, operation := range []string{"inspect", "open bundle"} {
		t.Run(operation, func(t *testing.T) {
			src, bundle, _, _ := testBundle(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			original := adapters[Codex]
			adapters[Codex] = replacingSourceAdapter{Adapter: original, afterInspect: func() error {
				cancel()
				return nil
			}}
			t.Cleanup(func() { adapters[Codex] = original })
			var err error
			if operation == "inspect" {
				_, err = Inspect(ctx, src, Budget{})
			} else {
				_, err = OpenBundle(ctx, bundle.Dir, Budget{})
			}
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("reported success after cancellation: %v", err)
			}
		})
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
