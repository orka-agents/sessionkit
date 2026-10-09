package fsx

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"golang.org/x/sys/unix"
)

func TestWalkFilesUsesRetainedDirectory(t *testing.T) {
	base, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	home := filepath.Join(base, "home")
	if err := os.MkdirAll(filepath.Join(home, "sessions", "2025"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(home, "sessions", "2025", "rollout.jsonl"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	root, err := OpenRoot(home)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = root.Close() }()
	if err := os.Rename(home, filepath.Join(base, "moved")); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(home, "sessions"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("missing", filepath.Join(home, "sessions", "2025")); err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	if err := root.WalkFiles("sessions", func(name string, dir bool) error {
		got[name] = dir
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{"sessions/2025": true, "sessions/2025/rollout.jsonl": false}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("visited %v, want %v", got, want)
	}
}

func TestWalkFilesRejectsUnsafeEntries(t *testing.T) {
	for _, kind := range []string{"symlink", "fifo"} {
		t.Run(kind, func(t *testing.T) {
			home, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if err := os.Mkdir(filepath.Join(home, "sessions"), 0700); err != nil {
				t.Fatal(err)
			}
			name := filepath.Join(home, "sessions", "unsafe")
			if kind == "symlink" {
				err = os.Symlink("missing", name)
			} else {
				err = unix.Mkfifo(name, 0600)
			}
			if err != nil {
				t.Fatal(err)
			}
			root, err := OpenRoot(home)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = root.Close() }()
			visited := 0
			err = root.WalkFiles("sessions", func(string, bool) error {
				visited++
				return nil
			})
			if err == nil || visited != 1 {
				t.Fatalf("unsafe entry was skipped or accepted: visited=%d err=%v", visited, err)
			}
		})
	}
}
