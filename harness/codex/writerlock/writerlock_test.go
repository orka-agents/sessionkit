package writerlock

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/orka-agents/sessionkit/internal/model"
	"golang.org/x/sys/unix"
)

const testID = "0195e76b-7c5e-7123-8123-456789abcdef"

func canonicalTemp(t *testing.T) string {
	t.Helper()
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return dir
}

// This helper deliberately uses flock directly, as Codex's Rust file lock does.
func TestProcessLockHelper(t *testing.T) {
	home := os.Getenv("SESSIONKIT_LOCK_HELPER_HOME")
	if home == "" {
		return
	}
	if err := os.MkdirAll(filepath.Join(home, Directory), 0700); err != nil {
		t.Fatal(err)
	}
	coordination, err := os.OpenFile(filepath.Join(home, CoordinationFile), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if err = unix.Flock(int(coordination.Fd()), unix.LOCK_EX); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = coordination.Close() }()
	if os.Getenv("SESSIONKIT_LOCK_HELPER_MODE") == "thread" {
		thread, err := os.OpenFile(filepath.Join(home, Directory, testID+".lock"), os.O_CREATE|os.O_RDWR, 0600)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = thread.Close() }()
		if err = unix.Flock(int(thread.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
			t.Fatal(err)
		}
		_ = coordination.Close()
	}
	if _, err := fmt.Fprintln(os.Stdout, "locked"); err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(io.Discard, os.Stdin); err != nil {
		t.Fatal(err)
	}
}

func startHolder(t *testing.T, home, mode string) func() {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestProcessLockHelper$")
	cmd.Env = append(os.Environ(), "SESSIONKIT_LOCK_HELPER_HOME="+home, "SESSIONKIT_LOCK_HELPER_MODE="+mode)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err = cmd.Start(); err != nil {
		cancel()
		t.Fatal(err)
	}
	released := false
	release := func() {
		if released {
			return
		}
		released = true
		_ = stdin.Close()
		err := cmd.Wait()
		cancel()
		if err != nil {
			t.Errorf("lock helper: %v", err)
		}
	}
	t.Cleanup(release)
	line, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil || line != "locked\n" {
		t.Fatalf("helper readiness %q: %v", line, err)
	}
	return release
}

func TestActiveWriterAcrossProcesses(t *testing.T) {
	home := canonicalTemp(t)
	release := startHolder(t, home, "thread")
	for _, acquire := range []func(context.Context, string, string) (io.Closer, error){Source, Publication} {
		lock, err := acquire(context.Background(), home, testID)
		if lock != nil {
			_ = lock.Close()
			t.Fatal("acquired an active writer")
		}
		var active *model.ActiveWriterError
		if !errors.As(err, &active) || active.ThreadID != testID {
			t.Fatalf("got %v", err)
		}
	}
	release()
	lock, err := Source(context.Background(), home, testID)
	if err != nil {
		t.Fatal(err)
	}
	if err = lock.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCoordinationWaitHonorsContext(t *testing.T) {
	home := canonicalTemp(t)
	release := startHolder(t, home, "coordination")
	for _, acquire := range []func(context.Context) (io.Closer, error){func(ctx context.Context) (io.Closer, error) { return Source(ctx, home, testID) }, func(ctx context.Context) (io.Closer, error) { return Publication(ctx, home, testID) }} {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		lock, err := acquire(ctx)
		cancel()
		if lock != nil {
			_ = lock.Close()
			t.Fatal("acquired held coordination")
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("got %v", err)
		}
	}
	release()
	lock, err := Publication(context.Background(), home, testID)
	if err != nil {
		t.Fatal(err)
	}
	if err = lock.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestInvalidThreadAndSymlinkLock(t *testing.T) {
	home := canonicalTemp(t)
	if lock, err := Source(context.Background(), home, "../escape"); err == nil {
		_ = lock.Close()
		t.Fatal("accepted traversal")
	}
	if _, err := os.Stat(filepath.Join(home, Directory)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("invalid ID mutated home: %v", err)
	}
	outside := canonicalTemp(t)
	if err := os.Symlink(outside, filepath.Join(home, Directory)); err != nil {
		t.Fatal(err)
	}
	if lock, err := Source(context.Background(), home, testID); err == nil {
		_ = lock.Close()
		t.Fatal("accepted symlink lock directory")
	}
}
