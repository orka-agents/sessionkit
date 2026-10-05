// Package writerlock uses Codex 0.160.0's flock namespace. Lock paths and the
// coordination protocol follow codex-rs/rollout/src/writer_lock.rs at a956835d.
package writerlock

import (
	"context"
	"errors"
	"io"
	"os"
	"regexp"
	"time"

	"github.com/orka-agents/sessionkit/internal/fsx"
	"github.com/orka-agents/sessionkit/internal/model"
	"golang.org/x/sys/unix"
)

const Directory = "thread-writer-locks"
const CoordinationFile = Directory + "/.coordination.lock"

var uuidV7 = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func ValidThreadID(id string) bool { return uuidV7.MatchString(id) }

// Source first holds coordination, then attempts the thread lock without
// waiting. The returned lock stays held until all source reads finish.
func Source(ctx context.Context, home, threadID string) (io.Closer, error) {
	root, err := fsx.OpenRoot(home)
	if err != nil {
		return nil, err
	}
	defer func() { _ = root.Close() }()
	return SourceAt(ctx, root, threadID)
}

// SourceAt acquires the writer lock through the root used for source reads.
// The caller must keep root open until those reads finish.
func SourceAt(ctx context.Context, root *fsx.Root, threadID string) (io.Closer, error) {
	if !ValidThreadID(threadID) {
		return nil, &model.RejectionError{Rejections: []model.Rejection{{Code: "thread_id", Message: "thread ID must be a canonical UUIDv7"}}}
	}
	coordination, err := coordinate(ctx, root)
	if err != nil {
		return nil, err
	}
	defer func() { _ = coordination.Close() }()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	file, err := root.LockFile(Directory + "/" + threadID + ".lock")
	if err != nil {
		return nil, err
	}
	if err = lockWriter(file, threadID); err != nil {
		_ = file.Close()
		return nil, err
	}
	// Leave the inode in place when closing. Codex removes stale locks under
	// coordination; unlinking without that lock would split writer ownership.
	return file, nil
}

// Publication probes the thread's writer lock while holding coordination, then
// keeps coordination held until the caller finishes publication.
func Publication(ctx context.Context, home, threadID string) (io.Closer, error) {
	if !ValidThreadID(threadID) {
		return nil, &model.RejectionError{Rejections: []model.Rejection{{Code: "thread_id", Message: "thread ID must be a canonical UUIDv7"}}}
	}
	root, err := fsx.OpenRoot(home)
	if err != nil {
		return nil, err
	}
	defer func() { _ = root.Close() }()
	file, err := coordinate(ctx, root)
	if err != nil {
		return nil, err
	}
	probe, err := root.Open(Directory + "/" + threadID + ".lock")
	if err == nil {
		err = lockWriter(probe, threadID)
		_ = probe.Close()
	} else if errors.Is(err, os.ErrNotExist) {
		err = nil
	}
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	return file, nil
}

func lockWriter(file *os.File, threadID string) error {
	err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB)
	if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
		return &model.ActiveWriterError{ThreadID: threadID}
	}
	return err
}

func coordinate(ctx context.Context, root *fsx.Root) (*os.File, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := root.MkdirAll(Directory); err != nil {
		return nil, err
	}
	file, err := root.LockFile(CoordinationFile)
	if err != nil {
		return nil, err
	}
	for {
		if err := ctx.Err(); err != nil {
			_ = file.Close()
			return nil, err
		}
		err = unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			return file, nil
		}
		if !errors.Is(err, unix.EWOULDBLOCK) && !errors.Is(err, unix.EAGAIN) && !errors.Is(err, unix.EINTR) {
			_ = file.Close()
			return nil, err
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			_ = file.Close()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}
