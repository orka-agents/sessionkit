// Package journal records installation progress. An operation lock serializes
// retries; each phase replaces the receipt atomically and is written only once.
package journal

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/orka-agents/sessionkit/internal/budget"
	"github.com/orka-agents/sessionkit/internal/fsx"
	"github.com/orka-agents/sessionkit/internal/jsonl"
	"github.com/orka-agents/sessionkit/internal/model"
	"golang.org/x/sys/unix"
)

type State struct {
	OperationID  string `json:"operationID"`
	ThreadID     string `json:"threadID"`
	TargetPath   string `json:"targetPath"`
	BundleDigest string `json:"bundleDigest"`
	TargetDigest string `json:"targetDigest"`
	Destination  string `json:"destination"`
	Phase        string `json:"phase"`
	TempPath     string `json:"tempPath,omitempty"`
}
type Journal struct {
	root *fsx.Root
	lock *os.File
	id   string
	dir  string
}

func Open(ctx context.Context, dir, id string) (*Journal, error) {
	if len(id) != 32 {
		return nil, fmt.Errorf("invalid operation ID")
	}
	for _, c := range id {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return nil, fmt.Errorf("invalid operation ID")
		}
	}
	r, err := fsx.OpenRoot(dir)
	if err != nil {
		return nil, err
	}
	f, err := r.LockFile(id + ".lock")
	if err != nil {
		_ = r.Close()
		return nil, err
	}
	for {
		err = unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			return &Journal{r, f, id, dir}, nil
		}
		if !errors.Is(err, unix.EWOULDBLOCK) && !errors.Is(err, unix.EINTR) {
			_ = f.Close()
			_ = r.Close()
			return nil, err
		}
		select {
		case <-ctx.Done():
			_ = f.Close()
			_ = r.Close()
			return nil, ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}
func (j *Journal) Close() error {
	e := j.lock.Close()
	other := j.root.Close()
	if e != nil {
		return e
	}
	return other
}
func (j *Journal) CheckPath() error { return j.root.CheckPath(j.dir) }
func (j *Journal) Read(tracker *budget.Tracker) (State, error) {
	var s State
	if err := j.root.CheckPath(j.dir); err != nil {
		return s, err
	}
	f, err := j.root.Open(j.id + ".json")
	if err != nil {
		return s, err
	}
	defer func() { _ = f.Close() }()
	if err = tracker.Check(); err != nil {
		return s, err
	}
	readLimit := int64(16385)
	if remaining := tracker.RemainingBytes(); remaining < readLimit {
		readLimit = max(1, remaining+1)
	}
	data, err := io.ReadAll(io.LimitReader(f, readLimit))
	if err != nil {
		return s, err
	}
	if err = tracker.Bytes(int64(len(data))); err != nil {
		return s, err
	}
	if len(data) > 16384 {
		return s, &model.BudgetError{Limit: "journal bytes"}
	}
	if _, err = jsonl.Decode(data, tracker); err != nil {
		return s, err
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err = d.Decode(&s); err != nil {
		return s, &model.IntegrityError{Component: "journal", Reason: "invalid schema"}
	}
	var extra any
	if err = d.Decode(&extra); err != io.EOF {
		return s, fmt.Errorf("invalid journal tail")
	}
	return s, nil
}
func (j *Journal) Write(s State, tracker *budget.Tracker) error {
	if err := j.root.CheckPath(j.dir); err != nil {
		return err
	}
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	if err = tracker.Temp(int64(len(data) + 1)); err != nil {
		return err
	}
	if err = j.root.WriteAtomic(j.id+".json", append(data, '\n')); err != nil {
		return err
	}
	return j.root.CheckPath(j.dir)
}
