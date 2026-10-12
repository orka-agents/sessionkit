// Package bundle handles bounded manifest serialization. Rollout interpretation
// belongs to the selected adapter.
package bundle

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	"github.com/orka-agents/sessionkit/internal/budget"
	"github.com/orka-agents/sessionkit/internal/fsx"
	"github.com/orka-agents/sessionkit/internal/model"
)

const ManifestLimit = 1 << 20
const RolloutPath = "components/rollout.jsonl"

func Read(root *fsx.Root, tracker *budget.Tracker) ([]byte, error) {
	f, err := root.Open("manifest.json")
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if st.Size() > ManifestLimit {
		return nil, &model.BudgetError{Limit: "manifest bytes"}
	}
	var out bytes.Buffer
	bufferSize := int64(4096)
	if remaining := tracker.RemainingBytes(); remaining < bufferSize {
		bufferSize = max(1, remaining+1)
	}
	buf := make([]byte, int(bufferSize))
	for {
		if err = tracker.Check(); err != nil {
			return nil, err
		}
		readBuf := buf
		if remaining := tracker.RemainingBytes(); remaining < int64(len(readBuf)) {
			readBuf = readBuf[:max(0, remaining)+1]
		}
		n, e := f.Read(readBuf)
		if int64(out.Len()+n) > ManifestLimit {
			return nil, &model.BudgetError{Limit: "manifest bytes"}
		}
		if err = tracker.Bytes(int64(n)); err != nil {
			return nil, err
		}
		out.Write(buf[:n])
		if e == io.EOF {
			break
		}
		if e != nil {
			return nil, e
		}
	}
	return out.Bytes(), nil
}

func Encode(value any) ([]byte, error) {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return nil, err
	}
	if len(data)+1 > ManifestLimit {
		return nil, fmt.Errorf("manifest exceeds size limit")
	}
	return append(data, '\n'), nil
}

func Write(root *fsx.Root, data []byte) error {
	f, err := root.Create("manifest.json")
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	e := f.Close()
	if err == nil {
		err = e
	}
	if err != nil {
		return err
	}
	return root.SyncDir(".")
}
