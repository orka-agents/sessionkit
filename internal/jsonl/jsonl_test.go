package jsonl

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/orka-agents/sessionkit/internal/budget"
	"github.com/orka-agents/sessionkit/internal/model"
)

func TestDecodeStrictAndBounded(t *testing.T) {
	cases := []struct {
		name, data, code, limit string
		limits                  model.Budget
	}{
		{name: "duplicate", data: `{"x":1,"x":2}`, code: "duplicate_key"},
		{name: "escaped duplicate", data: `{"x":1,"\u0078":2}`, code: "duplicate_key"},
		{name: "nested duplicate", data: `{"x":{"y":null,"y":2}}`, code: "duplicate_key"},
		{name: "invalid UTF8", data: "{\"x\":\"\xff\"}", code: "invalid_utf8"},
		{name: "unpaired high surrogate", data: `{"x":"\ud800"}`, code: "invalid_unicode"},
		{name: "unpaired low surrogate", data: `{"x":"\udc00"}`, code: "invalid_unicode"},
		{name: "invalid surrogate pair", data: `{"x":"\ud800\u0020"}`, code: "invalid_unicode"},
		{name: "truncated", data: `{"x":`, code: "invalid_json"},
		{name: "two values", data: `{} {}`, code: "invalid_json"},
		{name: "array", data: `[]`, code: "invalid_record"},
		{name: "depth", data: `{"x":[{}]}`, limit: "depth", limits: model.Budget{MaxDepth: 2}},
		{name: "nodes", data: `{"x":1}`, limit: "nodes", limits: model.Budget{MaxNodes: 2}},
		{name: "line", data: `{"x":1}`, limit: "line_bytes", limits: model.Budget{MaxLineBytes: 2}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Decode([]byte(tc.data), budget.New(context.Background(), tc.limits))
			if tc.limit != "" {
				var e *model.BudgetError
				if !errors.As(err, &e) || e.Limit != tc.limit {
					t.Fatalf("want %s budget, got %v", tc.limit, err)
				}
			} else {
				var e *Error
				if !errors.As(err, &e) || e.Code != tc.code {
					t.Fatalf("want %s, got %v", tc.code, err)
				}
			}
		})
	}
	object, err := Decode([]byte(`{"large":9007199254740993,"decimal":1.234567890123456789,"nested":{"x":true},"other":{"x":false}}`), budget.New(context.Background(), model.Budget{}))
	if err != nil {
		t.Fatal(err)
	}
	if object["large"] != json.Number("9007199254740993") || object["decimal"] != json.Number("1.234567890123456789") {
		t.Fatalf("numbers changed: %#v", object)
	}
	object, err = Decode([]byte(`{"paired":"\ud83d\ude00","escaped":"\\ud800","quote":"\""}`), budget.New(context.Background(), model.Budget{}))
	if err != nil {
		t.Fatal(err)
	}
	if object["paired"] != "😀" || object["escaped"] != `\ud800` || object["quote"] != `"` {
		t.Fatalf("Unicode decoding: %#v", object)
	}
}

func TestReaderBoundaries(t *testing.T) {
	cases := []struct {
		name, data  string
		limits      model.Budget
		lines       int
		code, limit string
	}{
		{name: "empty", data: ""},
		{name: "complete", data: "{}\n{}\n", lines: 2},
		{name: "CRLF", data: "{}\r\n", lines: 1},
		{name: "tail", data: "{}", code: "incomplete_tail"},
		{name: "tail after record", data: "{}\n{}", lines: 1, code: "incomplete_tail"},
		{name: "exact line", data: "{}\n", limits: model.Budget{MaxLineBytes: 2}, lines: 1},
		{name: "large line", data: "{}\n", limits: model.Budget{MaxLineBytes: 1}, limit: "line_bytes"},
		{name: "multi buffer line", data: strings.Repeat("x", 64001) + "\n", limits: model.Budget{MaxLineBytes: 64000}, limit: "line_bytes"},
		{name: "records", data: "{}\n{}\n", limits: model.Budget{MaxRecords: 1}, lines: 1, limit: "records"},
		{name: "exact bytes", data: "{}\n", limits: model.Budget{MaxBytes: 3}, lines: 1},
		{name: "bytes", data: "{}\n", limits: model.Budget{MaxBytes: 2}, limit: "bytes"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reader := New(strings.NewReader(tc.data), budget.New(context.Background(), tc.limits))
			count := 0
			var err error
			for {
				_, err = reader.Next()
				if err != nil {
					break
				}
				count++
			}
			if count != tc.lines {
				t.Fatalf("lines %d want %d", count, tc.lines)
			}
			if tc.limit != "" {
				var e *model.BudgetError
				if !errors.As(err, &e) || e.Limit != tc.limit {
					t.Fatalf("want %s budget, got %v", tc.limit, err)
				}
			} else if tc.code != "" {
				var e *Error
				if !errors.As(err, &e) || e.Code != tc.code {
					t.Fatalf("want %s, got %v", tc.code, err)
				}
			} else if !errors.Is(err, io.EOF) {
				t.Fatal(err)
			}
		})
	}
}

type countRead struct {
	reader io.Reader
	n      int
}

func (r *countRead) Read(p []byte) (int, error) { n, err := r.reader.Read(p); r.n += n; return n, err }

func TestLimitsStopReadingBeforeLargeAllocation(t *testing.T) {
	input := &countRead{reader: strings.NewReader("{}\n" + strings.Repeat("x", 1<<20) + "\n")}
	reader := New(input, budget.New(context.Background(), model.Budget{MaxRecords: 1}))
	if _, err := reader.Next(); err != nil {
		t.Fatal(err)
	}
	var exceeded *model.BudgetError
	if _, err := reader.Next(); !errors.As(err, &exceeded) || exceeded.Limit != "records" {
		t.Fatalf("got %v", err)
	}
	if input.n > 32<<10 {
		t.Fatalf("read %d bytes after record budget exhausted", input.n)
	}
	input = &countRead{reader: strings.NewReader(strings.Repeat("x", 1<<20))}
	reader = New(input, budget.New(context.Background(), model.Budget{MaxBytes: 8}))
	if _, err := reader.Next(); !errors.As(err, &exceeded) {
		t.Fatalf("got %v", err)
	}
	if input.n != 9 {
		t.Fatalf("byte budget read %d bytes", input.n)
	}
}
