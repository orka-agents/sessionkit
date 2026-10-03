//go:build native

package codex_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

// These event shapes follow Codex a956835d core/tests/common/responses.rs.
// Only a fixed echo command is executed while generating fixtures.
type responseTurn struct {
	items   []map[string]any
	entered chan struct{}
	release chan struct{}
}

type stubProvider struct {
	server   *httptest.Server
	mu       sync.Mutex
	turns    []responseTurn
	requests []map[string]any
	errors   []string
}

func newStub(t *testing.T) *stubProvider {
	t.Helper()
	s := &stubProvider{}
	s.server = httptest.NewServer(http.HandlerFunc(s.serve))
	t.Cleanup(func() {
		s.server.Close()
		s.mu.Lock()
		defer s.mu.Unlock()
		for _, err := range s.errors {
			t.Error(err)
		}
		if len(s.turns) != 0 {
			t.Errorf("stub has %d unused responses", len(s.turns))
		}
	})
	return s
}

func assistant(text string) responseTurn {
	return responseTurn{items: []map[string]any{{"type": "message", "role": "assistant", "id": "message-sessionkit", "content": []any{map[string]any{"type": "output_text", "text": text}}}}}
}

func (s *stubProvider) enqueue(turns ...responseTurn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.turns = append(s.turns, turns...)
}

func (s *stubProvider) recorded() []map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]map[string]any(nil), s.requests...)
}

func (s *stubProvider) serve(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" || r.URL.Path != "/v1/responses" {
		http.Error(w, "only POST /v1/responses is implemented", http.StatusNotFound)
		return
	}
	var body map[string]any
	if err := json.NewDecoder(io.LimitReader(r.Body, 16<<20)).Decode(&body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	s.requests = append(s.requests, body)
	id := fmt.Sprintf("sessionkit-response-%d", len(s.requests))
	if len(s.turns) == 0 {
		s.errors = append(s.errors, "unexpected Responses request")
		s.mu.Unlock()
		http.Error(w, "no scripted response", http.StatusInternalServerError)
		return
	}
	turn := s.turns[0]
	s.turns = s.turns[1:]
	s.mu.Unlock()
	if turn.entered != nil {
		close(turn.entered)
	}
	if turn.release != nil {
		select {
		case <-turn.release:
		case <-r.Context().Done():
			return
		}
	}
	w.Header().Set("Content-Type", "text/event-stream")
	writeEvent := func(v map[string]any) {
		b, _ := json.Marshal(v)
		_, _ = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", v["type"], b)
	}
	writeEvent(map[string]any{"type": "response.created", "response": map[string]any{"id": id}})
	for _, item := range turn.items {
		writeEvent(map[string]any{"type": "response.output_item.added", "output_index": 0, "item": item})
		writeEvent(map[string]any{"type": "response.output_item.done", "output_index": 0, "item": item})
	}
	writeEvent(map[string]any{"type": "response.completed", "response": map[string]any{"id": id, "usage": map[string]any{"input_tokens": 10, "output_tokens": 10, "total_tokens": 20}}})
}
