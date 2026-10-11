//go:build native

package codex_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/orka-agents/sessionkit"
)

const interruptedPrompt = "SESSIONKIT_INTERRUPTED_PROMPT_23dc: continue until interrupted."
const undeliveredReply = "SESSIONKIT_UNDELIVERED_REPLY_982a"

func TestNativeInterruptedHandoff(t *testing.T) {
	s := newStub(t)
	home, workspace, path, original, id := fixtureHome(t, "basic", s)
	a, turnID := blockedTurn(t, home, workspace, id, s)
	a.call("turn/interrupt", map[string]any{"threadId": id, "turnId": turnID})
	a.waitTurnStatus(id, turnID, "interrupted")
	// Native unload flushes the rollout; capture must only see the stopped source.
	a.call("thread/unsubscribe", map[string]any{"threadId": id})
	a.close()
	stopped := read(t, path)
	if !bytes.HasPrefix(stopped, original) {
		t.Fatal("interruption rewrote the original rollout")
	}
	assertTurnLifecycle(t, stopped, turnID, "turn_aborted")
	preserveSource(t, path, stopped)

	ctx := context.Background()
	source := sessionkit.Source{Harness: sessionkit.Codex, Root: home, ThreadID: id}
	bundle, err := sessionkit.Capture(ctx, source, sessionkit.CaptureOptions{BundleDir: filepath.Join(tempDir(t), "bundle")})
	must(t, err)
	opened, err := sessionkit.OpenBundle(ctx, bundle.Dir, sessionkit.Budget{})
	must(t, err)
	destinationHome, destinationWorkspace := tempDir(t), tempDir(t)
	configHome(t, destinationHome, s)
	plan, installedPath := installBundle(t, opened, destination(t, destinationHome, destinationWorkspace))
	if !bytes.Equal(read(t, installedPath), stopped) {
		t.Fatal("install rewrote the interrupted rollout")
	}

	const reply = "SESSIONKIT_REPLY_AFTER_INTERRUPT_b613"
	const prompt = "SESSIONKIT_RESUME_AFTER_INTERRUPT: continue in the destination."
	s.enqueue(assistant(reply))
	resumed := newApp(t, destinationHome, destinationWorkspace)
	params := map[string]any{}
	for key, value := range plan.ResumeHints.AppServerParams {
		params[key] = value
	}
	params["modelProvider"] = "stub"
	params["approvalPolicy"] = "never"
	params["approvalsReviewer"] = "user"
	params["sandbox"] = "danger-full-access"
	assertEffectiveSettings(t, resumed.call("thread/resume", params), destinationWorkspace)
	resumed.call("turn/start", map[string]any{"threadId": id, "input": []any{map[string]any{"type": "text", "text": prompt}}})
	resumed.waitTurn()
	resumed.close()

	requests := s.recorded()
	if len(requests) != 2 {
		t.Fatalf("interruption and resume made %d requests, want 2", len(requests))
	}
	assertOriginalHistory(t, requests[0], original)
	var expected []map[string]any
	for _, line := range decodeLines(t, stopped) {
		p, _ := line["payload"].(map[string]any)
		if line["type"] != "response_item" {
			continue
		}
		if p["type"] == "function_call" || p["type"] == "function_call_output" || p["role"] == "assistant" || p["role"] == "user" && (strings.Contains(string(marshal(t, p)), fixturePrompt) || strings.Contains(string(marshal(t, p)), interruptedPrompt)) {
			expected = append(expected, p)
		}
	}
	if len(expected) != 5 {
		t.Fatalf("interrupted history must have user, call, output, assistant, interrupted user; got %d", len(expected))
	}
	expected = append(expected, map[string]any{"type": "message", "role": "user", "content": []any{map[string]any{"type": "input_text", "text": prompt}}})
	assertHistoryItems(t, requests[1], expected)
	if bytes.Contains(marshal(t, requests[1]), []byte(undeliveredReply)) || bytes.Contains(stopped, []byte(undeliveredReply)) {
		t.Fatal("interrupted provider reply was persisted or replayed")
	}
	assertAppended(t, read(t, installedPath), stopped, reply, destinationWorkspace, true)
	assertStateRow(t, destinationHome, id, installedPath)
}

func TestNativeKilledTurnRejected(t *testing.T) {
	s := newStub(t)
	home, workspace, path, original, id := fixtureHome(t, "basic", s)
	a, turnID := blockedTurn(t, home, workspace, id, s)
	// Wait for native persistence, not merely turn/start acknowledgement. SIGKILL
	// must leave a real unfinished turn, without an unload or synthesized abort.
	deadline := time.After(10 * time.Second)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for !hasTurnStart(t, read(t, path), turnID) {
		select {
		case <-deadline:
			t.Fatal("native client did not persist the blocked turn before SIGKILL")
		case <-ticker.C:
		}
	}
	a.kill()
	stopped := read(t, path)
	if !bytes.HasPrefix(stopped, original) {
		t.Fatal("killed client rewrote the original rollout")
	}
	startOrdinal := assertTurnLifecycle(t, stopped, turnID, "")
	preserveSource(t, path, stopped)
	bundleDir := filepath.Join(tempDir(t), "rejected-bundle")
	bundle, err := sessionkit.Capture(context.Background(), sessionkit.Source{Harness: sessionkit.Codex, Root: home, ThreadID: id}, sessionkit.CaptureOptions{BundleDir: bundleDir})
	var rejected *sessionkit.RejectionError
	if !errors.As(err, &rejected) || len(rejected.Rejections) != 1 || rejected.Rejections[0].Code != "in_flight_turn" || rejected.Rejections[0].Ordinal != startOrdinal {
		t.Fatalf("killed unfinished turn was not rejected at start ordinal %d: %v", startOrdinal, err)
	}
	if bundle.Dir != "" {
		t.Fatal("rejected capture returned a bundle")
	}
	if _, err := os.Stat(bundleDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("rejected capture published a bundle: %v", err)
	}
	if len(s.recorded()) != 1 {
		t.Fatal("unfinished-turn rejection contacted the provider")
	}
}

func blockedTurn(t *testing.T, home, workspace, id string, s *stubProvider) (*appClient, string) {
	t.Helper()
	entered, release := make(chan struct{}), make(chan struct{})
	turn := assistant(undeliveredReply)
	turn.entered, turn.release = entered, release
	s.enqueue(turn)
	t.Cleanup(func() { close(release) })
	a := newApp(t, home, workspace)
	a.call("thread/resume", map[string]any{"threadId": id, "cwd": workspace, "runtimeWorkspaceRoots": []string{workspace}, "approvalPolicy": "never", "approvalsReviewer": "user", "sandbox": "danger-full-access", "modelProvider": "stub"})
	started := a.call("turn/start", map[string]any{"threadId": id, "input": []any{map[string]any{"type": "text", "text": interruptedPrompt}}})
	active, _ := started["turn"].(map[string]any)
	turnID, _ := active["id"].(string)
	if turnID == "" || active["status"] != "inProgress" {
		t.Fatalf("turn/start did not return an active turn: %v", active)
	}
	select {
	case <-entered:
	case <-time.After(30 * time.Second):
		t.Fatal("client did not reach the blocked provider")
	}
	return a, turnID
}

func hasTurnStart(t *testing.T, rollout []byte, turnID string) bool {
	t.Helper()
	// A live writer can leave a partial last line while this test polls.
	end := bytes.LastIndexByte(rollout, '\n')
	if end < 0 {
		return false
	}
	for _, line := range decodeLines(t, rollout[:end+1]) {
		p, _ := line["payload"].(map[string]any)
		if line["type"] == "event_msg" && p["type"] == "task_started" && p["turn_id"] == turnID {
			return true
		}
	}
	return false
}

func assertTurnLifecycle(t *testing.T, rollout []byte, turnID, terminal string) uint64 {
	t.Helper()
	var kinds []string
	var startOrdinal uint64
	for _, line := range decodeLines(t, rollout) {
		p, _ := line["payload"].(map[string]any)
		if line["type"] != "event_msg" || p["turn_id"] != turnID {
			continue
		}
		switch p["type"] {
		case "task_started":
			startOrdinal = uint64(line["ordinal"].(float64))
			kinds = append(kinds, "task_started")
		case "task_complete", "turn_aborted":
			kinds = append(kinds, p["type"].(string))
			if p["type"] == "turn_aborted" && p["reason"] != "interrupted" {
				t.Fatalf("native abort reason = %v, want interrupted", p["reason"])
			}
		}
	}
	want := []string{"task_started"}
	if terminal != "" {
		want = append(want, terminal)
	}
	if !reflect.DeepEqual(kinds, want) {
		t.Fatalf("native turn lifecycle = %v, want %v", kinds, want)
	}
	return startOrdinal
}

func preserveSource(t *testing.T, path string, original []byte) {
	t.Helper()
	before, err := os.Stat(path)
	must(t, err)
	t.Cleanup(func() {
		if !bytes.Equal(read(t, path), original) {
			t.Error("handoff changed the stopped source")
		}
		after, err := os.Stat(path)
		must(t, err)
		if !before.ModTime().Equal(after.ModTime()) {
			t.Error("handoff changed the stopped source mtime")
		}
	})
}
