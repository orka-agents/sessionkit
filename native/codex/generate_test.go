//go:build native

package codex_test

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

const fixtureNonce = "SESSIONKIT_NONCE_7a8381a91bcf"
const fixtureCallID = "sessionkit-call-001"
const fixturePrompt = "Run the shell command to print the SessionKit nonce, then acknowledge it."
const fixtureReply = "SessionKit original reply: nonce received."
const compactionSummary = "SESSIONKIT_COMPACTION_SUMMARY_475b13: the shell printed the nonce and the prior task finished."

func TestGenerateFixtures(t *testing.T) {
	if os.Getenv("SESSIONKIT_UPDATE_FIXTURES") != "1" {
		t.Skip("set SESSIONKIT_UPDATE_FIXTURES=1 only to regenerate pinned-client fixtures")
	}
	s := newStub(t)
	home := tempDir(t)
	cwd := tempDir(t)
	configHome(t, home, s)
	args := `{"cmd":"printf 'SESSIONKIT_NONCE_7a8381a91bcf\\n'","yield_time_ms":1000,"max_output_tokens":100}`
	s.enqueue(responseTurn{items: []map[string]any{{"type": "function_call", "call_id": fixtureCallID, "name": "exec_command", "arguments": args}}}, assistant(fixtureReply))
	a := newApp(t, home, cwd)
	started := a.call("thread/start", map[string]any{"cwd": cwd, "runtimeWorkspaceRoots": []string{cwd}, "approvalPolicy": "never", "approvalsReviewer": "user", "sandbox": "danger-full-access", "modelProvider": "stub", "baseInstructions": "You are generating a SessionKit compatibility fixture. Use the shell tool once as instructed."})
	thread := started["thread"].(map[string]any)
	id := thread["id"].(string)
	a.call("turn/start", map[string]any{"threadId": id, "input": []any{map[string]any{"type": "text", "text": fixturePrompt}}})
	a.waitTurn()
	path := findRollout(t, home)
	// Read after a native unload so all rollout buffers have flushed.
	a.call("thread/unsubscribe", map[string]any{"threadId": id})
	a.close()
	basic := read(t, path)
	validOutput := false
	for _, line := range decodeLines(t, basic) {
		p, _ := line["payload"].(map[string]any)
		if line["type"] == "response_item" && p["type"] == "function_call_output" {
			output, _ := p["output"].(string)
			validOutput = bytes.Contains([]byte(output), []byte(fixtureNonce)) && bytes.Contains([]byte(output), []byte("Process exited with code 0"))
		}
	}
	if !validOutput {
		t.Fatal("native tool output did not contain a successful nonce command")
	}
	writeFixture(t, "basic", basic, cwd, path, home)

	s.enqueue(assistant(compactionSummary))
	a = newApp(t, home, cwd)
	a.call("thread/resume", map[string]any{"threadId": id, "cwd": cwd, "runtimeWorkspaceRoots": []string{cwd}, "approvalPolicy": "never", "approvalsReviewer": "user", "sandbox": "danger-full-access", "modelProvider": "stub"})
	a.call("thread/compact/start", map[string]any{"threadId": id})
	a.waitTurn()
	a.call("thread/unsubscribe", map[string]any{"threadId": id})
	a.close()
	compacted := read(t, path)
	complete := false
	for _, line := range decodeLines(t, compacted) {
		if line["type"] == "compacted" {
			p := line["payload"].(map[string]any)
			complete = p["replacement_history"] != nil && p["window_number"] != nil
		}
	}
	if !complete {
		t.Fatal("native client did not write a complete compaction")
	}
	writeFixture(t, "compacted", compacted, cwd, path, home)
}

func writeFixture(t *testing.T, name string, b []byte, cwd, path, home string) {
	t.Helper()
	// Sanitization is confined to reproducible workspace paths and git metadata.
	// The source session itself is never changed.
	b = bytes.ReplaceAll(b, []byte(cwd), []byte("/sessionkit/source-workspace"))
	b = bytes.ReplaceAll(b, []byte(home), []byte("/sessionkit/source-home"))
	var out bytes.Buffer
	for _, line := range decodeLines(t, b) {
		if line["type"] == "session_meta" {
			delete(line["payload"].(map[string]any), "git")
		}
		encoded, err := json.Marshal(line)
		must(t, err)
		out.Write(encoded)
		out.WriteByte('\n')
	}
	rel, err := filepath.Rel(home, path)
	must(t, err)
	dir := filepath.Join("..", "..", "harness", "codex", "testdata", name)
	previous, err := filepath.Glob(filepath.Join(dir, "sessions", "*", "*", "*", "rollout-*.jsonl"))
	must(t, err)
	for _, previousPath := range previous {
		must(t, os.Remove(previousPath))
	}
	must(t, os.MkdirAll(filepath.Join(dir, filepath.Dir(rel)), 0700))
	must(t, os.WriteFile(filepath.Join(dir, rel), out.Bytes(), 0600))
	t.Logf("wrote %s/%s (%d bytes)", name, rel, out.Len())
}
