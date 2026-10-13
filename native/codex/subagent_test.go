//go:build native

package codex_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orka-agents/sessionkit"
)

const subagentParentPrompt = "SESSIONKIT_SUBAGENT_PARENT_PROMPT_41c2: delegate to one child, wait for it, then close it."
const subagentChildPrompt = "SESSIONKIT_SUBAGENT_CHILD_PROMPT_8e07: answer the parent."
const subagentChildReply = "SESSIONKIT_SUBAGENT_CHILD_REPLY_d3a9"
const subagentParentReply = "SESSIONKIT_SUBAGENT_PARENT_REPLY_6b15"
const subagentOmitted = "subagent threads (1)"

const spawnCallID = "sessionkit-spawn-001"
const waitCallID = "sessionkit-wait-001"
const closeCallID = "sessionkit-close-001"
const sendCallID = "sessionkit-send-001"

// Both pinned clients enable the stable multi_agent feature by default and
// expose its v1 tools in this namespace.
const multiAgentNamespace = "multi_agent_v1"

// latest holds a value that a stub response derives from a native request.
type latest struct {
	mu    sync.Mutex
	value string
}

func (l *latest) get() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.value
}

func (l *latest) set(value string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.value = value
}

func agentCall(callID, name string, arguments map[string]any) map[string]any {
	encoded, _ := json.Marshal(arguments)
	return map[string]any{"type": "function_call", "call_id": callID, "namespace": multiAgentNamespace, "name": name, "arguments": string(encoded)}
}

func requestText(request map[string]any) string {
	encoded, _ := json.Marshal(request["input"])
	return string(encoded)
}

// lastOutput returns the call ID and text of the newest tool output in a request.
func lastOutput(request map[string]any) (string, string) {
	input, _ := request["input"].([]any)
	for i := len(input) - 1; i >= 0; i-- {
		item, _ := input[i].(map[string]any)
		if item["type"] == "function_call_output" {
			callID, _ := item["call_id"].(string)
			output, _ := item["output"].(string)
			return callID, output
		}
	}
	return "", ""
}

// afterOutput routes a parent request by the prompt and newest tool output.
func afterOutput(prompt, callID string) func(map[string]any) bool {
	return func(request map[string]any) bool {
		last, _ := lastOutput(request)
		return strings.Contains(requestText(request), prompt) && last == callID
	}
}

func isChildRequest(request map[string]any) bool {
	text := requestText(request)
	return strings.Contains(text, subagentChildPrompt) && !strings.Contains(text, subagentParentPrompt)
}

// spawnParent runs one native parent turn that spawns a child, which answers.
// With closeChild the parent waits for the child and closes it; otherwise the
// parent finishes its turn while the child stays open.
//
// A child's completion injects a <subagent_notification> into its parent. In a
// running turn that becomes pending input for the next request, so the script
// orders it deterministically: the closing parent continues only after the
// child's turn is persisted, and the open child answers only after the parent's
// turn ends, when the notification is recorded without a new turn.
func spawnParent(t *testing.T, s *stubProvider, home, cwd string, closeChild bool) (id, child, path string) {
	t.Helper()
	spawned := &latest{}
	childID := func(request map[string]any) string {
		_, output := lastOutput(request)
		var result struct {
			AgentID string `json:"agent_id"`
		}
		_ = json.Unmarshal([]byte(output), &result)
		spawned.set(result.AgentID)
		return result.AgentID
	}
	childTurn := assistant(subagentChildReply)
	childTurn.match = isChildRequest
	turns := []responseTurn{{match: afterOutput(subagentParentPrompt, ""), items: []map[string]any{agentCall(spawnCallID, "spawn_agent", map[string]any{"message": subagentChildPrompt})}}}
	if closeChild {
		turns = append(turns, childTurn,
			responseTurn{match: afterOutput(subagentParentPrompt, spawnCallID), build: func(request map[string]any) []map[string]any {
				child := childID(request)
				if !waitForRollout(home, child, `"type":"task_complete"`) {
					t.Error("subagent turn did not complete")
				}
				return []map[string]any{agentCall(waitCallID, "wait_agent", map[string]any{"targets": []string{child}, "timeout_ms": 30000})}
			}},
			responseTurn{match: afterOutput(subagentParentPrompt, waitCallID), build: func(map[string]any) []map[string]any {
				return []map[string]any{agentCall(closeCallID, "close_agent", map[string]any{"target": spawned.get()})}
			}},
			responseTurn{match: afterOutput(subagentParentPrompt, closeCallID), items: assistant(subagentParentReply).items},
		)
	} else {
		childTurn.entered, childTurn.release = make(chan struct{}), make(chan struct{})
		turns = append(turns, childTurn, responseTurn{match: afterOutput(subagentParentPrompt, spawnCallID), build: func(request map[string]any) []map[string]any {
			childID(request)
			return assistant(subagentParentReply).items
		}})
	}
	s.enqueue(turns...)
	a := newApp(t, home, cwd)
	started := a.call("thread/start", map[string]any{"cwd": cwd, "runtimeWorkspaceRoots": []string{cwd}, "approvalPolicy": "never", "approvalsReviewer": "user", "sandbox": "danger-full-access", "modelProvider": "stub"})
	id = started["thread"].(map[string]any)["id"].(string)
	a.waitThreadTurn(id, a.startTurn(id, subagentParentPrompt), "completed")
	child = spawned.get()
	if child == "" {
		t.Fatal("native spawn_agent output named no child")
	}
	if !closeChild {
		select {
		case <-childTurn.entered:
		case <-time.After(30 * time.Second):
			t.Fatal("subagent never reached the provider")
		}
		close(childTurn.release)
		if !waitForRollout(home, id, "<subagent_notification>") {
			t.Fatal("idle parent did not record the subagent notification")
		}
	}
	a.call("thread/unsubscribe", map[string]any{"threadId": id})
	a.close()
	// The child is a real native thread with its own rollout in this home.
	for _, thread := range []string{id, child} {
		matches, err := filepath.Glob(filepath.Join(home, "sessions", "*", "*", "*", "rollout-*-"+thread+".jsonl"))
		must(t, err)
		if len(matches) != 1 {
			t.Fatalf("rollouts for %s: %v", thread, matches)
		}
		if thread == id {
			path = matches[0]
		}
	}
	return id, child, path
}

// waitForRollout polls a live thread's rollout for natively persisted text.
func waitForRollout(home, thread, needle string) bool {
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		matches, _ := filepath.Glob(filepath.Join(home, "sessions", "*", "*", "*", "rollout-*-"+thread+".jsonl"))
		for _, match := range matches {
			if b, err := os.ReadFile(match); err == nil && bytes.Contains(b, []byte(needle)) {
				return true
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

func (a *appClient) startTurn(threadID, prompt string) string {
	a.t.Helper()
	started := a.call("turn/start", map[string]any{"threadId": threadID, "input": []any{map[string]any{"type": "text", "text": prompt}}})
	turn, _ := started["turn"].(map[string]any)
	turnID, _ := turn["id"].(string)
	if turnID == "" {
		a.t.Fatalf("turn/start returned no turn: %v", started)
	}
	return turnID
}

// waitThreadTurn skips completions of other threads, such as a subagent's.
func (a *appClient) waitThreadTurn(threadID, turnID, status string) {
	a.t.Helper()
	for {
		msg := a.next()
		params, _ := msg["params"].(map[string]any)
		if msg["method"] != "turn/completed" || params["threadId"] != threadID {
			continue
		}
		turn, _ := params["turn"].(map[string]any)
		if turn["id"] != turnID || turn["status"] != status {
			a.t.Fatalf("turn completion = %v %v, want %s %s", turn["id"], turn["status"], turnID, status)
		}
		return
	}
}

// agentItems returns the persisted CollabAgentToolCall items by tool tag.
func agentItems(t *testing.T, rollout []byte) map[string][]map[string]any {
	t.Helper()
	items := map[string][]map[string]any{}
	for _, line := range decodeLines(t, rollout) {
		p, _ := line["payload"].(map[string]any)
		item, _ := p["item"].(map[string]any)
		if line["type"] == "event_msg" && p["type"] == "item_completed" && item["type"] == "CollabAgentToolCall" {
			item["ordinal"] = line["ordinal"]
			items[item["tool"].(string)] = append(items[item["tool"].(string)], item)
		}
	}
	return items
}

func TestNativeSubagentParentHandoff(t *testing.T) {
	ctx := context.Background()
	s := newStub(t)
	homeA, workspaceA := tempDir(t), tempDir(t)
	configHome(t, homeA, s)
	id, child, sourcePath := spawnParent(t, s, homeA, workspaceA, true)
	original := read(t, sourcePath)
	items := agentItems(t, original)
	for _, tool := range []string{"spawn_agent", "wait", "close_agent"} {
		if len(items[tool]) != 1 || items[tool][0]["status"] != "completed" || !slices.Equal(items[tool][0]["receiver_thread_ids"].([]any), []any{child}) {
			t.Fatalf("native %s item: %v", tool, items[tool])
		}
	}
	preserveSource(t, sourcePath, original)

	bundle, err := sessionkit.Capture(ctx, sessionkit.Source{Harness: sessionkit.Codex, Root: homeA, ThreadID: id}, sessionkit.CaptureOptions{BundleDir: filepath.Join(tempDir(t), "bundle")})
	must(t, err)
	if !slices.Contains(bundle.Manifest.Omitted, subagentOmitted) {
		t.Fatalf("manifest omissions: %v", bundle.Manifest.Omitted)
	}
	opened, err := sessionkit.OpenBundle(ctx, bundle.Dir, sessionkit.Budget{})
	must(t, err)
	homeB, workspaceB := tempDir(t), tempDir(t)
	configHome(t, homeB, s)
	plan, installedPath := installBundle(t, opened, destination(t, homeB, workspaceB))
	if !slices.Contains(plan.Omitted, subagentOmitted) {
		t.Fatalf("plan omissions: %v", plan.Omitted)
	}
	if !bytes.Equal(read(t, installedPath), original) {
		t.Fatal("install rewrote the parent rollout")
	}

	const resumePrompt = "SESSIONKIT_SUBAGENT_RESUME_PROMPT_2f7a: continue without the child."
	const resumeReply = "SESSIONKIT_SUBAGENT_RESUME_REPLY_90c4"
	const stalePrompt = "SESSIONKIT_SUBAGENT_STALE_PROMPT_c51e: message the old child."
	const staleReply = "SESSIONKIT_SUBAGENT_STALE_REPLY_5d88"
	staleOutput := &latest{}
	s.enqueue(
		responseTurn{match: afterOutput(resumePrompt, closeCallID), items: assistant(resumeReply).items},
		responseTurn{match: afterOutput(stalePrompt, closeCallID), items: []map[string]any{agentCall(sendCallID, "send_input", map[string]any{"target": child, "message": "SESSIONKIT_SUBAGENT_STALE_INPUT"})}},
		responseTurn{match: afterOutput(stalePrompt, sendCallID), build: func(request map[string]any) []map[string]any {
			_, output := lastOutput(request)
			staleOutput.set(output)
			return assistant(staleReply).items
		}},
	)
	before := len(s.recorded())
	a := newApp(t, homeB, workspaceB)
	params := map[string]any{}
	for key, value := range plan.ResumeHints.AppServerParams {
		params[key] = value
	}
	params["modelProvider"] = "stub"
	params["approvalPolicy"] = "never"
	params["approvalsReviewer"] = "user"
	params["sandbox"] = "danger-full-access"
	assertEffectiveSettings(t, a.call("thread/resume", params), workspaceB)
	a.waitThreadTurn(id, a.startTurn(id, resumePrompt), "completed")
	a.waitThreadTurn(id, a.startTurn(id, stalePrompt), "completed")
	a.close()

	requests := s.recorded()[before:]
	if len(requests) != 3 {
		t.Fatalf("resumed parent made %d provider requests, want 3", len(requests))
	}
	var expected []map[string]any
	for _, line := range decodeLines(t, original) {
		p, _ := line["payload"].(map[string]any)
		if line["type"] != "response_item" {
			continue
		}
		if p["type"] == "function_call" || p["type"] == "function_call_output" || p["role"] == "assistant" || p["role"] == "user" && strings.Contains(string(marshal(t, p)), subagentParentPrompt) {
			expected = append(expected, p)
		}
	}
	var calls []string
	for _, item := range expected {
		if item["type"] == "function_call" {
			calls = append(calls, item["name"].(string)+"/"+item["call_id"].(string))
		}
	}
	if len(expected) != 8 || !slices.Equal(calls, []string{"spawn_agent/" + spawnCallID, "wait_agent/" + waitCallID, "close_agent/" + closeCallID}) {
		t.Fatalf("parent history must have user, three agent tool pairs, and assistant; got %d %v", len(expected), calls)
	}
	expected = append(expected, map[string]any{"type": "message", "role": "user", "content": []any{map[string]any{"type": "input_text", "text": resumePrompt}}})
	assertHistoryItems(t, requests[0], expected)
	t.Logf("stale send_input output: %s", staleOutput.get())
	// The destination has no child: a stale ID is a model-visible tool error.
	if want := "agent with id " + child + " not found"; !strings.Contains(staleOutput.get(), want) {
		t.Fatalf("stale send_input output = %q, want %q", staleOutput.get(), want)
	}
	installed := read(t, installedPath)
	assertAppended(t, installed, original, resumeReply, workspaceB, true)
	if !bytes.Contains(installed[len(original):], []byte(staleReply)) {
		t.Fatal("reply after the stale tool error was not persisted")
	}
	assertStateRow(t, homeB, id, installedPath)
	if findRollout(t, homeB) != installedPath {
		t.Fatal("destination created a child rollout")
	}

	// The failed send_input item is accepted: the parent still closes its child.
	failed := agentItems(t, installed[len(original):])["send_input"]
	if len(failed) != 1 || failed[0]["status"] != "failed" {
		t.Fatalf("native stale send_input item: %v", failed)
	}
	again, err := sessionkit.Capture(ctx, sessionkit.Source{Harness: sessionkit.Codex, Root: homeB, ThreadID: id}, sessionkit.CaptureOptions{BundleDir: filepath.Join(tempDir(t), "again")})
	must(t, err)
	if !slices.Contains(again.Manifest.Omitted, subagentOmitted) {
		t.Fatalf("recaptured omissions: %v", again.Manifest.Omitted)
	}
}

func TestNativeSubagentOpenRejected(t *testing.T) {
	s := newStub(t)
	home, workspace := tempDir(t), tempDir(t)
	configHome(t, home, s)
	id, child, path := spawnParent(t, s, home, workspace, false)
	stopped := read(t, path)
	items := agentItems(t, stopped)
	if len(items["spawn_agent"]) != 1 || len(items["close_agent"]) != 0 || !slices.Equal(items["spawn_agent"][0]["receiver_thread_ids"].([]any), []any{child}) {
		t.Fatalf("native open-child items: %v", items)
	}
	spawnOrdinal := uint64(items["spawn_agent"][0]["ordinal"].(float64))
	preserveSource(t, path, stopped)
	requests := len(s.recorded())
	bundleDir := filepath.Join(tempDir(t), "rejected-bundle")
	bundle, err := sessionkit.Capture(context.Background(), sessionkit.Source{Harness: sessionkit.Codex, Root: home, ThreadID: id}, sessionkit.CaptureOptions{BundleDir: bundleDir})
	var rejected *sessionkit.RejectionError
	if !errors.As(err, &rejected) || len(rejected.Rejections) != 1 || rejected.Rejections[0].Code != "open_subagent" || rejected.Rejections[0].Ordinal != spawnOrdinal {
		t.Fatalf("open child was not rejected at spawn ordinal %d: %v", spawnOrdinal, err)
	}
	if bundle.Dir != "" {
		t.Fatal("rejected capture returned a bundle")
	}
	if _, err := os.Stat(bundleDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("rejected capture published a bundle: %v", err)
	}
	if len(s.recorded()) != requests {
		t.Fatal("open-child rejection contacted the provider")
	}
}
