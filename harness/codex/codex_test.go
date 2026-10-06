package codex

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/orka-agents/sessionkit/internal/budget"
	"github.com/orka-agents/sessionkit/internal/fsx"
	"github.com/orka-agents/sessionkit/internal/model"
)

const testThread = "0195e76b-7c5e-7123-8123-456789abcdef"
const otherThread = "0195e76b-7c5e-7123-8123-456789abcdee"
const testPath = "sessions/2025/03/31/rollout-2025-04-01T00-30-00-" + testThread + ".jsonl"

func meta() map[string]any {
	return map[string]any{"id": testThread, "cli_version": "0.160.0", "history_mode": "paginated", "history_base": nil, "cwd": "/source/work", "source": "exec", "model_provider": "stub", "runtime_workspace_roots": []string{"/source/work"}}
}
func line(t *testing.T, ordinal uint64, kind string, payload any) string {
	t.Helper()
	data, err := json.Marshal(map[string]any{"timestamp": "2025-04-01T00:30:00Z", "ordinal": ordinal, "type": kind, "payload": payload})
	if err != nil {
		t.Fatal(err)
	}
	return string(data) + "\n"
}
func inspect(t *testing.T, data string) (model.Inspection, error) {
	t.Helper()
	return (Adapter{}).Inspect(context.Background(), strings.NewReader(data), testPath, budget.New(context.Background(), model.Budget{}))
}

func TestWarningTraversalStopsWhenCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tracker := budget.New(ctx, model.Budget{})
	warnings := 0
	err := walkWarnings([]any{
		map[string]any{"encrypted_content": "first"},
		map[string]any{"encrypted_content": "second"},
	}, 1, func(string, string, uint64) {
		warnings++
		cancel()
	}, tracker)
	if !errors.Is(err, context.Canceled) || warnings != 1 {
		t.Fatalf("traversal continued after cancellation: warnings=%d err=%v", warnings, err)
	}
	if _, _, err := stringList([]any{"/work"}, tracker); !errors.Is(err, context.Canceled) {
		t.Fatalf("workspace roots ignored cancellation: %v", err)
	}
	if _, err := toolOutput([]any{map[string]any{"type": "input_text", "text": "output"}}, tracker); !errors.Is(err, context.Canceled) {
		t.Fatalf("structured tool output ignored cancellation: %v", err)
	}
}

func TestInspectionPreservesNumbersAndSummarizesOwnedSettings(t *testing.T) {
	data := line(t, 0, "session_meta", meta()) +
		line(t, 1, "response_item", map[string]any{"type": "function_call", "call_id": "call-1", "name": "shell", "arguments": `{"command":"echo nonce"}`}) +
		line(t, 2, "response_item", map[string]any{"type": "function_call_output", "call_id": "call-1", "output": "nonce"}) +
		line(t, 4, "event_msg", map[string]any{"type": "thread_settings_applied", "thread_id": testThread, "thread_settings": map[string]any{"cwd": "/new/work", "runtime_workspace_roots": []string{"/new/work"}, "model_provider_id": "new-provider"}}) +
		line(t, 4, "event_msg", map[string]any{"type": "thread_settings_applied", "thread_id": otherThread, "thread_settings": map[string]any{"cwd": "/wrong/work"}}) +
		line(t, 5, "response_item", map[string]any{"type": "future_item", "decimal": json.Number("1.234567890123456789")}) +
		line(t, 9007199254740993, "future_record", map[string]any{"number": json.Number("9007199254740993")})
	got, err := inspect(t, data)
	if err != nil {
		t.Fatal(err)
	}
	if got.Profile != model.CodexPaginated || got.ThreadID != testThread || got.CLIVersion != "0.160.0" || got.HistoryMode != "paginated" {
		t.Fatalf("profile: %+v", got)
	}
	if got.RecordedCWD != "/source/work" || got.LatestCWD != "/new/work" || got.ModelProvider != "new-provider" || !reflect.DeepEqual(got.RuntimeWorkspaceRoots, []string{"/new/work"}) {
		t.Fatalf("settings: %+v", got)
	}
	if got.Records.Total != 7 || got.Records.ToolPairs != 1 || got.Records.OrdinalGaps != 2 || got.Records.LastOrdinal != 9007199254740993 || got.Records.ResponseItems["future_item"] != 1 || got.Records.ByType["future_record"] != 1 {
		t.Fatalf("records: %+v", got.Records)
	}
	digest := sha256.Sum256([]byte(data))
	if got.SourceDigest != hex.EncodeToString(digest[:]) || got.SourceSizeBytes != int64(len(data)) {
		t.Fatal("digest or byte count changed")
	}
	if !reflect.DeepEqual(got.Omitted, []string{"thread name", "git metadata", "memory mode"}) {
		t.Fatalf("omissions: %+v", got.Omitted)
	}
	rootWarnings := 0
	for _, warning := range got.Warnings {
		if warning.Code == "workspace_root_outside_cwd" {
			rootWarnings++
			if warning.Ordinal != 4 {
				t.Fatalf("workspace root warning must identify the settings change: %+v", warning)
			}
		}
	}
	if rootWarnings != 1 {
		t.Fatalf("expected one workspace root warning, got %d", rootWarnings)
	}
}

func TestCustomThreadSourcesPreserveCase(t *testing.T) {
	for _, source := range []string{"Guardian_Review", "Memory_Consolidation", "SUBAGENT"} {
		t.Run(source, func(t *testing.T) {
			metadata := meta()
			metadata["thread_source"] = source
			if _, err := inspect(t, line(t, 0, "session_meta", metadata)); err != nil {
				t.Fatalf("custom feature source was classified as a reserved source: %v", err)
			}
		})
	}
}

func TestProfileRejections(t *testing.T) {
	cases := []struct {
		name, code string
		mutate     func(map[string]any)
		suffix     string
	}{
		{name: "legacy", code: "history_mode", mutate: func(m map[string]any) { m["history_mode"] = "legacy" }},
		{name: "unknown mode", code: "history_mode", mutate: func(m map[string]any) { m["history_mode"] = "future" }},
		{name: "version", code: "cli_version", mutate: func(m map[string]any) { m["cli_version"] = "0.159.1" }},
		{name: "mismatched ID", code: "thread_id", mutate: func(m map[string]any) { m["id"] = otherThread }},
		{name: "different root session", code: "lineage", mutate: func(m map[string]any) { m["session_id"] = otherThread }},
		{name: "null root session", code: "lineage", mutate: func(m map[string]any) { m["session_id"] = nil }},
		{name: "history base", code: "lineage", mutate: func(m map[string]any) { m["history_base"] = map[string]any{"thread_id": otherThread} }},
		{name: "fork", code: "lineage", mutate: func(m map[string]any) { m["forked_from_id"] = otherThread }},
		{name: "fork ordinal zero", code: "lineage", mutate: func(m map[string]any) { m["forked_from_ordinal_exclusive"] = 0 }},
		{name: "parent", code: "lineage", mutate: func(m map[string]any) { m["parent_thread_id"] = otherThread }},
		{name: "subagent ordinal", code: "lineage", mutate: func(m map[string]any) { m["subagent_history_start_ordinal"] = 0 }},
		{name: "subagent object", code: "subagent", mutate: func(m map[string]any) {
			m["source"] = map[string]any{"subagent": map[string]any{"thread_spawn": map[string]any{"parent_thread_id": otherThread}}}
		}},
		{name: "subagent string", code: "subagent", mutate: func(m map[string]any) { m["source"] = "SubAgent" }},
		{name: "internal guardian", code: "lineage", mutate: func(m map[string]any) { m["source"] = map[string]any{"internal": "guardian"} }},
		{name: "internal memory consolidation", code: "lineage", mutate: func(m map[string]any) { m["source"] = map[string]any{"internal": "memory_consolidation"} }},
		{name: "subagent thread source", code: "subagent", mutate: func(m map[string]any) { m["thread_source"] = "subagent" }},
		{name: "guardian thread source", code: "lineage", mutate: func(m map[string]any) { m["thread_source"] = "guardian_review" }},
		{name: "memory consolidation thread source", code: "lineage", mutate: func(m map[string]any) { m["thread_source"] = "memory_consolidation" }},
		{name: "subagent nickname", code: "subagent", mutate: func(m map[string]any) { m["agent_nickname"] = "worker" }},
		{name: "subagent role", code: "subagent", mutate: func(m map[string]any) { m["agent_role"] = "worker" }},
		{name: "subagent role alias", code: "subagent", mutate: func(m map[string]any) { m["agent_type"] = "worker" }},
		{name: "subagent path", code: "subagent", mutate: func(m map[string]any) { m["agent_path"] = "/worker" }},
		{name: "inter-agent communication", code: "lineage", suffix: line(t, 1, "inter_agent_communication", map[string]any{})},
		{name: "inter-agent metadata", code: "lineage", suffix: line(t, 1, "inter_agent_communication_metadata", map[string]any{})},
		{name: "second metadata", code: "session_meta", suffix: line(t, 1, "session_meta", meta())},
		{name: "orphan output", code: "orphan_tool_output", suffix: line(t, 1, "response_item", map[string]any{"type": "function_call_output", "call_id": "absent", "output": "private body"})},
		{name: "orphan call", code: "orphan_tool_call", suffix: line(t, 1, "response_item", map[string]any{"type": "custom_tool_call", "call_id": "absent", "name": "test", "input": "private body"})},
		{name: "missing final ordinal", code: "ordinal", suffix: `{"type":"response_item","payload":{"type":"message"}}` + "\n"},
		{name: "fractional ordinal", code: "ordinal", suffix: `{"ordinal":1.5,"type":"future_record"}` + "\n"},
		{name: "duplicate", code: "duplicate_key", suffix: `{"ordinal":1,"type":"future_record","private key":0,"private key":1}` + "\n"},
		{name: "truncated tail", code: "incomplete_tail", suffix: `{"ordinal":1`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			metadata := meta()
			if tc.mutate != nil {
				tc.mutate(metadata)
			}
			got, err := inspect(t, line(t, 0, "session_meta", metadata)+tc.suffix)
			assertRejection(t, err, tc.code)
			if len(got.Rejections) != 1 || got.Rejections[0].Code != tc.code {
				t.Fatalf("inspection did not retain rejection: %+v", got.Rejections)
			}
			if strings.Contains(err.Error(), "private") {
				t.Fatal("error exposed record content")
			}
		})
	}
	_, err := inspect(t, line(t, 2, "session_meta", meta())+line(t, 1, "future", nil))
	assertRejection(t, err, "ordinal_order")
}

func TestRecordTimestamps(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value any
		valid bool
	}{
		{"missing", nil, false},
		{"null", nil, false},
		{"number", 1, false},
		{"boolean", true, false},
		{"object", map[string]any{}, false},
		{"empty string", "", true},
	} {
		for _, first := range []bool{true, false} {
			t.Run(tc.name+map[bool]string{true: "/first", false: "/later"}[first], func(t *testing.T) {
				record := map[string]any{"ordinal": 0, "timestamp": tc.value, "type": "session_meta", "payload": meta()}
				data := ""
				if !first {
					data = line(t, 0, "session_meta", meta())
					record["ordinal"], record["type"], record["payload"] = 1, "future_record", nil
				}
				if tc.name == "missing" {
					delete(record, "timestamp")
				}
				encoded, err := json.Marshal(record)
				if err != nil {
					t.Fatal(err)
				}
				_, err = inspect(t, data+string(encoded)+"\n")
				if !tc.valid {
					assertRejection(t, err, "timestamp")
				} else if err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestSubagentActivityRejectsAfterParentCompletes(t *testing.T) {
	events := []map[string]any{{"type": "sub_agent_activity", "kind": "started", "agent_thread_id": otherThread}}
	for _, kind := range []string{"collab_agent_spawn_begin", "collab_agent_spawn_end", "collab_agent_interaction_begin", "collab_agent_interaction_end", "collab_waiting_begin", "collab_waiting_end", "collab_close_begin", "collab_close_end", "collab_resume_begin", "collab_resume_end"} {
		events = append(events, map[string]any{"type": kind})
	}
	for _, event := range []string{"item_started", "item_completed"} {
		for _, item := range []string{"SubAgentActivity", "CollabAgentToolCall", "AgentMessage"} {
			events = append(events, map[string]any{"type": event, "item": map[string]any{"type": item, "kind": "started", "agent_thread_id": otherThread}})
		}
	}
	for _, event := range events {
		name := event["type"].(string)
		item, _ := event["item"].(map[string]any)
		if item != nil {
			name += "/" + item["type"].(string)
		}
		t.Run(name, func(t *testing.T) {
			data := line(t, 0, "session_meta", meta()) +
				line(t, 1, "event_msg", map[string]any{"type": "task_started", "turn_id": "parent"}) +
				line(t, 2, "event_msg", event) +
				line(t, 3, "event_msg", map[string]any{"type": "task_complete", "turn_id": "parent"})
			_, err := inspect(t, data)
			if item["type"] == "AgentMessage" {
				if err != nil {
					t.Fatal(err)
				}
			} else {
				assertRejection(t, err, "subagent")
			}
		})
	}
}

func TestExecCommandRequiresProcessCompletion(t *testing.T) {
	call := map[string]any{"type": "function_call", "name": "exec_command", "call_id": "command", "arguments": "{}"}
	output := map[string]any{"type": "function_call_output", "call_id": "command", "output": "process still running"}
	prefix := line(t, 0, "session_meta", meta()) +
		line(t, 1, "response_item", call) + line(t, 2, "response_item", output) +
		line(t, 3, "event_msg", map[string]any{"type": "task_started", "turn_id": "parent"}) +
		line(t, 4, "event_msg", map[string]any{"type": "task_complete", "turn_id": "parent"})
	for _, compacted := range []bool{false, true} {
		for _, tc := range []struct {
			name, id, thread, status string
			valid                    bool
		}{
			{"no completion", "", "", "", false},
			{"other command", "other", testThread, "completed", false},
			{"other thread", "command", otherThread, "completed", false},
			{"in progress", "command", testThread, "in_progress", false},
			{"completed", "command", testThread, "completed", true},
			{"failed", "command", testThread, "failed", true},
			{"declined", "command", testThread, "declined", true},
		} {
			t.Run(tc.name+map[bool]string{true: "/compacted", false: "/plain"}[compacted], func(t *testing.T) {
				data := prefix
				if compacted {
					data += line(t, 5, "compacted", map[string]any{"message": "summary", "replacement_history": []any{}, "window_number": 1})
				}
				if tc.id != "" {
					data += line(t, 6, "event_msg", map[string]any{"type": "item_completed", "thread_id": tc.thread, "item": map[string]any{"type": "CommandExecution", "id": tc.id, "status": tc.status}})
				}
				got, err := inspect(t, data)
				if tc.valid {
					if err != nil {
						t.Fatal(err)
					}
				} else {
					assertRejection(t, err, "active_command")
					if got.Rejections[0].Ordinal != 1 {
						t.Fatalf("rejection must identify the command call: %+v", got.Rejections)
					}
				}
			})
		}
	}
	// Reusing a still-active command ID must not let one completion hide a process.
	_, err := inspect(t, prefix+line(t, 5, "response_item", call)+line(t, 6, "response_item", output)+
		line(t, 7, "event_msg", map[string]any{"type": "item_completed", "thread_id": testThread, "item": map[string]any{"type": "CommandExecution", "id": "command", "status": "completed"}}))
	assertRejection(t, err, "active_command")
}

func TestCompactionExecCommandCompletion(t *testing.T) {
	call := map[string]any{"type": "function_call", "name": "exec_command", "call_id": "command", "arguments": "{}"}
	output := map[string]any{"type": "function_call_output", "call_id": "command", "output": "process yielded"}
	completion := map[string]any{"type": "item_completed", "thread_id": testThread, "item": map[string]any{"type": "CommandExecution", "id": "command", "status": "completed"}}
	for _, when := range []string{"missing", "before", "after"} {
		t.Run(when, func(t *testing.T) {
			data := line(t, 0, "session_meta", meta())
			if when == "before" {
				data += line(t, 1, "event_msg", completion)
			}
			data += line(t, 2, "compacted", map[string]any{"message": "summary", "window_number": 1, "replacement_history": []any{call, output}})
			if when == "after" {
				data += line(t, 3, "event_msg", completion)
			}
			got, err := inspect(t, data)
			if when == "missing" {
				assertRejection(t, err, "active_command")
				if got.Rejections[0].Ordinal != 2 {
					t.Fatalf("rejection must identify the restoring compaction: %+v", got.Rejections)
				}
			} else if err != nil {
				t.Fatal(err)
			}
		})
	}
	_, err := inspect(t, line(t, 0, "session_meta", meta())+
		line(t, 1, "compacted", map[string]any{"message": "summary", "window_number": 1, "replacement_history": []any{call, output, call, output}})+
		line(t, 2, "event_msg", completion))
	assertRejection(t, err, "active_command")
}

func TestCustomToolsCompactionAndWarnings(t *testing.T) {
	metadata := meta()
	metadata["git"] = map[string]any{"repository_url": "https://user:secret@example.test/repo"}
	metadata["runtime_workspace_roots"] = []string{"/outside"}
	data := line(t, 0, "session_meta", metadata) + line(t, 1, "response_item", map[string]any{"type": "custom_tool_call", "call_id": "custom", "name": "test", "input": "data"}) + line(t, 2, "response_item", map[string]any{"type": "custom_tool_call_output", "call_id": "custom", "output": "done"}) +
		line(t, 3, "compacted", map[string]any{"message": "summary", "replacement_history": []any{}, "window_number": 1, "encrypted_content": "secret"})
	got, err := inspect(t, data)
	if err != nil {
		t.Fatal(err)
	}
	if got.Records.ToolPairs != 1 || got.Compaction.Count != 1 || !got.Compaction.NewestComplete || got.Compaction.NewestCompleteBoundaryOrdinal == nil || *got.Compaction.NewestCompleteBoundaryOrdinal != 3 {
		t.Fatalf("summary: %+v", got)
	}
	warnings := map[string]bool{}
	for _, warning := range got.Warnings {
		warnings[warning.Code] = true
		if strings.Contains(warning.Message, "secret") {
			t.Fatal("warning exposed secret")
		}
	}
	for _, code := range []string{"repository_url_userinfo", "encrypted_content", "workspace_root_outside_cwd"} {
		if !warnings[code] {
			t.Fatalf("missing %s", code)
		}
	}
}

func TestPathsAndSelection(t *testing.T) {
	for _, tc := range []struct{ path, code string }{
		{strings.Replace(testPath, "sessions/", "archived_sessions/", 1), "archived"},
		{testPath + ".zst", "compressed"},
		{strings.TrimSuffix(testPath, ".jsonl") + "_" + otherThread + ".jsonl", "lineage"},
		{"../" + testPath, "source_path"},
		{strings.Replace(testPath, "2025/03/31", "2025/02/31", 1), "source_path"},
	} {
		t.Run(tc.code, func(t *testing.T) { _, err := ValidatePath(tc.path); assertRejection(t, err, tc.code) })
	}
	if id, err := ValidatePath(testPath); err != nil || id != testThread {
		t.Fatalf("preserved local date path failed: %s %v", id, err)
	}
	home, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	write := func(relative string) {
		t.Helper()
		full := filepath.Join(home, filepath.FromSlash(relative))
		if err := os.MkdirAll(filepath.Dir(full), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(line(t, 0, "session_meta", meta())), 0600); err != nil {
			t.Fatal(err)
		}
	}
	write(testPath)
	src := model.Source{Harness: model.Codex, Root: home, ThreadID: testThread}
	root, err := fsx.OpenRoot(home)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = root.Close() }()
	selected, err := (Adapter{}).Select(context.Background(), src, root, budget.New(context.Background(), model.Budget{}))
	if err != nil || selected != testPath {
		t.Fatalf("selection: %s %v", selected, err)
	}
	_, err = (Adapter{}).Select(context.Background(), src, root, budget.New(context.Background(), model.Budget{MaxNodes: 1}))
	var exceeded *model.BudgetError
	if !errors.As(err, &exceeded) {
		t.Fatalf("selection budget: %v", err)
	}
	write(strings.Replace(testPath, "T00-30-00", "T00-30-01", 1))
	_, err = (Adapter{}).Select(context.Background(), src, root, budget.New(context.Background(), model.Budget{}))
	assertRejection(t, err, "duplicate_source")
}

func assertRejection(t *testing.T, err error, code string) {
	t.Helper()
	var rejected *model.RejectionError
	if !errors.As(err, &rejected) || len(rejected.Rejections) == 0 || rejected.Rejections[0].Code != code {
		t.Fatalf("want rejection %s, got %v", code, err)
	}
}

func TestTurnLifecycle(t *testing.T) {
	event := func(kind, id string) map[string]any {
		out := map[string]any{"type": kind, "turn_id": id}
		if kind == "turn_aborted" {
			out["reason"] = "interrupted"
		}
		return out
	}
	for _, tc := range []struct {
		name   string
		events []map[string]any
		code   string
	}{
		{"unfinished", []map[string]any{event("task_started", "turn-1")}, "in_flight_turn"},
		{"completed", []map[string]any{event("task_started", "turn-1"), event("task_complete", "turn-1")}, ""},
		{"aliases", []map[string]any{event("turn_started", "turn-1"), event("turn_complete", "turn-1")}, ""},
		{"aborted", []map[string]any{event("task_started", "turn-1"), event("turn_aborted", "turn-1")}, ""},
		{"replaced", []map[string]any{event("task_started", "turn-1"), {"type": "turn_aborted", "turn_id": "turn-1", "reason": "replaced"}}, ""},
		{"review ended", []map[string]any{event("task_started", "turn-1"), {"type": "turn_aborted", "turn_id": "turn-1", "reason": "review_ended"}}, ""},
		{"budget limited", []map[string]any{event("task_started", "turn-1"), {"type": "turn_aborted", "turn_id": "turn-1", "reason": "budget_limited"}}, ""},
		{"missing abort reason", []map[string]any{event("task_started", "turn-1"), {"type": "turn_aborted", "turn_id": "turn-1"}}, "turn_lifecycle"},
		{"invalid abort reason", []map[string]any{event("task_started", "turn-1"), {"type": "turn_aborted", "turn_id": "turn-1", "reason": "unknown"}}, "turn_lifecycle"},
		{"numeric abort reason", []map[string]any{event("task_started", "turn-1"), {"type": "turn_aborted", "turn_id": "turn-1", "reason": 1}}, "turn_lifecycle"},
		{"unmatched completion", []map[string]any{event("task_started", "turn-1"), event("task_complete", "turn-2")}, "in_flight_turn"},
		{"unmatched abort", []map[string]any{event("task_started", "turn-1"), event("turn_aborted", "turn-2")}, "in_flight_turn"},
		{"unidentified abort", []map[string]any{event("task_started", "turn-1"), {"type": "turn_aborted", "reason": "interrupted"}}, "in_flight_turn"},
		{"multiple outstanding turns", []map[string]any{event("task_started", "turn-1"), event("task_started", "turn-2"), event("task_complete", "turn-2")}, "in_flight_turn"},
		{"missing ID", []map[string]any{{"type": "task_started"}}, "turn_lifecycle"},
		{"duplicate start", []map[string]any{event("task_started", "turn-1"), event("task_started", "turn-1")}, "turn_lifecycle"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := line(t, 0, "session_meta", meta())
			for i, event := range tc.events {
				data += line(t, uint64(i+1), "event_msg", event)
			}
			got, err := inspect(t, data)
			if tc.code == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			assertRejection(t, err, tc.code)
			if tc.code == "in_flight_turn" && got.Rejections[0].Ordinal != 1 {
				t.Fatalf("rejection must identify the outstanding start: %+v", got.Rejections)
			}
		})
	}
}

func TestCompactionReplacementHistory(t *testing.T) {
	call := map[string]any{"type": "function_call", "call_id": "call-1", "name": "test", "arguments": "{}"}
	output := map[string]any{"type": "function_call_output", "call_id": "call-1", "output": "done"}
	for _, tc := range []struct {
		name    string
		payload map[string]any
		code    string
	}{
		{"partial", map[string]any{"message": "partial"}, "compacted"},
		{"missing message", map[string]any{"window_number": 1, "replacement_history": []any{}}, "compacted"},
		{"numeric message", map[string]any{"message": 0, "window_number": 1, "replacement_history": []any{}}, "compacted"},
		{"missing window", map[string]any{"message": "summary", "replacement_history": []any{}}, "compacted"},
		{"negative window", map[string]any{"message": "summary", "replacement_history": []any{}, "window_number": -1}, "compacted"},
		{"null history", map[string]any{"message": "summary", "replacement_history": nil, "window_number": 1}, "compacted"},
		{"non-object item", map[string]any{"message": "summary", "replacement_history": []any{nil}, "window_number": 1}, "response_item"},
		{"missing item type", map[string]any{"message": "summary", "replacement_history": []any{map[string]any{}}, "window_number": 1}, "response_item"},
		{"orphan output", map[string]any{"message": "summary", "replacement_history": []any{output}, "window_number": 1}, "orphan_tool_output"},
		{"orphan call", map[string]any{"message": "summary", "replacement_history": []any{call}, "window_number": 1}, "orphan_tool_call"},
		{"duplicate call", map[string]any{"message": "summary", "replacement_history": []any{call, call, output}, "window_number": 1}, "tool_call"},
		{"wrong output kind", map[string]any{"message": "summary", "replacement_history": []any{call, map[string]any{"type": "custom_tool_call_output", "call_id": "call-1"}}, "window_number": 1}, "orphan_tool_output"},
		{"complete pair", map[string]any{"message": "summary", "replacement_history": []any{call, output}, "window_number": 1}, ""},
		{"metadata is scalar", map[string]any{"message": "summary", "replacement_history": []any{}, "window_number": 1, "replacement_history_metadata": "invalid"}, "compacted"},
		{"metadata count mismatch", map[string]any{"message": "summary", "replacement_history": []any{call, output}, "window_number": 1, "replacement_history_metadata": []any{}}, "compacted"},
		{"metadata entry is scalar", map[string]any{"message": "summary", "replacement_history": []any{call, output}, "window_number": 1, "replacement_history_metadata": []any{0, 0}}, "compacted"},
		{"matching metadata", map[string]any{"message": "summary", "replacement_history": []any{call, output}, "window_number": 1, "replacement_history_metadata": []any{map[string]any{}, map[string]any{}}}, ""},
		{"null metadata", map[string]any{"message": "summary", "replacement_history": []any{call, output}, "window_number": 1, "replacement_history_metadata": nil}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := inspect(t, line(t, 0, "session_meta", meta())+line(t, 1, "compacted", tc.payload))
			if tc.code != "" {
				assertRejection(t, err, tc.code)
			} else if err != nil {
				t.Fatal(err)
			}
		})
	}
	// A call in the discarded prefix cannot satisfy a replacement output.
	_, err := inspect(t, line(t, 0, "session_meta", meta())+line(t, 1, "response_item", call)+line(t, 2, "response_item", output)+line(t, 3, "compacted", map[string]any{"message": "summary", "replacement_history": []any{output}, "window_number": 1}))
	assertRejection(t, err, "orphan_tool_output")
	// Compaction discards pending calls from the previous effective history.
	prefix := line(t, 0, "session_meta", meta()) + line(t, 1, "response_item", call) +
		line(t, 2, "compacted", map[string]any{"message": "summary", "replacement_history": []any{}, "window_number": 1})
	if _, err := inspect(t, prefix); err != nil {
		t.Fatal(err)
	}
	_, err = inspect(t, prefix+line(t, 3, "response_item", output))
	assertRejection(t, err, "orphan_tool_output")
	if _, err := inspect(t, prefix+line(t, 3, "response_item", call)+line(t, 4, "response_item", output)); err != nil {
		t.Fatal(err)
	}
}

func TestUnvalidatedToolShapesReject(t *testing.T) {
	for _, tc := range []struct{ kind, code string }{
		{"local_shell_call", "unsupported_tool"},
		{"tool_search_call", "unsupported_tool"},
		{"tool_search_output", "unsupported_tool"},
		{"agent_message", "subagent"},
	} {
		kind := tc.kind
		for _, compacted := range []bool{false, true} {
			t.Run(kind+map[bool]string{false: "/rollout", true: "/replacement"}[compacted], func(t *testing.T) {
				item := map[string]any{"type": kind, "call_id": "call-1"}
				data := line(t, 0, "session_meta", meta())
				if compacted {
					data += line(t, 1, "compacted", map[string]any{"message": "summary", "replacement_history": []any{item}, "window_number": 1})
				} else {
					data += line(t, 1, "response_item", item)
				}
				_, err := inspect(t, data)
				assertRejection(t, err, tc.code)
			})
		}
	}
}

func TestSupportedToolFields(t *testing.T) {
	for _, tc := range []struct {
		name   string
		call   map[string]any
		output any
		valid  bool
	}{
		{"missing name", map[string]any{"type": "function_call", "arguments": "{}"}, "done", false},
		{"missing arguments", map[string]any{"type": "function_call", "name": "test"}, "done", false},
		{"numeric arguments", map[string]any{"type": "function_call", "name": "test", "arguments": 1}, "done", false},
		{"numeric custom input", map[string]any{"type": "custom_tool_call", "name": "test", "input": 1}, "done", false},
		{"numeric output", nil, 1, false},
		{"malformed output content", nil, []any{map[string]any{"type": "input_text", "text": 1}}, false},
		{"malformed image", nil, []any{map[string]any{"type": "input_image", "image_url": 1}}, false},
		{"invalid image detail", nil, []any{map[string]any{"type": "input_image", "image_url": "data:image/png;base64,YQ==", "detail": "invalid"}}, false},
		{"text output", nil, "done", true},
		{"structured output", nil, []any{map[string]any{"type": "input_text", "text": "done"}, map[string]any{"type": "input_image", "image_url": "data:image/png;base64,YQ==", "detail": "high"}}, true},
	} {
		for _, replacement := range []bool{false, true} {
			t.Run(tc.name+map[bool]string{false: "/rollout", true: "/replacement"}[replacement], func(t *testing.T) {
				call := tc.call
				if call == nil {
					call = map[string]any{"type": "function_call", "name": "test", "arguments": "{}"}
				}
				call["call_id"] = "call-1"
				output := map[string]any{"type": call["type"].(string) + "_output", "call_id": "call-1", "output": tc.output}
				data := line(t, 0, "session_meta", meta())
				if replacement {
					data += line(t, 1, "compacted", map[string]any{"message": "summary", "replacement_history": []any{call, output}, "window_number": 1})
				} else {
					data += line(t, 1, "response_item", call) + line(t, 2, "response_item", output)
				}
				_, err := inspect(t, data)
				if tc.valid {
					if err != nil {
						t.Fatal(err)
					}
				} else {
					assertRejection(t, err, "response_item")
				}
			})
		}
	}
}

func TestContentReferencesMustBeInline(t *testing.T) {
	for _, tc := range []struct {
		name    string
		content map[string]any
		valid   bool
	}{
		{"provider image", map[string]any{"type": "input_image", "file_id": "file-image"}, false},
		{"remote image", map[string]any{"type": "input_image", "image_url": "https://example.test/image.png"}, false},
		{"remote audio", map[string]any{"type": "input_audio", "audio_url": "https://example.test/audio.wav"}, false},
		{"inline image", map[string]any{"type": "input_image", "image_url": "data:image/png;base64,YQ=="}, true},
		{"inline audio", map[string]any{"type": "input_audio", "audio_url": "data:audio/wav;base64,YQ=="}, true},
	} {
		for _, kind := range []string{"message", "function_call_output", "custom_tool_call_output"} {
			for _, replacement := range []bool{false, true} {
				t.Run(tc.name+"/"+kind+map[bool]string{false: "/rollout", true: "/replacement"}[replacement], func(t *testing.T) {
					item := map[string]any{"type": kind}
					items := []any{}
					if kind == "message" {
						item["role"] = "user"
						item["content"] = []any{tc.content}
					} else {
						call := map[string]any{"type": strings.TrimSuffix(kind, "_output"), "call_id": "call-1", "name": "test", "arguments": "{}", "input": "input"}
						items = append(items, call)
						item["call_id"] = "call-1"
						item["output"] = []any{tc.content}
					}
					items = append(items, item)
					data := line(t, 0, "session_meta", meta())
					if replacement {
						data += line(t, 1, "compacted", map[string]any{"message": "summary", "replacement_history": items, "window_number": 1})
					} else {
						for index, item := range items {
							data += line(t, uint64(index+1), "response_item", item)
						}
					}
					_, err := inspect(t, data)
					if tc.valid {
						if err != nil {
							t.Fatal(err)
						}
					} else {
						assertRejection(t, err, "external_reference")
					}
				})
			}
		}
	}
}
