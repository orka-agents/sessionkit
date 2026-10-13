package codex

import (
	"strconv"

	"github.com/orka-agents/sessionkit/harness/codex/writerlock"
	"github.com/orka-agents/sessionkit/internal/budget"
)

// agentNamespace holds the stable multi-agent v1 tools of both pinned clients.
const agentNamespace = "multi_agent_v1"

// agentTools maps each supported CollabAgentTool tag to its v1 function name.
// The v2 tags (send_message, followup_task, interrupt_agent, list_agents) and
// items for plain, unnamespaced v2 calls have no native parent evidence and are
// rejected.
var agentTools = map[string]string{
	"spawn_agent":  "spawn_agent",
	"send_input":   "send_input",
	"resume_agent": "resume_agent",
	"wait":         "wait_agent",
	"close_agent":  "close_agent",
}

// agentCall is a multi-agent function call waiting for its output.
type agentCall struct {
	name, namespace string
	callOrdinal     uint64
	// Set from the call's persisted CollabAgentToolCall item_completed event.
	item        bool
	itemOrdinal uint64
	tool        string
	status      string
	receivers   []string
}

// subagents follows a parent thread's multi-agent v1 calls in rollout order.
// Child threads are never captured. A parent is accepted only when every child
// thread its spawns or completed resumes opened is closed again by a completed
// close_agent call, so the moved parent leaves no open child behind.
type subagents struct {
	parent string
	calls  map[string]*agentCall
	open   map[string]uint64
	seen   map[string]bool
}

func newSubagents(parent string) *subagents {
	return &subagents{parent: parent, calls: make(map[string]*agentCall), open: make(map[string]uint64), seen: make(map[string]bool)}
}

// call tracks a live function call that can belong to the multi-agent tools.
// Unnamespaced spawn_agent and resume_agent calls, the v2 names, are tracked so
// that they too require a native item before their output.
func (s *subagents) call(payload map[string]any, ordinal uint64) {
	name, _ := payload["name"].(string)
	namespace, _ := payload["namespace"].(string)
	if namespace == agentNamespace || namespace == "" && opensChild(name) {
		s.calls[payload["call_id"].(string)] = &agentCall{name: name, namespace: namespace, callOrdinal: ordinal}
	}
}

func opensChild(name string) bool { return name == "spawn_agent" || name == "resume_agent" }

// item attaches a CollabAgentToolCall item_completed event to its pending call.
// Native clients persist it after the call and before the call's output.
func (s *subagents) item(event, item map[string]any, component string, ordinal uint64, tracker *budget.Tracker) error {
	bad := func(message string) error { return reject(component, "subagent", message, ordinal) }
	if event["thread_id"] != s.parent || item["sender_thread_id"] != s.parent {
		return bad("subagent tool calls must belong to the selected thread")
	}
	tool, _ := item["tool"].(string)
	name, known := agentTools[tool]
	if !known {
		return bad("subagent tool is not supported by this profile")
	}
	status, _ := item["status"].(string)
	if status != "completed" && status != "failed" {
		return bad("subagent tool call must be completed or failed")
	}
	callID, _ := item["id"].(string)
	call := s.calls[callID]
	if call == nil || call.item || call.namespace != agentNamespace || call.name != name {
		return bad("subagent tool item has no matching pending native call")
	}
	receivers, ok, err := stringList(item["receiver_thread_ids"], tracker)
	if err != nil {
		return err
	}
	if !ok {
		return bad("subagent receivers must be thread IDs")
	}
	for _, receiver := range receivers {
		if !writerlock.ValidThreadID(receiver) || receiver == s.parent {
			return bad("subagent receivers must be other canonical thread IDs")
		}
	}
	call.item, call.itemOrdinal, call.tool, call.status, call.receivers = true, ordinal, tool, status, receivers
	return nil
}

// output completes a tracked pair and applies its child lifecycle effect.
func (s *subagents) output(callID, component string) error {
	call := s.calls[callID]
	if call == nil {
		return nil
	}
	delete(s.calls, callID)
	if !call.item {
		if opensChild(call.name) {
			return reject(component, "subagent", "subagent spawn or resume has no native completion item", call.callOrdinal)
		}
		return nil
	}
	switch {
	// A failed spawn can still name an errored child, which then needs a close.
	case call.tool == "spawn_agent", call.tool == "resume_agent" && call.status == "completed":
		for _, child := range call.receivers {
			if _, open := s.open[child]; !open {
				s.open[child] = call.itemOrdinal
			}
			s.seen[child] = true
		}
	case call.tool == "close_agent" && call.status == "completed":
		for _, child := range call.receivers {
			delete(s.open, child)
		}
	}
	return nil
}

// compacted rejects a multi-agent call whose output a compaction discarded;
// its child lifecycle would be unknown.
func (s *subagents) compacted(component string, last uint64, tracker *budget.Tracker) error {
	if len(s.calls) == 0 {
		return nil
	}
	ordinal := last
	for _, call := range s.calls {
		if err := tracker.Check(); err != nil {
			return err
		}
		ordinal = min(ordinal, call.callOrdinal)
	}
	return reject(component, "subagent", "subagent tool call has no output before compaction", ordinal)
}

// finish rejects at the earliest opening item of a child that was never closed.
func (s *subagents) finish(component string, last uint64, tracker *budget.Tracker) error {
	if len(s.open) == 0 {
		return nil
	}
	ordinal := last
	for _, opened := range s.open {
		if err := tracker.Check(); err != nil {
			return err
		}
		ordinal = min(ordinal, opened)
	}
	return reject(component, "open_subagent", "spawned subagent thread is not closed by a completed close_agent call", ordinal)
}

// omitted names the child threads that stay behind in the source home.
func (s *subagents) omitted() []string {
	if len(s.seen) == 0 {
		return nil
	}
	return []string{"subagent threads (" + strconv.Itoa(len(s.seen)) + ")"}
}
