// Package codex inspects byte-preserving Codex paginated rollouts. The supported
// wire shapes are pinned to rust-v0.160.0, commit a956835d020762cb2b570053af06f643a11c0ecc.
package codex

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/orka-agents/sessionkit/harness/codex/writerlock"
	"github.com/orka-agents/sessionkit/internal/budget"
	"github.com/orka-agents/sessionkit/internal/fsx"
	"github.com/orka-agents/sessionkit/internal/harness"
	"github.com/orka-agents/sessionkit/internal/jsonl"
	"github.com/orka-agents/sessionkit/internal/model"
)

const AdapterVersion = "1"
const CLIVersion = "0.160.0"

type Adapter struct{}

var _ harness.Adapter = Adapter{}

func (Adapter) LockSource(ctx context.Context, src model.Source, root *fsx.Root) (io.Closer, error) {
	return writerlock.SourceAt(ctx, root, src.ThreadID)
}
func (Adapter) LockPublication(ctx context.Context, root *fsx.Root, threadID string) (io.Closer, error) {
	return writerlock.PublicationAt(ctx, root, threadID)
}

func reject(component, code, message string, ordinal uint64) *model.RejectionError {
	return &model.RejectionError{Rejections: []model.Rejection{{Code: code, Component: component, Ordinal: ordinal, Message: message}}}
}

// ValidatePath checks the retained path without deriving date directories from
// the filename timestamp. Codex writes those directories using local time.
func ValidatePath(relative string) (string, error) {
	bad := func(code, message string) (string, error) { return "", reject(relative, code, message, 0) }
	if !fsx.ValidPath(relative) {
		return bad("source_path", "source path must be a safe relative path")
	}
	parts := strings.Split(relative, "/")
	if len(parts) > 0 && parts[0] == "archived_sessions" {
		return bad("archived", "archived sessions are not supported")
	}
	if strings.HasSuffix(relative, ".zst") {
		return bad("compressed", "compressed rollouts are not supported")
	}
	if len(parts) != 5 || parts[0] != "sessions" {
		return bad("source_path", "source must be under sessions/YYYY/MM/DD")
	}
	date := strings.Join(parts[1:4], "/")
	parsed, err := time.Parse("2006/01/02", date)
	if err != nil || parsed.Format("2006/01/02") != date {
		return bad("source_path", "source date directories are invalid")
	}
	base := parts[4]
	if !strings.HasPrefix(base, "rollout-") || !strings.HasSuffix(base, ".jsonl") {
		return bad("source_path", "source filename is not a canonical rollout filename")
	}
	core := strings.TrimSuffix(strings.TrimPrefix(base, "rollout-"), ".jsonl")
	if strings.Contains(core, "_") {
		return bad("lineage", "rollouts with a separate rollout ID are not supported")
	}
	if len(core) != 19+1+36 || core[19] != '-' {
		return bad("source_path", "source filename is not a canonical rollout filename")
	}
	stamp, err := time.Parse("2006-01-02T15-04-05", core[:19])
	if err != nil || stamp.Format("2006-01-02T15-04-05") != core[:19] {
		return bad("source_path", "source filename timestamp is invalid")
	}
	id := core[20:]
	if !writerlock.ValidThreadID(id) {
		return bad("thread_id", "thread ID must be a canonical UUIDv7")
	}
	return id, nil
}

func (Adapter) Select(ctx context.Context, src model.Source, root *fsx.Root, tracker *budget.Tracker) (string, error) {
	if !writerlock.ValidThreadID(src.ThreadID) {
		return "", reject("", "thread_id", "thread ID must be a canonical UUIDv7", 0)
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	var selected string
	for _, dir := range []string{"sessions", "archived_sessions"} {
		err := root.WalkFiles(dir, func(name string, isDir bool) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := tracker.Node(); err != nil {
				return err
			}
			if isDir || !strings.Contains(path.Base(name), src.ThreadID) {
				return nil
			}
			id, err := ValidatePath(name)
			if err != nil {
				return err
			}
			if id != src.ThreadID {
				return nil
			}
			if selected != "" {
				return reject(name, "duplicate_source", "multiple rollouts carry this thread ID", 0)
			}
			selected = name
			return nil
		})
		if err != nil {
			return "", err
		}
	}
	if selected == "" {
		return "", reject("", "source_not_found", "no rollout found for this thread ID", 0)
	}
	return selected, nil
}

func (Adapter) Layout(inspection model.Inspection, relative string) (string, error) {
	id, err := ValidatePath(relative)
	if err != nil {
		return "", err
	}
	if inspection.Profile != model.CodexPaginated {
		return "", reject(relative, "profile", "unsupported Codex profile", 0)
	}
	if id != inspection.ThreadID {
		return "", reject(relative, "thread_id", "filename and session metadata thread IDs differ", 0)
	}
	return relative, nil
}

type countReader struct {
	input io.Reader
	size  int64
}

func (r *countReader) Read(p []byte) (int, error) {
	n, err := r.input.Read(p)
	r.size += int64(n)
	return n, err
}

type toolCall struct {
	kind    string
	ordinal uint64
}

func (Adapter) Inspect(ctx context.Context, input io.Reader, relative string, tracker *budget.Tracker) (model.Inspection, error) {
	inspection := model.Inspection{Profile: model.CodexPaginated, Omitted: []string{"thread name", "git metadata", "memory mode"}, Records: model.RecordSummary{ByType: map[string]int64{}, ResponseItems: map[string]int64{}}}
	fail := func(err error) (model.Inspection, error) {
		var rejected *model.RejectionError
		if errors.As(err, &rejected) {
			inspection.Rejections = append(inspection.Rejections, rejected.Rejections...)
		}
		return inspection, err
	}
	id, err := ValidatePath(relative)
	if err != nil {
		return fail(err)
	}
	inspection.ThreadID = id
	digest := sha256.New()
	counted := &countReader{input: io.TeeReader(input, digest)}
	reader := jsonl.New(counted, tracker)
	pending := make(map[string]toolCall)
	turns := make(map[string]uint64)
	commands := make(map[string]uint64)
	warned := make(map[string]bool)
	warn := func(code, message string, ordinal uint64) {
		if !warned[code] {
			warned[code] = true
			inspection.Warnings = append(inspection.Warnings, model.Warning{Code: code, Component: relative, Ordinal: ordinal, Message: message})
		}
	}
	for {
		if err := ctx.Err(); err != nil {
			return fail(err)
		}
		line, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fail(parseError(relative, err))
		}
		record, err := jsonl.Decode(line, tracker)
		if err != nil {
			return fail(parseError(relative, err))
		}
		ordinal, ok := unsigned(record["ordinal"])
		if !ok {
			return fail(reject(relative, "ordinal", "every record must carry an unsigned integer ordinal", 0))
		}
		if _, ok := record["timestamp"].(string); !ok {
			return fail(reject(relative, "timestamp", "every record must carry a string timestamp", ordinal))
		}
		if inspection.Records.Total > 0 && ordinal < inspection.Records.LastOrdinal {
			return fail(reject(relative, "ordinal_order", "record ordinals must be non-decreasing", ordinal))
		}
		if inspection.Records.Total == 0 {
			inspection.Records.FirstOrdinal = ordinal
		} else if ordinal > inspection.Records.LastOrdinal && ordinal-inspection.Records.LastOrdinal > 1 {
			inspection.Records.OrdinalGaps++
		}
		inspection.Records.LastOrdinal = ordinal
		kind, ok := record["type"].(string)
		if !ok || kind == "" {
			return fail(reject(relative, "record_type", "record type must be a nonempty string", ordinal))
		}
		payload, _ := record["payload"].(map[string]any)
		if inspection.Records.Total == 0 && kind != "session_meta" {
			return fail(reject(relative, "session_meta", "first record must be session_meta", ordinal))
		}
		inspection.Records.Total++
		inspection.Records.ByType[kind]++
		switch kind {
		case "inter_agent_communication", "inter_agent_communication_metadata":
			return fail(reject(relative, "lineage", "inter-agent communication is not supported", ordinal))
		case "session_meta":
			if inspection.Records.Total != 1 {
				return fail(reject(relative, "session_meta", "additional session_meta records are not supported", ordinal))
			}
			if err := metadata(&inspection, payload, relative, ordinal); err != nil {
				return fail(err)
			}
		case "response_item":
			if err := responseItem(payload, pending, relative, ordinal); err != nil {
				return fail(err)
			}
			itemKind := payload["type"].(string)
			inspection.Records.ResponseItems[itemKind]++
			switch itemKind {
			case "function_call", "custom_tool_call":
				inspection.Records.ToolCalls++
				if itemKind == "function_call" && payload["name"] == "exec_command" {
					id := payload["call_id"].(string)
					if _, active := commands[id]; active {
						return fail(reject(relative, "active_command", "exec command call ID is already active", ordinal))
					}
					commands[id] = ordinal
				}
			case "function_call_output", "custom_tool_call_output":
				inspection.Records.ToolOutputs++
				inspection.Records.ToolPairs++
			}
		case "compacted":
			if _, ok := payload["message"].(string); !ok {
				return fail(reject(relative, "compacted", "compaction requires a string message", ordinal))
			}
			history, complete := payload["replacement_history"].([]any)
			_, window := unsigned(payload["window_number"])
			if !complete || !window {
				return fail(reject(relative, "compacted", "compaction requires replacement history and an unsigned window number", ordinal))
			}
			if value := payload["replacement_history_metadata"]; value != nil {
				metadata, ok := value.([]any)
				if !ok || len(metadata) != len(history) {
					return fail(reject(relative, "compacted", "replacement history metadata must match the history array", ordinal))
				}
				for _, entry := range metadata {
					if _, ok := entry.(map[string]any); !ok {
						return fail(reject(relative, "compacted", "replacement history metadata entries must be objects", ordinal))
					}
				}
			}
			replacementCalls := make(map[string]toolCall)
			for _, item := range history {
				payload, _ := item.(map[string]any)
				if err := responseItem(payload, replacementCalls, relative, ordinal); err != nil {
					return fail(err)
				}
			}
			if len(replacementCalls) > 0 {
				return fail(reject(relative, "orphan_tool_call", "compaction tool call has no matching output", ordinal))
			}
			clear(pending)
			inspection.Compaction.Count++
			inspection.Compaction.NewestComplete = true
			boundary := ordinal
			inspection.Compaction.NewestCompleteBoundaryOrdinal = &boundary
		case "event_msg":
			eventKind, _ := payload["type"].(string)
			if eventKind == "sub_agent_activity" || strings.HasPrefix(eventKind, "collab_") {
				return fail(reject(relative, "subagent", "subagent activity is not supported", ordinal))
			}
			switch payload["type"] {
			case "item_started", "item_completed":
				item, _ := payload["item"].(map[string]any)
				if item["type"] == "SubAgentActivity" || item["type"] == "CollabAgentToolCall" {
					return fail(reject(relative, "subagent", "subagent turn items are not supported", ordinal))
				}
				if eventKind == "item_completed" && item["type"] == "CommandExecution" && payload["thread_id"] == inspection.ThreadID {
					switch item["status"] {
					case "completed", "failed", "declined":
						if id, ok := item["id"].(string); ok {
							delete(commands, id)
						}
					}
				}
			case "task_started", "turn_started", "task_complete", "turn_complete":
				turnID, ok := payload["turn_id"].(string)
				if !ok || turnID == "" {
					return fail(reject(relative, "turn_lifecycle", "turn lifecycle event must have a turn ID", ordinal))
				}
				if payload["type"] == "task_started" || payload["type"] == "turn_started" {
					if _, exists := turns[turnID]; exists {
						return fail(reject(relative, "turn_lifecycle", "turn ID is already active", ordinal))
					}
					turns[turnID] = ordinal
				} else {
					delete(turns, turnID)
				}
			case "turn_aborted":
				if turnID, ok := payload["turn_id"].(string); ok && turnID != "" {
					delete(turns, turnID)
				}
			}
			if payload["type"] == "thread_settings_applied" && payload["thread_id"] == inspection.ThreadID {
				settings, _ := payload["thread_settings"].(map[string]any)
				cwd, ok := settings["cwd"].(string)
				if !ok {
					return fail(reject(relative, "thread_settings", "thread settings cwd must be a string", ordinal))
				}
				inspection.LatestCWD = cwd
				if roots, present := settings["runtime_workspace_roots"]; present && roots != nil {
					parsed, ok := stringList(roots)
					if !ok {
						return fail(reject(relative, "workspace_roots", "runtime workspace roots must be strings", ordinal))
					}
					inspection.RuntimeWorkspaceRoots = parsed
				}
				if provider, ok := settings["model_provider_id"].(string); ok {
					inspection.ModelProvider = provider
				}
			}
		}
		walkWarnings(record, ordinal, warn)
		for _, root := range inspection.RuntimeWorkspaceRoots {
			if !under(inspection.RecordedCWD, root) {
				warn("workspace_root_outside_cwd", "runtime workspace roots include a path outside the recorded cwd", ordinal)
				break
			}
		}
	}
	if inspection.Records.Total == 0 {
		return fail(reject(relative, "session_meta", "rollout is empty", 0))
	}
	if len(turns) > 0 {
		ordinal := inspection.Records.LastOrdinal
		for _, started := range turns {
			ordinal = min(ordinal, started)
		}
		return fail(reject(relative, "in_flight_turn", "turn has no matching completion or abort", ordinal))
	}
	if len(pending) > 0 {
		ordinal := inspection.Records.LastOrdinal
		for _, call := range pending {
			if call.ordinal < ordinal {
				ordinal = call.ordinal
			}
		}
		return fail(reject(relative, "orphan_tool_call", "tool call has no matching output", ordinal))
	}
	if len(commands) > 0 {
		ordinal := inspection.Records.LastOrdinal
		for _, started := range commands {
			ordinal = min(ordinal, started)
		}
		return fail(reject(relative, "active_command", "exec command has no terminal command completion", ordinal))
	}
	if inspection.Records.OrdinalGaps > 0 {
		warn("ordinal_gaps", "record ordinal gaps are preserved", inspection.Records.LastOrdinal)
	}
	sort.Slice(inspection.Warnings, func(i, j int) bool { return inspection.Warnings[i].Code < inspection.Warnings[j].Code })
	if err := tracker.Check(); err != nil {
		return fail(err)
	}
	inspection.SourceDigest = hex.EncodeToString(digest.Sum(nil))
	inspection.SourceSizeBytes = counted.size
	return inspection, nil
}

func responseItem(payload map[string]any, pending map[string]toolCall, component string, ordinal uint64) error {
	kind, ok := payload["type"].(string)
	if !ok || kind == "" {
		return reject(component, "response_item", "response item type must be a nonempty string", ordinal)
	}
	switch kind {
	case "local_shell_call", "tool_search_call", "tool_search_output":
		return reject(component, "unsupported_tool", "tool record shape is not supported by this profile", ordinal)
	case "function_call", "custom_tool_call":
		callID, ok := payload["call_id"].(string)
		if !ok || callID == "" {
			return reject(component, "tool_call", "tool call must have a call ID", ordinal)
		}
		if _, exists := pending[callID]; exists {
			return reject(component, "tool_call", "tool call ID is already pending", ordinal)
		}
		fields := []string{"name", "arguments"}
		if kind == "custom_tool_call" {
			fields = []string{"name", "input"}
		}
		for _, field := range fields {
			if _, ok := payload[field].(string); !ok {
				return reject(component, "response_item", "tool call requires string name and input fields", ordinal)
			}
		}
		pending[callID] = toolCall{kind, ordinal}
	case "function_call_output", "custom_tool_call_output":
		callID, ok := payload["call_id"].(string)
		call, exists := pending[callID]
		if !ok || !exists || call.kind+"_output" != kind {
			return reject(component, "orphan_tool_output", "tool output has no matching pending call", ordinal)
		}
		if !toolOutput(payload["output"]) {
			return reject(component, "response_item", "tool output must be text or native content items", ordinal)
		}
		delete(pending, callID)
	}
	return nil
}

func toolOutput(value any) bool {
	if _, ok := value.(string); ok {
		return true
	}
	items, ok := value.([]any)
	if !ok {
		return false
	}
	for _, value := range items {
		item, _ := value.(map[string]any)
		field := ""
		switch item["type"] {
		case "input_text":
			field = "text"
		case "input_audio":
			field = "audio_url"
		case "encrypted_content":
			field = "encrypted_content"
		case "input_image":
			_, url := item["image_url"].(string)
			_, file := item["file_id"].(string)
			if !url && !file {
				return false
			}
			if detail := item["detail"]; detail != nil && detail != "auto" && detail != "low" && detail != "high" && detail != "original" {
				return false
			}
			continue
		default:
			return false
		}
		if _, ok := item[field].(string); !ok {
			return false
		}
	}
	return true
}

func parseError(component string, err error) error {
	var malformed *jsonl.Error
	if errors.As(err, &malformed) {
		return reject(component, malformed.Code, malformed.Message, 0)
	}
	return err
}
func unsigned(value any) (uint64, bool) {
	number, ok := value.(json.Number)
	if !ok {
		return 0, false
	}
	n, err := strconv.ParseUint(string(number), 10, 64)
	return n, err == nil
}
func stringList(value any) ([]string, bool) {
	array, ok := value.([]any)
	if !ok {
		return nil, false
	}
	result := make([]string, 0, len(array))
	for _, v := range array {
		s, ok := v.(string)
		if !ok {
			return nil, false
		}
		result = append(result, s)
	}
	return result, true
}

func metadata(out *model.Inspection, payload map[string]any, component string, ordinal uint64) error {
	bad := func(code, message string) error { return reject(component, code, message, ordinal) }
	if payload == nil {
		return bad("session_meta", "session metadata payload must be an object")
	}
	if payload["id"] != out.ThreadID {
		return bad("thread_id", "filename and session metadata thread IDs differ")
	}
	// The pinned native deserializer defaults an omitted session_id to id.
	if sessionID, present := payload["session_id"]; present && sessionID != out.ThreadID {
		return bad("lineage", "root session ID must match the selected thread ID")
	}
	version, ok := payload["cli_version"].(string)
	if !ok {
		return bad("cli_version", "session metadata must name its CLI version")
	}
	out.CLIVersion = version
	if version != CLIVersion {
		return bad("cli_version", "only Codex CLI 0.160.0 is supported")
	}
	mode, ok := payload["history_mode"].(string)
	if !ok || mode != "paginated" {
		return bad("history_mode", "only paginated history is supported")
	}
	out.HistoryMode = mode
	for _, field := range []string{"history_base", "forked_from_id", "forked_from_ordinal_exclusive", "parent_thread_id", "subagent_history_start_ordinal"} {
		if payload[field] != nil {
			return bad("lineage", "lineage and inherited subagent history are not supported")
		}
	}
	for _, field := range []string{"agent_nickname", "agent_role", "agent_type", "agent_path"} {
		if payload[field] != nil {
			return bad("subagent", "subagent metadata is not supported")
		}
	}
	if source, ok := payload["thread_source"].(string); ok && strings.EqualFold(source, "subagent") {
		return bad("subagent", "subagent sessions are not supported")
	}
	source := payload["source"]
	if text, ok := source.(string); ok && strings.EqualFold(text, "subagent") {
		return bad("subagent", "subagent sessions are not supported")
	}
	if object, ok := source.(map[string]any); ok {
		for key := range object {
			if strings.EqualFold(key, "subagent") {
				return bad("subagent", "subagent sessions are not supported")
			}
		}
	}
	cwd, ok := payload["cwd"].(string)
	if !ok || cwd == "" {
		return bad("cwd", "session metadata cwd must be a nonempty string")
	}
	out.RecordedCWD = cwd
	out.LatestCWD = cwd
	if value := payload["runtime_workspace_roots"]; value != nil {
		roots, ok := stringList(value)
		if !ok {
			return bad("workspace_roots", "runtime workspace roots must be strings")
		}
		out.RuntimeWorkspaceRoots = roots
	}
	if provider, ok := payload["model_provider"].(string); ok {
		out.ModelProvider = provider
	}
	return nil
}

func under(cwd, root string) bool {
	if !filepath.IsAbs(cwd) || !filepath.IsAbs(root) {
		return false
	}
	relative, err := filepath.Rel(cwd, root)
	return err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}
func walkWarnings(value any, ordinal uint64, warn func(string, string, uint64)) {
	switch value := value.(type) {
	case map[string]any:
		for key, child := range value {
			if key == "encrypted_content" && child != nil {
				warn("encrypted_content", "encrypted content may be bound to its original provider", ordinal)
			}
			if key == "repository_url" {
				if raw, ok := child.(string); ok {
					if parsed, err := url.Parse(raw); err == nil && parsed.User != nil {
						warn("repository_url_userinfo", "repository URL contains user information", ordinal)
					}
				}
			}
			walkWarnings(child, ordinal, warn)
		}
	case []any:
		for _, child := range value {
			walkWarnings(child, ordinal, warn)
		}
	}
}
