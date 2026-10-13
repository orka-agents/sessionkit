//go:build native

package codex_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/orka-agents/sessionkit"
)

const providerStatePrompt = "SESSIONKIT_PROVIDER_STATE_PROMPT_5c1e: reason, then reply."
const providerStateReply = "SESSIONKIT_PROVIDER_STATE_REPLY_7d02"

// A fixed base64-like value; the native client must replay it unchanged.
const providerStateBlob = "gAAAAABpSESSIONKIT_PROVIDER_STATE_9f31+opaque/bytes=="

func TestNativeProviderStateHandoff(t *testing.T) {
	ctx := context.Background()
	s := newStub(t)
	home, workspace, path, original, id := fixtureHome(t, "basic", s)
	reasoning := map[string]any{"type": "reasoning", "id": "rs_sessionkit", "summary": []any{map[string]any{"type": "summary_text", "text": "SessionKit reasoning summary."}}, "encrypted_content": providerStateBlob}
	s.enqueue(responseTurn{items: []map[string]any{reasoning, assistant(providerStateReply).items[0]}})
	runExec(t, home, workspace, id, providerStatePrompt)
	stopped := read(t, path)
	if !bytes.HasPrefix(stopped, original) {
		t.Fatal("native turn rewrote the original rollout")
	}
	var reasoningOrdinal uint64
	for _, line := range decodeLines(t, stopped) {
		p, _ := line["payload"].(map[string]any)
		if line["type"] == "response_item" && p["type"] == "reasoning" {
			if reasoningOrdinal != 0 || p["encrypted_content"] != providerStateBlob {
				t.Fatal("native client did not persist exactly one scripted reasoning item")
			}
			reasoningOrdinal = uint64(line["ordinal"].(float64))
		}
	}
	if reasoningOrdinal == 0 {
		t.Fatal("native client did not persist the reasoning item")
	}
	preserveSource(t, path, stopped)

	source := sessionkit.Source{Harness: sessionkit.Codex, Root: home, ThreadID: id}
	rejectedDir := filepath.Join(tempDir(t), "rejected-bundle")
	_, err := sessionkit.Capture(ctx, source, sessionkit.CaptureOptions{BundleDir: rejectedDir})
	var rejected *sessionkit.RejectionError
	if !errors.As(err, &rejected) || len(rejected.Rejections) != 1 || rejected.Rejections[0].Code != "encrypted_content" || rejected.Rejections[0].Ordinal != reasoningOrdinal {
		t.Fatalf("capture without the opt-in was not rejected at ordinal %d: %v", reasoningOrdinal, err)
	}
	if _, err := os.Stat(rejectedDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("rejected capture published a bundle: %v", err)
	}

	source.AllowProviderState = true
	bundle, err := sessionkit.Capture(ctx, source, sessionkit.CaptureOptions{BundleDir: filepath.Join(tempDir(t), "bundle")})
	must(t, err)
	want := sessionkit.ProviderStateSummary{ReasoningItems: 1}
	if !bundle.Manifest.AllowProviderState || bundle.Manifest.ProviderState != want {
		t.Fatalf("manifest provider state: allow=%t %+v", bundle.Manifest.AllowProviderState, bundle.Manifest.ProviderState)
	}
	opened, err := sessionkit.OpenBundle(ctx, bundle.Dir, sessionkit.Budget{})
	must(t, err)
	destinationHome, destinationWorkspace := tempDir(t), tempDir(t)
	configHome(t, destinationHome, s)
	dst := destination(t, destinationHome, destinationWorkspace)
	_, err = sessionkit.PlanInstall(ctx, opened, dst)
	if !errors.As(err, &rejected) || len(rejected.Rejections) != 1 || rejected.Rejections[0].Code != "provider_state" {
		t.Fatalf("destination without the opt-in accepted provider state: %v", err)
	}
	dst.AllowProviderState = true
	plan, installedPath := installBundle(t, opened, dst)
	if plan.ProviderState != want {
		t.Fatalf("plan provider state: %+v", plan.ProviderState)
	}
	if !bytes.Equal(read(t, installedPath), stopped) {
		t.Fatal("install rewrote the provider state rollout")
	}

	const reply = "SESSIONKIT_REPLY_AFTER_PROVIDER_STATE_e4a0"
	const prompt = "SESSIONKIT_RESUME_WITH_PROVIDER_STATE: continue in the destination."
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
		t.Fatalf("source turn and resume made %d requests, want 2", len(requests))
	}
	var expected []map[string]any
	for _, line := range decodeLines(t, stopped) {
		p, _ := line["payload"].(map[string]any)
		if line["type"] != "response_item" {
			continue
		}
		if p["type"] == "function_call" || p["type"] == "function_call_output" || p["type"] == "reasoning" || p["role"] == "assistant" || p["role"] == "user" && (strings.Contains(string(marshal(t, p)), fixturePrompt) || strings.Contains(string(marshal(t, p)), providerStatePrompt)) {
			expected = append(expected, p)
		}
	}
	if len(expected) != 7 {
		t.Fatalf("history must have user, call, output, assistant, user, reasoning, assistant; got %d", len(expected))
	}
	expected = append(expected, map[string]any{"type": "message", "role": "user", "content": []any{map[string]any{"type": "input_text", "text": prompt}}})
	assertHistoryItems(t, requests[1], expected)
	input, _ := requests[1]["input"].([]any)
	var replayed []map[string]any
	for _, raw := range input {
		if item, _ := raw.(map[string]any); item["type"] == "reasoning" {
			replayed = append(replayed, item)
		}
	}
	if len(replayed) != 1 || replayed[0]["encrypted_content"] != providerStateBlob {
		t.Fatal("resumed provider request did not replay the reasoning encrypted content unchanged")
	}
	assertAppended(t, read(t, installedPath), stopped, reply, destinationWorkspace, true)
}
