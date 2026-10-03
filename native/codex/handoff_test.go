//go:build native

package codex_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/orka-agents/sessionkit"
)

func TestNativeHandoff(t *testing.T) {
	ctx := context.Background()
	s := newStub(t)
	homeA, workspaceA, sourcePath, original, id := fixtureHome(t, "basic", s)
	source := sessionkit.Source{Harness: sessionkit.Codex, Root: homeA, ThreadID: id}
	before, err := os.Stat(sourcePath)
	must(t, err)
	bundle, err := sessionkit.Capture(ctx, source, sessionkit.CaptureOptions{BundleDir: filepath.Join(tempDir(t), "bundle")})
	must(t, err)
	if !bytes.Equal(read(t, sourcePath), original) {
		t.Fatal("capture changed source")
	}
	after, err := os.Stat(sourcePath)
	must(t, err)
	if !before.ModTime().Equal(after.ModTime()) {
		t.Fatal("capture changed source mtime")
	}
	opened, err := sessionkit.OpenBundle(ctx, bundle.Dir, sessionkit.Budget{})
	must(t, err)

	homeB, workspaceB := tempDir(t), tempDir(t)
	configHome(t, homeB, s)
	dstB := destination(t, homeB, workspaceB)
	planB, pathB := installBundle(t, opened, dstB)
	if !bytes.Equal(read(t, pathB), original) {
		t.Fatal("install rewrote fixture")
	}
	if _, err := os.Stat(filepath.Join(homeB, "state_5.sqlite")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("library created native state database: %v", err)
	}

	const replyB = "SESSIONKIT_REPLY_FROM_B_112037"
	s.enqueue(assistant(replyB))
	a := newApp(t, homeB, workspaceB)
	params := map[string]any{}
	for k, v := range planB.ResumeHints.AppServerParams {
		params[k] = v
	}
	params["modelProvider"] = "stub"
	params["approvalPolicy"] = "never"
	params["approvalsReviewer"] = "user"
	params["sandbox"] = "danger-full-access"
	resumed := a.call("thread/resume", params)
	assertEffectiveSettings(t, resumed, workspaceB)
	a.call("turn/start", map[string]any{"threadId": id, "input": []any{map[string]any{"type": "text", "text": "Continue in workspace B."}}})
	a.waitTurn()
	a.close()
	requests := s.recorded()
	if len(requests) != 1 {
		t.Fatalf("first resume made %d requests", len(requests))
	}
	assertOriginalHistory(t, requests[0], original)
	assertAppended(t, read(t, pathB), original, replyB, workspaceB, true)
	assertStateRow(t, homeB, id, pathB)

	const replyRestart = "SESSIONKIT_REPLY_AFTER_RESTART_f36e"
	s.enqueue(assistant(replyRestart))
	runExec(t, homeB, workspaceB, id, "Continue after restarting Codex.")
	requests = s.recorded()
	assertContains(t, requests[len(requests)-1], replyB)
	assertAppended(t, read(t, pathB), original, replyRestart, workspaceB, false)

	returned, err := sessionkit.Capture(ctx, sessionkit.Source{Harness: sessionkit.Codex, Root: homeB, ThreadID: id}, sessionkit.CaptureOptions{BundleDir: filepath.Join(tempDir(t), "return-bundle")})
	must(t, err)
	homeC := tempDir(t)
	configHome(t, homeC, s)
	_, pathC := installBundle(t, returned, destination(t, homeC, workspaceA))
	s.enqueue(assistant("SESSIONKIT_REPLY_FROM_C"))
	runExec(t, homeC, workspaceA, id, "Continue after returning beside the original workspace.")
	requests = s.recorded()
	assertContains(t, requests[len(requests)-1], replyB)
	assertContains(t, requests[len(requests)-1], replyRestart)
	if !bytes.Contains(read(t, pathC), []byte("SESSIONKIT_REPLY_FROM_C")) {
		t.Fatal("returned reply not persisted")
	}
	if !bytes.Equal(read(t, sourcePath), original) {
		t.Fatal("round trip changed home A")
	}

	_, err = sessionkit.PlanInstall(ctx, returned, destination(t, homeA, workspaceA))
	var collision *sessionkit.CollisionError
	if !errors.As(err, &collision) {
		t.Fatalf("expected source collision, got %v", err)
	}
	if !bytes.Equal(read(t, sourcePath), original) {
		t.Fatal("collision changed source")
	}

	// exec resume appends a turn. thread/read proves native UUID resolution in A
	// without violating the source-unchanged assertion.
	a = newApp(t, homeA, workspaceA)
	result := a.call("thread/read", map[string]any{"threadId": id, "includeTurns": true})
	thread, _ := result["thread"].(map[string]any)
	if thread["id"] != id {
		t.Fatalf("home A native UUID resolution: %v", thread["id"])
	}
	if thread["path"] != sourcePath || thread["preview"] != fixturePrompt || thread["historyMode"] != "paginated" {
		t.Fatal("home A did not resolve the original paginated rollout")
	}
	a.close()
	if !bytes.Equal(read(t, sourcePath), original) {
		t.Fatal("native read changed source")
	}

	t.Run("active writer", func(t *testing.T) {
		entered, release := make(chan struct{}), make(chan struct{})
		turn := assistant("SESSIONKIT_WRITER_RELEASED")
		turn.entered = entered
		turn.release = release
		s.enqueue(turn)
		a := newApp(t, homeB, workspaceB)
		a.call("thread/resume", map[string]any{"threadId": id, "cwd": workspaceB, "runtimeWorkspaceRoots": []string{workspaceB}, "approvalPolicy": "never", "sandbox": "danger-full-access", "modelProvider": "stub"})
		a.call("turn/start", map[string]any{"threadId": id, "input": []any{map[string]any{"type": "text", "text": "Hold the writer lock until the test releases the provider."}}})
		select {
		case <-entered:
		case <-time.After(30 * time.Second):
			t.Fatal("client did not reach provider")
		}
		_, captureErr := sessionkit.Capture(ctx, sessionkit.Source{Harness: sessionkit.Codex, Root: homeB, ThreadID: id}, sessionkit.CaptureOptions{BundleDir: filepath.Join(tempDir(t), "active-bundle")})
		close(release)
		a.waitTurn()
		a.close()
		var active *sessionkit.ActiveWriterError
		if !errors.As(captureErr, &active) {
			t.Fatalf("expected native active writer rejection, got %v", captureErr)
		}
	})
}

func TestNativeCompaction(t *testing.T) {
	s := newStub(t)
	home, _, _, original, id := fixtureHome(t, "compacted", s)
	bundle, err := sessionkit.Capture(context.Background(), sessionkit.Source{Harness: sessionkit.Codex, Root: home, ThreadID: id}, sessionkit.CaptureOptions{BundleDir: filepath.Join(tempDir(t), "bundle")})
	must(t, err)
	if !bundle.Manifest.Inspection.Compaction.NewestComplete {
		t.Fatal("inspection did not report complete compaction")
	}
	destinationHome, workspace := tempDir(t), tempDir(t)
	configHome(t, destinationHome, s)
	_, path := installBundle(t, bundle, destination(t, destinationHome, workspace))
	s.enqueue(assistant("SESSIONKIT_REPLY_AFTER_COMPACTION"))
	runExec(t, destinationHome, workspace, id, "Continue the compacted session.")
	requests := s.recorded()
	if len(requests) != 1 {
		t.Fatalf("compaction resume requests = %d", len(requests))
	}
	assertContains(t, requests[0], compactionSummary)
	var replacement []map[string]any
	for _, line := range decodeLines(t, original) {
		if line["type"] != "compacted" {
			continue
		}
		payload, _ := line["payload"].(map[string]any)
		replacement = nil
		for _, item := range payload["replacement_history"].([]any) {
			replacement = append(replacement, item.(map[string]any))
		}
	}
	if len(replacement) == 0 {
		t.Fatal("fixture has no replacement history")
	}
	assertHistoryItems(t, requests[0], replacement)
	input, _ := requests[0]["input"].([]any)
	if len(input) == 0 {
		t.Fatal("no resumed model history")
	}
	for _, raw := range input {
		item, _ := raw.(map[string]any)
		if item["type"] == "function_call" || item["type"] == "function_call_output" {
			t.Fatal("native loader replayed pre-compaction tool items")
		}
		if strings.Contains(string(marshal(t, item)), fixtureReply) {
			t.Fatal("native loader replayed pre-compaction assistant reply")
		}
	}
	assertAppended(t, read(t, path), original, "SESSIONKIT_REPLY_AFTER_COMPACTION", workspace, false)
}

func TestNativeLegacyRejectedBeforeClient(t *testing.T) {
	s := newStub(t)
	home, _, path, b, id := fixtureHome(t, "basic", s)
	b = bytes.Replace(b, []byte(`"history_mode":"paginated"`), []byte(`"history_mode":"legacy"`), 1)
	must(t, os.WriteFile(path, b, 0600))
	_, err := sessionkit.Capture(context.Background(), sessionkit.Source{Harness: sessionkit.Codex, Root: home, ThreadID: id}, sessionkit.CaptureOptions{BundleDir: filepath.Join(tempDir(t), "bundle")})
	var rejected *sessionkit.RejectionError
	if !errors.As(err, &rejected) {
		t.Fatalf("legacy profile accepted: %v", err)
	}
	if len(s.recorded()) != 0 {
		t.Fatal("legacy rejection contacted native provider")
	}
}

func fixtureHome(t *testing.T, name string, s *stubProvider) (home, cwd, path string, b []byte, id string) {
	t.Helper()
	fixtureRoot := filepath.Join("..", "..", "harness", "codex", "testdata", name)
	originalPath := findRollout(t, fixtureRoot)
	b = read(t, originalPath)
	rel, err := filepath.Rel(fixtureRoot, originalPath)
	must(t, err)
	home, cwd = tempDir(t), tempDir(t)
	configHome(t, home, s)
	path = filepath.Join(home, rel)
	must(t, os.MkdirAll(filepath.Dir(path), 0700))
	must(t, os.WriteFile(path, b, 0600))
	id = decodeLines(t, b)[0]["payload"].(map[string]any)["id"].(string)
	return
}

func destination(t *testing.T, home, cwd string) sessionkit.Destination {
	return sessionkit.Destination{Harness: sessionkit.Codex, Root: home, WorkingDir: cwd, JournalDir: tempDir(t)}
}

func installBundle(t *testing.T, b sessionkit.Bundle, d sessionkit.Destination) (sessionkit.Plan, string) {
	t.Helper()
	p, err := sessionkit.PlanInstall(context.Background(), b, d)
	must(t, err)
	r, err := sessionkit.Install(context.Background(), p)
	must(t, err)
	if r.Outcome != sessionkit.Installed {
		t.Fatalf("install outcome: %s", r.Outcome)
	}
	v, err := sessionkit.Verify(context.Background(), r, d)
	must(t, err)
	if !v.Valid {
		t.Fatal("installed file verification failed")
	}
	return p, filepath.Join(d.Root, p.TargetPath)
}

func assertEffectiveSettings(t *testing.T, v map[string]any, cwd string) {
	t.Helper()
	for key, want := range map[string]any{"cwd": cwd, "modelProvider": "stub", "approvalPolicy": "never", "approvalsReviewer": "user"} {
		if v[key] != want {
			t.Fatalf("effective %s = %v, want %v", key, v[key], want)
		}
	}
	if !reflect.DeepEqual(v["runtimeWorkspaceRoots"], []any{cwd}) {
		t.Fatalf("effective runtime roots: %v", v["runtimeWorkspaceRoots"])
	}
	sandbox, _ := v["sandbox"].(map[string]any)
	if sandbox["type"] != "dangerFullAccess" {
		t.Fatalf("effective sandbox: %v", sandbox)
	}
}

func assertOriginalHistory(t *testing.T, request map[string]any, fixture []byte) {
	t.Helper()
	var expected []map[string]any
	for _, line := range decodeLines(t, fixture) {
		if line["type"] != "response_item" {
			continue
		}
		p := line["payload"].(map[string]any)
		if p["type"] == "function_call" || p["type"] == "function_call_output" || p["role"] == "assistant" || (p["role"] == "user" && strings.Contains(string(marshal(t, p)), fixturePrompt)) {
			expected = append(expected, p)
		}
	}
	if len(expected) != 4 {
		t.Fatalf("fixture must have user, call, output, assistant; got %d", len(expected))
	}
	assertHistoryItems(t, request, expected)
}

func assertHistoryItems(t *testing.T, request map[string]any, expected []map[string]any) {
	t.Helper()
	input, _ := request["input"].([]any)
	cursor := 0
	for _, want := range expected {
		count := 0
		for _, raw := range input {
			got, _ := raw.(map[string]any)
			if sameHistoryItem(got, want) {
				count++
			}
		}
		if count != 1 {
			t.Fatalf("native history has %d copies of %v %v, want 1", count, want["type"], want["role"])
		}
		found := false
		for cursor < len(input) {
			got, _ := input[cursor].(map[string]any)
			cursor++
			if sameHistoryItem(got, want) {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("native history lost or reordered %v %v", want["type"], want["role"])
		}
	}
}

func sameHistoryItem(got, want map[string]any) bool {
	for _, key := range []string{"type", "role", "name", "call_id", "arguments", "output", "content"} {
		if !reflect.DeepEqual(got[key], want[key]) {
			return false
		}
	}
	return true
}

func assertAppended(t *testing.T, b, original []byte, reply, cwd string, settingsRequired bool) {
	t.Helper()
	if !bytes.HasPrefix(b, original) {
		t.Fatal("native resume changed original rollout prefix")
	}
	prior := decodeLines(t, original)
	last := prior[len(prior)-1]["ordinal"].(float64)
	appended := decodeLines(t, b[len(original):])
	sawReply, sawContext, sawSettings := false, false, false
	for _, line := range appended {
		ordinal, ok := line["ordinal"].(float64)
		if !ok || ordinal <= last {
			t.Fatalf("appended ordinal did not increase: %v after %v", line["ordinal"], last)
		}
		last = ordinal
		p, _ := line["payload"].(map[string]any)
		if line["type"] == "response_item" && p["role"] == "assistant" && strings.Contains(string(marshal(t, p)), reply) {
			sawReply = true
		}
		sandbox, _ := p["sandbox_policy"].(map[string]any)
		if line["type"] == "turn_context" && p["cwd"] == cwd && p["approval_policy"] == "never" && p["approvals_reviewer"] == "user" && sandbox["type"] == "danger-full-access" && reflect.DeepEqual(p["workspace_roots"], []any{cwd}) {
			sawContext = true
		}
		if p["type"] == "thread_settings_applied" {
			settings, _ := p["thread_settings"].(map[string]any)
			permission, _ := settings["permission_profile"].(map[string]any)
			if settings["cwd"] == cwd && settings["approval_policy"] == "never" && settings["model_provider_id"] == "stub" && settings["approvals_reviewer"] == "user" && permission["type"] == "disabled" && reflect.DeepEqual(settings["runtime_workspace_roots"], []any{cwd}) {
				sawSettings = true
			}
		}
	}
	if !sawReply || !sawContext || settingsRequired && !sawSettings {
		t.Fatalf("missing appended native data: reply=%v context=%v settings=%v", sawReply, sawContext, sawSettings)
	}
}

func assertStateRow(t *testing.T, home, id, path string) {
	t.Helper()
	// Read-only CLI use is limited to the native gate. The library never opens SQLite.
	query := fmt.Sprintf("SELECT history_mode || '|' || rollout_path FROM threads WHERE id = '%s';", id)
	out, err := exec.Command("sqlite3", "-readonly", filepath.Join(home, "state_5.sqlite"), query).CombinedOutput()
	must(t, err)
	if strings.TrimSpace(string(out)) != "paginated|"+path {
		t.Fatalf("native state row: %q", out)
	}
}

func marshal(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	must(t, err)
	return b
}

func assertContains(t *testing.T, v any, s string) {
	t.Helper()
	if !bytes.Contains(marshal(t, v), []byte(s)) {
		t.Fatalf("native request/result missing %q", s)
	}
}

// Keep a fixture checksum assertion independent of the bundle implementation.
func TestFixtureBytes(t *testing.T) {
	for _, name := range []string{"basic", "compacted"} {
		p := findRollout(t, filepath.Join("..", "..", "harness", "codex", "testdata", name))
		b := read(t, p)
		if !bytes.HasSuffix(b, []byte{'\n'}) {
			t.Fatal("fixture has incomplete final line")
		}
		t.Logf("%s sha256=%x", name, sha256.Sum256(b))
	}
}
