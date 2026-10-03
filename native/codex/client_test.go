//go:build native

package codex_test

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

var codexBinary string

func TestMain(m *testing.M) {
	codexBinary = os.Getenv("SESSIONKIT_CODEX_BIN")
	if codexBinary == "" {
		fmt.Fprintln(os.Stderr, "SESSIONKIT_CODEX_BIN is required; run scripts/install-codex.sh")
		os.Exit(1)
	}
	resolved, err := filepath.Abs(codexBinary)
	if err == nil {
		resolved, err = filepath.EvalSymlinks(resolved)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	codexBinary = resolved
	want := map[string]string{
		"linux/amd64":  "12eb3e81114588aca3b7998f4f19e8997b056aca08e57a7ca7c8a3ec8c652aad",
		"linux/arm64":  "50b06603bdcdac39b714f5c3e68583c002b8ad8779ebfdaaf4932ff016b379c0",
		"darwin/amd64": "5383ef71dd1bd8d2f3658c04a219e2cf165c7969aebd0cceced1bc9f0f68877f",
		"darwin/arm64": "112fae7a5a1223e673c8a1791d32338f37df8b527ff1159bb8adac6c4dbf1b4b",
	}[runtime.GOOS+"/"+runtime.GOARCH]
	f, err := os.Open(codexBinary)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	h := sha256.New()
	_, err = io.Copy(h, f)
	_ = f.Close()
	if err != nil || want == "" || hex.EncodeToString(h.Sum(nil)) != want {
		fmt.Fprintln(os.Stderr, "SESSIONKIT_CODEX_BIN digest does not match pinned Codex 0.160.0 for this platform")
		os.Exit(1)
	}
	os.Exit(m.Run())
}

func configHome(t *testing.T, home string, s *stubProvider) {
	t.Helper()
	must(t, os.MkdirAll(home, 0700))
	config := fmt.Sprintf(`model = "gpt-5.4"
model_provider = "stub"
approval_policy = "never"
sandbox_mode = "danger-full-access"
check_for_update_on_startup = false
model_auto_compact_token_limit = 1000000
web_search = "disabled"
[features]
shell_snapshot = false
[model_providers.stub]
name = "SessionKit test provider"
base_url = %q
env_key = "SESSIONKIT_TEST_API_KEY"
wire_api = "responses"
requires_openai_auth = false
supports_websockets = false
request_max_retries = 0
stream_max_retries = 0
`, s.server.URL+"/v1")
	must(t, os.WriteFile(filepath.Join(home, "config.toml"), []byte(config), 0600))
}

func clientEnvironment(home string) []string {
	var result []string
	for _, value := range os.Environ() {
		key, _, _ := strings.Cut(value, "=")
		if key == "HOME" || strings.HasPrefix(key, "CODEX_") || strings.HasPrefix(key, "OPENAI_") || strings.HasPrefix(key, "AZURE_") {
			continue
		}
		result = append(result, value)
	}
	return append(result, "HOME="+home, "CODEX_HOME="+home, "SESSIONKIT_TEST_API_KEY=sessionkit-fake-key", "NO_COLOR=1")
}

func runExec(t *testing.T, home, cwd, id, prompt string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	args := []string{"exec", "--skip-git-repo-check", "--dangerously-bypass-approvals-and-sandbox", "--json", "-C", cwd}
	if id != "" {
		args = append(args, "resume", id)
	}
	args = append(args, prompt)
	cmd := exec.CommandContext(ctx, codexBinary, args...)
	cmd.Env = clientEnvironment(home)
	cmd.Dir = cwd
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("codex exec: %v\n%s", err, output)
	}
}

type appClient struct {
	t      *testing.T
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	lines  chan map[string]any
	stderr bytes.Buffer
	nextID int
	cancel context.CancelFunc
	closed bool
}

func newApp(t *testing.T, home, cwd string) *appClient {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	a := &appClient{t: t, lines: make(chan map[string]any, 512), cancel: cancel}
	a.cmd = exec.CommandContext(ctx, codexBinary, "app-server", "--listen", "stdio://")
	a.cmd.Env = clientEnvironment(home)
	a.cmd.Dir = cwd
	a.cmd.Stderr = &a.stderr
	var err error
	a.stdin, err = a.cmd.StdinPipe()
	must(t, err)
	stdout, err := a.cmd.StdoutPipe()
	must(t, err)
	must(t, a.cmd.Start())
	go func() {
		defer close(a.lines)
		scanner := bufio.NewScanner(stdout)
		scanner.Buffer(make([]byte, 4096), 16<<20)
		for scanner.Scan() {
			var msg map[string]any
			if json.Unmarshal(scanner.Bytes(), &msg) == nil {
				a.lines <- msg
			}
		}
	}()
	t.Cleanup(a.close)
	a.call("initialize", map[string]any{"clientInfo": map[string]any{"name": "sessionkit_native_gate", "version": "1"}, "capabilities": map[string]any{"experimentalApi": true}})
	a.send(map[string]any{"method": "initialized"})
	return a
}

func (a *appClient) send(msg map[string]any) {
	a.t.Helper()
	b, err := json.Marshal(msg)
	must(a.t, err)
	_, err = a.stdin.Write(append(b, '\n'))
	must(a.t, err)
}

func (a *appClient) next() map[string]any {
	a.t.Helper()
	select {
	case msg, ok := <-a.lines:
		if !ok {
			a.t.Fatalf("app-server closed: %s", a.stderr.String())
		}
		return msg
	case <-time.After(60 * time.Second):
		a.t.Fatalf("app-server response timeout: %s", a.stderr.String())
		return nil
	}
}

func (a *appClient) call(method string, params map[string]any) map[string]any {
	a.t.Helper()
	a.nextID++
	id := a.nextID
	a.send(map[string]any{"id": id, "method": method, "params": params})
	for {
		msg := a.next()
		if msg["id"] != float64(id) {
			continue
		}
		if msg["error"] != nil {
			a.t.Fatalf("%s: %v", method, msg["error"])
		}
		result, _ := msg["result"].(map[string]any)
		return result
	}
}

func (a *appClient) waitTurn() {
	a.t.Helper()
	for {
		msg := a.next()
		if msg["method"] == "turn/completed" {
			params, _ := msg["params"].(map[string]any)
			turn, _ := params["turn"].(map[string]any)
			if turn["status"] != "completed" {
				a.t.Fatalf("turn did not complete: %v", turn)
			}
			return
		}
	}
}

func (a *appClient) close() {
	if a.closed {
		return
	}
	a.closed = true
	_ = a.stdin.Close()
	done := make(chan error, 1)
	go func() { done <- a.cmd.Wait() }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		a.cancel()
		<-done
	}
	a.cancel()
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func read(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	must(t, err)
	return b
}

func tempDir(t *testing.T) string {
	t.Helper()
	path, err := filepath.EvalSymlinks(t.TempDir())
	must(t, err)
	return path
}

func findRollout(t *testing.T, home string) string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(home, "sessions", "*", "*", "*", "rollout-*.jsonl"))
	must(t, err)
	if len(matches) != 1 {
		t.Fatalf("expected one rollout, found %v", matches)
	}
	return matches[0]
}

func decodeLines(t *testing.T, b []byte) []map[string]any {
	t.Helper()
	var result []map[string]any
	for _, line := range bytes.Split(bytes.TrimSpace(b), []byte{'\n'}) {
		var v map[string]any
		must(t, json.Unmarshal(line, &v))
		result = append(result, v)
	}
	return result
}
