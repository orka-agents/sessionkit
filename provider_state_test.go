package sessionkit

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

const testProviderState = "gAAAAABsessionkit+opaque/provider=state"

// withReasoning appends a native reasoning item that carries provider state.
func withReasoning(t *testing.T, raw []byte) []byte {
	t.Helper()
	lines := bytes.Split(bytes.TrimSuffix(raw, []byte{'\n'}), []byte{'\n'})
	var last struct {
		Ordinal uint64 `json:"ordinal"`
	}
	if err := json.Unmarshal(lines[len(lines)-1], &last); err != nil {
		t.Fatal(err)
	}
	record, err := json.Marshal(map[string]any{"timestamp": "2026-10-03T04:57:45.000Z", "ordinal": last.Ordinal + 1, "type": "response_item",
		"payload": map[string]any{"type": "reasoning", "id": "rs_sessionkit", "summary": []any{}, "encrypted_content": testProviderState}})
	if err != nil {
		t.Fatal(err)
	}
	return append(append(bytes.Clone(raw), record...), '\n')
}

func providerStateSource(t *testing.T) (Source, string, []byte) {
	t.Helper()
	src, rel, raw := testSource(t)
	raw = withReasoning(t, raw)
	if err := os.WriteFile(filepath.Join(src.Root, rel), raw, 0600); err != nil {
		t.Fatal(err)
	}
	return src, rel, raw
}

func assertCode(t *testing.T, err error, code string) {
	t.Helper()
	var rejected *RejectionError
	if !errors.As(err, &rejected) || len(rejected.Rejections) != 1 || rejected.Rejections[0].Code != code {
		t.Fatalf("want rejection %s, got %v", code, err)
	}
}

func assertEmpty(t *testing.T, dirs ...string) {
	t.Helper()
	for _, dir := range dirs {
		entries, err := os.ReadDir(dir)
		if err != nil || len(entries) != 0 {
			t.Fatalf("%s changed: %v %v", dir, entries, err)
		}
	}
}

func TestProviderStateRequiresBothOptIns(t *testing.T) {
	ctx := context.Background()
	src, rel, raw := providerStateSource(t)
	_, err := Inspect(ctx, src, Budget{})
	assertCode(t, err, "encrypted_content")
	dir := filepath.Join(tempDir(t), "bundle")
	_, err = Capture(ctx, src, CaptureOptions{BundleDir: dir})
	assertCode(t, err, "encrypted_content")
	if _, err = os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("rejected capture left a bundle: %v", err)
	}

	src.AllowProviderState = true
	want := ProviderStateSummary{ReasoningItems: 1}
	in, err := Inspect(ctx, src, Budget{})
	if err != nil || in.ProviderState != want {
		t.Fatalf("inspection: %+v %v", in.ProviderState, err)
	}
	bundle, err := Capture(ctx, src, CaptureOptions{BundleDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	m := bundle.Manifest
	if !m.AllowProviderState || m.ProviderState != want || m.Inspection.ProviderState != want {
		t.Fatalf("manifest: allow=%t %+v %+v", m.AllowProviderState, m.ProviderState, m.Inspection.ProviderState)
	}
	if !bytes.Equal(raw, mustRead(t, filepath.Join(dir, "components", "rollout.jsonl"))) {
		t.Fatal("captured bytes changed")
	}
	opened, err := OpenBundle(ctx, dir, Budget{})
	if err != nil {
		t.Fatal(err)
	}

	dst := testDestination(t)
	_, err = PlanInstall(ctx, opened, dst)
	assertCode(t, err, "provider_state")
	assertEmpty(t, dst.Root, dst.JournalDir)
	dst.AllowProviderState = true
	plan, err := PlanInstall(ctx, opened, dst)
	if err != nil {
		t.Fatal(err)
	}
	if plan.ProviderState != want {
		t.Fatalf("plan provider state: %+v", plan.ProviderState)
	}

	encoded, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	var decoded Plan
	if err = json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	reencoded, err := json.Marshal(decoded)
	if err != nil || !bytes.Equal(encoded, reencoded) || !decoded.destination.AllowProviderState || decoded.ProviderState != want {
		t.Fatalf("plan round trip: %v", err)
	}
	for _, edit := range [][2]string{{`"allowProviderState":true`, `"allowProviderState":false`}, {`"reasoningItems":1`, `"reasoningItems":0`}} {
		if bytes.Count(encoded, []byte(edit[0])) != 1 {
			t.Fatalf("plan JSON lacks %s", edit[0])
		}
		var altered Plan
		var integrity *IntegrityError
		if err := json.Unmarshal(bytes.Replace(encoded, []byte(edit[0]), []byte(edit[1]), 1), &altered); !errors.As(err, &integrity) {
			t.Fatalf("plan seal ignored %s: %v", edit[0], err)
		}
	}
	receipt, err := Install(ctx, decoded)
	if err != nil || receipt.Outcome != Installed {
		t.Fatalf("install: %+v %v", receipt, err)
	}
	if !bytes.Equal(raw, mustRead(t, filepath.Join(dst.Root, rel))) {
		t.Fatal("installed bytes changed")
	}
}

func TestOpenBundleReappliesCapturePolicy(t *testing.T) {
	ctx := context.Background()
	t.Run("manifest policy removed", func(t *testing.T) {
		src, _, _ := providerStateSource(t)
		src.AllowProviderState = true
		bundle, err := Capture(ctx, src, CaptureOptions{BundleDir: filepath.Join(tempDir(t), "bundle")})
		if err != nil {
			t.Fatal(err)
		}
		manifest := filepath.Join(bundle.Dir, "manifest.json")
		data := mustRead(t, manifest)
		if bytes.Count(data, []byte(`"allowProviderState": true`)) != 1 {
			t.Fatal("manifest does not record the capture policy")
		}
		data = bytes.Replace(data, []byte(`"allowProviderState": true`), []byte(`"allowProviderState": false`), 1)
		if err = os.WriteFile(manifest, data, 0600); err != nil {
			t.Fatal(err)
		}
		_, err = OpenBundle(ctx, bundle.Dir, Budget{})
		assertCode(t, err, "encrypted_content")
	})
	t.Run("rollout gained provider state", func(t *testing.T) {
		_, bundle, _, raw := testBundle(t)
		if err := os.WriteFile(filepath.Join(bundle.Dir, "components", "rollout.jsonl"), withReasoning(t, raw), 0600); err != nil {
			t.Fatal(err)
		}
		_, err := OpenBundle(ctx, bundle.Dir, Budget{})
		assertCode(t, err, "encrypted_content")
		dst := testDestination(t)
		dst.AllowProviderState = true
		_, err = PlanInstall(ctx, bundle, dst)
		assertCode(t, err, "encrypted_content")
		assertEmpty(t, dst.Root, dst.JournalDir)
	})
	t.Run("opt-in without provider state", func(t *testing.T) {
		src, _, _ := testSource(t)
		src.AllowProviderState = true
		bundle, err := Capture(ctx, src, CaptureOptions{BundleDir: filepath.Join(tempDir(t), "bundle")})
		if err != nil {
			t.Fatal(err)
		}
		if !bundle.Manifest.AllowProviderState || bundle.Manifest.ProviderState.Present() {
			t.Fatalf("manifest: %+v", bundle.Manifest)
		}
		if _, err = PlanInstall(ctx, bundle, testDestination(t)); err != nil {
			t.Fatalf("bundle without provider state required a destination opt-in: %v", err)
		}
	})
}

func TestInstallRechecksDestinationProviderState(t *testing.T) {
	ctx := context.Background()
	src, rel, _ := providerStateSource(t)
	src.AllowProviderState = true
	bundle, err := Capture(ctx, src, CaptureOptions{BundleDir: filepath.Join(tempDir(t), "bundle")})
	if err != nil {
		t.Fatal(err)
	}
	dst := testDestination(t)
	dst.AllowProviderState = true
	plan, err := PlanInstall(ctx, bundle, dst)
	if err != nil {
		t.Fatal(err)
	}
	// Install enforces the rule even when a plan was resealed without the opt-in.
	plan.destination.AllowProviderState = false
	plan.seal = planSeal(plan)
	receipt, err := Install(ctx, plan)
	assertCode(t, err, "provider_state")
	if receipt.Outcome != RejectedBeforeMutation {
		t.Fatalf("receipt: %+v", receipt)
	}
	if _, err = os.Stat(filepath.Join(dst.Root, rel)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("rejected install published: %v", err)
	}
}
