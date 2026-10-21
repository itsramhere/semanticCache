package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"semanticcache/internal/semanticcache"
)

func TestCLI(t *testing.T) {
	cases := []struct {
		name     string
		args     []string
		code     int
		contains string
	}{
		{"help", nil, 0, "Phase 1"},
		{"valid", []string{"validate-config", "-config", "../../configs/experiment.json"}, 0, "phase0-faq-v1"},
		{"missing", []string{"validate-config", "-config", "missing.json"}, 1, ""},
		{"later phase", []string{"replay"}, 2, "unknown command"},
		{"extra argument", []string{"validate-config", "extra"}, 2, "unexpected positional"},
		{"bad flag", []string{"validate-config", "-typo"}, 2, "flag provided but not defined"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var out, errOut bytes.Buffer
			if got := run(tc.args, &out, &errOut); got != tc.code {
				t.Fatalf("exit %d, want %d: %s", got, tc.code, errOut.String())
			}
			if !strings.Contains(out.String()+errOut.String(), tc.contains) {
				t.Fatalf("missing output %q", tc.contains)
			}
		})
	}
}

func TestDemoCLILiveAdapter(t *testing.T) {
	// Exercise the complete HTTP adapter/CLI/cache path using the real recorded
	// vectors. This is a protocol integration test, not live model inference.
	recorded, err := semanticcache.DemoRecordedEmbedder()
	if err != nil {
		t.Fatal(err)
	}
	var infoCalls, embedCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/info":
			infoCalls.Add(1)
			json.NewEncoder(w).Encode(map[string]string{"model_id": recorded.Identity().Model, "model_sha": recorded.Identity().Revision})
		case "/embed":
			embedCalls.Add(1)
			var input struct {
				Inputs string `json:"inputs"`
			}
			if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
				t.Error(err)
				http.Error(w, "bad input", http.StatusBadRequest)
				return
			}
			vector, err := recorded.Embed(context.Background(), input.Inputs)
			if err != nil {
				t.Error(err)
				http.Error(w, "unknown text", http.StatusBadRequest)
				return
			}
			json.NewEncoder(w).Encode([][]float64{vector})
		default:
			t.Errorf("unexpected path %q", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	var out, errOut bytes.Buffer
	if code := run([]string{"demo", "-config", "../../configs/experiment.json", "-tei", server.URL, "-output", t.TempDir()}, &out, &errOut); code != 0 {
		t.Fatalf("live adapter demo exit %d: %s", code, errOut.String())
	}
	if infoCalls.Load() != 4 || embedCalls.Load() != 4 {
		t.Fatalf("HTTP calls: info=%d embed=%d; want four each", infoCalls.Load(), embedCalls.Load())
	}
	if !strings.Contains(out.String(), "live-real-minilm-tei") || !strings.Contains(out.String(), "demo-002 source=cache success=true searches=1 generations=0 writes=0") {
		t.Fatal(out.String())
	}
}

func TestDemoCLIFailureEvidence(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "embedding backend unavailable", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	dir := t.TempDir()
	var out, errOut bytes.Buffer
	if code := run([]string{"demo", "-config", "../../configs/experiment.json", "-tei", server.URL, "-output", dir}, &out, &errOut); code != 1 {
		t.Fatalf("outage exit %d; want 1", code)
	}
	if !strings.Contains(errOut.String(), "HTTP 503") {
		t.Fatal(errOut.String())
	}
	data, err := os.ReadFile(filepath.Join(dir, "requests.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	lines := bytes.Split(bytes.TrimSpace(data), []byte("\n"))
	if len(lines) != 4 {
		t.Fatalf("saved %d requests; want four", len(lines))
	}
	for _, line := range lines {
		var result semanticcache.Result
		if err := json.Unmarshal(line, &result); err != nil {
			t.Fatal(err)
		}
		if result.Success || result.Answer != "" || result.Error == "" || result.SearchCount != 0 || result.GenerationCount != 0 || result.WriteCount != 0 {
			t.Fatalf("outage misreported: %+v", result)
		}
	}
}

func TestDemoCLI(t *testing.T) {
	var out, errOut bytes.Buffer
	if code := run([]string{"demo", "-config", "../../configs/experiment.json", "-output", t.TempDir()}, &out, &errOut); code != 0 {
		t.Fatalf("demo exit %d: %s", code, errOut.String())
	}
	if !strings.Contains(out.String(), "demo-002 source=cache") || !strings.Contains(out.String(), "demo-003 source=generation") {
		t.Fatal(out.String())
	}
}

func TestDemoCLIExternalConfigSourceIdentity(t *testing.T) {
	config, err := os.ReadFile("../../configs/experiment.json")
	if err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(t.TempDir(), "external.json")
	if err := os.WriteFile(configPath, config, 0600); err != nil {
		t.Fatal(err)
	}
	output := t.TempDir()
	var out, errOut bytes.Buffer
	if code := run([]string{"demo", "-config", configPath, "-output", output}, &out, &errOut); code != 0 {
		t.Fatalf("external config demo exit %d: %s", code, errOut.String())
	}
	data, err := os.ReadFile(filepath.Join(output, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest struct {
		Sources map[string]string `json:"source_files_sha256"`
	}
	if err := json.Unmarshal(data, &manifest); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"go.mod", "cmd/semcache/demo.go", "internal/semanticcache/cache.go"} {
		source, err := os.ReadFile(filepath.Join("../..", name))
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(source)
		if manifest.Sources[name] != hex.EncodeToString(sum[:]) {
			t.Errorf("manifest does not identify actual source %q", name)
		}
	}
}
