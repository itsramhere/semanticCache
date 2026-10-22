package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"semanticcache/internal/semanticcache"
)

func TestPhase6RemoteAccountCLI(t *testing.T) {
	dir := t.TempDir()
	configBytes, err := os.ReadFile("../../configs/experiment.json")
	if err != nil {
		t.Fatal(err)
	}
	config, err := semanticcache.LoadConfig(bytes.NewReader(configBytes))
	if err != nil {
		t.Fatal(err)
	}
	request := semanticcache.DemoRequests()[0]
	runs := []semanticcache.RemoteRun{{Router: "always-search"}, {Router: "exploratory-lsh"}}
	for i := range runs {
		decision := semanticcache.Decision{Search: i == 0, Reason: "fixture", ConfigurationID: "fixture"}
		row := semanticcache.RemoteRow{Request: request, Result: semanticcache.Result{RequestID: request.ID, Success: true, EmbeddingCount: 1, GenerationCount: 1, Decision: &decision, Timings: semanticcache.StageTimings{TotalNS: 1000}}}
		if decision.Search {
			row.Result.SearchCount = 1
			row.Result.Timings.SearchNS = 100
			row.Calls = []semanticcache.RemoteCall{{Method: "POST", Path: "/collections/test/points/query", DurationNS: 100}}
		}
		runs[i].Rows = []semanticcache.RemoteRow{row}
	}
	input := filepath.Join(dir, "run.json")
	encoded, err := json.Marshal(map[string]any{"runs": runs})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(input, encoded, 0644); err != nil {
		t.Fatal(err)
	}
	comparison, err := semanticcache.AccountRemote(config, runs)
	if err != nil {
		t.Fatal(err)
	}
	want, err := json.MarshalIndent(comparison, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(dir, "accounting.json")
	args := []string{"remote-account", "-run", input, "-config", "../../configs/experiment.json", "-output", output}
	var out, errOut bytes.Buffer
	if code := run(args, &out, &errOut); code != 0 {
		t.Fatalf("remote-account exit %d: %s", code, errOut.String())
	}
	got, err := os.ReadFile(output)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("offline accounting differs: %v", err)
	}
	if code := run(args, &out, &errOut); code != 1 {
		t.Fatal("existing accounting evidence overwritten")
	}
	got, err = os.ReadFile(output)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatal("failed overwrite changed evidence")
	}
}

func TestPhase6RemoteLiveCLI(t *testing.T) {
	endpoint := os.Getenv("SEMCACHE_QDRANT_URL")
	if endpoint == "" {
		t.Skip("set SEMCACHE_QDRANT_URL for live remote CLI evidence")
	}
	dir := filepath.Join(t.TempDir(), "remote")
	var out, errOut bytes.Buffer
	args := []string{"remote-demo", "-endpoint", endpoint, "-config", "../../configs/experiment.json", "-output", dir, "-repeats", "2", "-rebuild-every", "4"}
	if code := run(args, &out, &errOut); code != 0 {
		t.Fatalf("remote-demo exit %d: %s", code, errOut.String())
	}
	if code := run([]string{"remote-account", "-run", filepath.Join(dir, "run.json"), "-config", filepath.Join(dir, "config.json"), "-output", filepath.Join(dir, "recomputed.json")}, &out, &errOut); code != 0 {
		t.Fatalf("remote-account exit %d: %s", code, errOut.String())
	}
	original, err := os.ReadFile(filepath.Join(dir, "accounting.json"))
	if err != nil {
		t.Fatal(err)
	}
	recomputed, err := os.ReadFile(filepath.Join(dir, "recomputed.json"))
	if err != nil || !bytes.Equal(original, recomputed) {
		t.Fatalf("live accounting does not reproduce byte for byte: %v", err)
	}
	manifestBytes, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest struct {
		Hashes map[string]string `json:"artifacts_sha256"`
	}
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		t.Fatal(err)
	}
	for _, artifact := range []string{"run.json", "config.json", "backend.json", "accounting.json", "always-search-requests.jsonl", "exploratory-lsh-requests.jsonl", "sources/internal/semanticcache/testdata/demo_embeddings.json"} {
		if manifest.Hashes[artifact] == "" {
			t.Fatalf("manifest omits %s", artifact)
		}
	}
	for name, want := range manifest.Hashes {
		b, err := os.ReadFile(filepath.Join(dir, filepath.FromSlash(name)))
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(b)
		if hex.EncodeToString(sum[:]) != want {
			t.Fatalf("artifact hash differs: %s", name)
		}
	}
	if code := run(args, &out, &errOut); code != 1 {
		t.Fatal("existing remote run overwritten")
	}
}
