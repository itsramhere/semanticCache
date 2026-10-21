package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPhase2CLIArtifacts(t *testing.T) {
	data := t.TempDir()
	var out, errOut bytes.Buffer
	if code := run([]string{"build-data", "-catalog", "../../data/catalog.json", "-seed", "101", "-output", data}, &out, &errOut); code != 0 {
		t.Fatalf("build-data %d: %s", code, errOut.String())
	}
	paths := []string{"validate-data"}
	for _, split := range []string{"development", "tuning", "final"} {
		paths = append(paths, filepath.Join(data, split+"-101.json"))
	}
	if code := run(paths, &out, &errOut); code != 0 {
		t.Fatalf("validate-data %d: %s", code, errOut.String())
	}
	for _, mode := range []string{"isolation", "end-to-end"} {
		dir := t.TempDir()
		if code := run([]string{"replay", "-data", paths[1], "-config", "../../configs/phase2_fixture.json", "-mode", mode, "-start", "warm", "-output", dir}, &out, &errOut); code != 0 {
			t.Fatalf("replay %d: %s", code, errOut.String())
		}
		b, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
		if err != nil {
			t.Fatal(err)
		}
		var manifest struct {
			Artifacts map[string]string `json:"artifacts_sha256"`
			Sources   map[string]string `json:"source_files_sha256"`
			Seed      int64             `json:"workload_seed"`
		}
		if err = json.Unmarshal(b, &manifest); err != nil {
			t.Fatal(err)
		}
		if manifest.Seed != 101 || manifest.Sources["internal/semanticcache/replay.go"] == "" {
			t.Fatal("missing manifest identity")
		}
		for name, hash := range manifest.Artifacts {
			b, err := os.ReadFile(filepath.Join(dir, name))
			if err != nil {
				t.Fatal(err)
			}
			sum := sha256.Sum256(b)
			if hex.EncodeToString(sum[:]) != hash {
				t.Fatalf("hash mismatch %s", name)
			}
		}
		b, err = os.ReadFile(filepath.Join(dir, "metrics.json"))
		if err != nil {
			t.Fatal(err)
		}
		var metrics []map[string]any
		if err = json.Unmarshal(b, &metrics); err != nil {
			t.Fatal(err)
		}
		if len(metrics) != 2 || metrics[0]["eligible_decision_bypasses"] != float64(0) || metrics[0]["cache_hit_recall"] != float64(1) || metrics[1]["unsuccessful_routed_fraction"] != "N/A" {
			t.Fatal("invalid saved metrics")
		}
		if mode == "end-to-end" && metrics[1]["production_searches"] != float64(0) {
			t.Fatal("always-skip searched")
		}
		b, err = os.ReadFile(filepath.Join(dir, "metrics.csv"))
		if err != nil {
			t.Fatal(err)
		}
		records, err := csv.NewReader(strings.NewReader(string(b))).ReadAll()
		if err != nil || len(records) != 3 {
			t.Fatal("invalid CSV")
		}
	}
	// The actual validation command must reject malformed external input.
	b, err := os.ReadFile(paths[1])
	if err != nil {
		t.Fatal(err)
	}
	var w map[string]any
	if err = json.Unmarshal(b, &w); err != nil {
		t.Fatal(err)
	}
	requests := w["requests"].([]any)
	first := requests[0].(map[string]any)["request"].(map[string]any)
	second := requests[1].(map[string]any)["request"].(map[string]any)
	second["id"] = first["id"]
	bad := filepath.Join(data, "bad.json")
	if err = writeJSON(bad, w); err != nil {
		t.Fatal(err)
	}
	if code := run([]string{"validate-data", bad}, &out, &errOut); code != 1 {
		t.Fatal("CLI accepted duplicate IDs")
	}
}
