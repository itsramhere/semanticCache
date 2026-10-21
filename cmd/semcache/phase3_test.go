package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"semanticcache/internal/semanticcache"
)

func TestPhase3CLIReplay(t *testing.T) {
	dir := t.TempDir()
	var out, errOut bytes.Buffer
	invoke := func(args ...string) {
		t.Helper()
		if code := run(args, &out, &errOut); code != 0 {
			t.Fatalf("%v: %d %s", args, code, errOut.String())
		}
	}
	invoke("build-data", "-catalog", "../../data/catalog.json", "-output", dir, "-seed", "101")
	planes := filepath.Join(dir, "planes.json")
	invoke("build-projections", "-config", "../../configs/lsh_development.json", "-output", planes)
	b, err := os.ReadFile(planes)
	if err != nil {
		t.Fatal(err)
	}
	var p semanticcache.Projections
	if err := json.Unmarshal(b, &p); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"isolation", "end-to-end"} {
		output := filepath.Join(dir, mode)
		invoke("replay", "-config", "../../configs/phase2_fixture.json", "-data", filepath.Join(dir, "development-101.json"), "-lsh", planes, "-mode", mode, "-start", "warm", "-output", output)
		saved, err := os.ReadFile(filepath.Join(output, p.ID()+"-projections.json"))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(b, saved) {
			t.Fatal("saved projections differ")
		}
		mb, err := os.ReadFile(filepath.Join(output, "metrics.json"))
		if err != nil {
			t.Fatal(err)
		}
		var metrics []map[string]any
		if err := json.Unmarshal(mb, &metrics); err != nil {
			t.Fatal(err)
		}
		if len(metrics) != 3 || metrics[2]["eligible_decision_bypasses"].(float64) == 0 || metrics[2]["failed_requests"].(float64) != 0 {
			t.Fatal("missing LSH behavior", string(mb))
		}
		manifestBytes, err := os.ReadFile(filepath.Join(output, "manifest.json"))
		if err != nil {
			t.Fatal(err)
		}
		var manifest struct {
			Seeds  map[string]int64  `json:"projection_seeds"`
			Hashes map[string]string `json:"artifacts_sha256"`
		}
		if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
			t.Fatal(err)
		}
		if manifest.Seeds[p.ID()] != 1001 || manifest.Hashes[p.ID()+"-projections.json"] == "" {
			t.Fatal("missing projection provenance")
		}
	}
}
