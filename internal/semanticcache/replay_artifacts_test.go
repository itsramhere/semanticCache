package semanticcache

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestSaveReplayRejectsUnpairedEvidence(t *testing.T) {
	c := fixtureConfig(t)
	w := fourRequestWorkload(t)
	o := ReplayOptions{"end-to-end", "warm", 100, 1, true}
	cases := []struct {
		name   string
		mutate func([]ReplayRun)
	}{
		{"duplicate predictor", func(r []ReplayRun) { r[1].PredictorID = r[0].PredictorID }},
		{"different mode", func(r []ReplayRun) { r[1].Options.Mode = "isolation" }},
		{"different start", func(r []ReplayRun) { r[1].Options.Start = "cold" }},
		{"different capacity", func(r []ReplayRun) { r[1].Options.Capacity++ }},
		{"different request count", func(r []ReplayRun) { r[1].Rows = r[1].Rows[:3] }},
		{"different request ID", func(r []ReplayRun) { r[1].Rows[0].RequestID = "other" }},
		{"different result ID", func(r []ReplayRun) { r[1].Rows[0].Result.RequestID = "other" }},
		{"different labels", func(r []ReplayRun) { r[1].Rows[0].GroupID = "other" }},
		{"different request order", func(r []ReplayRun) { r[1].Rows[0], r[1].Rows[1] = r[1].Rows[1], r[1].Rows[0] }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runs := []ReplayRun{runFixture(t, w, o, &ConstantPredictor{true}), runFixture(t, w, o, &ConstantPredictor{false})}
			tc.mutate(runs)
			dir := filepath.Join(t.TempDir(), "invalid")
			if err := SaveReplay(dir, "../..", c, w, runs, nil); err == nil {
				t.Fatal("saved an invalid paired comparison")
			}
			if _, err := os.Stat(dir); !os.IsNotExist(err) {
				t.Fatal("invalid evidence created output artifacts")
			}
		})
	}
}

func TestSaveReplayInputIdentity(t *testing.T) {
	c := fixtureConfig(t)
	w := fourRequestWorkload(t)
	run := runFixture(t, w, ReplayOptions{"end-to-end", "warm", 100, 1, true}, &ConstantPredictor{true})
	for _, name := range []string{"config.json", "workload.json"} {
		t.Run(name, func(t *testing.T) {
			var value any
			if name == "config.json" {
				other := c
				other.ID += "-other"
				value = other
			} else {
				other := w
				other.Seed++
				value = other
			}
			b, err := json.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			if err := SaveReplay(t.TempDir(), "../..", c, w, []ReplayRun{run}, map[string][]byte{name: b}); err == nil {
				t.Fatal("saved inputs do not describe the evaluated replay")
			}
		})
	}
	dir := t.TempDir()
	if err := SaveReplay(dir, "../..", c, w, []ReplayRun{run}, nil); err != nil {
		t.Fatal(err)
	}
	cb, err := os.ReadFile(filepath.Join(dir, "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	gotConfig, err := LoadConfig(bytes.NewReader(cb))
	if err != nil || !reflect.DeepEqual(gotConfig, c) {
		t.Fatalf("saved configuration: %v", err)
	}
	wb, err := os.ReadFile(filepath.Join(dir, "workload.json"))
	if err != nil {
		t.Fatal(err)
	}
	gotWorkload, err := LoadWorkload(bytes.NewReader(wb))
	if err != nil || !reflect.DeepEqual(gotWorkload, w) {
		t.Fatalf("saved workload: %v", err)
	}
}

func TestSaveReplayIncludesSourcesInsideOutputDirectory(t *testing.T) {
	c := fixtureConfig(t)
	w := fourRequestWorkload(t)
	run := runFixture(t, w, ReplayOptions{"end-to-end", "warm", 100, 1, true}, &ConstantPredictor{true})
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "go.mod"), []byte("module fixture\n"), 0644); err != nil {
		t.Fatal(err)
	}
	packageDir := filepath.Join(root, "internal")
	if err := os.MkdirAll(packageDir, 0755); err != nil {
		t.Fatal(err)
	}
	source := []byte("package fixture\n")
	if err := os.WriteFile(filepath.Join(packageDir, "fixture.go"), source, 0644); err != nil {
		t.Fatal(err)
	}
	for _, dir := range []string{root, packageDir} {
		if err := SaveReplay(dir, root, c, w, []ReplayRun{run}, nil); err != nil {
			t.Fatal(err)
		}
		b, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
		if err != nil {
			t.Fatal(err)
		}
		var m struct {
			Sources map[string]string `json:"source_files_sha256"`
		}
		if err := json.Unmarshal(b, &m); err != nil {
			t.Fatal(err)
		}
		if m.Sources["internal/fixture.go"] != hashBytes(source) || m.Sources["go.mod"] == "" {
			t.Fatalf("output directory omitted source identity: %v", m.Sources)
		}
	}
}
