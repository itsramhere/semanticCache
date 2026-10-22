package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"semanticcache/internal/semanticcache"
)

func TestTuneRejectsFinalAndExistingOutput(t *testing.T) {
	dir := t.TempDir()
	var out, errOut bytes.Buffer
	for _, seed := range []string{"101", "202", "303"} {
		if code := run([]string{"build-data", "-catalog", "../../data/catalog.json", "-output", dir, "-seed", seed}, &out, &errOut); code != 0 {
			t.Fatal(code, errOut.String())
		}
		b, err := os.ReadFile(filepath.Join(dir, "final-"+seed+".json"))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "tuning-"+seed+".json"), b, 0644); err != nil {
			t.Fatal(err)
		}
	}
	output := filepath.Join(dir, "out")
	args := []string{"tune", "-config", "../../configs/phase2_fixture.json", "-grid", "../../configs/tuning_grid.json", "-data-dir", dir, "-output", output}
	errOut.Reset()
	if code := run(args, &out, &errOut); code == 0 || !strings.Contains(errOut.String(), "tuning workloads") {
		t.Fatal(code, errOut.String())
	}
	if _, err := os.Stat(output); !os.IsNotExist(err) {
		t.Fatal("rejection created output")
	}
	// Rebuild the proper tuning files. An existing destination must remain intact.
	for _, seed := range []string{"101", "202", "303"} {
		if code := run([]string{"build-data", "-catalog", "../../data/catalog.json", "-output", dir, "-seed", seed}, &out, &errOut); code != 0 {
			t.Fatal(code, errOut.String())
		}
	}
	if err := os.Mkdir(output, 0755); err != nil {
		t.Fatal(err)
	}
	errOut.Reset()
	if code := run(args, &out, &errOut); code == 0 || !strings.Contains(errOut.String(), "output already exists") {
		t.Fatal(code, errOut.String())
	}
}

func TestFrozenFallbackReplayCLI(t *testing.T) {
	dir := t.TempDir()
	var out, errOut bytes.Buffer
	if code := run([]string{"build-data", "-catalog", "../../data/catalog.json", "-output", dir, "-seed", "101"}, &out, &errOut); code != 0 {
		t.Fatal(code, errOut.String())
	}
	b, err := os.ReadFile("../../configs/phase2_fixture.json")
	if err != nil {
		t.Fatal(err)
	}
	c, err := semanticcache.LoadConfig(bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	f := semanticcache.FinalCandidate{Version: "phase4-candidate-v1", Router: "always-search", FallbackRouter: "always-search", Status: "no-qualifying-lsh", SelectedVariant: "always-search", Experiment: c, Identity: semanticcache.FixtureIdentity(), Reason: "test fallback", TuningDirectory: "test", TuningManifestSHA256: strings.Repeat("0", 64), SweepSHA256: strings.Repeat("1", 64), FrozenAt: time.Now().UTC()}
	path := filepath.Join(dir, "candidate.json")
	if err := writeJSON(path, f); err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(dir, "replay")
	args := []string{"replay", "-candidate", path, "-data", filepath.Join(dir, "development-101.json"), "-start", "warm", "-output", destination}
	if code := run(args, &out, &errOut); code != 0 {
		t.Fatal(code, errOut.String())
	}
	saved, err := os.ReadFile(filepath.Join(destination, "candidate.json"))
	if err != nil {
		t.Fatal(err)
	}
	original, _ := os.ReadFile(path)
	if !bytes.Equal(saved, original) {
		t.Fatal("candidate provenance changed")
	}
	mb, err := os.ReadFile(filepath.Join(destination, "metrics.json"))
	if err != nil {
		t.Fatal(err)
	}
	var metrics []map[string]any
	if err := json.Unmarshal(mb, &metrics); err != nil {
		t.Fatal(err)
	}
	if len(metrics) != 1 || metrics[0]["predictor_id"] != "always-search" || metrics[0]["eligible_decision_bypasses"].(float64) != 0 {
		t.Fatal(string(mb))
	}
	errOut.Reset()
	if code := run(append(args, "-config", "../../configs/phase2_fixture.json"), &out, &errOut); code == 0 || !strings.Contains(errOut.String(), "omit -config") {
		t.Fatal(code, errOut.String())
	}
}

func TestFreezeRejectsAlteredEvidence(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "sweep.json"), []byte(`{}`), 0644); err != nil {
		t.Fatal(err)
	}
	manifest := map[string]any{"phase": "4.1", "split": "tuning", "artifacts_sha256": map[string]string{"sweep.json": strings.Repeat("0", 64)}}
	if err := writeJSON(filepath.Join(dir, "manifest.json"), manifest); err != nil {
		t.Fatal(err)
	}
	var out, errOut bytes.Buffer
	destination := filepath.Join(dir, "candidate.json")
	if code := run([]string{"freeze-candidate", "-tuning", dir, "-output", destination}, &out, &errOut); code == 0 || !strings.Contains(errOut.String(), "checksum mismatch") {
		t.Fatal(code, errOut.String())
	}
	if _, err := os.Stat(destination); !os.IsNotExist(err) {
		t.Fatal("invalid evidence froze a candidate")
	}
}

// A tiny complete sweep exercises the freeze command with actual paired traces,
// without opening final data or depending on performance thresholds.
func freezeFixture(t *testing.T) (string, semanticcache.TuningResult) {
	t.Helper()
	b, err := os.ReadFile("../../configs/phase2_fixture.json")
	if err != nil {
		t.Fatal(err)
	}
	c, err := semanticcache.LoadConfig(bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	c.RequestsPerSplit, c.Policy.CapacityEntries = 1, []int{100}
	b, err = os.ReadFile("../../data/catalog.json")
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := semanticcache.LoadCatalog(bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	result := semanticcache.TuningResult{Config: c, Grid: semanticcache.TuningGrid{ID: "freeze-test", Tables: []int{1}, Bits: []int{4}, Modes: []string{"isolation", "end-to-end"}, Starts: []string{"warm", "cold"}}}
	dir := t.TempDir()
	for _, seed := range c.WorkloadSeeds {
		ws, err := semanticcache.BuildWorkloads(catalog, seed)
		if err != nil {
			t.Fatal(err)
		}
		var w semanticcache.Workload
		for _, candidate := range ws {
			if candidate.Split == "tuning" {
				w = candidate
			}
		}
		w.Requests = w.Requests[:1]
		for _, mode := range result.Grid.Modes {
			for _, start := range result.Grid.Starts {
				o := semanticcache.ReplayOptions{Mode: mode, Start: start, Capacity: 100, OracleEvery: 1, OracleLogging: true}
				base, err := semanticcache.Replay(context.Background(), c, w, o, &semanticcache.ConstantPredictor{Search: true})
				if err != nil {
					t.Fatal(err)
				}
				baseline, err := semanticcache.Summarize(c, base)
				if err != nil {
					t.Fatal(err)
				}
				add := func(variant string, ps int64, tables, bits int, predictor semanticcache.ReplayPredictor) {
					run := base
					runs := []semanticcache.ReplayRun{base}
					if variant != "always-search" {
						run, err = semanticcache.Replay(context.Background(), c, w, o, predictor)
						if err != nil {
							t.Fatal(err)
						}
						runs = append(runs, run)
					}
					m, err := semanticcache.Summarize(c, run)
					if err != nil {
						t.Fatal(err)
					}
					m.GenerationDelta = m.ProductionGenerations - baseline.ProductionGenerations
					m.CostDeltaUSD = m.SimulatedCostUSD - baseline.SimulatedCostUSD
					rel := fmt.Sprintf("%d-%s-%s-%s-%d", seed, mode, start, variant, ps)
					if err := semanticcache.SaveReplay(filepath.Join(dir, rel), "../..", c, w, runs, nil); err != nil {
						t.Fatal(err)
					}
					result.Rows = append(result.Rows, semanticcache.TuningRow{Variant: variant, WorkloadSeed: seed, ProjectionSeed: ps, Tables: tables, Bits: bits, Options: o, Metrics: m, BaselineCostUSD: baseline.SimulatedCostUSD, BaselineP95MS: baseline.SimulatedP95MS, Evidence: rel, Memory: semanticcache.SummaryMemoryMeasurement{RetainedHeapDelta: 100}})
				}
				add("always-search", 0, 0, 0, nil)
				add("always-skip", 0, 0, 0, &semanticcache.ConstantPredictor{Search: false})
				add("exact-vector-repeat-v1", 0, 0, 0, &semanticcache.ExactRepeat{})
				for _, ps := range c.ProjectionSeeds {
					p, err := semanticcache.NewProjections(semanticcache.LSHConfig{Version: "cosine-hyperplanes-v1", Identity: w.Identity, Seed: ps, Tables: 1, Bits: 4})
					if err != nil {
						t.Fatal(err)
					}
					s, err := semanticcache.NewLSHSummary(p)
					if err != nil {
						t.Fatal(err)
					}
					add("lsh-L1-K4", ps, 1, 4, s)
				}
			}
		}
	}
	if err := writeJSON(filepath.Join(dir, "sweep.json"), result); err != nil {
		t.Fatal(err)
	}
	if err := writeJSON(filepath.Join(dir, "config.json"), c); err != nil {
		t.Fatal(err)
	}
	if err := writeJSON(filepath.Join(dir, "grid.json"), result.Grid); err != nil {
		t.Fatal(err)
	}
	if err := saveTuningManifest(dir); err != nil {
		t.Fatal(err)
	}
	return dir, result
}

func TestFreezeVerifiesEveryRow(t *testing.T) {
	dir, original := freezeFixture(t)
	var out, errOut bytes.Buffer
	if code := run([]string{"freeze-candidate", "-tuning", dir, "-output", filepath.Join(t.TempDir(), "valid.json")}, &out, &errOut); code != 0 {
		t.Fatal(code, errOut.String())
	}
	for _, name := range []string{"later-final-workload", "missing-later-evidence", "changed-paired-cost", "changed-metrics", "changed-trace", "changed-grid", "changed-config"} {
		t.Run(name, func(t *testing.T) {
			b, err := json.Marshal(original)
			if err != nil {
				t.Fatal(err)
			}
			var result semanticcache.TuningResult
			if err := json.Unmarshal(b, &result); err != nil {
				t.Fatal(err)
			}
			row := &result.Rows[1]
			var changedPath string
			var before []byte
			switch name {
			case "later-final-workload":
				changedPath = filepath.Join(dir, row.Evidence, "workload.json")
				before, err = os.ReadFile(changedPath)
				if err != nil {
					t.Fatal(err)
				}
				var w semanticcache.Workload
				if err := json.Unmarshal(before, &w); err != nil {
					t.Fatal(err)
				}
				w.Split = "final"
				for i := range w.Groups {
					w.Groups[i].Split = "final"
				}
				if err := writeJSON(changedPath, w); err != nil {
					t.Fatal(err)
				}
			case "missing-later-evidence":
				row.Evidence = "absent"
			case "changed-paired-cost":
				row.BaselineCostUSD++
			case "changed-metrics":
				row.Metrics.CacheHitRecall = semanticcache.Metric{Value: 0.5, Defined: true}
			case "changed-grid", "changed-config":
				file := "grid.json"
				if name == "changed-config" {
					file = "config.json"
				}
				path := filepath.Join(dir, file)
				before, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				defer func() {
					if err := os.WriteFile(path, before, 0644); err != nil {
						t.Error(err)
					}
				}()
				var input map[string]any
				if err := json.Unmarshal(before, &input); err != nil {
					t.Fatal(err)
				}
				input["id"] = "changed"
				if err := writeJSON(path, input); err != nil {
					t.Fatal(err)
				}
			case "changed-trace":
				changedPath = filepath.Join(dir, row.Evidence, row.Metrics.PredictorID+".jsonl")
				before, err = os.ReadFile(changedPath)
				if err != nil {
					t.Fatal(err)
				}
				var entry semanticcache.ReplayEvidence
				if err := json.Unmarshal(before, &entry); err != nil {
					t.Fatal(err)
				}
				entry.Result.Timings.TotalNS++
				if err := writeJSON(changedPath, entry); err != nil {
					t.Fatal(err)
				}
			}
			// Rehash an internally inconsistent capture: checksums alone cannot
			// establish that the sweep still describes its paired evidence.
			if changedPath != "" {
				defer func() {
					if err := os.WriteFile(changedPath, before, 0644); err != nil {
						t.Error(err)
					}
				}()
				manifestPath := filepath.Join(dir, row.Evidence, "manifest.json")
				mb, err := os.ReadFile(manifestPath)
				if err != nil {
					t.Fatal(err)
				}
				defer func() {
					if err := os.WriteFile(manifestPath, mb, 0644); err != nil {
						t.Error(err)
					}
				}()
				var child map[string]any
				if err := json.Unmarshal(mb, &child); err != nil {
					t.Fatal(err)
				}
				hash, err := fileSHA(changedPath)
				if err != nil {
					t.Fatal(err)
				}
				child["artifacts_sha256"].(map[string]any)[filepath.Base(changedPath)] = hash
				if err := writeJSON(manifestPath, child); err != nil {
					t.Fatal(err)
				}
			}
			if err := writeJSON(filepath.Join(dir, "sweep.json"), result); err != nil {
				t.Fatal(err)
			}
			if err := saveTuningManifest(dir); err != nil {
				t.Fatal(err)
			}
			errOut.Reset()
			destination := filepath.Join(t.TempDir(), "invalid.json")
			if code := run([]string{"freeze-candidate", "-tuning", dir, "-output", destination}, &out, &errOut); code == 0 {
				t.Fatal("inconsistent tuning evidence froze a candidate")
			}
			if _, err := os.Stat(destination); !os.IsNotExist(err) {
				t.Fatal("invalid evidence created candidate")
			}
		})
	}
}
