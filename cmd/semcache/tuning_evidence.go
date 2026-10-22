package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"

	"semanticcache/internal/semanticcache"
)

// Checksums protect captured files; these checks also bind every matrix row to
// its tuning input, paired baseline, projection family and per-request metrics.
func verifyTuningEvidence(dir string, hashes map[string]string, result semanticcache.TuningResult) (semanticcache.EmbeddingIdentity, error) {
	var identity semanticcache.EmbeddingIdentity
	workloads := map[int64]semanticcache.Workload{}
	workloadHashes := map[int64]string{}
	source := ""
	for _, input := range []struct {
		name string
		want any
	}{{"config.json", result.Config}, {"grid.json", result.Grid}} {
		if hashes[input.name] == "" {
			return identity, fmt.Errorf("unverified tuning input: %s", input.name)
		}
		b, err := os.ReadFile(filepath.Join(dir, input.name))
		if err != nil {
			return identity, err
		}
		want, err := json.Marshal(input.want)
		if err != nil {
			return identity, err
		}
		var gotValue, wantValue any
		if err := json.Unmarshal(b, &gotValue); err != nil {
			return identity, err
		}
		if err := json.Unmarshal(want, &wantValue); err != nil {
			return identity, err
		}
		if !reflect.DeepEqual(gotValue, wantValue) {
			return identity, fmt.Errorf("tuning %s differs from recorded input", input.name)
		}
	}
	for _, row := range result.Rows {
		if !filepath.IsLocal(filepath.FromSlash(row.Evidence)) {
			return identity, fmt.Errorf("invalid tuning evidence path")
		}
		read := func(name string) ([]byte, error) {
			rel := filepath.ToSlash(filepath.Join(filepath.FromSlash(row.Evidence), name))
			if hashes[rel] == "" {
				return nil, fmt.Errorf("unverified tuning artifact: %s", rel)
			}
			return os.ReadFile(filepath.Join(dir, filepath.FromSlash(rel)))
		}
		var manifest struct {
			Split   string                      `json:"split"`
			Seed    int64                       `json:"workload_seed"`
			Source  string                      `json:"source_sha256"`
			Options semanticcache.ReplayOptions `json:"options"`
			Hashes  map[string]string           `json:"artifacts_sha256"`
		}
		b, err := read("manifest.json")
		if err != nil {
			return identity, err
		}
		if err := json.Unmarshal(b, &manifest); err != nil {
			return identity, err
		}
		if manifest.Split != "tuning" || manifest.Seed != row.WorkloadSeed || manifest.Options != row.Options || manifest.Source == "" {
			return identity, fmt.Errorf("tuning evidence manifest mismatch: %s", row.Evidence)
		}
		if source == "" {
			source = manifest.Source
		}
		if source != manifest.Source {
			return identity, fmt.Errorf("mixed implementation sources in tuning evidence")
		}
		for name, hash := range manifest.Hashes {
			if !filepath.IsLocal(filepath.FromSlash(name)) || hash == "" || hashes[filepath.ToSlash(filepath.Join(row.Evidence, name))] != hash {
				return identity, fmt.Errorf("unverified paired artifact: %s/%s", row.Evidence, name)
			}
		}
		b, err = read("workload.json")
		if err != nil {
			return identity, err
		}
		w, err := semanticcache.LoadWorkload(bytes.NewReader(b))
		if err != nil {
			return identity, err
		}
		if w.Split != "tuning" || w.Seed != row.WorkloadSeed {
			return identity, fmt.Errorf("candidate selection accepts matching tuning data only")
		}
		hash := hashes[filepath.ToSlash(filepath.Join(row.Evidence, "workload.json"))]
		if previous, ok := workloadHashes[w.Seed]; ok && previous != hash {
			return identity, fmt.Errorf("inconsistent tuning workload for seed %d", w.Seed)
		}
		workloadHashes[w.Seed], workloads[w.Seed] = hash, w
		if identity.Model == "" {
			identity = w.Identity
		}
		if identity != w.Identity {
			return identity, fmt.Errorf("mixed tuning embedding identities")
		}
		b, err = read("config.json")
		if err != nil {
			return identity, err
		}
		c, err := semanticcache.LoadConfig(bytes.NewReader(b))
		if err != nil {
			return identity, err
		}
		if !reflect.DeepEqual(c, result.Config) {
			return identity, fmt.Errorf("tuning configuration differs from evidence")
		}

		id := row.Variant
		if row.Tables > 0 {
			config := semanticcache.LSHConfig{Version: "cosine-hyperplanes-v1", Identity: identity, Seed: row.ProjectionSeed, Tables: row.Tables, Bits: row.Bits}
			p, err := semanticcache.NewProjections(config)
			if err != nil {
				return identity, err
			}
			id = p.ID()
			b, err := read(id + "-projections.json")
			if err != nil {
				return identity, err
			}
			loaded, err := semanticcache.LoadProjections(bytes.NewReader(b), config)
			if err != nil {
				return identity, err
			}
			if loaded.ID() != id {
				return identity, fmt.Errorf("tuning projection identity mismatch")
			}
		}
		b, err = read("metrics.json")
		if err != nil {
			return identity, err
		}
		var metrics []semanticcache.Metrics
		if err := json.Unmarshal(b, &metrics); err != nil {
			return identity, err
		}
		expectedRuns := 2
		if id == "always-search" {
			expectedRuns = 1
		}
		if len(metrics) != expectedRuns || metrics[0].PredictorID != "always-search" || metrics[len(metrics)-1].PredictorID != id {
			return identity, fmt.Errorf("tuning paired routers differ from evidence")
		}
		if !reflect.DeepEqual(row.Metrics, metrics[len(metrics)-1]) || row.BaselineCostUSD != metrics[0].SimulatedCostUSD || row.BaselineP95MS != metrics[0].SimulatedP95MS {
			return identity, fmt.Errorf("tuning metrics differ from paired evidence: %s", row.Evidence)
		}
		for _, saved := range metrics {
			b, err := read(saved.PredictorID + ".jsonl")
			if err != nil {
				return identity, err
			}
			run := semanticcache.ReplayRun{PredictorID: saved.PredictorID, Options: row.Options, BuildNS: saved.BuildNS, MemoryBytes: saved.MemoryBytes, DiagnosticSearches: saved.DiagnosticSearches}
			dec := json.NewDecoder(bytes.NewReader(b))
			for {
				var entry semanticcache.ReplayEvidence
				if err := dec.Decode(&entry); err == io.EOF {
					break
				} else if err != nil {
					return identity, err
				}
				i := len(run.Rows)
				if i >= len(w.Requests) || entry.Index != i || entry.RequestID != w.Requests[i].Request.ID || entry.Result.RequestID != entry.RequestID || entry.Eligible != w.Requests[i].Request.CacheEligible {
					return identity, fmt.Errorf("tuning trace differs from workload")
				}
				run.Rows = append(run.Rows, entry)
			}
			if len(run.Rows) != len(w.Requests) {
				return identity, fmt.Errorf("incomplete tuning trace")
			}
			got, err := semanticcache.Summarize(c, run)
			if err != nil {
				return identity, err
			}
			got.GenerationDelta = got.ProductionGenerations - metrics[0].ProductionGenerations
			got.CostDeltaUSD = got.SimulatedCostUSD - metrics[0].SimulatedCostUSD
			if !reflect.DeepEqual(got, saved) {
				return identity, fmt.Errorf("tuning metrics differ from per-request evidence: %s", row.Evidence)
			}
		}
	}
	var ws []semanticcache.Workload
	for _, seed := range result.Config.WorkloadSeeds {
		ws = append(ws, workloads[seed])
	}
	return identity, semanticcache.ValidateTuningInputs(result.Config, ws)
}
