package semanticcache

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"
)

func SaveReplay(dir, sourceRoot string, c Config, w Workload, runs []ReplayRun, inputs map[string][]byte) error {
	if len(runs) == 0 {
		return fmt.Errorf("no replay runs")
	}
	if runs[0].PredictorID != "always-search" {
		return fmt.Errorf("first run must be the always-search baseline")
	}
	if err := ValidateWorkloads([]Workload{w}); err != nil {
		return err
	}
	artifacts := map[string][]byte{}
	var summaries []Metrics
	seen := map[string]bool{}
	projectionSeeds := map[string]int64{}
	for _, run := range runs {
		if run.Projections != nil {
			if err := run.Projections.Validate(); err != nil {
				return err
			}
			if run.Projections.ID() != run.PredictorID || run.Projections.Config.Identity != w.Identity {
				return fmt.Errorf("run projection identity mismatch")
			}
			b, err := json.MarshalIndent(run.Projections, "", "  ")
			if err != nil {
				return err
			}
			artifacts[run.PredictorID+"-projections.json"] = b
			projectionSeeds[run.PredictorID] = run.Projections.Config.Seed
		}
		if filepath.Base(run.PredictorID) != run.PredictorID || strings.ContainsAny(run.PredictorID, `/\`) || strings.TrimSpace(run.PredictorID) == "" || seen[run.PredictorID] {
			return fmt.Errorf("invalid or duplicate predictor ID")
		}
		seen[run.PredictorID] = true
		if run.Options != runs[0].Options || len(run.Rows) != len(w.Requests) {
			return fmt.Errorf("paired runs require identical options and request streams")
		}
		for i, row := range run.Rows {
			r := w.Requests[i]
			if row.Index != i || row.RequestID != r.Request.ID || row.Result.RequestID != r.Request.ID || row.GroupID != r.GroupID || row.Category != r.Category || row.Eligible != r.Request.CacheEligible {
				return fmt.Errorf("run %q request %d does not match the saved workload", run.PredictorID, i)
			}
		}
		m, err := Summarize(c, run)
		if err != nil {
			return err
		}
		summaries = append(summaries, m)
	}
	for i := range summaries {
		summaries[i].GenerationDelta = summaries[i].ProductionGenerations - summaries[0].ProductionGenerations
		summaries[i].CostDeltaUSD = summaries[i].SimulatedCostUSD - summaries[0].SimulatedCostUSD
	}
	metricsJSON, err := json.MarshalIndent(summaries, "", "  ")
	if err != nil {
		return err
	}
	artifacts["metrics.json"] = metricsJSON
	var table bytes.Buffer
	csvOut := csv.NewWriter(&table)
	if err := csvOut.Write([]string{"predictor", "mode", "requests", "eligible", "labeled", "tp", "fp", "fn", "tn", "lookup_reduction", "hit_recall", "missed_hit_fraction", "unsuccessful_routed_fraction", "correctly_avoided_miss_fraction", "searches", "llm_calls", "writes", "diagnostic_searches", "failed_requests", "write_failures", "answer_reuse_accuracy", "measured_routing_p50_ms", "measured_routing_p95_ms", "simulated_end_to_end_p50_ms", "simulated_end_to_end_p95_ms", "predictor_ns", "update_ns", "build_ns", "predictor_memory_bytes", "simulated_serving_cost_usd", "generation_delta_vs_baseline", "simulated_cost_delta_vs_baseline_usd"}); err != nil {
		return err
	}
	for _, m := range summaries {
		if err := csvOut.Write([]string{m.PredictorID, m.Mode, fmt.Sprint(m.Requests), fmt.Sprint(m.EligibleRequests), fmt.Sprint(m.LabeledRequests), fmt.Sprint(m.Confusion.TP), fmt.Sprint(m.Confusion.FP), fmt.Sprint(m.Confusion.FN), fmt.Sprint(m.Confusion.TN), m.LookupReduction.String(), m.CacheHitRecall.String(), m.MissedHitFraction.String(), m.UnsuccessfulRoutedFraction.String(), m.CorrectlyAvoidedMissFraction.String(), fmt.Sprint(m.ProductionSearches), fmt.Sprint(m.ProductionGenerations), fmt.Sprint(m.ProductionWrites), fmt.Sprint(m.DiagnosticSearches), fmt.Sprint(m.Failures), fmt.Sprint(m.WriteFailures), m.ReuseAccuracy.String(), m.MeasuredP50MS.String(), m.MeasuredP95MS.String(), m.SimulatedP50MS.String(), m.SimulatedP95MS.String(), fmt.Sprint(m.PredictorNS), fmt.Sprint(m.UpdateNS), fmt.Sprint(m.BuildNS), fmt.Sprint(m.MemoryBytes), fmt.Sprint(m.SimulatedCostUSD), fmt.Sprint(m.GenerationDelta), fmt.Sprint(m.CostDeltaUSD)}); err != nil {
			return err
		}
	}
	csvOut.Flush()
	if err := csvOut.Error(); err != nil {
		return err
	}
	artifacts["metrics.csv"] = table.Bytes()
	// Programmatic callers also get the exact typed inputs when no raw CLI
	// bytes were supplied; every saved replay must be reproducible.
	for name, value := range map[string]any{"config.json": c, "workload.json": w} {
		b, err := json.MarshalIndent(value, "", "  ")
		if err != nil {
			return err
		}
		artifacts[name] = b
	}
	for name, b := range inputs {
		var value any
		switch name {
		case "config.json":
			loaded, err := LoadConfig(bytes.NewReader(b))
			if err != nil {
				return fmt.Errorf("saved configuration: %w", err)
			}
			value = loaded
		case "workload.json":
			loaded, err := LoadWorkload(bytes.NewReader(b))
			if err != nil {
				return fmt.Errorf("saved workload: %w", err)
			}
			value = loaded
		default:
			return fmt.Errorf("unexpected input artifact")
		}
		canonical, err := json.MarshalIndent(value, "", "  ")
		if err != nil {
			return err
		}
		if !bytes.Equal(canonical, artifacts[name]) {
			return fmt.Errorf("input artifact %q does not match the evaluated replay", name)
		}
		artifacts[name] = b
	}
	for _, run := range runs {
		var logs bytes.Buffer
		e := json.NewEncoder(&logs)
		for _, row := range run.Rows {
			if err := e.Encode(row); err != nil {
				return err
			}
		}
		artifacts[run.PredictorID+".jsonl"] = logs.Bytes()
		b, err := json.MarshalIndent(run, "", "  ")
		if err != nil {
			return err
		}
		artifacts[run.PredictorID+"-run.json"] = b
	}
	hashes := map[string]string{}
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	for name, b := range artifacts {
		if err := os.WriteFile(filepath.Join(dir, name), b, 0644); err != nil {
			return err
		}
		hashes[name] = hashBytes(b)
	}
	sources := map[string]string{}
	outputAbs, err := filepath.Abs(dir)
	if err != nil {
		return err
	}
	err = filepath.WalkDir(sourceRoot, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if d.Name() == ".git" || d.Name() == "results" || d.Name() == "generated" || d.Name() == "node_modules" {
				return filepath.SkipDir
			}
			return nil
		}
		// An output directory can contain implementation files (including the
		// source root itself). Exclude generated files, never the whole directory.
		abs, err := filepath.Abs(path)
		if err != nil {
			return err
		}
		if filepath.Dir(abs) == outputAbs {
			if _, generated := artifacts[d.Name()]; generated || d.Name() == "manifest.json" {
				return nil
			}
		}
		ext := filepath.Ext(path)
		if ext != ".go" && ext != ".json" && d.Name() != "go.mod" && d.Name() != "go.sum" {
			return nil
		}
		rel, err := filepath.Rel(sourceRoot, path)
		if err != nil {
			return err
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		sources[filepath.ToSlash(rel)] = hashBytes(b)
		return nil
	})
	if err != nil {
		return err
	}
	keys := make([]string, 0, len(sources))
	for k := range sources {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var digest strings.Builder
	for _, k := range keys {
		fmt.Fprintf(&digest, "%s %s\n", k, sources[k])
	}
	revision := "unknown; source hashes recorded"
	status := "unknown"
	git := func(args ...string) (string, error) {
		cmd := exec.Command("git", args...)
		cmd.Dir = sourceRoot
		b, err := cmd.Output()
		return strings.TrimSpace(string(b)), err
	}
	if s, err := git("rev-parse", "HEAD"); err == nil {
		revision = s
	}
	if s, err := git("status", "--porcelain"); err == nil {
		status = s
	}
	manifest := map[string]any{
		"run_id":     fmt.Sprintf("phase2-%s-%d-%s-%s", w.Split, w.Seed, runs[0].Options.Mode, runs[0].Options.Start),
		"created_at": time.Now().UTC(), "go_version": runtime.Version(), "platform": runtime.GOOS + "/" + runtime.GOARCH,
		"source_revision": revision, "git_status": status, "source_sha256": hashBytes([]byte(digest.String())), "source_files_sha256": sources,
		"artifacts_sha256": hashes, "dataset_id": w.ID, "split": w.Split, "workload_seed": w.Seed,
		"configuration_id": c.ID, "embedding_identity": w.Identity, "vector_source": w.VectorSource, "options": runs[0].Options,
		"command": os.Args, "generation": "deterministic simulated fake LLM",
		"projection_seeds":        projectionSeeds,
		"summary_memory_boundary": "conservative structural estimate including projections, entries, vectors, keys, strings and map/container allowances; not a retained-heap or process measurement",
		"timing_boundary":         "measured local routing with precomputed vectors; excludes original embedding inference, evaluation snapshot copies, oracle and logging; includes vector loading, prediction, generation CPU, synchronous maintenance snapshots and Observe; build separately amortized; no sleeps",
	}
	b, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "manifest.json"), b, 0644)
}
