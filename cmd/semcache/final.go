package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"time"

	"semanticcache/internal/semanticcache"
)

func finalHash(b []byte) string { s := sha256.Sum256(b); return hex.EncodeToString(s[:]) }
func finalJSON(v any) []byte {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		panic(err)
	}
	return b
}

type finalManifest struct {
	Created      time.Time         `json:"created_at_utc"`
	Command      []string          `json:"command"`
	GoVersion    string            `json:"go_version"`
	Platform     string            `json:"platform"`
	Revision     string            `json:"revision"`
	GitStatus    string            `json:"git_status"`
	SourceSHA256 string            `json:"source_snapshot_sha256"`
	Files        map[string]string `json:"artifacts_sha256"`
	Error        string            `json:"error,omitempty"`
}

func finalWrite(dir, name string, b []byte, hashes map[string]string) error {
	if !filepath.IsLocal(filepath.FromSlash(name)) {
		return fmt.Errorf("unsafe artifact path")
	}
	p := filepath.Join(dir, filepath.FromSlash(name))
	if err := os.MkdirAll(filepath.Dir(p), 0755); err != nil {
		return err
	}
	f, err := os.OpenFile(p, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return err
	}
	_, err = f.Write(b)
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	hashes[name] = finalHash(b)
	return nil
}

func saveFinalRun(dir string, run semanticcache.FinalRun, source string, runErr error) error {
	files := map[string]string{}
	if err := finalWrite(dir, "run.json", finalJSON(run), files); err != nil {
		return err
	}
	var trace bytes.Buffer
	enc := json.NewEncoder(&trace)
	for _, row := range run.Rows {
		if err := enc.Encode(row); err != nil {
			return err
		}
	}
	if err := finalWrite(dir, "requests.jsonl", trace.Bytes(), files); err != nil {
		return err
	}
	if err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			return nil
		}
		rel, e := filepath.Rel(dir, path)
		if e != nil {
			return e
		}
		rel = filepath.ToSlash(rel)
		if _, ok := files[rel]; ok {
			return nil
		}
		b, e := os.ReadFile(path)
		if e != nil {
			return e
		}
		files[rel] = finalHash(b)
		return nil
	}); err != nil {
		return err
	}
	m := finalManifest{Created: time.Now().UTC(), GoVersion: runtime.Version(), Platform: runtime.GOOS + "/" + runtime.GOARCH, SourceSHA256: source, Files: files}
	if runErr != nil {
		m.Error = runErr.Error()
	}
	return finalWrite(dir, "manifest.json", finalJSON(m), map[string]string{})
}

type finalTableRow struct {
	Scenario        string                   `json:"scenario"`
	Backend         string                   `json:"backend"`
	Role            string                   `json:"role"`
	WorkloadSeed    int64                    `json:"workload_seed"`
	ProjectionSeed  int64                    `json:"projection_seed"`
	Evidence        string                   `json:"evidence"`
	Stats           semanticcache.FinalStats `json:"stats"`
	SavedSearches   int                      `json:"saved_searches"`
	GenerationDelta int                      `json:"generation_delta"`
	CostDelta       float64                  `json:"simulated_paired_cost_delta_usd"`
	P95Delta        float64                  `json:"mixed_paired_p95_delta_ms"`
	Violations      []string                 `json:"budget_violations"`
}

func finalRunKey(s string, seed int64, role string, projection int64) string {
	return fmt.Sprintf("%s/%d/%s-%d", s, seed, role, projection)
}

func finalTables(c semanticcache.Config, runs []semanticcache.FinalRun) ([]finalTableRow, error) {
	baselines := map[string]semanticcache.FinalStats{}
	seen := map[string]bool{}
	for _, run := range runs {
		key := finalRunKey(run.Scenario.ID, run.WorkloadSeed, run.Role, run.ProjectionSeed)
		if seen[key] {
			return nil, fmt.Errorf("duplicate final condition")
		}
		seen[key] = true
		if run.Role == "baseline" {
			baselines[finalRunKey(run.Scenario.ID, run.WorkloadSeed, "baseline", 0)] = run.Stats
		}
	}
	var rows []finalTableRow
	for _, run := range runs {
		b, ok := baselines[finalRunKey(run.Scenario.ID, run.WorkloadSeed, "baseline", 0)]
		if !ok {
			return nil, fmt.Errorf("missing paired baseline")
		}
		row := finalTableRow{Scenario: run.Scenario.ID, Backend: run.Scenario.Backend, Role: run.Role, WorkloadSeed: run.WorkloadSeed, ProjectionSeed: run.ProjectionSeed, Evidence: finalRunKey(run.Scenario.ID, run.WorkloadSeed, run.Role, run.ProjectionSeed), Stats: run.Stats}
		row.SavedSearches = b.Metrics.ProductionSearches - run.Stats.Metrics.ProductionSearches
		row.GenerationDelta = run.Stats.Metrics.ProductionGenerations - b.Metrics.ProductionGenerations
		row.CostDelta = run.Stats.Metrics.SimulatedCostUSD - b.Metrics.SimulatedCostUSD
		row.P95Delta = run.Stats.MixedP95MS.Value - b.MixedP95MS.Value
		if run.Role != "baseline" {
			row.Violations = semanticcache.FinalViolations(c, b, run.Stats)
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func finalCSV(rows []finalTableRow) ([]byte, error) {
	var b bytes.Buffer
	w := csv.NewWriter(&b)
	if err := w.Write([]string{"scenario", "backend", "role", "workload_seed", "projection_seed", "requests", "tp", "fp", "fn", "tn", "recall", "reuse_accuracy", "searches", "generations", "saved_searches", "generation_delta", "mixed_p95_ms", "paired_p95_delta_ms", "simulated_cost_usd", "paired_cost_delta_usd", "summary_bytes", "rebuild_peak_bytes", "sampled_heap_bytes", "process_working_set_bytes", "predictor_ns", "update_ns", "build_ns", "final_recovery_ns", "fallbacks", "unseen_hits", "intrinsic_misses", "stale_searches", "http_attempts", "backend_mismatches", "failures", "violations", "evidence"}); err != nil {
		return nil, err
	}
	for _, r := range rows {
		s, m := r.Stats, r.Stats.Metrics
		x := m.Confusion
		if err := w.Write([]string{r.Scenario, r.Backend, r.Role, fmt.Sprint(r.WorkloadSeed), fmt.Sprint(r.ProjectionSeed), fmt.Sprint(m.Requests), fmt.Sprint(x.TP), fmt.Sprint(x.FP), fmt.Sprint(x.FN), fmt.Sprint(x.TN), m.CacheHitRecall.String(), m.ReuseAccuracy.String(), fmt.Sprint(m.ProductionSearches), fmt.Sprint(m.ProductionGenerations), fmt.Sprint(r.SavedSearches), fmt.Sprint(r.GenerationDelta), s.MixedP95MS.String(), fmt.Sprint(r.P95Delta), fmt.Sprint(m.SimulatedCostUSD), fmt.Sprint(r.CostDelta), fmt.Sprint(m.MemoryBytes), fmt.Sprint(s.PeakRebuildBytes), fmt.Sprint(s.SampledHeapBytes), fmt.Sprint(s.ProcessBytes), fmt.Sprint(m.PredictorNS), fmt.Sprint(m.UpdateNS), fmt.Sprint(m.BuildNS), fmt.Sprint(s.RecoveryNS), fmt.Sprint(s.Fallbacks), fmt.Sprint(s.UnseenHits), fmt.Sprint(s.IntrinsicMisses), fmt.Sprint(s.StaleSearches), fmt.Sprint(s.HTTPAttempts), fmt.Sprint(s.BackendMismatches), fmt.Sprint(m.Failures + m.WriteFailures), strings.Join(r.Violations, ";"), r.Evidence}); err != nil {
			return nil, err
		}
	}
	w.Flush()
	return b.Bytes(), w.Error()
}

type finalAggregate struct {
	Scenario              string                  `json:"scenario"`
	Role                  string                  `json:"role"`
	Runs                  int                     `json:"runs"`
	Requests              int                     `json:"requests"`
	Confusion             semanticcache.Confusion `json:"pooled_confusion"`
	RecallMinMedianMax    []float64               `json:"defined_recall_min_median_max"`
	UndefinedRecallRuns   int                     `json:"undefined_recall_runs"`
	CostDeltaMinMedianMax []float64               `json:"paired_cost_delta_min_median_max_usd"`
	P95DeltaMinMedianMax  []float64               `json:"paired_p95_delta_min_median_max_ms"`
	SavedSearches         int                     `json:"saved_searches"`
	GenerationDelta       int                     `json:"generation_delta"`
	Violations            map[string]int          `json:"budget_violation_run_counts"`
}

func finalRange(xs []float64) []float64 {
	if len(xs) == 0 {
		return nil
	}
	sort.Float64s(xs)
	middle := xs[len(xs)/2]
	if len(xs)%2 == 0 {
		middle = (xs[len(xs)/2-1] + middle) / 2
	}
	return []float64{xs[0], middle, xs[len(xs)-1]}
}

func finalAggregates(rows []finalTableRow) []finalAggregate {
	var out []finalAggregate
	keys := []string{}
	groups := map[string][]finalTableRow{}
	for _, r := range rows {
		key := r.Scenario + "/" + r.Role
		if _, ok := groups[key]; !ok {
			keys = append(keys, key)
		}
		groups[key] = append(groups[key], r)
	}
	for _, key := range keys {
		group := groups[key]
		a := finalAggregate{Scenario: group[0].Scenario, Role: group[0].Role, Violations: map[string]int{}}
		var recall, cost, p95 []float64
		for _, r := range group {
			a.Runs++
			a.Requests += r.Stats.Metrics.Requests
			x := r.Stats.Metrics.Confusion
			a.Confusion.TP += x.TP
			a.Confusion.FP += x.FP
			a.Confusion.FN += x.FN
			a.Confusion.TN += x.TN
			a.Confusion.N += x.N
			if r.Stats.Metrics.CacheHitRecall.Defined {
				recall = append(recall, r.Stats.Metrics.CacheHitRecall.Value)
			} else {
				a.UndefinedRecallRuns++
			}
			cost = append(cost, r.CostDelta)
			p95 = append(p95, r.P95Delta)
			a.SavedSearches += r.SavedSearches
			a.GenerationDelta += r.GenerationDelta
			for _, v := range r.Violations {
				a.Violations[v]++
			}
		}
		a.RecallMinMedianMax = finalRange(recall)
		a.CostDeltaMinMedianMax = finalRange(cost)
		a.P95DeltaMinMedianMax = finalRange(p95)
		out = append(out, a)
	}
	return out
}

func finalEvaluate(args []string, out, errOut io.Writer) int {
	flags := flag.NewFlagSet("final-evaluate", flag.ContinueOnError)
	flags.SetOutput(errOut)
	planPath := flags.String("plan", "configs/final_matrix.json", "predeclared final matrix JSON")
	candidatePath := flags.String("candidate", "configs/final_candidate.json", "frozen Phase 4 candidate")
	dataDir := flags.String("data-dir", "data/generated", "complete final workload files")
	output := flags.String("output", "results/phase7/final", "new evidence directory")
	endpoint := flags.String("endpoint", "http://127.0.0.1:16333", "Qdrant endpoint; remote conditions are never silently skipped")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if flags.NArg() != 0 {
		return 2
	}
	fail := func(err error) int { fmt.Fprintln(errOut, err); return 1 }
	pb, err := os.ReadFile(*planPath)
	if err != nil {
		return fail(err)
	}
	plan, err := semanticcache.LoadFinalPlan(bytes.NewReader(pb))
	if err != nil {
		return fail(err)
	}
	cb, err := os.ReadFile(*candidatePath)
	if err != nil {
		return fail(err)
	}
	candidate, err := semanticcache.LoadCandidate(bytes.NewReader(cb))
	if err != nil {
		return fail(err)
	}
	if err = plan.Validate(candidate); err != nil {
		return fail(err)
	}
	if finalHash(cb) != plan.CandidateSHA256 {
		return fail(fmt.Errorf("frozen candidate checksum differs from predeclared plan"))
	}
	root, err := findSourceRoot()
	if err != nil {
		return fail(err)
	}
	if err = os.MkdirAll(filepath.Dir(*output), 0755); err != nil {
		return fail(err)
	}
	if err = os.Mkdir(*output, 0755); err != nil {
		return fail(err)
	}
	files := map[string]string{}
	// Save immutable plan/candidate and source bytes before reading final inputs.
	if err = finalWrite(*output, "plan.json", pb, files); err != nil {
		return fail(err)
	}
	if err = finalWrite(*output, "candidate.json", cb, files); err != nil {
		return fail(err)
	}
	err = filepath.WalkDir(root, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "results", "data", "sites", "node_modules":
				return filepath.SkipDir
			}
			return nil
		}
		rel, e := filepath.Rel(root, path)
		if e != nil {
			return e
		}
		rel = filepath.ToSlash(rel)
		if filepath.Ext(path) != ".go" && rel != "go.mod" && rel != "go.sum" {
			return nil
		}
		b, e := os.ReadFile(path)
		if e != nil {
			return e
		}
		return finalWrite(*output, "sources/"+rel, b, files)
	})
	if err != nil {
		return fail(err)
	}
	sourceHash := finalHash(finalJSON(files))
	freeze := map[string]any{"recorded_before_final_inputs": time.Now().UTC(), "candidate_sha256": finalHash(cb), "plan_sha256": finalHash(pb), "source_snapshot_sha256": sourceHash, "boundary": "Original final splits are unchanged. Stress streams are separately named transformations. Diagnostic LSH retains development L2/K8 across all declared projection seeds; it cannot change selection. Ranges across three workload seeds (and the full 3x3 diagnostic seed family), no confidence intervals or independence claims. Measured routing/HTTP plus simulated model latencies and prices; summary memory is a conservative structural estimate; sampled heap/working set include the whole harness. Warm setup and cleanup are excluded and logged. Each seed alternates baseline/candidate execution order."}
	if err = finalWrite(*output, "freeze.json", finalJSON(freeze), files); err != nil {
		return fail(err)
	}
	var workloads []semanticcache.Workload
	for _, seed := range candidate.Experiment.WorkloadSeeds {
		name := fmt.Sprintf("final-%d.json", seed)
		b, e := os.ReadFile(filepath.Join(*dataDir, name))
		if e != nil {
			return fail(e)
		}
		w, e := semanticcache.LoadWorkload(bytes.NewReader(b))
		if e != nil {
			return fail(e)
		}
		if w.Seed != seed || w.Split != "final" || w.ID != candidate.Experiment.WorkloadID || len(w.Requests) != candidate.Experiment.RequestsPerSplit || w.Identity != candidate.Identity {
			return fail(fmt.Errorf("incomplete or incompatible final workload %s", name))
		}
		if e = finalWrite(*output, "inputs/"+name, b, files); e != nil {
			return fail(e)
		}
		workloads = append(workloads, w)
	}
	if err = semanticcache.ValidateWorkloads(workloads); err != nil {
		return fail(err)
	}
	for _, s := range plan.Scenarios {
		if s.Backend == "remote" {
			client := &http.Client{Timeout: 3 * time.Second}
			resp, e := client.Get(*endpoint + "/")
			if e != nil {
				return fail(e)
			}
			b, e := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
			resp.Body.Close()
			if e != nil {
				return fail(e)
			}
			if resp.StatusCode != 200 {
				return fail(fmt.Errorf("backend identity HTTP %d", resp.StatusCode))
			}
			if e = finalWrite(*output, "backend.json", b, files); e != nil {
				return fail(e)
			}
			break
		}
	}
	var runs []semanticcache.FinalRun
	for _, scenario := range plan.Scenarios {
		for wi, w := range workloads {
			stress, e := semanticcache.FinalWorkload(w, scenario)
			if e != nil {
				return fail(e)
			}
			input := fmt.Sprintf("streams/%s-%d.json", scenario.ID, w.Seed)
			if e = finalWrite(*output, input, finalJSON(stress), files); e != nil {
				return fail(e)
			}
			type variant struct {
				role       string
				projection *semanticcache.LSHConfig
			}
			variants := []variant{{"baseline", nil}}
			if candidate.Router == "always-search" {
				variants = append(variants, variant{"frozen-candidate", nil})
			} else {
				for _, p := range candidate.Projections {
					q := p
					variants = append(variants, variant{"frozen-candidate", &q})
				}
			}
			if wi%2 == 1 {
				variants[0], variants[len(variants)-1] = variants[len(variants)-1], variants[0]
			}
			if scenario.Diagnostic {
				for _, seed := range candidate.Experiment.ProjectionSeeds {
					p := plan.Diagnostic
					p.Seed = seed
					variants = append(variants, variant{"diagnostic-lsh", &p})
				}
			}
			for _, v := range variants {
				run, e := semanticcache.RunFinalCondition(context.Background(), candidate, scenario, stress, v.role, v.projection, *endpoint)
				key := finalRunKey(scenario.ID, w.Seed, v.role, run.ProjectionSeed)
				if e == nil && scenario.Fill && v.projection != nil {
					measurement, measureErr := semanticcache.MeasureSummary(filepath.Join(*output, filepath.FromSlash(key), "memory"), candidate.Identity, run.Initial, func() (semanticcache.ReplayPredictor, error) {
						p, err := semanticcache.NewProjections(*v.projection)
						if err != nil {
							return nil, err
						}
						return semanticcache.NewLSHSummary(p)
					})
					if measureErr != nil {
						e = measureErr
					} else {
						run.MemoryMeasurement = &measurement
						run.Stats, e = semanticcache.SummarizeFinal(candidate.Experiment, run)
					}
				}
				if saveErr := saveFinalRun(filepath.Join(*output, filepath.FromSlash(key)), run, sourceHash, e); saveErr != nil {
					return fail(saveErr)
				}
				if e != nil {
					return fail(e)
				}
				runs = append(runs, run)
				fmt.Fprintf(out, "%s: %d requests, searches=%d generations=%d recall=%s\n", key, run.Stats.Metrics.Requests, run.Stats.Metrics.ProductionSearches, run.Stats.Metrics.ProductionGenerations, run.Stats.Metrics.CacheHitRecall.String())
			}
		}
	}
	rows, err := finalTables(candidate.Experiment, runs)
	if err != nil {
		return fail(err)
	}
	csvBytes, err := finalCSV(rows)
	if err != nil {
		return fail(err)
	}
	for name, b := range map[string][]byte{"results.json": finalJSON(rows), "results.csv": csvBytes, "aggregates.json": finalJSON(finalAggregates(rows))} {
		if err = finalWrite(*output, name, b, files); err != nil {
			return fail(err)
		}
	}
	// Bind every per-run manifest/trace as well as the shared source/input bytes.
	err = filepath.WalkDir(*output, func(path string, d fs.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if d.IsDir() {
			return nil
		}
		rel, e := filepath.Rel(*output, path)
		if e != nil {
			return e
		}
		rel = filepath.ToSlash(rel)
		if _, ok := files[rel]; ok {
			return nil
		}
		b, e := os.ReadFile(path)
		if e != nil {
			return e
		}
		files[rel] = finalHash(b)
		return nil
	})
	if err != nil {
		return fail(err)
	}
	git := func(args ...string) string {
		c := exec.Command("git", args...)
		c.Dir = root
		b, _ := c.Output()
		return strings.TrimSpace(string(b))
	}
	m := finalManifest{Created: time.Now().UTC(), Command: os.Args, GoVersion: runtime.Version(), Platform: runtime.GOOS + "/" + runtime.GOARCH, Revision: git("rev-parse", "HEAD"), GitStatus: git("status", "--porcelain"), SourceSHA256: sourceHash, Files: files}
	if err = finalWrite(*output, "manifest.json", finalJSON(m), map[string]string{}); err != nil {
		return fail(err)
	}
	fmt.Fprintf(out, "saved %d runs and paired/aggregate tables to %s; budget failures remain in results.csv\n", len(rows), *output)
	return 0
}

func loadFinalEvidence(dir string) (semanticcache.FinalPlan, semanticcache.FinalCandidate, []semanticcache.FinalRun, error) {
	var plan semanticcache.FinalPlan
	var candidate semanticcache.FinalCandidate
	var runs []semanticcache.FinalRun
	var manifest finalManifest
	readBytes := func(name string) ([]byte, error) {
		b, e := os.ReadFile(filepath.Join(dir, filepath.FromSlash(name)))
		if e != nil {
			return nil, e
		}
		if name != "manifest.json" && manifest.Files[name] != finalHash(b) {
			return nil, fmt.Errorf("missing or invalid artifact checksum: %s", name)
		}
		return b, nil
	}
	read := func(name string, v any) error {
		b, err := readBytes(name)
		if err != nil {
			return err
		}
		dec := json.NewDecoder(bytes.NewReader(b))
		dec.DisallowUnknownFields()
		if err = dec.Decode(v); err != nil {
			return err
		}
		if err = dec.Decode(new(any)); err != io.EOF {
			return fmt.Errorf("trailing JSON in %s", name)
		}
		return nil
	}
	if err := read("manifest.json", &manifest); err != nil {
		return plan, candidate, nil, err
	}
	if manifest.Error != "" {
		return plan, candidate, nil, fmt.Errorf("incomplete final manifest: %s", manifest.Error)
	}
	for _, name := range []string{"results.json", "results.csv", "aggregates.json", "sources/go.mod"} {
		if manifest.Files[name] == "" {
			return plan, candidate, nil, fmt.Errorf("missing final artifact: %s", name)
		}
	}
	for name, want := range manifest.Files {
		if !filepath.IsLocal(filepath.FromSlash(name)) {
			return plan, candidate, nil, fmt.Errorf("unsafe manifest path")
		}
		b, e := os.ReadFile(filepath.Join(dir, filepath.FromSlash(name)))
		if e != nil {
			return plan, candidate, nil, e
		}
		if finalHash(b) != want {
			return plan, candidate, nil, fmt.Errorf("checksum mismatch: %s", name)
		}
	}
	sources := map[string]string{}
	for name, hash := range manifest.Files {
		if name == "candidate.json" || name == "plan.json" || strings.HasPrefix(name, "sources/") {
			sources[name] = hash
		}
	}
	if finalHash(finalJSON(sources)) != manifest.SourceSHA256 {
		return plan, candidate, nil, fmt.Errorf("source snapshot identity mismatch")
	}
	if err := read("plan.json", &plan); err != nil {
		return plan, candidate, nil, err
	}
	if err := read("candidate.json", &candidate); err != nil {
		return plan, candidate, nil, err
	}
	if err := plan.Validate(candidate); err != nil {
		return plan, candidate, nil, err
	}
	cb, err := os.ReadFile(filepath.Join(dir, "candidate.json"))
	if err != nil {
		return plan, candidate, nil, err
	}
	if finalHash(cb) != plan.CandidateSHA256 {
		return plan, candidate, nil, fmt.Errorf("candidate differs from plan")
	}
	workloads := map[int64]semanticcache.Workload{}
	var allWorkloads []semanticcache.Workload
	for _, seed := range candidate.Experiment.WorkloadSeeds {
		b, err := readBytes(fmt.Sprintf("inputs/final-%d.json", seed))
		if err != nil {
			return plan, candidate, nil, err
		}
		w, err := semanticcache.LoadWorkload(bytes.NewReader(b))
		if err != nil {
			return plan, candidate, nil, err
		}
		if w.Seed != seed || w.Split != "final" || w.ID != candidate.Experiment.WorkloadID || w.Identity != candidate.Identity || len(w.Requests) != candidate.Experiment.RequestsPerSplit {
			return plan, candidate, nil, fmt.Errorf("final workload differs from frozen seed/configuration: %d", seed)
		}
		workloads[seed] = w
		allWorkloads = append(allWorkloads, w)
	}
	if err := semanticcache.ValidateWorkloads(allWorkloads); err != nil {
		return plan, candidate, nil, err
	}
	for _, s := range plan.Scenarios {
		for _, seed := range candidate.Experiment.WorkloadSeeds {
			type variant struct {
				role string
				seed int64
			}
			variants := []variant{{"baseline", 0}}
			if candidate.Router == "always-search" {
				variants = append(variants, variant{"frozen-candidate", 0})
			} else {
				for _, p := range candidate.Projections {
					variants = append(variants, variant{"frozen-candidate", p.Seed})
				}
			}
			if s.Diagnostic {
				for _, p := range candidate.Experiment.ProjectionSeeds {
					variants = append(variants, variant{"diagnostic-lsh", p})
				}
			}
			for _, v := range variants {
				key := finalRunKey(s.ID, seed, v.role, v.seed)
				var runManifest finalManifest
				if err := read(key+"/manifest.json", &runManifest); err != nil {
					return plan, candidate, nil, err
				}
				if runManifest.SourceSHA256 != manifest.SourceSHA256 || runManifest.Error != "" {
					return plan, candidate, nil, fmt.Errorf("incomplete or mismatched run manifest")
				}
				for name, hash := range runManifest.Files {
					if !filepath.IsLocal(filepath.FromSlash(name)) || manifest.Files[key+"/"+name] != hash {
						return plan, candidate, nil, fmt.Errorf("run artifact differs from shared manifest: %s", key)
					}
				}
				if runManifest.Files["run.json"] == "" || runManifest.Files["requests.jsonl"] == "" {
					return plan, candidate, nil, fmt.Errorf("incomplete run artifacts: %s", key)
				}
				var run semanticcache.FinalRun
				if err := read(key+"/run.json", &run); err != nil {
					return plan, candidate, nil, err
				}
				if run.Scenario != s || run.WorkloadSeed != seed || run.Role != v.role || run.ProjectionSeed != v.seed {
					return plan, candidate, nil, fmt.Errorf("run identity mismatch: %s", key)
				}
				var expectedConfig *semanticcache.LSHConfig
				if v.role == "diagnostic-lsh" {
					config := plan.Diagnostic
					config.Seed = v.seed
					expectedConfig = &config
				}
				if v.role == "frozen-candidate" && candidate.Router == "lsh" {
					for _, config := range candidate.Projections {
						if config.Seed == v.seed {
							owned := config
							expectedConfig = &owned
						}
					}
					if expectedConfig == nil {
						return plan, candidate, nil, fmt.Errorf("missing frozen projection: %s", key)
					}
				}
				if expectedConfig == nil {
					if run.PredictorID != "always-search" || run.Projections != nil {
						return plan, candidate, nil, fmt.Errorf("unfrozen constant predictor: %s", key)
					}
				} else {
					p, err := semanticcache.NewProjections(*expectedConfig)
					if err != nil {
						return plan, candidate, nil, err
					}
					if run.Projections == nil || !reflect.DeepEqual(run.Projections, p) || run.PredictorID != p.ID() {
						return plan, candidate, nil, fmt.Errorf("run projection differs from declared family: %s", key)
					}
				}
				b, e := os.ReadFile(filepath.Join(dir, filepath.FromSlash(key+"/requests.jsonl")))
				if e != nil {
					return plan, candidate, nil, e
				}
				if manifest.Files[key+"/requests.jsonl"] != finalHash(b) {
					return plan, candidate, nil, fmt.Errorf("unhashed request trace")
				}
				dec := json.NewDecoder(bytes.NewReader(b))
				dec.DisallowUnknownFields()
				for {
					var row semanticcache.FinalRow
					e := dec.Decode(&row)
					if e == io.EOF {
						break
					}
					if e != nil {
						return plan, candidate, nil, e
					}
					run.Rows = append(run.Rows, row)
				}
				var w semanticcache.Workload
				if e = read(fmt.Sprintf("streams/%s-%d.json", s.ID, seed), &w); e != nil {
					return plan, candidate, nil, e
				}
				expected, e := semanticcache.FinalWorkload(workloads[seed], s)
				if e != nil {
					return plan, candidate, nil, e
				}
				if !reflect.DeepEqual(w, expected) {
					return plan, candidate, nil, fmt.Errorf("saved stress stream differs from declared transformation")
				}
				if len(run.Rows) != len(w.Requests) {
					return plan, candidate, nil, fmt.Errorf("incomplete trace")
				}
				for i, row := range run.Rows {
					r := w.Requests[i]
					if row.Evidence.RequestID != r.Request.ID || row.Evidence.Index != i || row.Evidence.Result.RequestID != r.Request.ID || row.Evidence.GroupID != r.GroupID || row.Evidence.Category != r.Category || row.Evidence.Eligible != r.Request.CacheEligible || (r.Request.CacheEligible && row.Evidence.OracleHit == nil) {
						return plan, candidate, nil, fmt.Errorf("trace/workload mismatch")
					}
				}
				m, e := semanticcache.SummarizeFinal(candidate.Experiment, run)
				if e != nil {
					return plan, candidate, nil, e
				}
				if !reflect.DeepEqual(m, run.Stats) {
					return plan, candidate, nil, fmt.Errorf("trace-derived metrics differ: %s", key)
				}
				runs = append(runs, run)
			}
		}
	}
	if err := verifyFinalTables(dir, candidate.Experiment, runs); err != nil {
		return plan, candidate, nil, err
	}
	return plan, candidate, runs, nil
}

func verifyFinalTables(dir string, config semanticcache.Config, runs []semanticcache.FinalRun) error {
	rows, err := finalTables(config, runs)
	if err != nil {
		return err
	}
	b, err := os.ReadFile(filepath.Join(dir, "results.json"))
	if err != nil {
		return err
	}
	var original []finalTableRow
	if err = json.Unmarshal(b, &original); err != nil {
		return err
	}
	saved := append([]finalTableRow(nil), original...)
	sort.Slice(rows, func(i, j int) bool { return rows[i].Evidence < rows[j].Evidence })
	sort.Slice(saved, func(i, j int) bool { return saved[i].Evidence < saved[j].Evidence })
	if !reflect.DeepEqual(rows, saved) {
		return fmt.Errorf("result table differs from traces in %s", dir)
	}
	expectedCSV, err := finalCSV(original)
	if err != nil {
		return err
	}
	gotCSV, err := os.ReadFile(filepath.Join(dir, "results.csv"))
	if err != nil || !bytes.Equal(gotCSV, expectedCSV) {
		return fmt.Errorf("CSV differs from traces in %s", dir)
	}
	gotAggregate, err := os.ReadFile(filepath.Join(dir, "aggregates.json"))
	if err != nil || !bytes.Equal(gotAggregate, finalJSON(finalAggregates(original))) {
		return fmt.Errorf("aggregate table differs from traces in %s", dir)
	}
	return nil
}

// Collection names and measured durations vary between independent runs. All
// remaining request evidence, including responses and fault attribution, must
// agree. Copy calls before normalization to preserve the loaded evidence.
func comparableFinalRow(row semanticcache.FinalRow) semanticcache.FinalRow {
	row.Evidence.Result.Timings = semanticcache.StageTimings{}
	row.Calls = append([]semanticcache.RemoteCall(nil), row.Calls...)
	for i := range row.Calls {
		row.Calls[i].DurationNS = 0
		parts := strings.SplitN(row.Calls[i].Path, "/", 4)
		if len(parts) >= 3 && parts[1] == "collections" {
			row.Calls[i].Path = "/collections/owned"
			if len(parts) == 4 {
				row.Calls[i].Path += "/" + parts[3]
			}
		}
	}
	return row
}

func finalVerify(args []string, out, errOut io.Writer) int {
	flags := flag.NewFlagSet("final-verify", flag.ContinueOnError)
	flags.SetOutput(errOut)
	input := flags.String("input", "", "completed final evidence directory")
	replay := flags.String("replay", "", "optional clean second evaluation to compare deterministic outcomes")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if *input == "" || flags.NArg() != 0 {
		fmt.Fprintln(errOut, "final-verify requires -input")
		return 2
	}
	fail := func(err error) int { fmt.Fprintln(errOut, err); return 1 }
	plan, candidate, runs, err := loadFinalEvidence(*input)
	if err != nil {
		return fail(err)
	}
	// Canonical table order does not depend on the paired execution order.
	order := func(rs []semanticcache.FinalRun) {
		sort.Slice(rs, func(i, j int) bool {
			a, b := rs[i], rs[j]
			return finalRunKey(a.Scenario.ID, a.WorkloadSeed, a.Role, a.ProjectionSeed) < finalRunKey(b.Scenario.ID, b.WorkloadSeed, b.Role, b.ProjectionSeed)
		})
	}
	order(runs)
	if *replay != "" {
		otherPlan, otherCandidate, other, err := loadFinalEvidence(*replay)
		if err != nil {
			return fail(err)
		}
		if !reflect.DeepEqual(plan, otherPlan) || !reflect.DeepEqual(candidate, otherCandidate) {
			return fail(fmt.Errorf("clean replay changed frozen inputs"))
		}
		for _, seed := range candidate.Experiment.WorkloadSeeds {
			name := fmt.Sprintf("final-%d.json", seed)
			a, err := os.ReadFile(filepath.Join(*input, "inputs", name))
			if err != nil {
				return fail(err)
			}
			b, err := os.ReadFile(filepath.Join(*replay, "inputs", name))
			if err != nil {
				return fail(err)
			}
			if !bytes.Equal(a, b) {
				return fail(fmt.Errorf("clean replay changed workload bytes: %s", name))
			}
		}
		order(other)
		if len(runs) != len(other) {
			return fail(fmt.Errorf("clean replay is incomplete"))
		}
		for i, a := range runs {
			b := other[i]
			x, y := a.Stats.Metrics, b.Stats.Metrics
			if a.Role != b.Role || a.Scenario != b.Scenario || a.WorkloadSeed != b.WorkloadSeed || a.ProjectionSeed != b.ProjectionSeed || x.Confusion != y.Confusion || x.ProductionSearches != y.ProductionSearches || x.ProductionGenerations != y.ProductionGenerations || x.ProductionWrites != y.ProductionWrites || x.CorrectReuse != y.CorrectReuse || x.ReuseLabeled != y.ReuseLabeled || a.Stats.UnseenHits != b.Stats.UnseenHits || a.Stats.IntrinsicMisses != b.Stats.IntrinsicMisses || a.Stats.Fallbacks != b.Stats.Fallbacks || a.Stats.BackendMismatches != b.Stats.BackendMismatches || !reflect.DeepEqual(a.Initial, b.Initial) || !reflect.DeepEqual(a.Final, b.Final) || len(a.Rows) != len(b.Rows) {
				return fail(fmt.Errorf("clean replay changed outcomes: %s", a.Scenario.ID))
			}
			for j := range a.Rows {
				if !reflect.DeepEqual(comparableFinalRow(a.Rows[j]), comparableFinalRow(b.Rows[j])) {
					return fail(fmt.Errorf("clean replay changed request evidence: %s/%s", a.Scenario.ID, a.Rows[j].Evidence.RequestID))
				}
			}
		}
		fmt.Fprintf(out, "clean replay reproduced %d runs: exact confusion/call/reuse/fault/final-state outcomes; measured timings, CPU-priced costs and sampled process memory vary\n", len(runs))
	}
	fmt.Fprintf(out, "verified %d complete runs, artifact hashes, trace-derived metrics, paired budgets, CSV and aggregate tables\n", len(runs))
	return 0
}
