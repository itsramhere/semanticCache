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
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"semanticcache/internal/semanticcache"
)

func tune(args []string, out, errOut io.Writer) int {
	fs := flag.NewFlagSet("tune", flag.ContinueOnError)
	fs.SetOutput(errOut)
	cp := fs.String("config", "configs/phase2_fixture.json", "frozen experiment configuration")
	gp := fs.String("grid", "configs/tuning_grid.json", "small recorded single-bucket grid")
	dp := fs.String("data-dir", "data/generated", "directory containing tuning-<seed>.json")
	op := fs.String("output", "results/phase4/tuning", "new output directory (must not exist)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() != 0 {
		fmt.Fprintln(errOut, "unexpected positional arguments")
		return 2
	}
	// Set once near process startup, before workload/replay allocations. This
	// makes sub-MiB summary attribution visible in sampled heap profiles.
	runtime.MemProfileRate = 4096
	fail := func(err error) int { fmt.Fprintln(errOut, err); return 1 }
	cb, err := os.ReadFile(*cp)
	if err != nil {
		return fail(err)
	}
	c, err := semanticcache.LoadConfig(bytes.NewReader(cb))
	if err != nil {
		return fail(err)
	}
	gb, err := os.ReadFile(*gp)
	if err != nil {
		return fail(err)
	}
	g, err := semanticcache.LoadTuningGrid(bytes.NewReader(gb))
	if err != nil {
		return fail(err)
	}
	var ws []semanticcache.Workload
	inputs := map[int64][]byte{}
	for _, seed := range c.WorkloadSeeds {
		b, err := os.ReadFile(filepath.Join(*dp, fmt.Sprintf("tuning-%d.json", seed)))
		if err != nil {
			return fail(err)
		}
		w, err := semanticcache.LoadWorkload(bytes.NewReader(b))
		if err != nil {
			return fail(err)
		}
		ws = append(ws, w)
		inputs[seed] = b
	}
	if err := semanticcache.ValidateTuningInputs(c, ws); err != nil {
		return fail(err)
	}
	root, err := findSourceRoot()
	if err != nil {
		return fail(err)
	}
	// Validate every resource bound before creating output or running outcomes.
	for _, l := range g.Tables {
		for _, k := range g.Bits {
			if err := (semanticcache.LSHConfig{Version: "cosine-hyperplanes-v1", Identity: ws[0].Identity, Seed: c.ProjectionSeeds[0], Tables: l, Bits: k}).Validate(); err != nil {
				return fail(err)
			}
		}
	}
	if _, err := os.Stat(*op); err == nil {
		return fail(fmt.Errorf("output already exists; choose a new directory"))
	} else if !os.IsNotExist(err) {
		return fail(err)
	}
	if err := os.MkdirAll(*op, 0755); err != nil {
		return fail(err)
	}
	if err := os.WriteFile(filepath.Join(*op, "grid.json"), gb, 0644); err != nil {
		return fail(err)
	}
	if err := os.WriteFile(filepath.Join(*op, "config.json"), cb, 0644); err != nil {
		return fail(err)
	}
	result := semanticcache.TuningResult{Grid: g, Config: c}
	for _, w := range ws {
		for _, mode := range g.Modes {
			for _, start := range g.Starts {
				for _, capacity := range c.Policy.CapacityEntries {
					o := semanticcache.ReplayOptions{Mode: mode, Start: start, Capacity: capacity, OracleEvery: 1, OracleLogging: true}
					condition := fmt.Sprintf("w%d-%s-%s-c%d", w.Seed, mode, start, capacity)
					base, err := semanticcache.Replay(context.Background(), c, w, o, &semanticcache.ConstantPredictor{Search: true})
					if err != nil {
						return fail(err)
					}
					baseline, err := semanticcache.Summarize(c, base)
					if err != nil {
						return fail(err)
					}
					base = compactReplay(base)
					add := func(variant string, ps int64, l, k int, factory func() (semanticcache.ReplayPredictor, error), baselineRun bool) error {
						var run semanticcache.ReplayRun
						if baselineRun {
							run = base
						} else {
							p, err := factory()
							if err != nil {
								return err
							}
							run, err = semanticcache.Replay(context.Background(), c, w, o, p)
							if err != nil {
								return err
							}
						}
						m, err := semanticcache.Summarize(c, run)
						if err != nil {
							return err
						}
						m.GenerationDelta = m.ProductionGenerations - baseline.ProductionGenerations
						m.CostDeltaUSD = m.SimulatedCostUSD - baseline.SimulatedCostUSD
						rel := filepath.Join(condition, fmt.Sprintf("%s-p%d", variant, ps))
						row := semanticcache.TuningRow{Variant: variant, WorkloadSeed: w.Seed, ProjectionSeed: ps, Tables: l, Bits: k, Options: o, Metrics: m, BaselineCostUSD: baseline.SimulatedCostUSD, BaselineP95MS: baseline.SimulatedP95MS, Evidence: filepath.ToSlash(rel)}
						row.ReferenceBytes = semanticcache.ReferenceStateBytes(run.MemorySnapshot)
						if row.ReferenceBytes > 0 {
							row.MemoryRatio = semanticcache.Metric{Value: float64(m.MemoryBytes) / float64(row.ReferenceBytes), Defined: true}
						}
						if variant != "always-search" && variant != "always-skip" {
							row.Memory, err = semanticcache.MeasureSummary(filepath.Join(*op, rel, "memory"), w.Identity, run.MemorySnapshot, factory)
							if err != nil {
								return fmt.Errorf("%s memory: %w", rel, err)
							}
						}
						row.Failures = semanticcache.TuningFailures(c, row)
						runs := []semanticcache.ReplayRun{base}
						if !baselineRun {
							runs = append(runs, compactReplay(run))
						}
						if err := semanticcache.SaveReplay(filepath.Join(*op, rel), root, c, w, runs, map[string][]byte{"config.json": cb, "workload.json": inputs[w.Seed]}); err != nil {
							return err
						}
						if err := writeJSON(filepath.Join(*op, rel, "assessment.json"), row); err != nil {
							return err
						}
						result.Rows = append(result.Rows, row)
						return nil
					}
					for _, control := range []string{"always-search", "always-skip", "exact-vector-repeat-v1"} {
						factory := func() (semanticcache.ReplayPredictor, error) {
							if control == "exact-vector-repeat-v1" {
								return &semanticcache.ExactRepeat{}, nil
							}
							return &semanticcache.ConstantPredictor{Search: control == "always-search"}, nil
						}
						if err := add(control, 0, 0, 0, factory, control == "always-search"); err != nil {
							return fail(err)
						}
					}
					for _, l := range g.Tables {
						for _, k := range g.Bits {
							for _, ps := range c.ProjectionSeeds {
								p, err := semanticcache.NewProjections(semanticcache.LSHConfig{Version: "cosine-hyperplanes-v1", Identity: w.Identity, Seed: ps, Tables: l, Bits: k})
								if err != nil {
									return fail(err)
								}
								factory := func() (semanticcache.ReplayPredictor, error) { return semanticcache.NewLSHSummary(p) }
								if err := add(fmt.Sprintf("lsh-L%d-K%d", l, k), ps, l, k, factory, false); err != nil {
									return fail(err)
								}
							}
						}
					}
					fmt.Fprintf(out, "completed %s: %d rows saved so far\n", condition, len(result.Rows))
				}
			}
		}
	}
	assessments, err := semanticcache.AssessTuning(result)
	if err != nil {
		return fail(err)
	}
	if err := writeJSON(filepath.Join(*op, "sweep.json"), result); err != nil {
		return fail(err)
	}
	if err := writeJSON(filepath.Join(*op, "assessments.json"), assessments); err != nil {
		return fail(err)
	}
	if err := saveTuningTables(*op, result, assessments); err != nil {
		return fail(err)
	}
	if err := saveTuningManifest(*op); err != nil {
		return fail(err)
	}
	fmt.Fprintf(out, "saved %d tuning conditions to %s; candidate selection remains a separate subphase\n", len(result.Rows), *op)
	return 0
}

// State sequence + input stream + initial/final states reproduce the omitted
// evaluation-only PostState copies without multiplying disk size by cache size.
func compactReplay(run semanticcache.ReplayRun) semanticcache.ReplayRun {
	for i := range run.Rows {
		run.Rows[i].PostState = semanticcache.CacheSnapshot{}
	}
	return run
}

func saveTuningTables(dir string, result semanticcache.TuningResult, assessments []semanticcache.VariantAssessment) error {
	f, err := os.Create(filepath.Join(dir, "tradeoffs.csv"))
	if err != nil {
		return err
	}
	w := csv.NewWriter(f)
	if err := w.Write([]string{"variant", "workload_seed", "projection_seed", "mode", "start", "capacity", "recall", "lookup_reduction", "structural_summary_bytes", "measured_retained_heap_delta_bytes", "process_working_set_bytes", "measured_predictor_ns", "simulated_cost_usd", "simulated_cost_delta_usd", "simulated_p95_ms", "baseline_simulated_p95_ms", "failures", "reference_cache_structural_estimate_bytes", "structural_summary_to_reference_ratio"}); err != nil {
		f.Close()
		return err
	}
	for _, r := range result.Rows {
		if err := w.Write([]string{r.Variant, fmt.Sprint(r.WorkloadSeed), fmt.Sprint(r.ProjectionSeed), r.Options.Mode, r.Options.Start, fmt.Sprint(r.Options.Capacity), r.Metrics.CacheHitRecall.String(), r.Metrics.LookupReduction.String(), fmt.Sprint(r.Metrics.MemoryBytes), fmt.Sprint(r.Memory.RetainedHeapDelta), fmt.Sprint(r.Memory.ProcessAfter), fmt.Sprint(r.Metrics.PredictorNS), fmt.Sprint(r.Metrics.SimulatedCostUSD), fmt.Sprint(r.Metrics.CostDeltaUSD), r.Metrics.SimulatedP95MS.String(), r.BaselineP95MS.String(), strings.Join(r.Failures, ";"), fmt.Sprint(r.ReferenceBytes), r.MemoryRatio.String()}); err != nil {
			f.Close()
			return err
		}
	}
	w.Flush()
	if err := w.Error(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	var report strings.Builder
	fmt.Fprintln(&report, "# Phase 4.1 tuning tradeoffs\n\nTuning only; simulated controlled vectors and fake generation. Costs and end-to-end latency use frozen simulated assumptions plus measured local wall time. No final outcomes are opened. Single-bucket probing hashes L*K*dimension coefficients and examines at most L buckets, in table order; any occupied bucket searches. Numerical ambiguity and unready/hash failures search. No nearby-bucket probing was added.\n\n| Configuration | Conditions | Passing all constraints | Minimum recall | Maximum budgeted bytes | Simulated Mode B total USD | Failure counts |\n| --- | ---: | ---: | ---: | ---: | ---: | --- |")
	for _, a := range assessments {
		b, _ := json.Marshal(a.Failures)
		fmt.Fprintf(&report, "| %s | %d | %d | %s | %d | %.9f | %s |\n", a.Variant, a.Conditions, a.PassingConditions, a.MinRecall.String(), a.MaxMemoryBytes, a.TotalCostUSD, b)
	}
	fmt.Fprintln(&report, "\n`tradeoffs.csv` shows recall versus lookup reduction and recall versus total memory per seed/condition, along with predictor time and every failed budget. `sweep.json` retains all stage metrics and per-condition measured heap/process memory. Each evidence directory includes scoped oracle/decision/serving evidence, initial workload, final states, coefficients and source/artifact hashes. Intermediate evaluation snapshots are omitted from JSONL; state sequences and the deterministic stream reproduce them.\n\nMemory gates use both the maximum incremental structural estimate (projections, entry ledger, vectors, keys, strings and container allowances) and measured independent rebuilt-summary retained heap. Before/after sampled heap profiles are separate from allocations/op. Process working set includes the harness and Go runtime; it is not summary size. Rebuild peak memory is not measured in this subphase. Profiling, GC, logging and oracle work are outside serving timing. Benchmark CPU time/allocations are separate from replay wall time.\n\nA configuration qualifies only if every declared seed/mode/start/capacity condition passes; Mode A establishes recall and memory, while paired cost and latency constraints use Mode B. Projection seeds form one configuration family and are never selected by outcomes. Candidate selection is deferred to 4.2.")
	return os.WriteFile(filepath.Join(dir, "report.md"), []byte(report.String()), 0644)
}

func fileSHA(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
func saveTuningManifest(dir string) error {
	hashes := map[string]string{}
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		if rel == "manifest.json" {
			return nil
		}
		h, err := fileSHA(path)
		if err != nil {
			return err
		}
		hashes[filepath.ToSlash(rel)] = h
		return nil
	})
	if err != nil {
		return err
	}
	return writeJSON(filepath.Join(dir, "manifest.json"), map[string]any{"phase": "4.1", "created_at": time.Now().UTC(), "go_version": runtime.Version(), "platform": runtime.GOOS + "/" + runtime.GOARCH, "command": os.Args, "artifacts_sha256": hashes, "source_identity": "each paired replay manifest records Git revision/status and exact source file hashes", "split": "tuning", "heap_profile_rate_bytes": runtime.MemProfileRate, "probes": "one bucket per table, in ascending table order; OR occupancy; at most L probes"})
}
