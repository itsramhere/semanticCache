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
	"reflect"
	"runtime"
	"sort"
	"strings"
	"time"
)

func SaveChurn(dir, sourceRoot string, run ChurnRun) error {
	steps, err := BuildChurnSteps(run.Config)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(steps, run.Steps) || len(run.Rows) != len(steps) || !run.Metrics.FinalAgreement {
		return fmt.Errorf("incomplete or inconsistent churn run")
	}
	artifacts := map[string][]byte{}
	for name, value := range map[string]any{"config.json": run.Config, "workload.json": run.Steps, "projections.json": run.Projections, "initial.json": run.Initial, "final.json": run.Final, "metrics.json": run.Metrics, "rebuilds.json": run.Rebuilds} {
		b, err := json.MarshalIndent(value, "", "  ")
		if err != nil {
			return err
		}
		artifacts[name] = b
	}
	jsonl := func(value any) ([]byte, error) {
		var buf bytes.Buffer
		enc := json.NewEncoder(&buf)
		rows := reflect.ValueOf(value)
		for i := 0; i < rows.Len(); i++ {
			if err := enc.Encode(rows.Index(i).Interface()); err != nil {
				return nil, err
			}
		}
		return buf.Bytes(), nil
	}
	for name, value := range map[string]any{"requests.jsonl": run.Rows, "events.jsonl": run.Events} {
		b, err := jsonl(value)
		if err != nil {
			return err
		}
		artifacts[name] = b
	}
	var table bytes.Buffer
	out := csv.NewWriter(&table)
	if err := out.Write([]string{"step", "request_id", "source_watermark", "last_applied", "ready", "reason", "oracle_hit", "search", "fresh_search", "unseen_upsert_missed_hit", "stale_removal_wasted_search", "intrinsic_lsh_missed_hit", "diagnostic_unapplied", "received_pending", "measured_predictor_ns", "measured_update_ns"}); err != nil {
		return err
	}
	for _, r := range run.Rows {
		if err := out.Write([]string{fmt.Sprint(r.Step), r.Result.RequestID, fmt.Sprint(r.SourceWatermark), fmt.Sprint(r.Status.LastApplied), fmt.Sprint(r.Status.Ready), r.Result.Decision.Reason, fmt.Sprint(r.OracleHit), fmt.Sprint(r.Result.Decision.Search), fmt.Sprint(r.FreshDecision.Search), fmt.Sprint(r.UnseenUpsertMiss), fmt.Sprint(r.StaleRemovalSearch), fmt.Sprint(r.IntrinsicLSHMiss), fmt.Sprint(r.DiagnosticUnapplied), fmt.Sprint(r.Status.PendingEvents), fmt.Sprint(r.Result.Timings.PredictorNS), fmt.Sprint(r.Result.Timings.UpdateNS)}); err != nil {
			return err
		}
	}
	out.Flush()
	if err := out.Error(); err != nil {
		return err
	}
	artifacts["requests.csv"] = table.Bytes()
	sources := map[string]string{}
	err = filepath.WalkDir(sourceRoot, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "results", "generated", "node_modules":
				return filepath.SkipDir
			}
			return nil
		}
		ext := filepath.Ext(path)
		if ext != ".go" && ext != ".json" && d.Name() != "go.mod" && d.Name() != "go.sum" {
			return nil
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(sourceRoot, path)
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
	git := func(args ...string) string {
		cmd := exec.Command("git", args...)
		cmd.Dir = sourceRoot
		b, err := cmd.Output()
		if err != nil {
			return "unknown; source hashes recorded"
		}
		return strings.TrimSpace(string(b))
	}
	hashes := map[string]string{}
	for name, b := range artifacts {
		hashes[name] = hashBytes(b)
	}
	manifest := map[string]any{
		"run_id": fmt.Sprintf("%s-%d-%d", run.Config.ID, run.Config.WorkloadSeed, run.Config.Faults.Seed), "created_at": time.Now().UTC(), "command": os.Args,
		"go_version": runtime.Version(), "platform": runtime.GOOS + "/" + runtime.GOARCH, "source_revision": git("rev-parse", "HEAD"), "git_status": git("status", "--porcelain"),
		"source_sha256": hashBytes([]byte(digest.String())), "source_files_sha256": sources, "artifacts_sha256": hashes, "configuration_id": run.Config.ID,
		"dataset_id": "controlled-churn-2d-v1", "workload_seed": run.Config.WorkloadSeed, "projection_seed": run.Config.ProjectionSeed, "transport_seed": run.Config.Faults.Seed,
		"embedding_identity": run.Projections.Config.Identity, "vector_source": "simulated hand-controlled normalized 2D vectors; no real embedding inference",
		"generation":      "deterministic simulated fake LLM; no real LLM latency or monetary cost measured",
		"timing_boundary": "measured instrumented local serving and maintenance with precomputed vectors; includes scheduler, in-memory audit recording, snapshot extraction and sampled heap instrumentation; excludes oracle/fresh-summary diagnostics and disk serialization; rebuild build excludes source snapshot extraction (included in update time); no slept delays",
		"memory_boundary": "structural estimate includes active/replacement summaries, version ledgers, received buffers, snapshot and catch-up slice during rebuild; sampled process HeapAlloc peak is a lower bound sampled at build/catch-up stages, includes harness allocations, not an OS working set or continuously observed maximum",
	}
	b, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	artifacts["manifest.json"] = b
	if err := os.MkdirAll(filepath.Dir(dir), 0755); err != nil {
		return err
	}
	if err := os.Mkdir(dir, 0755); err != nil {
		return fmt.Errorf("churn output must be new: %w", err)
	}
	for name, b := range artifacts {
		if err := os.WriteFile(filepath.Join(dir, name), b, 0644); err != nil {
			return err
		}
	}
	return nil
}
