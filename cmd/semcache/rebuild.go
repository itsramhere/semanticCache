package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"semanticcache/internal/semanticcache"
)

// rebuild exercises the same Reset operation used by serving recovery. It
// exports a verified rebuild report and exact inputs, not a hot-loadable store.
func rebuild(args []string, out, errOut io.Writer) int {
	flags := flag.NewFlagSet("rebuild", flag.ContinueOnError)
	flags.SetOutput(errOut)
	configPath := flags.String("config", "configs/phase2_fixture.json", "matching-policy and embedding configuration")
	projectionPath := flags.String("projections", "", "persisted projections JSON")
	runPath := flags.String("run", "", "saved replay *-run.json containing final_state")
	output := flags.String("output", "", "new rebuild evidence directory")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if flags.NArg() != 0 || *projectionPath == "" || *runPath == "" || *output == "" {
		fmt.Fprintln(errOut, "rebuild requires -projections, -run and -output")
		return 2
	}
	fail := func(err error) int { fmt.Fprintln(errOut, err); return 1 }
	cb, err := os.ReadFile(*configPath)
	if err != nil {
		return fail(err)
	}
	config, err := semanticcache.LoadConfig(bytes.NewReader(cb))
	if err != nil {
		return fail(err)
	}
	pb, err := os.ReadFile(*projectionPath)
	if err != nil {
		return fail(err)
	}
	var metadata semanticcache.Projections
	if err = json.Unmarshal(pb, &metadata); err != nil {
		return fail(err)
	}
	p, err := semanticcache.LoadProjections(bytes.NewReader(pb), metadata.Config)
	if err != nil {
		return fail(err)
	}
	id := p.Config.Identity
	if id.Model != config.Embedding.Model || id.Dimension != config.Embedding.Dimension || id.Preprocessing != config.Embedding.Preprocessing {
		return fail(fmt.Errorf("rebuild embedding identity mismatch"))
	}
	rb, err := os.ReadFile(*runPath)
	if err != nil {
		return fail(err)
	}
	var run semanticcache.ReplayRun
	dec := json.NewDecoder(bytes.NewReader(rb))
	dec.DisallowUnknownFields()
	if err = dec.Decode(&run); err != nil {
		return fail(err)
	}
	if err = dec.Decode(new(any)); err != io.EOF {
		return fail(fmt.Errorf("trailing replay JSON"))
	}
	if run.Options.Capacity <= 0 || run.PredictorID == "" {
		return fail(fmt.Errorf("missing replay capacity or identity"))
	}
	if run.Projections == nil || run.Projections.ID() != p.ID() || run.PredictorID != p.ID() {
		return fail(fmt.Errorf("replay/projection identity mismatch"))
	}
	at := run.FinalState.LastTime
	if at.IsZero() {
		at = time.Unix(0, 0).UTC()
	}
	// Restore validates FIFO versions, timestamps, dimensions and scope without
	// changing TTLs. Reset builds the counted summary from those exact bytes.
	cache, err := semanticcache.RestoreReferenceCache(config.Policy, id.Dimension, run.Options.Capacity, &semanticcache.LogicalClock{Time: at}, run.FinalState)
	if err != nil {
		return fail(err)
	}
	start := time.Now()
	summary, err := semanticcache.NewLSHSummary(p)
	if err != nil {
		return fail(err)
	}
	if err = summary.Reset(id, cache.Snapshot()); err != nil {
		return fail(err)
	}
	elapsed := time.Since(start).Nanoseconds()
	checked := 0
	for _, stored := range run.FinalState.Entries {
		e := stored.Entry
		if !e.Validity.Reusable || e.Validity.Revoked {
			continue
		}
		r := semanticcache.Request{ID: e.ID, Text: "offline rebuild membership check", Namespace: e.Scope.Namespace, Context: e.Scope.Context, ModelVersion: e.Scope.ModelVersion, Timestamp: at, CacheEligible: true}
		if !summary.Predict(r, e.Embedding).Search {
			return fail(fmt.Errorf("rebuilt summary lost stored entry %s", e.ID))
		}
		checked++
	}
	if err = os.MkdirAll(filepath.Dir(*output), 0755); err != nil {
		return fail(err)
	}
	if err = os.Mkdir(*output, 0755); err != nil {
		return fail(err)
	}
	files := map[string]string{}
	report := map[string]any{"projection_id": p.ID(), "embedding_identity": id, "ready": summary.Ready(), "snapshot_sequence": run.FinalState.Sequence, "snapshot_entries": len(run.FinalState.Entries), "verified_reusable_entries": checked, "structural_summary_bytes": summary.MemoryBytes(), "measured_rebuild_ns": elapsed, "boundary": "offline counted-summary rebuild from an owned replay snapshot; preserves TTL/FIFO; exact stored-vector decisions checked; expired entries may remain conservative positives; report is not a hot-loadable serving store"}
	for name, b := range map[string][]byte{"config.json": cb, "projections.json": pb, "replay-run.json": rb, "summary.json": finalJSON(report)} {
		if err = finalWrite(*output, name, b, files); err != nil {
			return fail(err)
		}
	}
	if err = finalWrite(*output, "manifest.json", finalJSON(map[string]any{"artifacts_sha256": files, "operation": "offline-summary-rebuild-v1"}), map[string]string{}); err != nil {
		return fail(err)
	}
	fmt.Fprintf(out, "rebuilt %d reusable entries; ready=%t; saved exact inputs and report to %s\n", checked, summary.Ready(), *output)
	return 0
}
