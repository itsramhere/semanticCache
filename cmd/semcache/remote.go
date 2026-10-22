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
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"semanticcache/internal/semanticcache"
)

func remoteDemo(args []string, out, errOut io.Writer) int {
	fs := flag.NewFlagSet("remote-demo", flag.ContinueOnError)
	fs.SetOutput(errOut)
	endpoint := fs.String("endpoint", "http://127.0.0.1:16333", "Qdrant HTTP endpoint")
	output := fs.String("output", "results/phase6/remote-demo", "new output directory")
	repeats := fs.Int("repeats", 1, "repeat four development requests")
	configPath := fs.String("config", "configs/experiment.json", "recorded simulated cost/model-latency assumptions")
	rebuildEvery := fs.Int("rebuild-every", 8, "synchronously rebuild after this many requests; zero disables")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() != 0 {
		fmt.Fprintln(errOut, "unexpected remote-demo arguments")
		return 2
	}
	fail := func(err error) int { fmt.Fprintln(errOut, err); return 1 }
	configBytes, err := os.ReadFile(*configPath)
	if err != nil {
		return fail(err)
	}
	config, err := semanticcache.LoadConfig(bytes.NewReader(configBytes))
	if err != nil {
		return fail(err)
	}
	// Cost inputs cannot silently change the adapter's fixed reference policy.
	if config.Policy.ID != semanticcache.PolicyV1ID || config.Policy.CosineThreshold != semanticcache.PolicyV1CosineThreshold || config.Policy.TTLSeconds != semanticcache.PolicyV1TTLSeconds {
		return fail(fmt.Errorf("remote-demo requires reference policy v1"))
	}
	if err := os.MkdirAll(filepath.Dir(*output), 0755); err != nil {
		return fail(err)
	}
	if err := os.Mkdir(*output, 0755); err != nil {
		return fail(err)
	}
	runs, runErr := semanticcache.RunRemoteMeasured(context.Background(), *endpoint, *repeats, *rebuildEvery)
	data := map[string]any{"runs": runs, "endpoint": *endpoint, "repeats": *repeats, "rebuild_every": *rebuildEvery, "go_version": runtime.Version(), "revision": sourceRevision(), "timing_source": "measured HTTP and routing with precomputed real embeddings and simulated fake LLM; no added network delay; endpoint location must be recorded", "projection_seed": 1001, "dataset": "development-demo-4-v1"}
	if runErr != nil {
		data["error"] = runErr.Error()
	}
	b, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return fail(err)
	}
	if err := os.WriteFile(filepath.Join(*output, "run.json"), b, 0644); err != nil {
		return fail(err)
	}
	if runErr != nil {
		return fail(runErr)
	}
	comparison, err := semanticcache.AccountRemote(config, runs)
	if err != nil {
		return fail(err)
	}
	root, err := findSourceRoot()
	if err != nil {
		return fail(err)
	}
	if err := saveRemoteEvidence(*output, root, *endpoint, configBytes, comparison); err != nil {
		return fail(err)
	}
	fmt.Fprintf(out, "saved paired remote development replay to %s\n", *output)
	return 0
}

func saveRemoteEvidence(dir, root, endpoint string, config []byte, c semanticcache.RemoteComparison) error {
	outputAbs, err := filepath.Abs(dir)
	if err != nil {
		return err
	}
	hashes := map[string]string{}
	write := func(name string, b []byte) error {
		path := filepath.Join(dir, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			return err
		}
		if err := os.WriteFile(path, b, 0644); err != nil {
			return err
		}
		sum := sha256.Sum256(b)
		hashes[name] = hex.EncodeToString(sum[:])
		return nil
	}
	if err := write("config.json", config); err != nil {
		return err
	}
	b, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	if err = write("accounting.json", b); err != nil {
		return err
	}
	raw, err := os.ReadFile(filepath.Join(dir, "run.json"))
	if err != nil {
		return err
	}
	if err = write("run.json", raw); err != nil {
		return err
	}
	var saved struct {
		Runs []semanticcache.RemoteRun `json:"runs"`
	}
	if err = json.Unmarshal(raw, &saved); err != nil {
		return err
	}
	for _, run := range saved.Runs {
		if run.Router != "always-search" && run.Router != "exploratory-lsh" {
			return fmt.Errorf("unexpected remote router")
		}
		var log bytes.Buffer
		enc := json.NewEncoder(&log)
		for _, row := range run.Rows {
			if err = enc.Encode(row); err != nil {
				return err
			}
		}
		if err = write(run.Router+"-requests.jsonl", log.Bytes()); err != nil {
			return err
		}
	}
	var table bytes.Buffer
	csvOut := csv.NewWriter(&table)
	if err = csvOut.Write([]string{"router", "requests", "searches", "generations", "http_writes", "admin_calls", "fn", "recall", "lookup_reduction", "measured_routing_p95_ms", "mixed_end_to_end_p95_ms", "simulated_total_cost_usd", "measured_rebuild_ns", "amortized_maintenance_ns", "structural_summary_bytes", "estimated_rebuild_overlap_bytes", "sampled_process_heap_bytes"}); err != nil {
		return err
	}
	for _, m := range c.Runs {
		if err = csvOut.Write([]string{m.Router, fmt.Sprint(m.Requests), fmt.Sprint(m.Searches), fmt.Sprint(m.Generations), fmt.Sprint(m.Writes), fmt.Sprint(m.AdminCalls), fmt.Sprint(m.Confusion.FN), m.Recall.String(), m.LookupReduction.String(), m.MeasuredP95MS.String(), m.EstimatedP95MS.String(), fmt.Sprint(m.ServingCostUSD), fmt.Sprint(m.RebuildNS), fmt.Sprint(m.AmortizedMaintenanceNS), fmt.Sprint(m.SummaryBytes), fmt.Sprint(m.PeakRebuildBytes), fmt.Sprint(m.SampledProcessHeapBytes)}); err != nil {
			return err
		}
	}
	csvOut.Flush()
	if err = csvOut.Error(); err != nil {
		return err
	}
	if err = write("metrics.csv", table.Bytes()); err != nil {
		return err
	}
	// Capture actual source bytes, including the recorded embedding input,
	// without traversing held-out datasets or existing generated results.
	err = filepath.WalkDir(root, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			abs, e := filepath.Abs(path)
			if e != nil {
				return e
			}
			if abs == outputAbs {
				return filepath.SkipDir
			}
			if d.Name() == ".git" || d.Name() == "results" || d.Name() == "data" || d.Name() == "sites" {
				return filepath.SkipDir
			}
			return nil
		}
		rel, e := filepath.Rel(root, path)
		if e != nil {
			return e
		}
		rel = filepath.ToSlash(rel)
		if filepath.Ext(path) != ".go" && rel != "go.mod" && rel != "go.sum" && rel != "internal/semanticcache/testdata/demo_embeddings.json" {
			return nil
		}
		b, e := os.ReadFile(path)
		if e != nil {
			return e
		}
		return write("sources/"+rel, b)
	})
	if err != nil {
		return err
	}
	client := &http.Client{Timeout: 3 * time.Second}
	response, err := client.Get(strings.TrimRight(endpoint, "/") + "/")
	if err != nil {
		return err
	}
	defer response.Body.Close()
	b, err = io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return err
	}
	if response.StatusCode != 200 {
		return fmt.Errorf("backend version HTTP %d", response.StatusCode)
	}
	if err = write("backend.json", b); err != nil {
		return err
	}
	manifest := map[string]any{"created_at": time.Now().UTC(), "command": os.Args, "go_version": runtime.Version(), "revision": sourceRevision(), "artifacts_sha256": hashes, "boundary": c.Boundary, "client_version": "stdlib-http-qdrant-v1", "backend_version_probe": "diagnostic HTTP GET / outside serving counters"}
	b, err = json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "manifest.json"), b, 0644)
}

func remoteAccount(args []string, out, errOut io.Writer) int {
	fs := flag.NewFlagSet("remote-account", flag.ContinueOnError)
	fs.SetOutput(errOut)
	runPath := fs.String("run", "", "saved run.json")
	configPath := fs.String("config", "", "saved config.json")
	output := fs.String("output", "", "new accounting JSON file")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() != 0 || *runPath == "" || *configPath == "" || *output == "" {
		fmt.Fprintln(errOut, "remote-account requires -run, -config, -output")
		return 2
	}
	fail := func(err error) int { fmt.Fprintln(errOut, err); return 1 }
	b, err := os.ReadFile(*configPath)
	if err != nil {
		return fail(err)
	}
	config, err := semanticcache.LoadConfig(bytes.NewReader(b))
	if err != nil {
		return fail(err)
	}
	b, err = os.ReadFile(*runPath)
	if err != nil {
		return fail(err)
	}
	var run struct {
		Runs []semanticcache.RemoteRun `json:"runs"`
	}
	if err = json.Unmarshal(b, &run); err != nil {
		return fail(err)
	}
	c, err := semanticcache.AccountRemote(config, run.Runs)
	if err != nil {
		return fail(err)
	}
	b, err = json.MarshalIndent(c, "", "  ")
	if err != nil {
		return fail(err)
	}
	f, err := os.OpenFile(*output, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return fail(err)
	}
	_, writeErr := f.Write(b)
	closeErr := f.Close()
	if writeErr != nil {
		return fail(writeErr)
	}
	if closeErr != nil {
		return fail(closeErr)
	}
	fmt.Fprintln(out, "recomputed remote accounting from saved inputs")
	return 0
}
