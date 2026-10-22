package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"

	"semanticcache/internal/semanticcache"
)

func replay(args []string, out, errOut io.Writer) int {
	fs := flag.NewFlagSet("replay", flag.ContinueOnError)
	fs.SetOutput(errOut)
	configPath := fs.String("config", "configs/phase2_fixture.json", "experiment JSON; default uses simulated vectors")
	dataPath := fs.String("data", "", "one validated workload JSON (required)")
	output := fs.String("output", "results/phase2-replay", "output directory; overwrites matching artifacts")
	mode := fs.String("mode", "end-to-end", "isolation (A) or end-to-end (B)")
	start := fs.String("start", "cold", "cold or warm")
	capacity := fs.Int("capacity", 100, "global cache capacity")
	oracleEvery := fs.Int("oracle-every", 1, "diagnose every Nth request; 0 disables oracle")
	oracleLogging := fs.Bool("oracle-log", true, "save diagnostic labels")
	projectionPath := fs.String("lsh", "", "optional persisted projection JSON to include an LSH predictor")
	candidatePath := fs.String("candidate", "", "frozen candidate JSON; includes its experiment settings and complete seed family")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() != 0 || *dataPath == "" {
		fmt.Fprintln(errOut, "replay requires -data <workload.json> and no positional arguments")
		return 2
	}
	fail := func(err error) int { fmt.Fprintln(errOut, err); return 1 }
	var candidate *semanticcache.FinalCandidate
	var candidateBytes, cb []byte
	var c semanticcache.Config
	var err error
	configExplicit := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "config" {
			configExplicit = true
		}
	})
	if *candidatePath != "" {
		if *projectionPath != "" || configExplicit {
			return fail(fmt.Errorf("-candidate includes frozen config/projections; omit -config and -lsh"))
		}
		candidateBytes, err = os.ReadFile(*candidatePath)
		if err != nil {
			return fail(err)
		}
		f, err := semanticcache.LoadCandidate(bytes.NewReader(candidateBytes))
		if err != nil {
			return fail(err)
		}
		candidate = &f
		c = f.Experiment
		cb, err = json.MarshalIndent(c, "", "  ")
		if err != nil {
			return fail(err)
		}
	} else {
		cb, err = os.ReadFile(*configPath)
		if err != nil {
			return fail(err)
		}
		c, err = semanticcache.LoadConfig(bytes.NewReader(cb))
		if err != nil {
			return fail(err)
		}
	}
	wb, err := os.ReadFile(*dataPath)
	if err != nil {
		return fail(err)
	}
	w, err := semanticcache.LoadWorkload(bytes.NewReader(wb))
	if err != nil {
		return fail(err)
	}
	o := semanticcache.ReplayOptions{Mode: *mode, Start: *start, Capacity: *capacity, OracleEvery: *oracleEvery, OracleLogging: *oracleLogging}
	var runs []semanticcache.ReplayRun
	predictors := []semanticcache.ReplayPredictor{&semanticcache.ConstantPredictor{Search: true}, &semanticcache.ConstantPredictor{Search: false}}
	if candidate != nil {
		if candidate.Identity != w.Identity {
			return fail(fmt.Errorf("candidate/workload identity mismatch"))
		}
		predictors, err = candidate.Predictors()
		if err != nil {
			return fail(err)
		}
	}
	if *projectionPath != "" {
		pb, err := os.ReadFile(*projectionPath)
		if err != nil {
			return fail(err)
		}
		var metadata semanticcache.Projections
		if err := json.Unmarshal(pb, &metadata); err != nil {
			return fail(err)
		}
		if metadata.Config.Identity != w.Identity {
			return fail(fmt.Errorf("LSH/workload identity mismatch"))
		}
		p, err := semanticcache.LoadProjections(bytes.NewReader(pb), metadata.Config)
		if err != nil {
			return fail(err)
		}
		s, err := semanticcache.NewLSHSummary(p)
		if err != nil {
			return fail(err)
		}
		predictors = append(predictors, s)
	}
	for _, predictor := range predictors {
		run, err := semanticcache.Replay(context.Background(), c, w, o, predictor)
		if err != nil {
			return fail(err)
		}
		runs = append(runs, run)
		fmt.Fprintf(out, "%s: %d requests, %d diagnostic searches; mode=%s start=%s\n", run.PredictorID, len(run.Rows), run.DiagnosticSearches, o.Mode, o.Start)
	}
	root, err := findSourceRoot()
	if err != nil {
		return fail(err)
	}
	inputs := map[string][]byte{"config.json": cb, "workload.json": wb}
	if candidate != nil {
		inputs["candidate.json"] = candidateBytes
	}
	if err = semanticcache.SaveReplay(*output, root, c, w, runs, inputs); err != nil {
		return fail(err)
	}
	fmt.Fprintf(out, "saved paired evidence and manifest to %s; vectors and generation simulated\n", *output)
	for _, run := range runs {
		for _, row := range run.Rows {
			if !row.Result.Success || row.Result.CacheWriteError != "" {
				return 1
			}
		}
	}
	return 0
}
