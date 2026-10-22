package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"

	"semanticcache/internal/semanticcache"
)

func churn(args []string, out, errOut io.Writer) int {
	fs := flag.NewFlagSet("churn", flag.ContinueOnError)
	fs.SetOutput(errOut)
	configPath := fs.String("config", "configs/churn.json", "seeded controlled churn configuration")
	output := fs.String("output", "results/phase5/churn", "new output directory")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() != 0 {
		fmt.Fprintln(errOut, "churn does not accept positional arguments")
		return 2
	}
	fail := func(err error) int { fmt.Fprintln(errOut, err); return 1 }
	f, err := os.Open(*configPath)
	if err != nil {
		return fail(err)
	}
	defer f.Close()
	var c semanticcache.ChurnConfig
	dec := json.NewDecoder(f)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&c); err != nil {
		return fail(err)
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		return fail(fmt.Errorf("trailing churn configuration JSON"))
	}
	run, err := semanticcache.RunChurn(context.Background(), c)
	if err != nil {
		return fail(err)
	}
	root, err := findSourceRoot()
	if err != nil {
		return fail(err)
	}
	if err := semanticcache.SaveChurn(*output, root, run); err != nil {
		return fail(err)
	}
	m := run.Metrics
	fmt.Fprintf(out, "saved %d controlled churn requests to %s\nunseen-upsert missed hits: %d; stale-removal wasted searches: %d; fallback requests: %d; final agreement: %v\n", m.Requests, *output, m.UnseenUpsertMisses, m.StaleRemovalSearches, m.FallbackRequests, m.FinalAgreement)
	return 0
}
