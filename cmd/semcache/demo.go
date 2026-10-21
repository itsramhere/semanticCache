package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"semanticcache/internal/semanticcache"
)

func demo(args []string, out, errOut io.Writer) int {
	flags := flag.NewFlagSet("demo", flag.ContinueOnError)
	flags.SetOutput(errOut)
	configPath := flags.String("config", "configs/experiment.json", "experiment JSON")
	output := flags.String("output", "results/demo", "output directory (overwrites previous demo artifacts)")
	endpoint := flags.String("tei", "", "live TEI base URL; defaults to recorded real MiniLM embeddings")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintln(errOut, "unexpected positional arguments")
		return 2
	}
	f, err := os.Open(*configPath)
	if err != nil {
		fmt.Fprintln(errOut, err)
		return 1
	}
	config, err := semanticcache.LoadConfig(f)
	f.Close()
	if err != nil {
		fmt.Fprintln(errOut, err)
		return 1
	}
	root, err := findSourceRoot()
	if err != nil {
		fmt.Fprintln(errOut, err)
		return 1
	}
	var embedder semanticcache.Embedder
	mode := "recorded-real-minilm; routing-only"
	if *endpoint != "" {
		embedder, err = semanticcache.NewTEIEmbedder(*endpoint, semanticcache.MiniLMIdentity())
		mode = "live-real-minilm-tei"
	} else {
		embedder, err = semanticcache.DemoRecordedEmbedder()
	}
	if err != nil {
		fmt.Fprintln(errOut, err)
		return 1
	}
	results, err := semanticcache.RunDemo(context.Background(), config, embedder)
	if err != nil {
		fmt.Fprintln(errOut, err)
		return 1
	}
	if err := semanticcache.SaveDemo(*output, root, mode, sourceRevision(), config, results); err != nil {
		fmt.Fprintln(errOut, err)
		return 1
	}
	fmt.Fprintf(out, "embedding: %s; generation: simulated fake LLM; timings: measured wall-clock\n", mode)
	code := 0
	for _, r := range results {
		fmt.Fprintf(out, "%s source=%s success=%t searches=%d generations=%d writes=%d\n", r.RequestID, r.Source, r.Success, r.SearchCount, r.GenerationCount, r.WriteCount)
		if !r.Success || r.CacheWriteError != "" {
			fmt.Fprintf(errOut, "%s error=%s cache_write_error=%s\n", r.RequestID, r.Error, r.CacheWriteError)
			code = 1
		}
	}
	fmt.Fprintf(out, "saved logs, reusable embeddings, workload, configuration and manifest to %s\n", *output)
	return code
}

// Source identity follows the checkout, independently of the configuration's
// location. Demo requires source files so uncommitted builds remain auditable.
func findSourceRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		info, err := os.Stat(filepath.Join(dir, "go.mod"))
		if err == nil && !info.IsDir() {
			return dir, nil
		}
		if err != nil && !os.IsNotExist(err) {
			return "", err
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("cannot locate go.mod; run demo from the source checkout to record source hashes")
		}
		dir = parent
	}
}
