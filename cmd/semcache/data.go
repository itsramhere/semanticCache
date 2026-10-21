package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"semanticcache/internal/semanticcache"
)

func writeJSON(path string, value any) error {
	b, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0644)
}

func dataCommand(command string, args []string, out, errOut io.Writer) int {
	fs := flag.NewFlagSet(command, flag.ContinueOnError)
	fs.SetOutput(errOut)
	catalog := fs.String("catalog", "data/catalog.json", "authored QA catalog")
	output := fs.String("output", "data/generated", "generated workload directory")
	seed := fs.Int64("seed", 101, "workload order seed")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	fail := func(err error) int { fmt.Fprintln(errOut, err); return 1 }
	if command == "build-data" {
		if fs.NArg() != 0 {
			fmt.Fprintln(errOut, "unexpected positional arguments")
			return 2
		}
		f, err := os.Open(*catalog)
		if err != nil {
			return fail(err)
		}
		c, err := semanticcache.LoadCatalog(f)
		f.Close()
		if err != nil {
			return fail(err)
		}
		ws, err := semanticcache.BuildWorkloads(c, *seed)
		if err != nil {
			return fail(err)
		}
		if err = os.MkdirAll(*output, 0755); err != nil {
			return fail(err)
		}
		for _, w := range ws {
			path := filepath.Join(*output, fmt.Sprintf("%s-%d.json", w.Split, w.Seed))
			if err = writeJSON(path, w); err != nil {
				return fail(err)
			}
			fmt.Fprintf(out, "saved %s: %d requests, %d warm entries; vectors simulated\n", path, len(w.Requests), len(w.Warm))
		}
		return 0
	}
	if fs.NArg() == 0 {
		fmt.Fprintln(errOut, "validate-data requires workload JSON paths (all splits for leakage checks)")
		return 2
	}
	var ws []semanticcache.Workload
	for _, path := range fs.Args() {
		f, err := os.Open(path)
		if err != nil {
			return fail(err)
		}
		w, err := semanticcache.LoadWorkload(f)
		f.Close()
		if err != nil {
			return fail(fmt.Errorf("%s: %w", path, err))
		}
		ws = append(ws, w)
	}
	if err := semanticcache.ValidateWorkloads(ws); err != nil {
		return fail(err)
	}
	fmt.Fprintf(out, "validated %d workload files; no outcomes computed\n", len(ws))
	return 0
}
