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

func buildProjections(args []string, out, errOut io.Writer) int {
	fs := flag.NewFlagSet("build-projections", flag.ContinueOnError)
	fs.SetOutput(errOut)
	config := fs.String("config", "configs/lsh_development.json", "LSH configuration JSON")
	output := fs.String("output", "results/phase3/projections.json", "persisted projections")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() != 0 {
		return 2
	}
	fail := func(err error) int { fmt.Fprintln(errOut, err); return 1 }
	f, err := os.Open(*config)
	if err != nil {
		return fail(err)
	}
	defer f.Close()
	var c semanticcache.LSHConfig
	dec := json.NewDecoder(f)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&c); err != nil {
		return fail(err)
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		return fail(fmt.Errorf("trailing configuration JSON"))
	}
	p, err := semanticcache.NewProjections(c)
	if err != nil {
		return fail(err)
	}
	b, err := json.MarshalIndent(p, "", "  ")
	if err != nil {
		return fail(err)
	}
	if err := os.MkdirAll(filepath.Dir(*output), 0755); err != nil {
		return fail(err)
	}
	if err := os.WriteFile(*output, b, 0644); err != nil {
		return fail(err)
	}
	fmt.Fprintf(out, "saved %s to %s\n", p.ID(), *output)
	return 0
}
