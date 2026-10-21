// Command semcache is the CLI entry point for the semantic-cache project.
package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	"runtime/debug"

	"semanticcache/internal/semanticcache"
)

const usageText = `semcache - semantic response cache with lookup-bypass prediction

Usage:
  semcache <command> [arguments]

Commands:
  demo             Replay the baseline; flags: -config, -output, -tei.
  version          Print module path, Go toolchain, and source revision.
  verify-policy    Execute the machine-checkable examples from docs/cache_policy.md.
  validate-config  Check a JSON experiment configuration:
                   validate-config -config <path>
  help             Print this message.

Phase 1 supplies the always-search cache baseline and demo. Prediction and
evaluation arrive in later phases.
`

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// run executes one command. It returns the exit code: 0 success, 1 validation
// failure, 2 usage error. Output either goes to out (results) or errOut
// (errors).
func run(args []string, out, errOut io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(out, usageText)
		return 0
	}
	switch args[0] {
	case "demo":
		return demo(args[1:], out, errOut)
	case "help", "-h", "--help":
		fmt.Fprint(out, usageText)
		return 0
	case "version":
		printVersion(out)
		return 0
	case "verify-policy":
		return verifyPolicy(out, errOut)
	case "validate-config":
		return validateConfig(args[1:], out, errOut)
	default:
		fmt.Fprintf(errOut, "unknown command %q (try \"semcache help\")\n", args[0])
		return 2
	}
}

// sourceRevision returns the VCS revision embedded by the Go build, or "".
func sourceRevision() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return ""
	}
	revision := ""
	for _, setting := range info.Settings {
		switch setting.Key {
		case "vcs.revision":
			revision = setting.Value
		case "vcs.modified":
			if setting.Value == "true" {
				revision += " (modified)"
			}
		}
	}
	return revision
}

func printVersion(out io.Writer) {
	module := "(unknown)"
	if info, ok := debug.ReadBuildInfo(); ok && info.Main.Path != "" {
		module = info.Main.Path
	}
	fmt.Fprintln(out, "module:", module)
	fmt.Fprintln(out, "go:", runtime.Version())
	if revision := sourceRevision(); revision != "" {
		fmt.Fprintln(out, "revision:", revision)
	} else {
		fmt.Fprintln(out, "revision: (unknown)")
	}
}

// validateConfig loads and validates the JSON experiment configuration.
func validateConfig(args []string, out, errOut io.Writer) int {
	fs := flag.NewFlagSet("validate-config", flag.ContinueOnError)
	fs.SetOutput(errOut)
	configPath := fs.String("config", "", "path to a JSON experiment configuration")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() > 0 {
		fmt.Fprintf(errOut, "unexpected positional argument %q\n", fs.Arg(0))
		return 2
	}
	if *configPath == "" {
		fmt.Fprintln(errOut, "validate-config requires -config <path>")
		return 2
	}
	file, err := os.Open(*configPath)
	if err != nil {
		fmt.Fprintf(errOut, "cannot open configuration %s: %v\n", *configPath, err)
		return 1
	}
	defer file.Close()
	config, err := semanticcache.LoadConfig(file)
	if err != nil {
		fmt.Fprintf(errOut, "invalid configuration %s: %v\n", *configPath, err)
		return 1
	}
	fmt.Fprintf(out, "configuration %s: policy %s (cosine threshold %.2f, ttl %d s)\n",
		config.ID, config.Policy.ID, config.Policy.CosineThreshold, config.Policy.TTLSeconds)
	fmt.Fprintf(out, "embedding: %s dimension %d, preprocessing %s\n",
		config.Embedding.Model, config.Embedding.Dimension, config.Embedding.Preprocessing)
	fmt.Fprintf(out, "workload %s: %d requests per split, %d workload seeds\n",
		config.WorkloadID, config.RequestsPerSplit, len(config.WorkloadSeeds))
	fmt.Fprintf(out, "go: %s\n", runtime.Version())
	if revision := sourceRevision(); revision != "" {
		fmt.Fprintln(out, "revision:", revision)
	}
	return 0
}

// verifyPolicy executes the hand-checkable examples from docs/cache_policy.md.
func verifyPolicy(out, errOut io.Writer) int {
	results, err := semanticcache.RunPolicyExamples()
	if err != nil {
		fmt.Fprintf(errOut, "error: %v\n", err)
		return 1
	}
	failures := 0
	for _, result := range results {
		status := "PASS"
		if !result.OK {
			status = "FAIL"
			failures++
		}
		fmt.Fprintf(out, "%s %-47s winner=%-14q want=%q\n", status, result.Name, result.Winner, result.ExpectedWinner)
	}
	fmt.Fprintf(out, "%d/%d policy examples passed\n", len(results)-failures, len(results))
	if failures > 0 {
		fmt.Fprintf(errOut, "error: %d policy example(s) failed\n", failures)
		return 1
	}
	return 0
}
