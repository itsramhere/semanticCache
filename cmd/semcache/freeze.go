package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"semanticcache/internal/semanticcache"
)

func freezeCandidate(args []string, out, errOut io.Writer) int {
	fs := flag.NewFlagSet("freeze-candidate", flag.ContinueOnError)
	fs.SetOutput(errOut)
	dir := fs.String("tuning", "results/phase4/tuning", "completed tuning directory")
	output := fs.String("output", "configs/final_candidate.json", "new frozen candidate file")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() != 0 {
		fmt.Fprintln(errOut, "unexpected positional arguments")
		return 2
	}
	fail := func(err error) int { fmt.Fprintln(errOut, err); return 1 }
	mb, err := os.ReadFile(filepath.Join(*dir, "manifest.json"))
	if err != nil {
		return fail(err)
	}
	var manifest struct {
		Phase  string            `json:"phase"`
		Split  string            `json:"split"`
		Hashes map[string]string `json:"artifacts_sha256"`
	}
	if err := json.Unmarshal(mb, &manifest); err != nil {
		return fail(err)
	}
	if manifest.Phase != "4.1" || manifest.Split != "tuning" || manifest.Hashes["sweep.json"] == "" {
		return fail(fmt.Errorf("requires a complete Phase 4.1 tuning manifest"))
	}
	for rel, want := range manifest.Hashes {
		if !filepath.IsLocal(filepath.FromSlash(rel)) {
			return fail(fmt.Errorf("invalid tuning artifact path"))
		}
		got, err := fileSHA(filepath.Join(*dir, filepath.FromSlash(rel)))
		if err != nil {
			return fail(err)
		}
		if got != want {
			return fail(fmt.Errorf("tuning artifact checksum mismatch: %s", rel))
		}
	}
	b, err := os.ReadFile(filepath.Join(*dir, "sweep.json"))
	if err != nil {
		return fail(err)
	}
	var result semanticcache.TuningResult
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&result); err != nil {
		return fail(err)
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		return fail(fmt.Errorf("trailing tuning JSON"))
	}
	if _, err := semanticcache.AssessTuning(result); err != nil {
		return fail(err)
	}
	identity, err := verifyTuningEvidence(*dir, manifest.Hashes, result)
	if err != nil {
		return fail(err)
	}
	f, err := semanticcache.SelectCandidate(result, identity)
	if err != nil {
		return fail(err)
	}
	f.TuningDirectory = filepath.ToSlash(filepath.Clean(*dir))
	f.TuningManifestSHA256, err = fileSHA(filepath.Join(*dir, "manifest.json"))
	if err != nil {
		return fail(err)
	}
	f.SweepSHA256 = manifest.Hashes["sweep.json"]
	f.FrozenAt = time.Now().UTC()
	if err := f.Validate(); err != nil {
		return fail(err)
	}
	encoded, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		return fail(err)
	}
	// O_EXCL prevents a later run from silently changing an already frozen choice.
	file, err := os.OpenFile(*output, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		return fail(err)
	}
	_, err = file.Write(append(encoded, '\n'))
	closeErr := file.Close()
	if err != nil {
		return fail(err)
	}
	if closeErr != nil {
		return fail(closeErr)
	}
	var rationale strings.Builder
	fmt.Fprintf(&rationale, "# Phase 4.2 frozen selection\n\nRouter: **%s**. Status: `%s`.\n\n%s\n\nSelection used only the complete tuning matrix and verified every artifact checksum. Each LSH family must satisfy recall, memory and error constraints in all conditions, plus strictly lower cost and non-increasing p95 in every Mode B condition. Lowest total Mode B cost breaks qualified-family choices; lexical variant order breaks exact ties. Projection seeds stay together. No held-out final results were opened.\n\n| Family | Passing / total | Minimum recall | Constraint failure counts |\n| --- | ---: | ---: | --- |\n", f.Router, f.Status, f.Reason)
	for _, a := range f.Assessments {
		if a.Tables == 0 {
			continue
		}
		counts, _ := json.Marshal(a.Failures)
		fmt.Fprintf(&rationale, "| %s | %d / %d | %s | %s |\n", a.Variant, a.PassingConditions, a.Conditions, a.MinRecall.String(), counts)
	}
	fmt.Fprintf(&rationale, "\nTuning manifest SHA256: `%s`. Sweep SHA256: `%s`. Frozen UTC: `%s`.\n", f.TuningManifestSHA256, f.SweepSHA256, f.FrozenAt.Format(time.RFC3339Nano))
	if err := os.WriteFile(filepath.Join(*dir, "selection.md"), []byte(rationale.String()), 0644); err != nil {
		return fail(err)
	}
	fmt.Fprintf(out, "frozen %s (%s) to %s\n%s\n", f.Router, f.Status, *output, f.Reason)
	return 0
}
