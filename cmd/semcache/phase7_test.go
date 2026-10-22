package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"semanticcache/internal/semanticcache"
)

func TestPhase7CLIReproductionAndTampering(t *testing.T) {
	dir := t.TempDir()
	cb, err := os.ReadFile("../../configs/final_candidate.json")
	if err != nil {
		t.Fatal(err)
	}
	f, err := semanticcache.LoadCandidate(bytes.NewReader(cb))
	if err != nil {
		t.Fatal(err)
	}
	plan := semanticcache.FinalPlan{ID: "unit-final-plan", CandidateSHA256: finalHash(cb), Diagnostic: semanticcache.LSHConfig{Version: "cosine-hyperplanes-v1", Identity: f.Identity, Seed: 1001, Tables: 2, Bits: 8}, Scenarios: []semanticcache.FinalScenario{{ID: "unit-final", Backend: "local", Mode: "end-to-end", Start: "warm", Capacity: 100, Stream: "all-hit", Requests: 12, Diagnostic: true}}}
	planPath := filepath.Join(dir, "plan.json")
	if err = os.WriteFile(planPath, finalJSON(plan), 0644); err != nil {
		t.Fatal(err)
	}
	c := semanticcache.Catalog{ID: f.Experiment.WorkloadID, Origin: "independent CLI unit catalog"}
	for _, split := range []string{"development", "tuning", "final"} {
		c.Groups = append(c.Groups, semanticcache.QAGroup{ID: "unit-" + split, Topic: "unit-" + split, Split: split, Action: "unit " + split, Instruction: "follow unit instruction"})
	}
	for _, seed := range f.Experiment.WorkloadSeeds {
		ws, e := semanticcache.BuildWorkloads(c, seed)
		if e != nil {
			t.Fatal(e)
		}
		if e = writeJSON(filepath.Join(dir, "final-"+fmtSeed(seed)+".json"), ws[2]); e != nil {
			t.Fatal(e)
		}
	}
	var out, errOut bytes.Buffer
	paths := []string{filepath.Join(dir, "a"), filepath.Join(dir, "b")}
	for _, path := range paths {
		args := []string{"final-evaluate", "-plan", planPath, "-candidate", "../../configs/final_candidate.json", "-data-dir", dir, "-output", path}
		if code := run(args, &out, &errOut); code != 0 {
			t.Fatalf("evaluate %d: %s", code, errOut.String())
		}
		if code := run(args, &out, &errOut); code != 1 {
			t.Fatal("existing evidence overwritten")
		}
		errOut.Reset()
	}
	verify := []string{"final-verify", "-input", paths[0], "-replay", paths[1]}
	if code := run(verify, &out, &errOut); code != 0 {
		t.Fatalf("verify %d: %s", code, errOut.String())
	}
	t.Run("rehashed clean aggregate must agree with traces", func(t *testing.T) {
		phase7AlterArtifact(t, paths[1], "aggregates.json", func([]byte) []byte { return []byte("[]") })
		var a, b bytes.Buffer
		if code := run(verify, &a, &b); code != 1 {
			t.Fatal("clean replay accepted a rehashed aggregate unrelated to its traces")
		}
	})
	t.Run("declared embedding must match frozen identity", func(t *testing.T) {
		for _, name := range []string{"inputs/final-101.json", "streams/unit-final-101.json"} {
			phase7AlterArtifact(t, paths[0], name, func(b []byte) []byte {
				var w semanticcache.Workload
				if err := json.Unmarshal(b, &w); err != nil {
					t.Fatal(err)
				}
				w.Identity.Revision = "wrong-revision"
				return finalJSON(w)
			})
		}
		var a, b bytes.Buffer
		if code := run(verify, &a, &b); code != 1 {
			t.Fatal("final evidence accepted a different embedding revision")
		}
	})
	t.Run("predictor identifier must match frozen projections", func(t *testing.T) {
		key := "unit-final/101/diagnostic-lsh-1001"
		phase7AlterArtifact(t, paths[0], key+"/run.json", func(b []byte) []byte {
			var r semanticcache.FinalRun
			json.Unmarshal(b, &r)
			r.PredictorID = "unfrozen-predictor"
			r.Stats.Metrics.PredictorID = r.PredictorID
			return finalJSON(r)
		})
		phase7AlterArtifact(t, paths[0], "results.json", func(b []byte) []byte {
			var rows []finalTableRow
			json.Unmarshal(b, &rows)
			for i := range rows {
				if rows[i].Evidence == key {
					rows[i].Stats.Metrics.PredictorID = "unfrozen-predictor"
				}
			}
			return finalJSON(rows)
		})
		var a, b bytes.Buffer
		if code := run(verify, &a, &b); code != 1 {
			t.Fatal("final evidence accepted an unbound predictor identity")
		}
	})
	t.Run("clean replay must reproduce each response", func(t *testing.T) {
		phase7AlterArtifact(t, paths[1], "unit-final/101/baseline-0/requests.jsonl", func(b []byte) []byte {
			dec := json.NewDecoder(bytes.NewReader(b))
			var log bytes.Buffer
			enc := json.NewEncoder(&log)
			for i := 0; i < 12; i++ {
				var r semanticcache.FinalRow
				if err := dec.Decode(&r); err != nil {
					t.Fatal(err)
				}
				if i == 0 {
					r.Evidence.Result.Answer = "changed cached response"
				}
				if err := enc.Encode(r); err != nil {
					t.Fatal(err)
				}
			}
			return log.Bytes()
		})
		var a, b bytes.Buffer
		if code := run(verify, &a, &b); code != 1 {
			t.Fatal("clean replay compared aggregates but missed a changed response")
		}
	})
	path := filepath.Join(paths[0], "unit-final", "101", "baseline-0", "requests.jsonl")
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, append(b, ' '), 0644); err != nil {
		t.Fatal(err)
	}
	if code := run(verify, &out, &errOut); code != 1 {
		t.Fatal("altered evidence accepted")
	}
	if code := run([]string{"final-verify"}, &out, &errOut); code != 2 {
		t.Fatal("missing input accepted")
	}
	// Unknown plan fields are rejected before opening final inputs.
	b = finalJSON(plan)
	var raw map[string]any
	json.Unmarshal(b, &raw)
	raw["typo"] = true
	os.WriteFile(planPath, finalJSON(raw), 0644)
	if code := run([]string{"final-evaluate", "-plan", planPath}, &out, &errOut); code != 1 {
		t.Fatal("unknown plan field accepted")
	}
}

func fmtSeed(seed int64) string { return strconv.FormatInt(seed, 10) }

// Alter content and update both enclosing manifests. Tests must exercise
// semantic verification rather than stopping at a deliberately stale checksum.
func phase7AlterArtifact(t *testing.T, dir, name string, change func([]byte) []byte) {
	t.Helper()
	backups := map[string][]byte{}
	read := func(name string) []byte {
		b, err := os.ReadFile(filepath.Join(dir, filepath.FromSlash(name)))
		if err != nil {
			t.Fatal(err)
		}
		backups[name] = append([]byte(nil), b...)
		return b
	}
	write := func(name string, b []byte) {
		if err := os.WriteFile(filepath.Join(dir, filepath.FromSlash(name)), b, 0644); err != nil {
			t.Fatal(err)
		}
	}
	var root finalManifest
	if err := json.Unmarshal(read("manifest.json"), &root); err != nil {
		t.Fatal(err)
	}
	b := change(read(name))
	write(name, b)
	root.Files[name] = finalHash(b)
	if base := filepath.Base(name); base == "run.json" || base == "requests.jsonl" {
		localName := filepath.ToSlash(filepath.Join(filepath.Dir(name), "manifest.json"))
		var local finalManifest
		if err := json.Unmarshal(read(localName), &local); err != nil {
			t.Fatal(err)
		}
		local.Files[base] = finalHash(b)
		lb := finalJSON(local)
		write(localName, lb)
		root.Files[localName] = finalHash(lb)
	}
	write("manifest.json", finalJSON(root))
	t.Cleanup(func() {
		for name, b := range backups {
			write(name, b)
		}
	})
}
