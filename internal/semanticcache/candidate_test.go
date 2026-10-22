package semanticcache

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

// A complete artificial matrix lets selection rules be checked without using
// final data or making tests depend on noisy measured performance.
func candidateMatrix(t *testing.T) TuningResult {
	t.Helper()
	c := fixtureConfig(t)
	g := TuningGrid{ID: "selection-test", Tables: []int{1, 2}, Bits: []int{4}, Modes: []string{"isolation", "end-to-end"}, Starts: []string{"warm", "cold"}}
	result := TuningResult{Grid: g, Config: c}
	for _, seed := range c.WorkloadSeeds {
		for _, mode := range g.Modes {
			for _, start := range g.Starts {
				for _, cap := range c.Policy.CapacityEntries {
					o := ReplayOptions{Mode: mode, Start: start, Capacity: cap, OracleEvery: 1, OracleLogging: true}
					add := func(variant string, l int, ps int64) {
						id := variant
						if l > 0 {
							p, err := NewProjections(LSHConfig{Version: "cosine-hyperplanes-v1", Identity: FixtureIdentity(), Seed: ps, Tables: l, Bits: 4})
							if err != nil {
								t.Fatal(err)
							}
							id = p.ID()
						}
						m := Metrics{PredictorID: id, Mode: mode, Requests: c.RequestsPerSplit, EligibleRequests: 900, LabeledRequests: 900, Confusion: Confusion{TP: 900, N: 900}, CacheHitRecall: Metric{1, true}, MemoryBytes: 100, SimulatedCostUSD: 1, SimulatedP95MS: Metric{10, true}}
						if l == 2 {
							m.SimulatedCostUSD = 1.5
						}
						result.Rows = append(result.Rows, TuningRow{Variant: variant, Tables: l, Bits: func() int {
							if l > 0 {
								return 4
							}
							return 0
						}(), ProjectionSeed: ps, WorkloadSeed: seed, Options: o, Metrics: m, BaselineCostUSD: 2, BaselineP95MS: Metric{10, true}, Memory: SummaryMemoryMeasurement{RetainedHeapDelta: 100}})
					}
					for _, control := range []string{"always-search", "always-skip", "exact-vector-repeat-v1"} {
						add(control, 0, 0)
					}
					for _, l := range g.Tables {
						for _, ps := range c.ProjectionSeeds {
							add(fmt.Sprintf("lsh-L%d-K4", l), l, ps)
						}
					}
				}
			}
		}
	}
	return result
}

func completeFreeze(f *FinalCandidate) {
	f.TuningDirectory = "test-evidence"
	f.TuningManifestSHA256 = strings.Repeat("0", 64)
	f.SweepSHA256 = strings.Repeat("1", 64)
	f.FrozenAt = time.Date(2026, 10, 22, 0, 0, 0, 0, time.UTC)
}

func TestCandidateSelectionAndFallback(t *testing.T) {
	r := candidateMatrix(t)
	f, err := SelectCandidate(r, FixtureIdentity())
	if err != nil {
		t.Fatal(err)
	}
	if f.Router != "lsh" || f.SelectedVariant != "lsh-L1-K4" || len(f.Projections) != 3 {
		t.Fatal(f)
	}
	completeFreeze(&f)
	b, _ := json.Marshal(f)
	loaded, err := LoadCandidate(bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	ps, err := loaded.Predictors()
	if err != nil || len(ps) != 4 {
		t.Fatal(ps, err)
	}
	// One failing seed/condition disqualifies the whole lower-cost family.
	for i := range r.Rows {
		if r.Rows[i].Variant == "lsh-L1-K4" {
			r.Rows[i].Metrics.CacheHitRecall = Metric{0.98, true}
			break
		}
	}
	f, err = SelectCandidate(r, FixtureIdentity())
	if err != nil || f.SelectedVariant != "lsh-L2-K4" {
		t.Fatal(f, err)
	}
	for i := range r.Rows {
		if r.Rows[i].Variant == "lsh-L2-K4" && r.Rows[i].Options.Mode == "end-to-end" {
			r.Rows[i].Metrics.SimulatedCostUSD = 2
			break
		}
	}
	f, err = SelectCandidate(r, FixtureIdentity())
	if err != nil || f.Router != "always-search" || f.Status != "no-qualifying-lsh" {
		t.Fatal(f, err)
	}
	completeFreeze(&f)
	ps, err = f.Predictors()
	if err != nil || len(ps) != 1 || ps[0].ID() != "always-search" {
		t.Fatal(ps, err)
	}
	r.Rows = r.Rows[1:]
	if _, err := SelectCandidate(r, FixtureIdentity()); err == nil {
		t.Fatal("incomplete matrix selected")
	}
}

func TestCandidateRejectsProjectionChanges(t *testing.T) {
	f, err := SelectCandidate(candidateMatrix(t), FixtureIdentity())
	if err != nil {
		t.Fatal(err)
	}
	completeFreeze(&f)
	f.Projections[0].Seed++
	if f.Validate() == nil {
		t.Fatal("changed seed accepted")
	}
}
