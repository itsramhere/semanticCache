package semanticcache

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"sort"
)

type TuningGrid struct {
	ID     string   `json:"id"`
	Tables []int    `json:"tables"`
	Bits   []int    `json:"bits"`
	Modes  []string `json:"modes"`
	Starts []string `json:"starts"`
}

func LoadTuningGrid(r io.Reader) (TuningGrid, error) {
	var g TuningGrid
	if err := decodeStrict(r, &g); err != nil {
		return g, err
	}
	if g.ID == "" || len(g.Tables) == 0 || len(g.Bits) == 0 || len(g.Tables)*len(g.Bits) > 16 {
		return g, fmt.Errorf("invalid tuning grid")
	}
	for i, xs := range [][]int{g.Tables, g.Bits} {
		seen := map[int]bool{}
		for _, n := range xs {
			limit := 256
			if i == 1 {
				limit = 64
			}
			if n < 1 || n > limit || seen[n] {
				return g, fmt.Errorf("invalid or duplicate grid value")
			}
			seen[n] = true
		}
	}
	if len(g.Modes) != 2 || g.Modes[0] != "isolation" || g.Modes[1] != "end-to-end" || len(g.Starts) != 2 || g.Starts[0] != "warm" || g.Starts[1] != "cold" {
		return g, fmt.Errorf("grid requires both modes and warm/cold starts in declared order")
	}
	return g, nil
}

// ValidateTuningInputs forbids selecting on final or development outcomes and
// requires the complete frozen seed/size/capacity experiment, without opening
// any held-out final workload.
func ValidateTuningInputs(c Config, ws []Workload) error {
	if err := c.Validate(); err != nil {
		return err
	}
	if len(ws) != len(c.WorkloadSeeds) {
		return fmt.Errorf("tuning requires every workload seed")
	}
	seen := map[int64]bool{}
	for _, w := range ws {
		allowed := false
		for _, seed := range c.WorkloadSeeds {
			if w.Seed == seed {
				allowed = true
			}
		}
		if w.Split != "tuning" || !allowed || seen[w.Seed] || len(w.Requests) != c.RequestsPerSplit || w.ID != c.WorkloadID {
			return fmt.Errorf("only complete, distinct declared tuning workloads are accepted")
		}
		seen[w.Seed] = true
	}
	return ValidateWorkloads(ws)
}

type TuningRow struct {
	Variant         string                   `json:"variant"`
	WorkloadSeed    int64                    `json:"workload_seed"`
	ProjectionSeed  int64                    `json:"projection_seed"`
	Tables          int                      `json:"tables"`
	Bits            int                      `json:"bits"`
	Options         ReplayOptions            `json:"options"`
	Metrics         Metrics                  `json:"metrics"`
	BaselineCostUSD float64                  `json:"baseline_simulated_cost_usd"`
	BaselineP95MS   Metric                   `json:"baseline_simulated_p95_ms"`
	Memory          SummaryMemoryMeasurement `json:"memory"`
	ReferenceBytes  int64                    `json:"reference_cache_structural_estimate_bytes"`
	MemoryRatio     Metric                   `json:"structural_summary_to_reference_ratio"`
	Failures        []string                 `json:"constraint_failures"`
	Evidence        string                   `json:"evidence"`
}

// TuningFailures reports every constraint per condition; isolation serving
// counts describe the baseline and therefore cannot establish cost benefit.
func TuningFailures(c Config, r TuningRow) []string {
	var failures []string
	m := r.Metrics
	if !m.CacheHitRecall.Defined || m.CacheHitRecall.Value < c.MinHitRecall {
		failures = append(failures, "recall")
	}
	if m.LabeledRequests != m.EligibleRequests || m.Failures != 0 || m.WriteFailures != 0 {
		failures = append(failures, "incomplete-labels-or-serving-errors")
	}
	if m.MemoryBytes > c.MaxSummaryBytes || r.Memory.RetainedHeapDelta > c.MaxSummaryBytes {
		failures = append(failures, "memory")
	}
	if r.Variant != "always-search" && r.Variant != "always-skip" && r.Memory.RetainedHeapDelta <= 0 {
		failures = append(failures, "memory-unmeasured")
	}
	if r.Options.Mode == "end-to-end" {
		if !(m.SimulatedCostUSD < r.BaselineCostUSD) {
			failures = append(failures, "paired-cost")
		}
		if !m.SimulatedP95MS.Defined || m.SimulatedP95MS.Value > c.MaxP95LatencyMS {
			failures = append(failures, "absolute-p95")
		}
		if !m.SimulatedP95MS.Defined || !r.BaselineP95MS.Defined || m.SimulatedP95MS.Value > r.BaselineP95MS.Value {
			failures = append(failures, "paired-p95")
		}
	}
	return failures
}

type TuningResult struct {
	Grid   TuningGrid  `json:"grid"`
	Config Config      `json:"config"`
	Rows   []TuningRow `json:"rows"`
}

// VariantAssessment keeps all seeds together: seed choice is never tuned.
type VariantAssessment struct {
	Variant           string         `json:"variant"`
	Tables            int            `json:"tables"`
	Bits              int            `json:"bits"`
	Conditions        int            `json:"conditions"`
	PassingConditions int            `json:"passing_conditions"`
	TotalCostUSD      float64        `json:"simulated_end_to_end_total_cost_usd"`
	MinRecall         Metric         `json:"minimum_recall"`
	MaxMemoryBytes    int64          `json:"maximum_budgeted_summary_bytes"`
	Failures          map[string]int `json:"failure_counts"`
}

// AssessTuning also validates matrix completeness so a partial/edited sweep
// cannot silently qualify a configuration by omitting its failing conditions.
func AssessTuning(result TuningResult) ([]VariantAssessment, error) {
	c, g := result.Config, result.Grid
	if err := c.Validate(); err != nil {
		return nil, err
	}
	gridBytes, err := json.Marshal(g)
	if err != nil {
		return nil, err
	}
	if _, err := LoadTuningGrid(bytes.NewReader(gridBytes)); err != nil {
		return nil, err
	}
	variants := map[string]*VariantAssessment{}
	expected := map[string]bool{}
	type shape struct{ tables, bits int }
	shapes := map[string]shape{"always-search": {}, "always-skip": {}, "exact-vector-repeat-v1": {}}
	for _, l := range g.Tables {
		for _, k := range g.Bits {
			shapes[fmt.Sprintf("lsh-L%d-K%d", l, k)] = shape{l, k}
		}
	}
	key := func(v string, seed, projection int64, o ReplayOptions) string {
		return fmt.Sprintf("%s/%d/%d/%s/%s/%d", v, seed, projection, o.Mode, o.Start, o.Capacity)
	}
	for _, seed := range c.WorkloadSeeds {
		for _, mode := range g.Modes {
			for _, start := range g.Starts {
				for _, cap := range c.Policy.CapacityEntries {
					o := ReplayOptions{Mode: mode, Start: start, Capacity: cap, OracleEvery: 1, OracleLogging: true}
					for _, control := range []string{"always-search", "always-skip", "exact-vector-repeat-v1"} {
						expected[key(control, seed, 0, o)] = true
					}
					for _, l := range g.Tables {
						for _, k := range g.Bits {
							for _, ps := range c.ProjectionSeeds {
								expected[key(fmt.Sprintf("lsh-L%d-K%d", l, k), seed, ps, o)] = true
							}
						}
					}
				}
			}
		}
	}
	for _, r := range result.Rows {
		k := key(r.Variant, r.WorkloadSeed, r.ProjectionSeed, r.Options)
		if !expected[k] || r.Options.OracleEvery != 1 || !r.Options.OracleLogging {
			return nil, fmt.Errorf("duplicate or unexpected tuning condition %s", k)
		}
		delete(expected, k)
		if want := shapes[r.Variant]; r.Tables != want.tables || r.Bits != want.bits {
			return nil, fmt.Errorf("invalid variant metadata for %s", r.Variant)
		}
		if r.Metrics.Requests != c.RequestsPerSplit || r.Metrics.Mode != r.Options.Mode {
			return nil, fmt.Errorf("incomplete tuning run")
		}
		v := variants[r.Variant]
		if v == nil {
			v = &VariantAssessment{Variant: r.Variant, Tables: r.Tables, Bits: r.Bits, Failures: map[string]int{}}
			variants[r.Variant] = v
		}
		if v.Tables != r.Tables || v.Bits != r.Bits {
			return nil, fmt.Errorf("inconsistent variant")
		}
		if r.Tables > 0 && r.Variant != fmt.Sprintf("lsh-L%d-K%d", r.Tables, r.Bits) {
			return nil, fmt.Errorf("invalid variant metadata")
		}
		v.Conditions++
		fs := TuningFailures(c, r)
		if len(fs) == 0 {
			v.PassingConditions++
		}
		for _, f := range fs {
			v.Failures[f]++
		}
		if r.Metrics.CacheHitRecall.Defined && (!v.MinRecall.Defined || r.Metrics.CacheHitRecall.Value < v.MinRecall.Value) {
			v.MinRecall = r.Metrics.CacheHitRecall
		}
		n := r.Metrics.MemoryBytes
		if r.Memory.RetainedHeapDelta > n {
			n = r.Memory.RetainedHeapDelta
		}
		if n > v.MaxMemoryBytes {
			v.MaxMemoryBytes = n
		}
		if r.Options.Mode == "end-to-end" {
			v.TotalCostUSD += r.Metrics.SimulatedCostUSD
		}
	}
	if len(expected) != 0 {
		return nil, fmt.Errorf("incomplete tuning matrix: %d missing conditions", len(expected))
	}
	var out []VariantAssessment
	for _, v := range variants {
		out = append(out, *v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Variant < out[j].Variant })
	return out, nil
}
