package semanticcache

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestTuningConstraintBoundaries(t *testing.T) {
	c := fixtureConfig(t)
	r := TuningRow{Variant: "lsh-L1-K4", Options: ReplayOptions{Mode: "end-to-end"}, BaselineCostUSD: 2, BaselineP95MS: Metric{10, true}, Memory: SummaryMemoryMeasurement{RetainedHeapDelta: 1}, Metrics: Metrics{CacheHitRecall: Metric{c.MinHitRecall, true}, MemoryBytes: c.MaxSummaryBytes, SimulatedCostUSD: 1, SimulatedP95MS: Metric{10, true}, EligibleRequests: 4, LabeledRequests: 4}}
	if failures := TuningFailures(c, r); len(failures) != 0 {
		t.Fatal(failures)
	}
	r.Metrics.CacheHitRecall = Metric{}
	r.Metrics.SimulatedCostUSD = 2
	r.Metrics.SimulatedP95MS.Value = 11
	r.Memory.RetainedHeapDelta = c.MaxSummaryBytes + 1
	fs := strings.Join(TuningFailures(c, r), ",")
	for _, want := range []string{"recall", "memory", "paired-cost", "paired-p95"} {
		if !strings.Contains(fs, want) {
			t.Fatal(want, fs)
		}
	}
	r.Options.Mode = "isolation"
	if strings.Contains(strings.Join(TuningFailures(c, r), ","), "paired-") {
		t.Fatal("isolation costs cannot qualify serving behavior")
	}
}

func TestTuningInputsRejectOtherSplitsAndPartialMatrix(t *testing.T) {
	c := fixtureConfig(t)
	if err := ValidateTuningInputs(c, nil); err == nil {
		t.Fatal("missing seeds accepted")
	}
	ws := make([]Workload, len(c.WorkloadSeeds))
	for i, seed := range c.WorkloadSeeds {
		ws[i] = Workload{Split: "final", Seed: seed}
	}
	if err := ValidateTuningInputs(c, ws); err == nil {
		t.Fatal("final accepted")
	}
	g, err := LoadTuningGrid(strings.NewReader(`{"id":"test","tables":[1],"bits":[4],"modes":["isolation","end-to-end"],"starts":["warm","cold"]}`))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := AssessTuning(TuningResult{Config: c, Grid: g}); err == nil {
		t.Fatal("partial results accepted")
	}
	if _, err := LoadTuningGrid(strings.NewReader(`{"id":"bad","tables":[1,1],"bits":[4],"modes":[],"starts":[]}`)); err == nil {
		t.Fatal("bad grid accepted")
	}
}

func TestMetricJSONRoundTrip(t *testing.T) {
	for _, m := range []Metric{{}, {0.99, true}} {
		b, _ := json.Marshal(m)
		var got Metric
		if err := json.Unmarshal(b, &got); err != nil || got != m {
			t.Fatal(string(b), got, err)
		}
	}
	var m Metric
	if json.Unmarshal([]byte("null"), &m) == nil {
		t.Fatal("null metric accepted")
	}
}

func TestMeasuredSummaryOwnsMemory(t *testing.T) {
	p := smallProjections(t)
	snapshot := CacheSnapshot{}
	for i := 0; i < 100; i++ {
		e := Entry{ID: strings.Repeat("x", i+1), Embedding: []float64{1, 0}, Scope: exampleScopeA, Validity: ValidityMetadata{Reusable: true}}
		snapshot.Entries = append(snapshot.Entries, SnapshotEntry{Entry: e})
	}
	m, err := MeasureSummary(t.TempDir(), p.Config.Identity, snapshot, func() (ReplayPredictor, error) { return NewLSHSummary(p) })
	if err != nil {
		t.Fatal(err)
	}
	if m.RetainedHeapDelta <= 0 || m.ProcessAfter <= 0 || m.StructuralBytes <= 0 {
		t.Fatal(m)
	}
}

func TestAssessmentRejectsInvalidGridAndVariantMetadata(t *testing.T) {
	for _, name := range []string{"empty-grid", "zero-tables", "wrong-bits", "control-with-bits"} {
		t.Run(name, func(t *testing.T) {
			r := candidateMatrix(t)
			switch name {
			case "empty-grid":
				r.Grid = TuningGrid{}
				r.Rows = nil
			case "zero-tables":
				for i := range r.Rows {
					if r.Rows[i].Variant == "lsh-L1-K4" {
						r.Rows[i].Tables = 0
					}
				}
			case "wrong-bits":
				r.Rows[0].Bits = 9
			case "control-with-bits":
				for i := range r.Rows {
					if r.Rows[i].Variant == "always-search" {
						r.Rows[i].Bits = 4
					}
				}
			}
			if _, err := AssessTuning(r); err == nil {
				t.Fatal("invalid tuning evidence accepted")
			}
		})
	}
}
