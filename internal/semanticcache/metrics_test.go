package semanticcache

import (
	"encoding/json"
	"math"
	"testing"
	"time"
)

type scriptedPredictor struct {
	decisions []bool
	next      int
}

func (p *scriptedPredictor) ID() string                                   { return "scripted" }
func (p *scriptedPredictor) Reset(EmbeddingIdentity, CacheSnapshot) error { p.next = 0; return nil }
func (p *scriptedPredictor) Observe(CacheSnapshot) error                  { return nil }
func (p *scriptedPredictor) MemoryBytes() int64                           { return 0 }
func (p *scriptedPredictor) Predict(Request, []float64) Decision {
	d := p.decisions[p.next]
	p.next++
	return Decision{d, "scripted", p.ID()}
}

func fourRequestWorkload(t *testing.T) Workload {
	t.Helper()
	w := testWorkloads(t)[0]
	w.Groups = w.Groups[:1]
	w.Warm = w.Warm[:1]
	w.Requests = nil
	base := w.Warm[0].Entry.InsertedAt.Add(time.Minute)
	for i, axis := range []int{0, 1, 0, 2} {
		v := make([]float64, w.Identity.Dimension)
		v[axis] = 1
		r := fixtureRequest(string(rune('a'+i)), "request "+string(rune('a'+i)), base.Add(time.Duration(i)*time.Second))
		w.Requests = append(w.Requests, LabeledRequest{r, w.Groups[0].ID, "exact_repeat", w.Groups[0].ID, true, v})
	}
	return w
}

func near(t *testing.T, got, want float64) {
	t.Helper()
	if math.Abs(got-want) > 1e-12 {
		t.Fatalf("got %.15g want %.15g", got, want)
	}
}

func TestHandComputedFourRequestMetrics(t *testing.T) {
	w := fourRequestWorkload(t)
	o := ReplayOptions{"end-to-end", "warm", 100, 1, true}
	run := runFixture(t, w, o, &scriptedPredictor{decisions: []bool{true, true, false, false}})
	// Fixed measured inputs make cost and nearest-rank expectations hand-checkable.
	run.BuildNS = 1_000_000
	for i := range run.Rows {
		run.Rows[i].Result.Timings = StageTimings{TotalNS: 1_000_000, PredictorNS: 100, UpdateNS: 200}
	}
	m, err := Summarize(fixtureConfig(t), run)
	if err != nil {
		t.Fatal(err)
	}
	if m.Confusion != (Confusion{TP: 1, FP: 1, FN: 1, TN: 1, N: 4}) {
		t.Fatalf("confusion %+v", m.Confusion)
	}
	for _, metric := range []Metric{m.LookupReduction, m.CacheHitRecall, m.MissedHitFraction, m.UnsuccessfulRoutedFraction, m.CorrectlyAvoidedMissFraction} {
		if !metric.Defined {
			t.Fatal("undefined ratio")
		}
		near(t, metric.Value, 0.5)
	}
	if m.ProductionSearches != 2 || m.ProductionEmbeddings != 4 || m.ProductionGenerations != 3 || m.ProductionWrites != 3 || m.DiagnosticSearches != 4 || m.Bypasses != 2 {
		t.Fatalf("operation totals %+v", m)
	}
	near(t, m.SimulatedCostUSD, 0.00630005)
	near(t, m.MeasuredP50MS.Value, 1)
	near(t, m.SimulatedP50MS.Value, 1008.25)
	near(t, m.SimulatedP95MS.Value, 1028.25)
	if m.PredictorNS != 400 || m.UpdateNS != 800 || m.CorrectReuse != 1 {
		t.Fatal("stage/reuse totals")
	}
	// Diagnostic calls are never charged as production operations.
	run.DiagnosticSearches = 999
	m2, err := Summarize(fixtureConfig(t), run)
	if err != nil {
		t.Fatal(err)
	}
	near(t, m2.SimulatedCostUSD, m.SimulatedCostUSD)
}

func TestMetricsControlsAndUndefined(t *testing.T) {
	w := fourRequestWorkload(t)
	o := ReplayOptions{"end-to-end", "warm", 100, 1, true}
	c := fixtureConfig(t)
	for _, search := range []bool{true, false} {
		run := runFixture(t, w, o, &ConstantPredictor{search})
		m, err := Summarize(c, run)
		if err != nil {
			t.Fatal(err)
		}
		if search {
			if m.Bypasses != 0 || m.Confusion.FN != 0 || m.CacheHitRecall.Value != 1 || m.ProductionSearches != 4 {
				t.Fatal("always-search metrics")
			}
		} else {
			if m.ProductionSearches != 0 || m.Confusion.FN != 2 || m.Confusion.TN != 2 || m.CacheHitRecall.Value != 0 {
				t.Fatal("always-skip metrics")
			}
			if m.UnsuccessfulRoutedFraction.Defined {
				t.Fatal("undefined routed fraction")
			}
		}
	}
	m, err := Summarize(c, ReplayRun{})
	if err != nil {
		t.Fatal(err)
	}
	for _, metric := range []Metric{m.LookupReduction, m.CacheHitRecall, m.MissedHitFraction, m.UnsuccessfulRoutedFraction, m.CorrectlyAvoidedMissFraction, m.MeasuredP95MS, m.ReuseAccuracy} {
		b, err := json.Marshal(metric)
		if err != nil || string(b) != `"N/A"` || metric.String() != "N/A" {
			t.Fatal("undefined metric not N/A")
		}
	}
	o.OracleEvery = 0
	run := runFixture(t, w, o, &ConstantPredictor{true})
	m, err = Summarize(c, run)
	if err != nil {
		t.Fatal(err)
	}
	if m.CacheHitRecall.Defined || m.LabeledRequests != 0 || m.ProductionSearches != 4 {
		t.Fatal("disabled oracle affected production or invented recall")
	}
}

func TestMetricsSeparateAnswerReuseAndFailures(t *testing.T) {
	w := fourRequestWorkload(t)
	w.Requests[0].AnswerClass += "/different-answer"
	run := runFixture(t, w, ReplayOptions{"end-to-end", "warm", 100, 1, true}, &ConstantPredictor{true})
	run.Rows[1].Result.Success = false
	run.Rows[1].Result.Error = "generation failed"
	run.Rows[1].Result.WriteCount = 0
	run.Rows[2].Result.CacheWriteError = "write failed"
	m, err := Summarize(fixtureConfig(t), run)
	if err != nil {
		t.Fatal(err)
	}
	if m.CacheHitRecall.Value != 1 || m.ReuseAccuracy.Value >= 1 || m.Failures != 1 || m.WriteFailures != 1 || !m.FailedP95MS.Defined {
		t.Fatal("routing concealed wrong reuse or errors")
	}
}

func TestNearestRankAndInvalidMetrics(t *testing.T) {
	near(t, quantile([]float64{4, 1, 3, 2}, 0.5).Value, 2)
	near(t, quantile([]float64{4, 1, 3, 2}, 0.95).Value, 4)
	run := ReplayRun{Rows: []ReplayEvidence{{Result: Result{Timings: StageTimings{TotalNS: -1}}}}}
	if _, err := Summarize(fixtureConfig(t), run); err == nil {
		t.Fatal("accepted negative timings")
	}
}
