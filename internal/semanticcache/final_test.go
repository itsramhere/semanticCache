package semanticcache

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
)

func finalFixture(t *testing.T) (FinalCandidate, Workload) {
	t.Helper()
	file, err := os.Open("../../configs/final_candidate.json")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	f, err := LoadCandidate(file)
	if err != nil {
		t.Fatal(err)
	}
	// Independent unit-test catalog: no held-out authored final outcomes opened.
	c := Catalog{ID: f.Experiment.WorkloadID, Origin: "independent final harness unit fixture"}
	for _, split := range []string{"development", "tuning", "final"} {
		c.Groups = append(c.Groups, QAGroup{ID: "unit-" + split, Topic: "unit-" + split, Split: split, Action: "perform unit " + split, Instruction: "perform the unit action"})
	}
	ws, err := BuildWorkloads(c, 101)
	if err != nil {
		t.Fatal(err)
	}
	return f, ws[2]
}

func TestFinalStressContracts(t *testing.T) {
	f, w := finalFixture(t)
	for _, stream := range []string{"all-hit", "all-miss", "empty", "low-hit", "churn"} {
		t.Run(stream, func(t *testing.T) {
			s := FinalScenario{ID: stream, Backend: "local", Mode: "end-to-end", Start: "warm", Capacity: 100, Stream: stream, Requests: 36}
			if stream == "empty" || stream == "all-miss" {
				s.Start = "cold"
			}
			stress, err := FinalWorkload(w, s)
			if err != nil {
				t.Fatal(err)
			}
			a, err := RunFinalCondition(context.Background(), f, s, stress, "baseline", nil, "")
			if err != nil {
				t.Fatal(err)
			}
			b, err := RunFinalCondition(context.Background(), f, s, stress, "frozen-candidate", nil, "")
			if err != nil {
				t.Fatal(err)
			}
			x, y := a.Stats.Metrics, b.Stats.Metrics
			if x.Confusion != y.Confusion || x.ProductionGenerations != y.ProductionGenerations || !reflect.DeepEqual(a.Final, b.Final) || x.Confusion.FN != 0 || x.Bypasses != 0 || x.MemoryBytes != 0 || x.Failures != 0 || !a.Stats.FinalAgreement {
				t.Fatal("frozen fallback differs from baseline")
			}
			if stream == "all-hit" && (x.ProductionGenerations != 0 || x.Confusion.TP != 36 || x.ReuseAccuracy.Value != 1) {
				t.Fatalf("all-hit contract: %+v", x)
			}
			if stream == "all-miss" || stream == "empty" {
				if x.CacheHitRecall.Defined || x.ProductionGenerations != 36 || x.Confusion.FP != 36 {
					t.Fatal("undefined recall or all-miss counts")
				}
			}
		})
	}
}

func TestFinalDelayedRecoveryAndMemoryGuard(t *testing.T) {
	f, w := finalFixture(t)
	for _, delay := range []int{0, 3} {
		s := FinalScenario{ID: "fault", Backend: "local", Mode: "end-to-end", Start: "warm", Capacity: 100, Stream: "churn", Requests: 60, DelaySteps: delay}
		stress, err := FinalWorkload(w, s)
		if err != nil {
			t.Fatal(err)
		}
		p := LSHConfig{"cosine-hyperplanes-v1", w.Identity, 1001, 2, 8}
		run, err := RunFinalCondition(context.Background(), f, s, stress, "diagnostic-lsh", &p, "")
		if err != nil {
			t.Fatal(err)
		}
		if !run.Stats.FinalAgreement || len(run.Rebuilds) < 2 || run.Stats.PeakRebuildBytes <= run.SummaryBytes || run.Stats.Metrics.DiagnosticSearches == 0 {
			t.Fatal("recovery or accounting missing")
		}
		if delay > 0 && run.Stats.Fallbacks == 0 {
			t.Fatal("delayed gaps never activate fallback")
		}
	}
	s := FinalScenario{ID: "guard", Backend: "local", Mode: "end-to-end", Start: "warm", Capacity: 100, Stream: "all-hit", Requests: 12, Fill: true, GuardBytes: 1}
	stress, err := FinalWorkload(w, s)
	if err != nil {
		t.Fatal(err)
	}
	p := LSHConfig{"cosine-hyperplanes-v1", w.Identity, 1001, 2, 8}
	run, err := RunFinalCondition(context.Background(), f, s, stress, "diagnostic-lsh", &p, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(run.Initial.Entries) != 100 || run.Stats.Fallbacks != 12 || run.SummaryBytes <= 1 || f.Experiment.MaxSummaryBytes != 8388608 {
		t.Fatal("memory stress changed the frozen budget or failed to fall back")
	}
	for _, row := range run.Rows {
		if row.Evidence.Decision.Reason != "memory-limit-fallback" {
			t.Fatal("memory guard allowed skipping")
		}
	}
	if _, err = RunFinalCondition(context.Background(), f, s, stress, "frozen-candidate", &p, ""); err == nil {
		t.Fatal("unfrozen LSH accepted as candidate")
	}
}

func TestFinalAccountingBoundaryAndBudgets(t *testing.T) {
	f, w := finalFixture(t)
	hit := true
	r := Result{RequestID: "one", Success: true, EmbeddingCount: 1, SearchCount: 1, Timings: StageTimings{TotalNS: 10000000, SearchNS: 7000000}}
	run := FinalRun{PredictorID: "always-search", Agreement: true, Scenario: FinalScenario{Backend: "remote", Mode: "end-to-end"}, Rows: []FinalRow{{Evidence: ReplayEvidence{Eligible: true, Decision: Decision{Search: true}, OracleHit: &hit, Result: r}, Calls: []RemoteCall{{Path: "/collections/unit/points/query", DurationNS: 7000000}}}}}
	m, err := SummarizeFinal(f.Experiment, run)
	if err != nil {
		t.Fatal(err)
	}
	if m.MixedP95MS.Value != 15 || m.Metrics.CostComponentsUSD["local_cpu_ms"] != 3*f.Experiment.Assumptions["local_cpu_ms"].CostUSD {
		t.Fatal("remote latency double counted or HTTP charged as CPU")
	}
	run.Rows[0].Calls[0].DurationNS = 11000000
	if _, err = SummarizeFinal(f.Experiment, run); err == nil {
		t.Fatal("impossible HTTP boundary accepted")
	}
	m.Metrics.MemoryBytes = f.Experiment.MaxSummaryBytes + 1
	m.Metrics.CacheHitRecall = Metric{.5, true}
	m.MixedP95MS = Metric{1300, true}
	violations := strings.Join(FinalViolations(f.Experiment, FinalStats{}, m), ",")
	for _, name := range []string{"recall", "summary-memory", "absolute-p95", "paired-cost"} {
		if !strings.Contains(violations, name) {
			t.Fatalf("missing budget %s: %s", name, violations)
		}
	}
	if _, err = FinalWorkload(Workload{Split: "tuning"}, FinalScenario{Stream: "original"}); err == nil {
		t.Fatal("non-final evaluation accepted")
	}
	_ = w
}

func TestFinalRemoteLive(t *testing.T) {
	endpoint := os.Getenv("SEMCACHE_QDRANT_URL")
	if endpoint == "" {
		t.Skip("live Qdrant not enabled")
	}
	f, w := finalFixture(t)
	s := FinalScenario{ID: "remote-unit", Backend: "remote", Mode: "end-to-end", Start: "warm", Capacity: 100, Stream: "churn", Requests: 24, DelaySteps: 3}
	stress, err := FinalWorkload(w, s)
	if err != nil {
		t.Fatal(err)
	}
	p := LSHConfig{"cosine-hyperplanes-v1", w.Identity, 1001, 2, 8}
	for _, projection := range []*LSHConfig{nil, &p} {
		role := "baseline"
		if projection != nil {
			role = "diagnostic-lsh"
		}
		run, err := RunFinalCondition(context.Background(), f, s, stress, role, projection, endpoint)
		if err != nil {
			t.Fatal(err)
		}
		if run.Stats.BackendMismatches != 0 || run.Stats.HTTPAttempts == 0 || len(run.CleanupCalls) != 1 || !run.Stats.FinalAgreement {
			t.Fatal("remote parity, cleanup or recovery failed")
		}
	}
}

func TestFinalAuditInvalidAccountingAndWarmState(t *testing.T) {
	f, w := finalFixture(t)
	t.Run("overlapping stage timings", func(t *testing.T) {
		hit := true
		r := FinalRun{Agreement: true, PredictorID: "always-search", Scenario: FinalScenario{Backend: "local", Mode: "end-to-end"}, Rows: []FinalRow{{Evidence: ReplayEvidence{Eligible: true, Decision: Decision{Search: true}, OracleHit: &hit, Result: Result{RequestID: "unit", Success: true, Source: "cache", EmbeddingCount: 1, SearchCount: 1, Timings: StageTimings{TotalNS: 10000000, EmbeddingNS: 4000000, SearchNS: 7000000}}}}}}
		if _, err := SummarizeFinal(f.Experiment, r); err == nil {
			t.Fatal("accepted stage totals exceeding the request boundary")
		}
	})
	t.Run("HTTP overshoot hidden by another request", func(t *testing.T) {
		hit := true
		r := FinalRun{Agreement: true, PredictorID: "always-search", Scenario: FinalScenario{Backend: "remote", Mode: "end-to-end"}}
		for i, ns := range []int64{11000000, 8000000} {
			r.Rows = append(r.Rows, FinalRow{Evidence: ReplayEvidence{Eligible: true, Decision: Decision{Search: true}, OracleHit: &hit, Result: Result{RequestID: fmt.Sprint(i), Success: true, Source: "cache", EmbeddingCount: 1, SearchCount: 1, Timings: StageTimings{TotalNS: 10000000, SearchNS: 7000000}}}, Calls: []RemoteCall{{Path: "/collections/unit/points/query", DurationNS: ns}}})
		}
		if _, err := SummarizeFinal(f.Experiment, r); err == nil {
			t.Fatal("accepted an impossible request HTTP duration because run totals balanced")
		}
	})
	t.Run("filled cache without canonical entries", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("invalid warm state panicked: %v", r)
			}
		}()
		w.Warm = nil
		s := FinalScenario{ID: "missing-warm", Backend: "local", Mode: "end-to-end", Start: "warm", Capacity: 100, Stream: "original", Fill: true}
		if _, err := RunFinalCondition(context.Background(), f, s, w, "baseline", nil, ""); err == nil {
			t.Fatal("accepted filled warm state without source entries")
		}
	})
}
