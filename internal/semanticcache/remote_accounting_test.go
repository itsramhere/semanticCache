package semanticcache

import (
	"math"
	"os"
	"strings"
	"testing"
)

func accountingFixture(t *testing.T) (Config, []RemoteRun) {
	t.Helper()
	b, err := os.ReadFile("../../configs/experiment.json")
	if err != nil {
		t.Fatal(err)
	}
	config, err := LoadConfig(strings.NewReader(string(b)))
	if err != nil {
		t.Fatal(err)
	}
	cpu := config.Assumptions["local_cpu_ms"]
	cpu.CostUSD = 0
	config.Assumptions["local_cpu_ms"] = cpu
	runs := []RemoteRun{{Router: "always-search"}, {Router: "exploratory-lsh", BuildNS: 5_000_000, Rebuilds: []RemoteRebuild{{AfterRequest: 2, DurationNS: 10_000_000, PeakStructuralBytes: 1000, SampledHeapBytes: 2000}}}}
	for i := 0; i < 2; i++ {
		r := exampleRequest(exampleScopeA)
		r.ID = string(rune('a' + i))
		for j := 0; j < 2; j++ {
			search := j == 0
			generations := 1
			if i == 0 && j == 0 {
				generations = 0
			}
			d := Decision{search, "fixture", "fixture"}
			result := Result{RequestID: r.ID, Success: true, EmbeddingCount: 1, GenerationCount: generations, WriteCount: generations, Decision: &d, Timings: StageTimings{TotalNS: 20_000_000, EmbeddingNS: 1_000_000, GenerationNS: int64(generations) * 2_000_000}}
			row := RemoteRow{Request: r, Result: result, OracleHit: j == 1 || i == 0}
			if search {
				row.Result.SearchCount = 1
				row.Result.Timings.SearchNS = 5_000_000
				row.Calls = append(row.Calls, RemoteCall{Path: "/collections/test/points/query", DurationNS: 5_000_000})
			}
			if generations > 0 {
				row.Calls = append(row.Calls, RemoteCall{Path: "/collections/test/points?wait=true", DurationNS: 1_000_000})
			}
			runs[j].Rows = append(runs[j].Rows, row)
		}
	}
	return config, runs
}

func TestRemoteAccountingPairedCostsAndRebuild(t *testing.T) {
	config, runs := accountingFixture(t)
	c, err := AccountRemote(config, runs)
	if err != nil {
		t.Fatal(err)
	}
	close := func(actual, want float64) {
		t.Helper()
		if math.Abs(actual-want) > 1e-10 {
			t.Fatalf("got %.12g want %.12g", actual, want)
		}
	}
	close(c.Runs[0].ServingCostUSD, .00224)
	close(c.Runs[1].ServingCostUSD, .00406)
	close(c.CostDeltaUSD, .00182)
	close(c.BreakEvenLookupCostUSD.Value, .00101)
	if c.GenerationDelta != 1 || c.Runs[1].Confusion.FN != 2 {
		t.Fatal("generation delta incorrectly inferred from FN")
	}
	close(c.Runs[1].AmortizedMaintenanceNS, 7_500_000)
	close(c.Runs[1].MeasuredRoutingTotalMS, 55)
	// Baseline 40 - embedding 2 - fake generation 2 + embedding 10 + LLM 1000.
	close(c.Runs[0].MixedTotalMS, 1046)
	// Predictor 40 - embedding 2 - fake generation 4 + embedding 10 + LLM 2000 + build/rebuild 15.
	close(c.Runs[1].MixedTotalMS, 2059)
	close(c.BreakEvenLookupLatencyMS.Value, 511.5)
	if c.Sensitivity[0].CostDeltaUSD <= 0 || c.Sensitivity[len(c.Sensitivity)-1].CostDeltaUSD >= 0 {
		t.Fatal("break-even signs do not cross")
	}
	if c.Runs[1].PeakRebuildBytes != 1000 || c.Runs[1].SampledProcessHeapBytes != 2000 {
		t.Fatal("memory boundaries lost")
	}
}

func TestRemoteAccountingRejectsBadPairsAndCounters(t *testing.T) {
	config, runs := accountingFixture(t)
	runs[1].Rows[0].Request.Text = "different"
	if _, err := AccountRemote(config, runs); err == nil {
		t.Fatal("accepted different paired inputs")
	}
	config, runs = accountingFixture(t)
	runs[0].Rows[0].Result.SearchCount = 0
	if _, err := AccountRemote(config, runs); err == nil {
		t.Fatal("accepted HTTP counter mismatch")
	}
}

func TestRemoteAccountingNoSavingsAndUndefinedRecall(t *testing.T) {
	config, runs := accountingFixture(t)
	runs[1] = runs[0]
	runs[1].Router = "exploratory-lsh"
	for j := range runs {
		for i := range runs[j].Rows {
			runs[j].Rows[i].OracleHit = false
		}
	}
	c, err := AccountRemote(config, runs)
	if err != nil {
		t.Fatal(err)
	}
	if c.BreakEvenLookupCostUSD.Defined || c.BreakEvenLookupLatencyMS.Defined || c.Runs[0].Recall.Defined || c.CostDeltaUSD != 0 {
		t.Fatal("undefined metric or identical-run delta")
	}
}

func TestRemoteAccountingRejectsInvalidMeasurements(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*RemoteRun)
	}{
		{"negative generation count", func(r *RemoteRun) { r.Rows[0].Result.GenerationCount = -1 }},
		{"negative embedding count", func(r *RemoteRun) { r.Rows[0].Result.EmbeddingCount = -1 }},
		{"negative write count", func(r *RemoteRun) { r.Rows[0].Result.WriteCount = -1 }},
		{"negative embedding time", func(r *RemoteRun) { r.Rows[0].Result.Timings.EmbeddingNS = -1 }},
		{"negative generation time", func(r *RemoteRun) { r.Rows[0].Result.Timings.GenerationNS = -1 }},
		{"negative validation time", func(r *RemoteRun) { r.Rows[0].Result.Timings.ValidationNS = -1 }},
		{"negative write time", func(r *RemoteRun) { r.Rows[0].Result.Timings.CacheWriteNS = -1 }},
		{"negative admin time", func(r *RemoteRun) { r.SetupCalls = []RemoteCall{{DurationNS: -1}} }},
		{"negative summary memory", func(r *RemoteRun) { r.MemoryBytes = -1 }},
		{"negative rebuild memory", func(r *RemoteRun) { r.Rebuilds = []RemoteRebuild{{AfterRequest: 1, PeakStructuralBytes: -1}} }},
		{"duplicate request IDs", func(r *RemoteRun) {
			r.Rows[1].Request.ID = r.Rows[0].Request.ID
			r.Rows[1].Result.RequestID = r.Rows[0].Request.ID
		}},
		{"unknown router", func(r *RemoteRun) { r.Router = "unknown" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			config, runs := accountingFixture(t)
			tc.mutate(&runs[1])
			// Keep paired inputs identical so validation reaches the measurements.
			for i := range runs[1].Rows {
				runs[0].Rows[i].Request = runs[1].Rows[i].Request
				runs[0].Rows[i].Result.RequestID = runs[1].Rows[i].Request.ID
			}
			if _, err := AccountRemote(config, runs); err == nil {
				t.Fatal("invalid evidence accepted for cost comparison")
			}
		})
	}
}
