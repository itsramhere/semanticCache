package semanticcache

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func testChurnConfig() ChurnConfig {
	return ChurnConfig{ID: "phase5-test", WorkloadSeed: 101, ProjectionSeed: 1001, Steps: 36, StepMS: 1000, Capacity: 3, TTLSeconds: 5, FreshnessLimitMS: 3000, HeartbeatEverySteps: 4, RebuildEverySteps: 12, Faults: FaultConfig{5001, 6000, .3, .15}}
}

func TestChurnServingFaultAttributionAndRecovery(t *testing.T) {
	for _, kind := range []string{"fresh", "mixed", "silent-tail"} {
		t.Run(kind, func(t *testing.T) {
			c := testChurnConfig()
			if kind == "fresh" {
				c.Faults = FaultConfig{Seed: 5001}
				c.HeartbeatEverySteps = 1
			}
			if kind == "silent-tail" {
				c.Faults = FaultConfig{Seed: 5001, DropProbability: 1}
				c.HeartbeatEverySteps = 0
			}
			run, err := RunChurn(context.Background(), c)
			if err != nil {
				t.Fatal(err)
			}
			m := run.Metrics
			if !m.FinalAgreement || m.Confusion.N != c.Steps || m.Confusion.TP+m.Confusion.FP+m.Confusion.FN+m.Confusion.TN != c.Steps {
				t.Fatalf("invalid churn accounting: %+v", m)
			}
			if m.UnseenUpsertMisses+m.IntrinsicLSHMisses != m.Confusion.FN || m.Generations != c.Steps-m.Confusion.TP {
				t.Fatalf("invalid attribution/generation accounting: %+v", m)
			}
			for _, row := range run.Rows {
				if (row.Result.Source == "cache") != (row.OracleHit && row.Result.Decision.Search) {
					t.Fatalf("retrieval/validation changed: %+v", row)
				}
				if row.StaleRemovalSearch && (row.OracleHit || row.FreshDecision.Search || !row.Status.Ready) {
					t.Fatal("fallback or LSH FP misattributed to stale removal")
				}
				if row.UnseenUpsertMiss && (!row.OracleHit || !row.FreshDecision.Search || row.Result.Decision.Search) {
					t.Fatal("intrinsic LSH miss misattributed to unseen update")
				}
			}
			switch kind {
			case "fresh":
				if m.Confusion.FN != 0 || m.UnseenUpsertMisses != 0 || m.StaleRemovalSearches != 0 || m.DroppedEvents != 0 {
					t.Fatalf("fresh path drifted: %+v", m)
				}
			case "mixed":
				if m.DroppedEvents == 0 || m.DuplicateDeliveries == 0 || m.ReorderedDeliveries == 0 || m.FallbackRequests == 0 || m.StaleRemovalSearches == 0 {
					t.Fatalf("faults missing: %+v", m)
				}
			case "silent-tail":
				if m.UnseenUpsertMisses == 0 || m.FallbackRequests == 0 {
					t.Fatalf("blind window or fallback absent: %+v", m)
				}
			}
		})
	}
}

func TestChurnDeterministicExceptMeasurements(t *testing.T) {
	strip := func(r ChurnRun) ChurnRun {
		r.Rebuilds = nil
		r.Metrics.UpdateNS = 0
		r.Metrics.PredictorNS = 0
		r.Metrics.FinalReconciliationNS = 0
		for i := range r.Rows {
			r.Rows[i].Result.Timings = StageTimings{}
		}
		return r
	}
	a, err := RunChurn(context.Background(), testChurnConfig())
	if err != nil {
		t.Fatal(err)
	}
	b, err := RunChurn(context.Background(), testChurnConfig())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(strip(a), strip(b)) {
		t.Fatal("seeded churn changed decisions/events/state")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := RunChurn(ctx, testChurnConfig()); err == nil {
		t.Fatal("cancelled replay ran")
	}
}

func TestChurnArtifactHashesAndOverwrite(t *testing.T) {
	run, err := RunChurn(context.Background(), testChurnConfig())
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(t.TempDir(), "run")
	if err := SaveChurn(dir, "../..", run); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest struct {
		Artifacts map[string]string `json:"artifacts_sha256"`
		Sources   map[string]string `json:"source_files_sha256"`
		Transport int64             `json:"transport_seed"`
	}
	if err := json.Unmarshal(b, &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.Transport != 5001 || manifest.Sources["internal/semanticcache/delayed_summary.go"] == "" || len(manifest.Artifacts) != 10 {
		t.Fatal("missing provenance")
	}
	for name, hash := range manifest.Artifacts {
		b, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil || hashBytes(b) != hash {
			t.Fatalf("artifact %s hash differs: %v", name, err)
		}
	}
	if err := SaveChurn(dir, "../..", run); err == nil {
		t.Fatal("existing output overwritten")
	}
	run.Steps[0].Mutation = "reset"
	if err := SaveChurn(filepath.Join(t.TempDir(), "bad"), "../..", run); err == nil {
		t.Fatal("mismatched workload accepted")
	}
}
