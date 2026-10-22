package semanticcache

import (
	"context"
	"fmt"
	"runtime"
	"time"
)

type RemoteRow struct {
	Request         Request      `json:"request"`
	Result          Result       `json:"result"`
	OracleHit       bool         `json:"oracle_hit"`
	BackendMismatch bool         `json:"backend_mismatch"`
	Calls           []RemoteCall `json:"http_calls"`
}

type RemoteRun struct {
	Router       string          `json:"router"`
	Collection   string          `json:"collection"`
	Rows         []RemoteRow     `json:"rows"`
	SetupCalls   []RemoteCall    `json:"setup_http_calls"`
	CleanupCalls []RemoteCall    `json:"cleanup_http_calls"`
	Projections  *Projections    `json:"projections,omitempty"`
	BuildNS      int64           `json:"measured_build_ns"`
	MemoryBytes  int64           `json:"structural_summary_bytes"`
	Rebuilds     []RemoteRebuild `json:"rebuilds"`
}

type RemoteRebuild struct {
	AfterRequest        int    `json:"after_request"`
	DurationNS          int64  `json:"measured_duration_ns"`
	PeakStructuralBytes int64  `json:"estimated_overlap_bytes"`
	SampledHeapBytes    uint64 `json:"sampled_process_heap_bytes"`
}

// RunRemoteDemo uses precomputed real MiniLM vectors on development requests.
// It never opens tuning or held-out data and does not change the frozen choice.
func RunRemoteDemo(ctx context.Context, endpoint string, repeats int) (runs []RemoteRun, err error) {
	return RunRemoteMeasured(ctx, endpoint, repeats, 0)
}

func RunRemoteMeasured(ctx context.Context, endpoint string, repeats, rebuildEvery int) (runs []RemoteRun, err error) {
	if repeats < 1 || repeats > 10000 {
		return nil, fmt.Errorf("repeats must be in [1,10000]")
	}
	if rebuildEvery < 0 {
		return nil, fmt.Errorf("negative rebuild interval")
	}
	identity := MiniLMIdentity()
	policy := PolicyConfig{ID: PolicyV1ID, CosineThreshold: PolicyV1CosineThreshold, TTLSeconds: PolicyV1TTLSeconds}
	for _, name := range []string{"always-search", "exploratory-lsh"} {
		embedder, e := DemoRecordedEmbedder()
		if e != nil {
			return nil, e
		}
		clock := &LogicalClock{Time: DemoRequests()[0].Timestamp}
		run := RemoteRun{Router: name, Collection: fmt.Sprintf("phase6_%d_%d", time.Now().UnixNano(), len(runs))}
		var summary *LSHSummary
		if name == "exploratory-lsh" {
			start := time.Now()
			p, e := NewProjections(LSHConfig{"cosine-hyperplanes-v1", identity, 1001, 2, 3})
			if e != nil {
				return nil, e
			}
			run.Projections = p
			summary, e = NewLSHSummary(p)
			if e != nil {
				return nil, e
			}
			run.BuildNS = time.Since(start).Nanoseconds()
		}
		cache, e := NewRemoteCache(ctx, endpoint, run.Collection, 3*time.Second, policy, identity, 100, clock, summary)
		if e != nil {
			return nil, e
		}
		run.SetupCalls = append([]RemoteCall(nil), cache.Calls...)
		run.BuildNS += cache.lastUpdateNS
		baseline := Baseline{Embedder: embedder, Cache: cache, Generator: FakeLLM{}, Clock: clock, PolicyID: policy.ID, Threshold: policy.CosineThreshold, Identity: identity, Predictor: summary}
		// Always clean up only the newly created collection, even on failure.
		serveErr := func() error {
			for pass := 0; pass < repeats; pass++ {
				for i, request := range DemoRequests() {
					request.ID = fmt.Sprintf("remote-%05d-%d", pass, i)
					request.Timestamp = DemoRequests()[0].Timestamp.Add(time.Duration(pass*4+i) * time.Second)
					clock.Time = request.Timestamp
					var oracle *Candidate
					before := len(cache.Calls)
					result := baseline.serve(ctx, request, func(v []float64) Decision {
						if summary == nil {
							return Decision{true, "always-search", "always-search"}
						}
						return cache.predict(request, v, identity)
					}, func(v []float64) error {
						var e error
						oracle, e = OracleSearch(ctx, policy, identity.Dimension, 100, cache.local.Snapshot(), request, v)
						return e
					})
					row := RemoteRow{Request: request, Result: result, OracleHit: oracle != nil, Calls: append([]RemoteCall(nil), cache.Calls[before:]...)}
					if result.SearchCount > 0 && result.Success {
						row.BackendMismatch = (oracle != nil) != (result.Source == "cache") || (oracle != nil && result.Source == "cache" && oracle.Entry.ID != result.MatchedEntryID)
					}
					run.Rows = append(run.Rows, row)
					if !result.Success || result.CacheWriteError != "" {
						return fmt.Errorf("remote serving failed: %s %s", result.Error, result.CacheWriteError)
					}
					if summary != nil && rebuildEvery > 0 && len(run.Rows)%rebuildEvery == 0 {
						start := time.Now()
						snapshot := cache.local.Snapshot()
						replacement, e := NewLSHSummary(run.Projections)
						if e != nil {
							return e
						}
						if e = replacement.Reset(identity, snapshot); e != nil {
							summary.Invalidate()
							return e
						}
						peak := summary.MemoryBytes() + replacement.MemoryBytes() + ReferenceStateBytes(snapshot)
						summary = replacement
						cache.summary = replacement
						baseline.Predictor = replacement
						duration := time.Since(start).Nanoseconds()
						var heap runtime.MemStats
						runtime.ReadMemStats(&heap)
						run.Rebuilds = append(run.Rebuilds, RemoteRebuild{len(run.Rows), duration, peak, heap.HeapAlloc})
					}
				}
			}
			return nil
		}()
		if summary != nil {
			run.MemoryBytes = summary.MemoryBytes()
		}
		before := len(cache.Calls)
		cleanCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		cleanErr := cache.call(cleanCtx, "DELETE", cache.path(""), nil, nil)
		cancel()
		run.CleanupCalls = append([]RemoteCall(nil), cache.Calls[before:]...)
		runs = append(runs, run)
		if serveErr != nil {
			return runs, serveErr
		}
		if cleanErr != nil {
			return runs, cleanErr
		}
	}
	return runs, nil
}
