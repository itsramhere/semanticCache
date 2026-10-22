package semanticcache

import (
	"context"
	"fmt"
	"io"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"time"
	"unsafe"
)

// FinalPlan is recorded before opening held-out requests. Diagnostic LSH uses
// the existing development configuration, never a selection on final outcomes.
type FinalPlan struct {
	ID              string          `json:"id"`
	CandidateSHA256 string          `json:"candidate_sha256"`
	Diagnostic      LSHConfig       `json:"diagnostic_lsh"`
	Scenarios       []FinalScenario `json:"scenarios"`
}

type FinalScenario struct {
	ID         string `json:"id"`
	Backend    string `json:"backend"`
	Mode       string `json:"mode"`
	Start      string `json:"start"`
	Capacity   int    `json:"capacity"`
	Stream     string `json:"stream"`
	Requests   int    `json:"requests"` // zero means the complete, unchanged final split
	Fill       bool   `json:"fill"`
	DelaySteps int    `json:"delay_steps"`
	Diagnostic bool   `json:"diagnostic"`
	GuardBytes int64  `json:"guard_bytes"` // a stress guard, never a replacement budget
}

func LoadFinalPlan(r io.Reader) (FinalPlan, error) {
	var p FinalPlan
	err := decodeStrict(r, &p)
	return p, err
}

func (p FinalPlan) Validate(f FinalCandidate) error {
	if err := f.Validate(); err != nil {
		return err
	}
	if p.ID == "" || len(p.CandidateSHA256) != 64 || len(p.Scenarios) == 0 || len(f.Experiment.WorkloadSeeds) < 3 || len(f.Experiment.ProjectionSeeds) < 3 {
		return fmt.Errorf("incomplete final plan or seed family")
	}
	if p.Diagnostic.Identity != f.Identity || p.Diagnostic.Version != "cosine-hyperplanes-v1" {
		return fmt.Errorf("diagnostic identity mismatch")
	}
	if _, err := NewProjections(p.Diagnostic); err != nil {
		return err
	}
	seen := map[string]bool{}
	for _, s := range p.Scenarios {
		if s.ID == "" || strings.ContainsAny(s.ID, `/\\.`) || seen[s.ID] || (s.Backend != "local" && s.Backend != "remote") || (s.Mode != "isolation" && s.Mode != "end-to-end") || (s.Start != "cold" && s.Start != "warm") || s.Capacity <= 0 || s.Capacity > 10000 || s.DelaySteps < 0 || s.DelaySteps > 100 || s.GuardBytes < 0 {
			return fmt.Errorf("invalid final scenario %q", s.ID)
		}
		seen[s.ID] = true
		switch s.Stream {
		case "original", "high-hit", "low-hit", "all-hit", "all-miss", "empty", "churn":
		default:
			return fmt.Errorf("invalid final stream")
		}
		if (s.Stream == "original" && s.Requests != 0) || (s.Stream != "original" && (s.Requests < 12 || s.Requests > 1000)) || (s.Stream == "all-hit" && s.Start != "warm") || (s.Stream == "empty" && s.Start != "cold") || (s.Mode == "isolation" && (s.DelaySteps > 0 || s.Fill || s.Diagnostic)) {
			return fmt.Errorf("inconsistent final scenario %q", s.ID)
		}
	}
	return nil
}

// FinalWorkload keeps the original split untouched. Separately identified
// stresses retain held-out answer groups; unique scopes force known misses.
func FinalWorkload(w Workload, s FinalScenario) (Workload, error) {
	if w.Split != "final" {
		return w, fmt.Errorf("final evaluation requires the final split")
	}
	if err := ValidateWorkloads([]Workload{w}); err != nil {
		return w, err
	}
	if s.Stream == "original" {
		return w, nil
	}
	pool := w.Requests
	if s.Stream == "high-hit" || s.Stream == "all-hit" {
		pool = nil
		for _, r := range w.Requests {
			if r.Category == "exact_repeat" {
				pool = append(pool, r)
			}
		}
	}
	if len(pool) == 0 || s.Requests < 1 {
		return w, fmt.Errorf("empty stress stream")
	}
	w.Requests = make([]LabeledRequest, s.Requests)
	base := pool[0].Request.Timestamp
	for i := range w.Requests {
		r := pool[i%len(pool)]
		r.Request.ID = fmt.Sprintf("%s-%d-%04d", s.ID, w.Seed, i)
		r.Request.Timestamp = base.Add(time.Duration(i) * time.Second)
		if s.Stream == "all-miss" || s.Stream == "empty" || (s.Stream == "low-hit" && i%10 != 0) {
			r.Request.Context.TenantID = fmt.Sprintf("final-stress-tenant-%d", i)
			r.Request.CacheEligible, r.Reusable = true, true
			r.Category = "scope_conflict"
		}
		w.Requests[i] = r
	}
	return w, ValidateWorkloads([]Workload{w})
}

type FinalRow struct {
	Evidence          ReplayEvidence   `json:"evidence"`
	Calls             []RemoteCall     `json:"http_calls,omitempty"`
	Events            []UpdateEvent    `json:"published_events,omitempty"`
	Freshness         *FreshnessStatus `json:"pre_decision_freshness,omitempty"`
	SourceWatermark   uint64           `json:"source_watermark"`
	ExternalMutations int              `json:"external_mutations"`
	BackendMismatch   bool             `json:"backend_mismatch"`
	UnseenHit         bool             `json:"unseen_upsert_missed_hit"`
	IntrinsicMiss     bool             `json:"intrinsic_lsh_missed_hit"`
	StaleSearch       bool             `json:"stale_removal_search"`
}

type FinalStats struct {
	Metrics           Metrics      `json:"metrics"`
	Stages            StageTimings `json:"measured_stage_totals"`
	MixedP50MS        Metric       `json:"mixed_end_to_end_p50_ms"`
	MixedP95MS        Metric       `json:"mixed_end_to_end_p95_ms"`
	HTTPAttempts      int          `json:"serving_http_attempts"`
	HTTPFailures      int          `json:"serving_http_failures"`
	BackendMismatches int          `json:"backend_mismatches"`
	ExternalMutations int          `json:"external_mutations"`
	Fallbacks         int          `json:"fallback_requests"`
	UnseenHits        int          `json:"unseen_upsert_missed_hits"`
	IntrinsicMisses   int          `json:"intrinsic_lsh_missed_hits"`
	StaleSearches     int          `json:"stale_removal_searches"`
	RecoveryNS        int64        `json:"measured_final_recovery_ns"`
	PeakRebuildBytes  int64        `json:"structural_rebuild_overlap_bytes"`
	SampledHeapBytes  uint64       `json:"sampled_process_heap_peak_bytes"`
	ProcessBytes      int64        `json:"sampled_process_working_set_bytes"`
	FinalAgreement    bool         `json:"final_summary_agreement"`
	RetainedLSHBytes  int64        `json:"measured_independent_lsh_retained_heap_bytes"`
}

type FinalRun struct {
	Role              string                    `json:"role"` // baseline, frozen-candidate, diagnostic-lsh
	Scenario          FinalScenario             `json:"scenario"`
	WorkloadSeed      int64                     `json:"workload_seed"`
	ProjectionSeed    int64                     `json:"projection_seed"` // zero for deterministic routers
	PredictorID       string                    `json:"predictor_id"`
	Projections       *Projections              `json:"projections,omitempty"`
	BuildNS           int64                     `json:"measured_build_ns"`
	RecoveryNS        int64                     `json:"measured_final_recovery_ns"`
	SummaryBytes      int64                     `json:"peak_structural_summary_bytes"`
	SampledHeapBytes  uint64                    `json:"sampled_process_heap_peak_bytes"`
	ProcessBytes      int64                     `json:"sampled_process_working_set_bytes"`
	Initial           CacheSnapshot             `json:"initial"`
	Final             CacheSnapshot             `json:"final"`
	SetupCalls        []RemoteCall              `json:"setup_http_calls,omitempty"`
	CleanupCalls      []RemoteCall              `json:"cleanup_http_calls,omitempty"`
	Rebuilds          []RebuildMeasurement      `json:"rebuilds,omitempty"`
	Rows              []FinalRow                `json:"-"`
	Stats             FinalStats                `json:"stats"`
	Agreement         bool                      `json:"verified_final_summary_agreement"`
	MemoryMeasurement *SummaryMemoryMeasurement `json:"independent_lsh_memory_measurement,omitempty"`
}

// finalCache controls only collections created for this run. Its ledger is used
// by diagnostics and mutation bookkeeping; serving retrieval still uses HTTP.
type finalCache struct {
	Cache
	local  *ReferenceCache
	remote *RemoteCache
}

func (c *finalCache) state() CacheSnapshot {
	if c.remote != nil {
		return c.remote.local.Snapshot()
	}
	return c.local.Snapshot()
}
func (c *finalCache) remove(ctx context.Context, clear bool, id string) error {
	if c.remote == nil {
		if clear {
			return c.local.Clear()
		}
		return c.local.Delete(id)
	}
	state := c.state()
	var ids []string
	for _, e := range state.Entries {
		if clear || e.Entry.ID == id {
			ids = append(ids, remoteID(e.Entry.ID))
		}
	}
	if len(ids) > 0 {
		if err := c.remote.call(ctx, "POST", c.remote.path("/points/delete?wait=true"), map[string]any{"points": ids}, nil); err != nil {
			return c.remote.poison(err)
		}
	}
	if clear {
		return c.remote.local.Clear()
	}
	return c.remote.local.Delete(id)
}

type finalDelivery struct {
	Due   int
	Event UpdateEvent
}

func finalJournalBytes(j *UpdateJournal) int64 {
	state := UpdateSnapshot{j.identity, j.streamID, j.sequence, j.versions, j.state}
	return snapshotMemoryBytes(state) + int64(j.peakVersions-len(j.versions))*112 + int64(unsafe.Sizeof(*j))
}

// RunFinalCondition uses the same serving path and exact policy as earlier
// phases. Fault transport and oracle work never query a backend during Predict.
func RunFinalCondition(ctx context.Context, f FinalCandidate, s FinalScenario, w Workload, role string, projection *LSHConfig, endpoint string) (run FinalRun, err error) {
	run = FinalRun{Role: role, Scenario: s, WorkloadSeed: w.Seed, PredictorID: "always-search"}
	if err = f.Validate(); err != nil {
		return run, err
	}
	if w.Identity != f.Identity || w.Split != "final" {
		return run, fmt.Errorf("frozen identity or split mismatch")
	}
	if role != "baseline" && role != "frozen-candidate" && role != "diagnostic-lsh" {
		return run, fmt.Errorf("invalid final role")
	}
	if role == "baseline" && projection != nil || role == "frozen-candidate" && (f.Router == "lsh") != (projection != nil) || role == "diagnostic-lsh" && projection == nil {
		return run, fmt.Errorf("router differs from frozen role")
	}
	if role == "frozen-candidate" && projection != nil {
		found := false
		for _, p := range f.Projections {
			if p == *projection {
				found = true
			}
		}
		if !found {
			return run, fmt.Errorf("unfrozen projection")
		}
	}
	if err = ValidateWorkloads([]Workload{w}); err != nil {
		return run, err
	}
	clock := &LogicalClock{Time: w.Requests[0].Request.Timestamp}
	if s.Start == "warm" && len(w.Warm) > 0 {
		clock.Time = w.Warm[0].Entry.InsertedAt
	}
	cache := &finalCache{}
	if s.Backend == "remote" {
		name := fmt.Sprintf("phase7_%d", time.Now().UnixNano())
		cache.remote, err = NewRemoteCache(ctx, endpoint, name, 3*time.Second, f.Experiment.Policy, w.Identity, s.Capacity, clock, nil)
		if err != nil {
			return run, err
		}
		cache.Cache = cache.remote
		defer func() {
			before := len(cache.remote.Calls)
			cleanup, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			cleanErr := cache.remote.call(cleanup, "DELETE", cache.remote.path(""), nil, nil)
			run.CleanupCalls = append([]RemoteCall(nil), cache.remote.Calls[before:]...)
			if err == nil {
				err = cleanErr
			}
		}()
	} else if s.Backend == "local" {
		cache.local, err = NewReferenceCache(f.Experiment.Policy, w.Identity.Dimension, s.Capacity, clock)
		if err != nil {
			return run, err
		}
		cache.Cache = cache.local
	} else {
		return run, fmt.Errorf("invalid backend")
	}
	labels := map[string]answerLabel{}
	if s.Start == "warm" {
		for _, e := range w.Warm {
			if err = cache.Put(ctx, e.Entry); err != nil {
				return run, err
			}
			labels[e.Entry.ID] = answerLabel{e.AnswerClass, e.Entry.Scope, true}
		}
	}
	if s.Fill {
		for i := len(cache.state().Entries); i < s.Capacity; i++ {
			e := cloneEntry(w.Warm[i%len(w.Warm)].Entry)
			e.ID = fmt.Sprintf("filler-%04d", i)
			e.Scope.Context.TenantID = e.ID
			if err = cache.Put(ctx, e); err != nil {
				return run, err
			}
		}
	}
	run.Initial = cache.state()
	if cache.remote != nil {
		run.SetupCalls = append([]RemoteCall(nil), cache.remote.Calls...)
	}
	clock.Time = w.Requests[0].Request.Timestamp
	var delayed *DelayedSummary
	var journal *UpdateJournal
	var pending []finalDelivery
	if projection != nil {
		start := time.Now()
		if projection.Identity != w.Identity {
			return run, fmt.Errorf("projection identity mismatch")
		}
		run.ProjectionSeed = projection.Seed
		run.Projections, err = NewProjections(*projection)
		if err != nil {
			return run, err
		}
		run.PredictorID = run.Projections.ID()
		journal, err = NewUpdateJournal("final-stream", w.Identity, run.Initial)
		if err != nil {
			return run, err
		}
		delayed, err = NewDelayedSummary(run.Projections, journal.Snapshot(), clock.Time, 3*time.Second)
		if err != nil {
			return run, err
		}
		run.BuildNS = time.Since(start).Nanoseconds()
	}
	memory := func() {
		if delayed != nil {
			n := delayed.MemoryBytes() + finalJournalBytes(journal) + int64(cap(pending))*int64(unsafe.Sizeof(finalDelivery{}))
			for _, q := range pending {
				n += eventMemoryBytes(q.Event)
			}
			if n > run.SummaryBytes {
				run.SummaryBytes = n
			}
		}
	}
	capture := func(i int) []UpdateEvent {
		if delayed == nil {
			return nil
		}
		events := journal.Capture(cache.state())
		for _, e := range events {
			pending = append(pending, finalDelivery{i + s.DelaySteps, e})
		}
		memory()
		return events
	}
	deliver := func(i int) error {
		if delayed == nil {
			return nil
		}
		var remaining []finalDelivery
		for _, q := range pending {
			if q.Due <= i {
				if e := delayed.Receive(q.Event, clock.Time); e != nil {
					return e
				}
			} else {
				remaining = append(remaining, q)
			}
		}
		pending = remaining
		if s.DelaySteps == 0 || i%4 == 0 {
			return delayed.ObserveWatermark(journal.sequence, clock.Time)
		}
		return nil
	}
	rebuild := func() error {
		if delayed == nil {
			return nil
		}
		if e := delayed.BeginRebuild(journal.Snapshot()); e != nil {
			return e
		}
		m, e := delayed.FinishRebuild(nil, journal.sequence, clock.Time)
		if e != nil {
			return e
		}
		// Include the reliable source journal and transport alongside overlap.
		m.PeakOwnedBytes += finalJournalBytes(journal)
		for _, q := range pending {
			m.PeakOwnedBytes += eventMemoryBytes(q.Event)
		}
		run.Rebuilds = append(run.Rebuilds, m)
		pending = nil
		return nil
	}
	answerEmbedder := fixtureEmbedder{w.Identity, map[string][]float64{}}
	for _, r := range w.Requests {
		answerEmbedder.vectors[r.Request.Text] = r.Vector
	}
	serving := Baseline{Embedder: answerEmbedder, Cache: cache.Cache, Generator: FakeLLM{}, Clock: clock, PolicyID: f.Experiment.Policy.ID, Threshold: f.Experiment.Policy.CosineThreshold, Identity: w.Identity}
	memory()
	for i, labeled := range w.Requests {
		if err = ctx.Err(); err != nil {
			return run, err
		}
		r := labeled.Request
		clock.Time = r.Timestamp
		row := FinalRow{Evidence: ReplayEvidence{Index: i, RequestID: r.ID, GroupID: labeled.GroupID, Category: labeled.Category, Eligible: r.CacheEligible}}
		beforeHTTP := 0
		if cache.remote != nil {
			beforeHTTP = len(cache.remote.Calls)
		}
		maintenance := time.Now()
		if s.Stream == "empty" || (s.Stream == "churn" && i%12 == 9) {
			if err = cache.remove(ctx, true, ""); err != nil {
				return run, err
			}
			row.ExternalMutations++
		} else if s.Stream == "churn" {
			switch i % 12 {
			case 0, 3:
				v := append([]float64(nil), labeled.Vector...)
				if i%12 == 3 {
					for j := range v {
						v[j] = -v[j]
					}
				}
				e := Entry{ID: "external-final", Embedding: v, Answer: "simulated external answer", Scope: r.Scope(), Validity: ValidityMetadata{Reusable: true, PolicyVersion: f.Experiment.Policy.ID}}
				if err = cache.Put(ctx, e); err != nil {
					return run, err
				}
				row.ExternalMutations++
				labels[e.ID] = answerLabel{labeled.AnswerClass, r.Scope(), labeled.Reusable}
			case 6:
				if err = cache.remove(ctx, false, "external-final"); err != nil {
					return run, err
				}
				row.ExternalMutations++
			}
		}
		row.Events = append(row.Events, capture(i)...)
		if err = deliver(i); err != nil {
			return run, err
		}
		if delayed != nil && i > 0 && i%16 == 0 {
			if err = rebuild(); err != nil {
				return run, err
			}
		}
		preNS := time.Since(maintenance).Nanoseconds()
		state := cache.state() // oracle snapshot, excluded from serving
		row.Evidence.CacheStateSequence = state.Sequence
		if delayed != nil {
			x := delayed.Status(clock.Time)
			row.Freshness = &x
			row.SourceWatermark = journal.sequence
		}
		freshSearch := true
		row.Evidence.Result = serving.serve(ctx, r, func(v []float64) Decision {
			d := Decision{true, "always-search", "always-search"}
			if delayed != nil {
				limit := f.Experiment.MaxSummaryBytes
				if s.GuardBytes > 0 && s.GuardBytes < limit {
					limit = s.GuardBytes
				}
				if run.SummaryBytes > limit {
					d = Decision{true, "memory-limit-fallback", run.PredictorID}
				} else {
					d = delayed.Predict(r, v)
				}
			}
			row.Evidence.Decision = d
			if s.Mode == "isolation" {
				return Decision{true, "authoritative-always-search", "always-search"}
			}
			return d
		}, func(v []float64) error {
			if !r.CacheEligible {
				return nil
			}
			o, e := OracleSearch(ctx, f.Experiment.Policy, w.Identity.Dimension, s.Capacity, state, r, v)
			if e != nil {
				return e
			}
			hit := o != nil
			row.Evidence.OracleHit = &hit
			if hit {
				row.Evidence.OracleEntryID = o.Entry.ID
			}
			if delayed != nil {
				fresh, e := NewLSHSummary(run.Projections)
				if e != nil {
					return e
				}
				if e = fresh.Reset(w.Identity, state); e != nil {
					return e
				}
				freshSearch = fresh.Predict(r, v).Search
			}
			return nil
		})
		if !r.CacheEligible {
			row.Evidence.Decision = Decision{false, "ineligible", run.PredictorID}
		}
		result := &row.Evidence.Result
		if result.Source == "cache" {
			label, ok := labels[result.MatchedEntryID]
			correct := ok && labeled.Reusable && label.Reusable && label.Class == labeled.AnswerClass && label.Scope == r.Scope()
			row.Evidence.AnswerReuseCorrect = &correct
		}
		if result.WriteCount > 0 {
			labels[r.ID] = answerLabel{labeled.AnswerClass, r.Scope(), labeled.Reusable}
		}
		if r.CacheEligible && result.SearchCount > 0 && result.Success {
			row.BackendMismatch = (result.Source == "cache") != *row.Evidence.OracleHit || (result.Source == "cache" && result.MatchedEntryID != row.Evidence.OracleEntryID)
		}
		if row.Evidence.OracleHit != nil && delayed != nil {
			row.UnseenHit = *row.Evidence.OracleHit && !row.Evidence.Decision.Search && freshSearch
			row.IntrinsicMiss = *row.Evidence.OracleHit && !row.Evidence.Decision.Search && !freshSearch
			row.StaleSearch = !*row.Evidence.OracleHit && row.Evidence.Decision.Search && !freshSearch && row.Freshness.Ready
		}
		maintenance = time.Now()
		row.Events = append(row.Events, capture(i)...)
		if s.DelaySteps == 0 {
			if err = deliver(i); err != nil {
				return run, err
			}
		}
		updateNS := preNS + time.Since(maintenance).Nanoseconds()
		result.Timings.UpdateNS += updateNS
		result.Timings.TotalNS += updateNS
		if cache.remote != nil {
			row.Calls = append([]RemoteCall(nil), cache.remote.Calls[beforeHTTP:]...)
		}
		memory()
		if i%32 == 0 || i == len(w.Requests)-1 {
			var heap runtime.MemStats
			runtime.ReadMemStats(&heap)
			if heap.HeapAlloc > run.SampledHeapBytes {
				run.SampledHeapBytes = heap.HeapAlloc
			}
			working, e := processWorkingSet()
			if e != nil {
				return run, e
			}
			if working > run.ProcessBytes {
				run.ProcessBytes = working
			}
		}
		run.Rows = append(run.Rows, row)
		if !result.Success || result.CacheWriteError != "" {
			return run, fmt.Errorf("final serving failed for %s: %s %s", r.ID, result.Error, result.CacheWriteError)
		}
	}
	if delayed != nil {
		start := time.Now()
		if err = rebuild(); err != nil {
			return run, err
		}
		run.RecoveryNS = time.Since(start).Nanoseconds()
	}
	run.Final = cache.state()
	run.Agreement = true
	if delayed != nil {
		fresh, e := NewLSHSummary(run.Projections)
		if e != nil {
			return run, e
		}
		if e = fresh.Reset(w.Identity, run.Final); e != nil {
			return run, e
		}
		run.Agreement = reflect.DeepEqual(fresh.buckets, delayed.active.Summary.buckets) && reflect.DeepEqual(fresh.entries, delayed.active.Summary.entries) && reflect.DeepEqual(delayed.active.versions, journal.versions) && delayed.active.LastApplied == journal.sequence
		if !run.Agreement {
			return run, fmt.Errorf("final summary differs from authoritative rebuild")
		}
	}
	run.Stats, err = SummarizeFinal(f.Experiment, run)
	return run, err
}

func SummarizeFinal(c Config, run FinalRun) (FinalStats, error) {
	m := FinalStats{RecoveryNS: run.RecoveryNS, SampledHeapBytes: run.SampledHeapBytes, ProcessBytes: run.ProcessBytes, FinalAgreement: run.Agreement}
	if run.MemoryMeasurement != nil {
		m.RetainedLSHBytes = run.MemoryMeasurement.RetainedHeapDelta
	}
	if run.BuildNS < 0 || run.RecoveryNS < 0 || len(run.Rows) == 0 {
		return m, fmt.Errorf("empty final run or negative initialization time")
	}
	r := ReplayRun{PredictorID: run.PredictorID, Options: ReplayOptions{Mode: run.Scenario.Mode}, BuildNS: run.BuildNS + run.RecoveryNS, MemoryBytes: run.SummaryBytes}
	var samples []float64
	var httpNS int64
	for _, row := range run.Rows {
		t := row.Evidence.Result.Timings
		remaining := t.TotalNS
		if remaining < 0 {
			return m, fmt.Errorf("negative request duration")
		}
		for _, duration := range []int64{t.EmbeddingNS, t.SearchNS, t.ValidationNS, t.GenerationNS, t.CacheWriteNS, t.PredictorNS, t.UpdateNS} {
			if duration < 0 || duration > remaining {
				return m, fmt.Errorf("stage durations exceed request boundary: %s", row.Evidence.Result.RequestID)
			}
			remaining -= duration
		}
		if row.ExternalMutations < 0 {
			return m, fmt.Errorf("negative external mutation count")
		}
		queries := 0
		r.Rows = append(r.Rows, row.Evidence)
		if row.Evidence.OracleHit != nil {
			r.DiagnosticSearches++
		}
		if row.BackendMismatch {
			m.BackendMismatches++
		}
		if row.UnseenHit {
			m.UnseenHits++
		}
		if row.IntrinsicMiss {
			m.IntrinsicMisses++
		}
		if row.StaleSearch {
			m.StaleSearches++
		}
		if strings.Contains(row.Evidence.Decision.Reason, "fallback") {
			m.Fallbacks++
		}
		m.ExternalMutations += row.ExternalMutations
		var requestHTTPNS int64
		for _, call := range row.Calls {
			if strings.HasSuffix(call.Path, "/points/query") {
				queries++
			}
			if call.DurationNS < 0 {
				return m, fmt.Errorf("negative HTTP duration")
			}
			if call.DurationNS > t.TotalNS-requestHTTPNS {
				return m, fmt.Errorf("HTTP durations exceed request boundary: %s", row.Evidence.Result.RequestID)
			}
			requestHTTPNS += call.DurationNS
			m.HTTPAttempts++
			httpNS += call.DurationNS
			if call.Error != "" {
				m.HTTPFailures++
			}
		}
		if run.Scenario.Backend == "remote" && queries != row.Evidence.Result.SearchCount {
			return m, fmt.Errorf("HTTP query and production search counts differ")
		}
		m.Stages.EmbeddingNS += t.EmbeddingNS
		m.Stages.SearchNS += t.SearchNS
		m.Stages.ValidationNS += t.ValidationNS
		m.Stages.GenerationNS += t.GenerationNS
		m.Stages.CacheWriteNS += t.CacheWriteNS
		m.Stages.PredictorNS += t.PredictorNS
		m.Stages.UpdateNS += t.UpdateNS
		m.Stages.TotalNS += t.TotalNS
		x := row.Evidence.Result
		estimate := float64(t.TotalNS)/1e6 + float64(run.BuildNS+run.RecoveryNS)/1e6/float64(len(run.Rows)) + float64(x.EmbeddingCount)*c.Assumptions["embedding"].LatencyMS + float64(x.GenerationCount)*c.Assumptions["generation"].LatencyMS
		if run.Scenario.Backend == "local" {
			estimate += float64(x.SearchCount)*c.Assumptions["search"].LatencyMS + float64(x.WriteCount+row.ExternalMutations)*c.Assumptions["cache_write"].LatencyMS
		}
		samples = append(samples, estimate)
	}
	var err error
	m.Metrics, err = Summarize(c, r)
	if err != nil {
		return m, err
	}
	m.MixedP50MS, m.MixedP95MS = quantile(samples, .5), quantile(samples, .95)
	localNS := m.Stages.TotalNS + run.BuildNS + run.RecoveryNS - httpNS
	if localNS < 0 {
		return m, fmt.Errorf("HTTP time exceeds measured serving boundary")
	}
	m.Metrics.CostComponentsUSD["local_cpu_ms"] = float64(localNS) / 1e6 * c.Assumptions["local_cpu_ms"].CostUSD
	m.Metrics.CostComponentsUSD["cache_write"] += float64(m.ExternalMutations) * c.Assumptions["cache_write"].CostUSD
	m.Metrics.SimulatedCostUSD = 0
	for _, key := range []string{"embedding", "search", "generation", "cache_write", "local_cpu_ms"} {
		m.Metrics.SimulatedCostUSD += m.Metrics.CostComponentsUSD[key]
	}
	m.Metrics.CostBoundary = "simulated fixed per-logical-operation prices and local wall CPU proxy after subtracting measured HTTP; external churn mutations and build/recovery amortized; warm setup and collection cleanup excluded and logged; precomputed simulated vectors and fake generation; no added network delay"
	for _, rebuild := range run.Rebuilds {
		if rebuild.PeakOwnedBytes > m.PeakRebuildBytes {
			m.PeakRebuildBytes = rebuild.PeakOwnedBytes
		}
		if rebuild.SampledHeapPeakBytes > m.SampledHeapBytes {
			m.SampledHeapBytes = rebuild.SampledHeapPeakBytes
		}
	}
	return m, nil
}

func FinalViolations(c Config, baseline, candidate FinalStats) []string {
	var v []string
	m := candidate.Metrics
	if m.CacheHitRecall.Defined && m.CacheHitRecall.Value < c.MinHitRecall {
		v = append(v, "recall")
	}
	if candidate.MixedP95MS.Defined && candidate.MixedP95MS.Value > c.MaxP95LatencyMS {
		v = append(v, "absolute-p95")
	}
	if candidate.MixedP95MS.Defined && baseline.MixedP95MS.Defined && candidate.MixedP95MS.Value > baseline.MixedP95MS.Value {
		v = append(v, "paired-p95")
	}
	if m.SimulatedCostUSD >= baseline.Metrics.SimulatedCostUSD {
		v = append(v, "paired-cost")
	}
	if m.MemoryBytes > c.MaxSummaryBytes || candidate.RetainedLSHBytes > c.MaxSummaryBytes {
		v = append(v, "summary-memory")
	}
	if m.Failures+m.WriteFailures+candidate.HTTPFailures+candidate.BackendMismatches > 0 || !candidate.FinalAgreement {
		v = append(v, "failures")
	}
	sort.Strings(v)
	return v
}
