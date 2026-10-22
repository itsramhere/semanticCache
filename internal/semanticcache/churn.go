package semanticcache

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"time"
)

// ChurnConfig describes a separate controlled lifecycle experiment. It does not
// change the frozen candidate, matching threshold, or final evaluation inputs.
type ChurnConfig struct {
	ID                  string      `json:"id"`
	WorkloadSeed        int64       `json:"workload_seed"`
	ProjectionSeed      int64       `json:"projection_seed"`
	Steps               int         `json:"steps"`
	StepMS              int         `json:"step_ms"`
	Capacity            int         `json:"capacity"`
	TTLSeconds          int         `json:"ttl_seconds"`
	FreshnessLimitMS    int         `json:"freshness_limit_ms"`
	HeartbeatEverySteps int         `json:"heartbeat_every_steps"`
	RebuildEverySteps   int         `json:"rebuild_every_steps"`
	Faults              FaultConfig `json:"faults"`
}

func (c ChurnConfig) Validate() error {
	if c.ID == "" || c.Steps < 12 || c.Steps > 10000 || c.StepMS <= 0 || c.StepMS > 86400000 || c.Capacity <= 0 || c.Capacity > 10000 || c.TTLSeconds <= 0 || int64(c.TTLSeconds) > maxTTLSeconds || c.FreshnessLimitMS <= 0 || c.FreshnessLimitMS > 86400000 || c.HeartbeatEverySteps < 0 || c.RebuildEverySteps < 2 {
		return fmt.Errorf("invalid churn configuration")
	}
	return c.Faults.Validate()
}

type ChurnStep struct {
	Index          int       `json:"index"`
	Time           time.Time `json:"time"`
	Mutation       string    `json:"mutation"`
	EntryID        string    `json:"entry_id,omitempty"`
	MutationVector []float64 `json:"mutation_vector,omitempty"`
	Request        Request   `json:"request"`
	QueryVector    []float64 `json:"query_vector"`
}

type ChurnRow struct {
	Step                  int             `json:"step"`
	SourceWatermark       uint64          `json:"source_watermark"`
	Status                FreshnessStatus `json:"pre_decision_summary"`
	OracleHit             bool            `json:"oracle_hit"`
	OracleEntryID         string          `json:"oracle_entry_id,omitempty"`
	FreshDecision         Decision        `json:"fresh_summary_decision"`
	Result                Result          `json:"result"`
	UnseenUpsertMiss      bool            `json:"unseen_upsert_missed_hit"`
	StaleRemovalSearch    bool            `json:"stale_removal_wasted_search"`
	IntrinsicLSHMiss      bool            `json:"intrinsic_lsh_missed_hit"`
	DiagnosticUnapplied   uint64          `json:"diagnostic_actual_unapplied_events"`
	DiagnosticOldestAgeMS *int64          `json:"diagnostic_actual_oldest_unapplied_age_ms"`
	ScheduledPending      int             `json:"diagnostic_transport_pending_deliveries"`
}

type ChurnEventLog struct {
	Step      int              `json:"step"`
	Action    string           `json:"action"`
	Update    *ScheduledUpdate `json:"update,omitempty"`
	Watermark uint64           `json:"watermark"`
	Status    FreshnessStatus  `json:"summary"`
	Error     string           `json:"error,omitempty"`
}

type ChurnMetrics struct {
	Requests              int       `json:"requests"`
	Confusion             Confusion `json:"confusion"`
	UnseenUpsertMisses    int       `json:"unseen_upsert_missed_hits"`
	StaleRemovalSearches  int       `json:"stale_removal_wasted_searches"`
	IntrinsicLSHMisses    int       `json:"intrinsic_lsh_missed_hits"`
	FallbackRequests      int       `json:"fallback_requests"`
	FallbackMissSearches  int       `json:"fallback_miss_searches"`
	Searches              int       `json:"searches"`
	Generations           int       `json:"simulated_fake_llm_calls"`
	UpdateNS              int64     `json:"measured_update_ns"`
	PredictorNS           int64     `json:"measured_predictor_ns"`
	FinalReconciliationNS int64     `json:"measured_final_reconciliation_ns"`
	PeakSummaryBytes      int64     `json:"estimated_peak_summary_bytes"`
	PeakBacklog           uint64    `json:"diagnostic_peak_unapplied_events"`
	DroppedEvents         int       `json:"injected_dropped_events"`
	DuplicateDeliveries   int       `json:"injected_duplicate_deliveries"`
	ReorderedDeliveries   int       `json:"observed_out_of_order_deliveries"`
	FinalAgreement        bool      `json:"final_rebuild_agreement"`
}

type ChurnRun struct {
	Config      ChurnConfig          `json:"config"`
	Projections *Projections         `json:"projections"`
	Initial     UpdateSnapshot       `json:"initial"`
	Steps       []ChurnStep          `json:"steps"`
	Rows        []ChurnRow           `json:"rows"`
	Events      []ChurnEventLog      `json:"events"`
	Rebuilds    []RebuildMeasurement `json:"rebuilds"`
	Metrics     ChurnMetrics         `json:"metrics"`
	Final       UpdateSnapshot       `json:"final"`
}

func BuildChurnSteps(c ChurnConfig) ([]ChurnStep, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	rng := rand.New(rand.NewSource(c.WorkloadSeed))
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	scope := Scope{"controlled-churn-qa", RequestContext{"tenant-a", "en", "v1", "v1"}, "fake-llm-v1"}
	steps := make([]ChurnStep, c.Steps)
	for i := range steps {
		at := start.Add(time.Duration(i) * time.Duration(c.StepMS) * time.Millisecond)
		step := ChurnStep{Index: i, Time: at, Mutation: "none", QueryVector: []float64{1, 0}}
		switch i % 12 {
		case 0:
			step.Mutation = "put"
			step.EntryID = "external-a"
			step.MutationVector = []float64{1, 0}
		case 2:
			step.Mutation = "delete"
			step.EntryID = "external-a"
		case 3:
			step.Mutation = "delete"
			step.EntryID = "external-a" // repeated deletion
		case 4:
			step.Mutation = "put"
			step.EntryID = "external-b"
			step.MutationVector = []float64{-1, 0}
		case 5:
			step.Mutation = "put"
			step.EntryID = "external-b"
			step.MutationVector = []float64{1, 0}
		case 6:
			step.Mutation = "revoke"
			step.EntryID = "external-b"
		case 7:
			step.Mutation = "evict"
		case 8:
			step.Mutation = "reset"
		case 9:
			step.Mutation = "put"
			step.EntryID = "external-ttl"
			step.MutationVector = []float64{1, 0}
		}
		if i%12 >= 4 && rng.Intn(2) == 0 {
			step.QueryVector = []float64{-1, 0}
		}
		text := "positive controlled fixture"
		if step.QueryVector[0] < 0 {
			text = "negative controlled fixture"
		}
		step.Request = Request{ID: fmt.Sprintf("churn-%04d", i), Text: text, Namespace: scope.Namespace, Context: scope.Context, ModelVersion: scope.ModelVersion, Timestamp: at, CacheEligible: true}
		steps[i] = step
	}
	return steps, nil
}

// RunChurn uses the real serving/validation/generation path with controlled,
// precomputed 2D vectors. The oracle and a fresh-summary counterfactual are
// diagnostics only. Faults change transport, never the authoritative cache.
func RunChurn(ctx context.Context, config ChurnConfig) (ChurnRun, error) {
	run := ChurnRun{Config: config}
	steps, err := BuildChurnSteps(config)
	if err != nil {
		return run, err
	}
	run.Steps = steps
	identity := EmbeddingIdentity{"simulated-churn-2d", "v1", 2, PreprocessingV1}
	p, err := NewProjections(LSHConfig{"cosine-hyperplanes-v1", identity, config.ProjectionSeed, 2, 4})
	if err != nil {
		return run, err
	}
	run.Projections = p
	clock := &LogicalClock{Time: steps[0].Time}
	policy := PolicyConfig{ID: PolicyV1ID, CosineThreshold: .9, TTLSeconds: config.TTLSeconds}
	cache, err := NewReferenceCache(policy, 2, config.Capacity, clock)
	if err != nil {
		return run, err
	}
	j, err := NewUpdateJournal(config.ID, identity, cache.Snapshot())
	if err != nil {
		return run, err
	}
	run.Initial = j.Snapshot()
	d, err := NewDelayedSummary(p, run.Initial, clock.Time, time.Duration(config.FreshnessLimitMS)*time.Millisecond)
	if err != nil {
		return run, err
	}
	scheduler, err := NewEventScheduler(config.Faults)
	if err != nil {
		return run, err
	}
	emb := fixtureEmbedder{identity, map[string][]float64{"positive controlled fixture": {1, 0}, "negative controlled fixture": {-1, 0}}}
	baseline := Baseline{Embedder: emb, Cache: cache, Generator: FakeLLM{}, Clock: clock, PolicyID: policy.ID, Threshold: policy.CosineThreshold, Identity: identity}
	var history []UpdateEvent
	schedule := func(step int, events []UpdateEvent) {
		history = append(history, events...)
		if backlog := j.sequence - d.active.LastApplied; backlog > run.Metrics.PeakBacklog {
			run.Metrics.PeakBacklog = backlog
		}
		for _, e := range events {
			for _, x := range scheduler.Schedule(e) {
				action := "scheduled"
				if x.Dropped {
					action = "dropped"
					run.Metrics.DroppedEvents++
				}
				if x.Duplicate {
					run.Metrics.DuplicateDeliveries++
				}
				run.Events = append(run.Events, ChurnEventLog{Step: step, Action: action, Update: &x, Watermark: j.sequence, Status: d.Status(clock.Time)})
			}
		}
	}
	lastDelivered := uint64(0)
	deliver := func(step int) {
		for _, x := range scheduler.Due(clock.Time) {
			if x.Event.Sequence < lastDelivered {
				run.Metrics.ReorderedDeliveries++
			}
			if x.Event.Sequence > lastDelivered {
				lastDelivered = x.Event.Sequence
			}
			err := d.Receive(x.Event, clock.Time)
			row := ChurnEventLog{Step: step, Action: "delivered", Update: &x, Watermark: j.sequence, Status: d.Status(clock.Time)}
			if err != nil {
				row.Error = err.Error()
			}
			run.Events = append(run.Events, row)
		}
	}
	finish := func(step int) error {
		first := 0
		for first < len(history) && history[first].Sequence <= d.build.measurement.SnapshotWatermark {
			first++
		}
		report, err := d.FinishRebuild(history[first:], j.sequence, clock.Time)
		if err != nil {
			return err
		}
		run.Rebuilds = append(run.Rebuilds, report)
		run.Events = append(run.Events, ChurnEventLog{Step: step, Action: "rebuild-complete", Watermark: j.sequence, Status: d.Status(clock.Time)})
		return nil
	}
	for i, step := range steps {
		if err := ctx.Err(); err != nil {
			return run, err
		}
		clock.Time = step.Time
		maintenanceStart := time.Now()
		if _, err := cache.Expire(); err != nil {
			return run, err
		}
		schedule(i, j.Capture(cache.Snapshot()))
		switch step.Mutation {
		case "put":
			err = cache.Put(ctx, Entry{ID: step.EntryID, Embedding: step.MutationVector, Answer: "controlled reusable answer", Scope: step.Request.Scope(), Validity: ValidityMetadata{Reusable: true, PolicyVersion: policy.ID}})
		case "delete":
			err = cache.Delete(step.EntryID)
		case "revoke":
			err = cache.Revoke(step.EntryID)
		case "evict":
			_, err = cache.Evict()
		case "reset":
			err = cache.Clear()
			if err == nil {
				schedule(i, []UpdateEvent{j.Reset(cache.Snapshot())})
			}
		}
		if err != nil {
			return run, err
		}
		if step.Mutation != "reset" {
			schedule(i, j.Capture(cache.Snapshot()))
		}
		deliver(i)
		if config.HeartbeatEverySteps > 0 && i%config.HeartbeatEverySteps == 0 {
			if err := d.ObserveWatermark(j.sequence, clock.Time); err != nil {
				return run, err
			}
			run.Events = append(run.Events, ChurnEventLog{Step: i, Action: "heartbeat", Watermark: j.sequence, Status: d.Status(clock.Time)})
		}
		if d.build != nil {
			if err := finish(i); err != nil {
				return run, err
			}
		}
		if i > 0 && i%config.RebuildEverySteps == 0 {
			if err := d.BeginRebuild(j.Snapshot()); err != nil {
				return run, err
			}
			run.Events = append(run.Events, ChurnEventLog{Step: i, Action: "rebuild-start", Watermark: j.sequence, Status: d.Status(clock.Time)})
		}
		maintenanceNS := time.Since(maintenanceStart).Nanoseconds()
		row := ChurnRow{Step: i, SourceWatermark: j.sequence, ScheduledPending: scheduler.Pending()}
		snapshot := cache.Snapshot() // evaluation-only copy
		row.Result = baseline.serve(ctx, step.Request, func(v []float64) Decision {
			decision := d.Predict(step.Request, v)
			row.Status = d.Status(clock.Time)
			return decision
		}, func(v []float64) error {
			oracle, err := OracleSearch(ctx, policy, 2, config.Capacity, snapshot, step.Request, v)
			if err != nil {
				return err
			}
			row.OracleHit = oracle != nil
			if oracle != nil {
				row.OracleEntryID = oracle.Entry.ID
			}
			fresh, err := NewLSHSummary(p)
			if err != nil {
				return err
			}
			if err := fresh.Reset(identity, snapshot); err != nil {
				return err
			}
			row.FreshDecision = fresh.Predict(step.Request, v)
			return nil
		})
		if !row.Result.Success || row.Result.CacheWriteError != "" {
			return run, fmt.Errorf("churn request %s failed: %s %s", step.Request.ID, row.Result.Error, row.Result.CacheWriteError)
		}
		// Attribution compares the actual decision against an otherwise identical
		// fresh summary. Fallback misses are separate from stale occupied buckets.
		decision := row.Result.Decision
		row.UnseenUpsertMiss = row.OracleHit && !decision.Search && row.FreshDecision.Search
		row.IntrinsicLSHMiss = row.OracleHit && !decision.Search && !row.FreshDecision.Search
		row.StaleRemovalSearch = !row.OracleHit && decision.Search && !row.FreshDecision.Search && decision.Reason == "occupied-bucket"
		row.DiagnosticUnapplied = row.SourceWatermark - row.Status.LastApplied
		for _, e := range history {
			if e.Sequence > row.Status.LastApplied {
				age := clock.Time.Sub(e.Time).Milliseconds()
				row.DiagnosticOldestAgeMS = &age
				break
			}
		}
		updateStart := time.Now()
		schedule(i, j.Capture(cache.Snapshot()))
		deliver(i)
		maintenanceNS += time.Since(updateStart).Nanoseconds()
		row.Result.Timings.UpdateNS += maintenanceNS
		row.Result.Timings.TotalNS += maintenanceNS
		run.Rows = append(run.Rows, row)
		m := &run.Metrics
		m.Requests++
		m.Confusion.N++
		m.Searches += row.Result.SearchCount
		m.Generations += row.Result.GenerationCount
		m.UpdateNS += row.Result.Timings.UpdateNS
		m.PredictorNS += row.Result.Timings.PredictorNS
		if decision.Search {
			if row.OracleHit {
				m.Confusion.TP++
			} else {
				m.Confusion.FP++
			}
		} else {
			if row.OracleHit {
				m.Confusion.FN++
			} else {
				m.Confusion.TN++
			}
		}
		if row.UnseenUpsertMiss {
			m.UnseenUpsertMisses++
		}
		if row.IntrinsicLSHMiss {
			m.IntrinsicLSHMisses++
		}
		if row.StaleRemovalSearch {
			m.StaleRemovalSearches++
		}
		if !row.Status.Ready {
			m.FallbackRequests++
			if !row.OracleHit {
				m.FallbackMissSearches++
			}
		}
		if row.DiagnosticUnapplied > m.PeakBacklog {
			m.PeakBacklog = row.DiagnosticUnapplied
		}
		if memory := d.MemoryBytes(); memory > m.PeakSummaryBytes {
			m.PeakSummaryBytes = memory
		}
	}
	finalStart := time.Now()
	if err := d.BeginRebuild(j.Snapshot()); err != nil {
		return run, err
	}
	if err := finish(len(steps)); err != nil {
		return run, err
	}
	run.Metrics.FinalReconciliationNS = time.Since(finalStart).Nanoseconds()
	run.Metrics.UpdateNS += run.Metrics.FinalReconciliationNS
	run.Final = j.Snapshot()
	fresh, _ := NewLSHSummary(p)
	if err := fresh.Reset(identity, cache.Snapshot()); err != nil {
		return run, err
	}
	run.Metrics.FinalAgreement = reflect.DeepEqual(d.active.Summary.buckets, fresh.buckets) && reflect.DeepEqual(d.active.Summary.entries, fresh.entries) && reflect.DeepEqual(d.active.versions, j.versions) && d.active.LastApplied == j.sequence
	if !run.Metrics.FinalAgreement {
		return run, fmt.Errorf("reconciled summary differs from authoritative rebuild")
	}
	return run, nil
}
