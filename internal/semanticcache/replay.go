package semanticcache

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

type Decision struct {
	Search          bool   `json:"search"`
	Reason          string `json:"reason"`
	ConfigurationID string `json:"configuration_id"`
}

// ReplayPredictor cannot access a cache during Predict. Reset/Observe receive
// the explicit serialized snapshot/update stream outside prediction.
type ReplayPredictor interface {
	ID() string
	Reset(EmbeddingIdentity, CacheSnapshot) error
	Predict(Request, []float64) Decision
	Observe(CacheSnapshot) error
	MemoryBytes() int64
}

type ConstantPredictor struct{ Search bool }

func (p *ConstantPredictor) ID() string {
	if p.Search {
		return "always-search"
	}
	return "always-skip"
}
func (p *ConstantPredictor) Reset(EmbeddingIdentity, CacheSnapshot) error { return nil }
func (p *ConstantPredictor) Observe(CacheSnapshot) error                  { return nil }
func (p *ConstantPredictor) MemoryBytes() int64                           { return 0 }
func (p *ConstantPredictor) Predict(Request, []float64) Decision {
	return Decision{p.Search, p.ID(), p.ID()}
}

type ReplayOptions struct {
	Mode          string `json:"mode"`  // isolation (A) or end-to-end (B)
	Start         string `json:"start"` // cold or warm
	Capacity      int    `json:"capacity"`
	OracleEvery   int    `json:"oracle_every"` // 0 disables diagnostics; 1 labels all
	OracleLogging bool   `json:"oracle_logging"`
}

type ReplayEvidence struct {
	Index              int      `json:"index"`
	RequestID          string   `json:"request_id"`
	GroupID            string   `json:"group_id"`
	Category           string   `json:"category"`
	Eligible           bool     `json:"eligible"`
	CacheStateSequence uint64   `json:"cache_state_sequence"`
	Decision           Decision `json:"decision"`
	OracleHit          *bool    `json:"oracle_hit,omitempty"`
	OracleEntryID      string   `json:"oracle_entry_id,omitempty"`
	Result             Result   `json:"result"`
	// Nil if no retrieved answer; independent of the vector-match oracle.
	AnswerReuseCorrect *bool         `json:"answer_reuse_correct,omitempty"`
	PostState          CacheSnapshot `json:"post_state"`
}

type ReplayRun struct {
	PredictorID        string           `json:"predictor_id"`
	Options            ReplayOptions    `json:"options"`
	Rows               []ReplayEvidence `json:"-"`
	DiagnosticSearches int              `json:"diagnostic_searches"`
	BuildNS            int64            `json:"build_ns"`
	MemoryBytes        int64            `json:"predictor_memory_bytes"`
	FinalState         CacheSnapshot    `json:"final_state"`
	Projections        *Projections     `json:"projections,omitempty"`
}

type fixtureEmbedder struct {
	identity EmbeddingIdentity
	vectors  map[string][]float64
}

func (e fixtureEmbedder) Identity() EmbeddingIdentity { return e.identity }
func (e fixtureEmbedder) Embed(ctx context.Context, text string) ([]float64, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	v, ok := e.vectors[text]
	if !ok {
		return nil, fmt.Errorf("missing fixture vector")
	}
	return append([]float64(nil), v...), nil
}

type answerLabel struct {
	Class    string
	Scope    Scope
	Reusable bool
}

// Replay creates independent caches and resets the predictor on every call.
// In A the authoritative always-search path supplies every state and update;
// decisions are evaluated only. In B decisions control the serving path.
func Replay(ctx context.Context, c Config, w Workload, o ReplayOptions, p ReplayPredictor) (ReplayRun, error) {
	run := ReplayRun{Options: o}
	if err := c.Validate(); err != nil {
		return run, err
	}
	if err := ValidateWorkloads([]Workload{w}); err != nil {
		return run, err
	}
	if p == nil || (o.Mode != "isolation" && o.Mode != "end-to-end") || (o.Start != "warm" && o.Start != "cold") || o.Capacity <= 0 || o.OracleEvery < 0 {
		return run, fmt.Errorf("invalid replay options")
	}
	if c.WorkloadID != w.ID || c.Embedding.Model != w.Identity.Model || c.Embedding.Dimension != w.Identity.Dimension || c.Embedding.Preprocessing != w.Identity.Preprocessing {
		return run, fmt.Errorf("configuration/workload embedding identity mismatch")
	}
	run.PredictorID = p.ID()
	if summary, ok := p.(*LSHSummary); ok {
		b, _ := json.Marshal(summary.projections)
		var owned Projections
		if err := json.Unmarshal(b, &owned); err != nil {
			return run, err
		}
		run.Projections = &owned
	}
	clock := &LogicalClock{Time: w.Requests[0].Request.Timestamp}
	initial := CacheSnapshot{Entries: []SnapshotEntry{}}
	labels := map[string]answerLabel{}
	if o.Start == "warm" {
		for _, e := range w.Warm {
			initial.Sequence++
			initial.Entries = append(initial.Entries, SnapshotEntry{cloneEntry(e.Entry), initial.Sequence})
			labels[e.Entry.ID] = answerLabel{e.AnswerClass, e.Entry.Scope, true}
		}
	}
	cache, err := RestoreReferenceCache(c.Policy, w.Identity.Dimension, o.Capacity, clock, initial)
	if err != nil {
		return run, err
	}
	emb := fixtureEmbedder{w.Identity, map[string][]float64{}}
	for _, r := range w.Requests {
		emb.vectors[r.Request.Text] = append([]float64(nil), r.Vector...)
	}
	b := Baseline{Embedder: emb, Cache: cache, Generator: FakeLLM{}, Clock: clock, PolicyID: c.Policy.ID, Threshold: c.Policy.CosineThreshold, Identity: w.Identity}
	initialState := cache.Snapshot()
	start := time.Now()
	ready := p.Reset(w.Identity, initialState) == nil
	run.BuildNS = time.Since(start).Nanoseconds()
	run.MemoryBytes = p.MemoryBytes()
	// End-to-end LSH mutations synchronously maintain occupancy before another
	// decision. Isolation continues to consume authoritative snapshot updates.
	if summary, ok := p.(*LSHSummary); ok && o.Mode == "end-to-end" {
		b.Cache = &SynchronizedCache{cache: cache, summary: summary}
	}
	for i, labeled := range w.Requests {
		if err := ctx.Err(); err != nil {
			return run, err
		}
		r := labeled.Request
		clock.Time = r.Timestamp
		row := ReplayEvidence{Index: i, RequestID: r.ID, GroupID: labeled.GroupID, Category: labeled.Category, Eligible: r.CacheEligible, CacheStateSequence: cache.sequence}
		var diagnose func([]float64) error
		if r.CacheEligible && o.OracleEvery > 0 && i%o.OracleEvery == 0 {
			diagnose = func(v []float64) error {
				candidate, err := OracleSearch(ctx, c.Policy, w.Identity.Dimension, o.Capacity, cache.Snapshot(), r, v)
				if err != nil {
					return err
				}
				run.DiagnosticSearches++
				if o.OracleLogging {
					hit := candidate != nil
					row.OracleHit = &hit
					if hit {
						row.OracleEntryID = candidate.Entry.ID
					}
				}
				return nil
			}
		}
		predict := func(v []float64) Decision {
			d := Decision{true, "predictor-unready-fallback", p.ID()}
			if ready {
				d = p.Predict(r, v)
				if d.ConfigurationID != p.ID() || d.Reason == "" {
					d = Decision{true, "invalid-prediction-fallback", p.ID()}
				}
			}
			row.Decision = d
			if o.Mode == "isolation" {
				return Decision{true, "authoritative-always-search", "always-search"}
			}
			return d
		}
		row.Result = b.serve(ctx, r, predict, diagnose)
		if !r.CacheEligible {
			row.Decision = Decision{false, "ineligible", p.ID()}
		}
		if row.Result.Source == "cache" {
			label, ok := labels[row.Result.MatchedEntryID]
			correct := ok && labeled.Reusable && label.Reusable && label.Class == labeled.AnswerClass && label.Scope == r.Scope()
			row.AnswerReuseCorrect = &correct
		}
		if row.Result.WriteCount > 0 {
			labels[r.ID] = answerLabel{labeled.AnswerClass, r.Scope(), labeled.Reusable}
		}
		// Copying authoritative state is evaluation overhead; Observe itself is
		// production maintenance and is charged separately exactly once.
		row.PostState = cache.Snapshot()
		updateState := cache.Snapshot()
		start = time.Now()
		if _, synchronized := b.Cache.(*SynchronizedCache); !synchronized {
			if err := p.Observe(updateState); err != nil {
				ready = false
			}
		}
		updateNS := time.Since(start).Nanoseconds()
		row.Result.Timings.UpdateNS += updateNS
		row.Result.Timings.TotalNS += updateNS
		if memory := p.MemoryBytes(); memory > run.MemoryBytes {
			run.MemoryBytes = memory
		}
		run.Rows = append(run.Rows, row)
	}
	run.FinalState = cache.Snapshot()
	return run, nil
}
