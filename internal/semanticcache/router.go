package semanticcache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type GeneratedAnswer struct {
	Answer   string
	Reusable bool
}
type Generator interface {
	Generate(context.Context, Request) (GeneratedAnswer, error)
}

// FakeLLM is deterministic simulated generation, not a real language model.
type FakeLLM struct{}

func (FakeLLM) Generate(ctx context.Context, r Request) (GeneratedAnswer, error) {
	if err := ctx.Err(); err != nil {
		return GeneratedAnswer{}, err
	}
	// Structured encoding keeps scope field boundaries unambiguous, including
	// when individual values contain whitespace or formatting characters.
	payload, err := json.Marshal(struct {
		Scope Scope  `json:"scope"`
		Text  string `json:"text"`
	}{r.Scope(), strings.TrimSpace(r.Text)})
	if err != nil {
		return GeneratedAnswer{}, err
	}
	sum := sha256.Sum256(payload)
	return GeneratedAnswer{Answer: "simulated FAQ answer " + hex.EncodeToString(sum[:8]), Reusable: r.CacheEligible}, nil
}

type StageTimings struct {
	Source       string `json:"source"`
	EmbeddingNS  int64  `json:"embedding_ns"`
	SearchNS     int64  `json:"search_ns"`
	ValidationNS int64  `json:"validation_ns"`
	GenerationNS int64  `json:"generation_ns"`
	CacheWriteNS int64  `json:"cache_write_ns"`
	PredictorNS  int64  `json:"predictor_ns"`
	UpdateNS     int64  `json:"update_ns"`
	TotalNS      int64  `json:"total_ns"`
}
type Result struct {
	RequestID       string           `json:"request_id"`
	Success         bool             `json:"success"`
	Answer          string           `json:"answer,omitempty"`
	Source          string           `json:"source"`
	GenerationKind  string           `json:"generation_kind"`
	MatchedEntryID  string           `json:"matched_entry_id,omitempty"`
	Similarity      float64          `json:"similarity,omitempty"`
	SearchCount     int              `json:"search_count"`
	EmbeddingCount  int              `json:"embedding_count"`
	GenerationCount int              `json:"generation_count"`
	WriteCount      int              `json:"write_count"`
	Error           string           `json:"error,omitempty"`
	CacheWriteError string           `json:"cache_write_error,omitempty"`
	SummaryError    string           `json:"summary_error,omitempty"`
	Timings         StageTimings     `json:"timings"`
	Embedding       *EmbeddingRecord `json:"embedding,omitempty"`
	Decision        *Decision        `json:"decision,omitempty"`
}

type Baseline struct {
	Embedder  Embedder
	Cache     Cache
	Generator Generator
	Clock     Clock
	PolicyID  string
	Threshold float64
	// A fixed identity prevents embeddings from different models sharing a cache.
	Identity      EmbeddingIdentity
	Predictor     *LSHSummary
	lastTimestamp time.Time
}

func (b *Baseline) Serve(ctx context.Context, r Request) (result Result) {
	if b.Predictor != nil {
		return b.serve(ctx, r, func(v []float64) Decision {
			if cache, ok := b.Cache.(*RemoteCache); ok {
				if cache.summary != b.Predictor {
					return Decision{true, "unsynchronized-summary-fallback", b.Predictor.ID()}
				}
				return cache.predict(r, v, b.Identity)
			}
			cache, ok := b.Cache.(*SynchronizedCache)
			if !ok || cache.summary != b.Predictor {
				return Decision{true, "unsynchronized-summary-fallback", b.Predictor.ID()}
			}
			if b.Predictor.projections.Config.Identity != b.Identity {
				return Decision{true, "summary-identity-mismatch-fallback", b.Predictor.ID()}
			}
			return b.Predictor.Predict(r, v)
		}, nil)
	}
	return b.serve(ctx, r, nil, nil)
}

// serve shares production validation and generation between replay controls.
// Diagnostic work runs after embedding and before the decision, and is excluded
// from serving timing. A nil predictor keeps the always-search baseline.
func (b *Baseline) serve(ctx context.Context, r Request, predict func([]float64) Decision, diagnose func([]float64) error) (result Result) {
	start := time.Now()
	var diagnosticNS int64
	result.RequestID = r.ID
	result.Source = "error"
	result.GenerationKind = "simulated-fake-llm"
	result.Timings.Source = "measured-wall-clock"
	defer func() { result.Timings.TotalNS = time.Since(start).Nanoseconds() - diagnosticNS }()
	defer func() {
		if c, ok := b.Cache.(*SynchronizedCache); ok && c.MaintenanceError() != nil {
			result.SummaryError = c.MaintenanceError().Error()
		}
		if c, ok := b.Cache.(*RemoteCache); ok && c.maintenanceError != nil {
			result.SummaryError = c.maintenanceError.Error()
		}
	}()
	fail := func(err error) Result { result.Error = err.Error(); return result }
	if err := ctx.Err(); err != nil {
		return fail(err)
	}
	if err := ValidateRequest(r); err != nil {
		return fail(err)
	}
	if b.Embedder == nil || b.Cache == nil || b.Generator == nil || b.Clock == nil || b.PolicyID == "" || !finite(b.Threshold) || b.Threshold < -1 || b.Threshold > 1 {
		return fail(fmt.Errorf("incomplete baseline configuration"))
	}
	if err := b.Identity.Validate(); err != nil {
		return fail(err)
	}
	if b.Embedder.Identity() != b.Identity {
		return fail(fmt.Errorf("embedding identity mismatch; reset cache before changing models"))
	}
	if !r.Timestamp.Equal(b.Clock.Now()) {
		return fail(fmt.Errorf("request timestamp must equal serving clock"))
	}
	if r.Timestamp.Before(b.lastTimestamp) {
		return fail(fmt.Errorf("request time moved backward"))
	}
	b.lastTimestamp = r.Timestamp
	if cache, ok := b.Cache.(*SynchronizedCache); ok {
		stage := time.Now()
		_, err := cache.Expire()
		result.Timings.UpdateNS += time.Since(stage).Nanoseconds()
		if err != nil {
			return fail(fmt.Errorf("expiration: %w", err))
		}
	}
	stage := time.Now()
	result.EmbeddingCount++
	v, err := b.Embedder.Embed(ctx, r.Text)
	if err == nil {
		v, err = normalizeDimension(v, b.Identity.Dimension)
	}
	result.Timings.EmbeddingNS = time.Since(stage).Nanoseconds()
	if err != nil {
		return fail(fmt.Errorf("embedding: %w", err))
	}
	result.Embedding = &EmbeddingRecord{r.ID, strings.TrimSpace(r.Text), b.Identity, append([]float64(nil), v...)}
	if diagnose != nil {
		stage = time.Now()
		err := diagnose(append([]float64(nil), v...))
		diagnosticNS += time.Since(stage).Nanoseconds()
		if err != nil {
			return fail(fmt.Errorf("oracle: %w", err))
		}
	}
	search := r.CacheEligible
	if r.CacheEligible && predict != nil {
		stage = time.Now()
		d := predict(append([]float64(nil), v...))
		result.Timings.PredictorNS = time.Since(stage).Nanoseconds()
		result.Decision = &d
		search = d.Search
	}
	if search {
		stage = time.Now()
		result.SearchCount++
		candidate, err := b.Cache.Search(ctx, r, v)
		result.Timings.SearchNS = time.Since(stage).Nanoseconds()
		if err != nil {
			return fail(fmt.Errorf("search: %w", err))
		}
		if err := ctx.Err(); err != nil {
			return fail(err)
		}
		stage = time.Now()
		if candidate != nil {
			e := candidate.Entry
			valid := e != nil && ValidAnswer(*e, r, b.Clock.Now(), b.PolicyID)
			if valid {
				stored, validationErr := normalizeDimension(e.Embedding, b.Identity.Dimension)
				if validationErr != nil {
					valid = false
				} else {
					// Repeat the cache's normalization/scoring exactly, including
					// at the inclusive float64 threshold boundary.
					query, _ := normalizeDimension(v, b.Identity.Dimension)
					score, _ := CosineSimilarity(query, stored)
					valid = score >= b.Threshold
					result.Similarity = score
				}
			}
			result.Timings.ValidationNS = time.Since(stage).Nanoseconds()
			if valid {
				result.Success = true
				result.Source = "cache"
				result.Answer = e.Answer
				result.MatchedEntryID = e.ID
				return result
			}
		}
		result.Timings.ValidationNS = time.Since(stage).Nanoseconds()
	}
	stage = time.Now()
	result.GenerationCount++
	answer, err := b.Generator.Generate(ctx, r)
	result.Timings.GenerationNS = time.Since(stage).Nanoseconds()
	if err != nil {
		return fail(fmt.Errorf("generation: %w", err))
	}
	if err := ctx.Err(); err != nil {
		return fail(err)
	}
	if strings.TrimSpace(answer.Answer) == "" {
		return fail(fmt.Errorf("generation returned an empty answer"))
	}
	result.Success = true
	result.Source = "generation"
	result.Answer = answer.Answer
	if r.CacheEligible && answer.Reusable {
		entry := Entry{ID: r.ID, Embedding: append([]float64(nil), v...), Answer: answer.Answer, Scope: r.Scope(), Validity: ValidityMetadata{Reusable: true, PolicyVersion: b.PolicyID}}
		stage = time.Now()
		err = b.Cache.Put(ctx, entry)
		result.Timings.CacheWriteNS = time.Since(stage).Nanoseconds()
		if cache, ok := b.Cache.(*SynchronizedCache); ok {
			result.Timings.CacheWriteNS -= cache.lastUpdateNS
			result.Timings.UpdateNS += cache.lastUpdateNS
		}
		if cache, ok := b.Cache.(*RemoteCache); ok {
			result.Timings.CacheWriteNS -= cache.lastUpdateNS
			result.Timings.UpdateNS += cache.lastUpdateNS
		}
		if err != nil {
			result.CacheWriteError = err.Error()
		} else {
			result.WriteCount++
		}
	}
	return result
}
