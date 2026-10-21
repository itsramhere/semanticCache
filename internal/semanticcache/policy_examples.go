package semanticcache

import (
	"fmt"
	"math"
	"time"
)

// This file executes the frozen decision rule documented in
// docs/cache_policy.md on a fixed set of hand-checkable examples. The CLI
// command "semcache verify-policy" and the unit tests both call it, so the
// document and the implementation cannot drift apart silently.

// exampleEmbeddingModel is the embedding model identity shared by the examples.
const exampleEmbeddingModel = "fake-embed-1"

// exampleNow is the logical timestamp used by every example request.
var exampleNow = time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)

// Two scopes shared by the examples: identical except for the tenant.
var (
	exampleScopeA = Scope{Context: RequestContext{TenantID: "tenant-a", Locale: "en", ProductVersion: "v1", KnowledgeVersion: "kb1"}, Namespace: "it-faq", ModelVersion: "serving-model-v1"}
	exampleScopeB = Scope{Context: RequestContext{TenantID: "tenant-b", Locale: "en", ProductVersion: "v1", KnowledgeVersion: "kb1"}, Namespace: "it-faq", ModelVersion: "serving-model-v1"}
)

// PolicyExample describes one hand-checkable decision from docs/cache_policy.md.
type PolicyExample struct {
	// Name is stable and is reported by the CLI and the tests.
	Name string
	// Description states the arithmetic and the conditions being exercised.
	Description string
	// Request supplies the logical decision time and the request scope.
	Request Request
	// QueryEmbedding is the raw request embedding; Normalize is applied.
	QueryEmbedding []float64
	// EmbeddingModelID is the embedding model identity of the request.
	EmbeddingModelID string
	// Entries are the candidate stored entries.
	Entries []Entry
	// ExpectedWinner is the entry ID that must win, or "" for a miss.
	ExpectedWinner string
}

// PolicyExampleResult pairs an example with the achieved winner.
type PolicyExampleResult struct {
	Name           string
	Description    string
	Winner         string
	ExpectedWinner string
	OK             bool
}

// EvaluatePolicyExample executes the frozen decision rule for one example:
// scan candidates, apply validity, model identity, scope, and liveness
// preconditions, keep entries with similarity >= threshold, then apply the
// tie-break order. It returns the winning entry ID, or "" when nothing
// qualified.
func EvaluatePolicyExample(example PolicyExample) (string, error) {
	query, err := Normalize(example.QueryEmbedding)
	if err != nil {
		return "", fmt.Errorf("example %q: query embedding: %w", example.Name, err)
	}
	requestScope := example.Request.Scope()
	now := example.Request.Timestamp

	var best Candidate
	for i := range example.Entries {
		entry := &example.Entries[i]
		if !example.Request.CacheEligible || !entry.Validity.Reusable || entry.Validity.Revoked || entry.Validity.PolicyVersion != PolicyV1ID {
			continue
		}
		if example.EmbeddingModelID != exampleEmbeddingModel {
			continue
		}
		if !SameScope(entry.Scope, requestScope) {
			continue
		}
		if !entry.IsLive(now) {
			continue
		}
		entryVector, err := Normalize(entry.Embedding)
		if err != nil {
			continue // malformed stored embeddings are never matches
		}
		similarity, err := CosineSimilarity(query, entryVector)
		if err != nil {
			continue // a dimension mismatch makes vectors incomparable
		}
		if similarity < PolicyV1CosineThreshold {
			continue
		}
		candidate := Candidate{Entry: entry, Similarity: similarity}
		if best.Entry == nil || BetterCandidate(candidate, best) == candidate {
			best = candidate
		}
	}
	if best.Entry == nil {
		return "", nil
	}
	return best.Entry.ID, nil
}

// RunPolicyExamples evaluates every canonical example and returns the results.
func RunPolicyExamples() ([]PolicyExampleResult, error) {
	examples := AllPolicyExamples()
	results := make([]PolicyExampleResult, 0, len(examples))
	for _, example := range examples {
		winner, err := EvaluatePolicyExample(example)
		if err != nil {
			return nil, fmt.Errorf("example %q: %w", example.Name, err)
		}
		results = append(results, PolicyExampleResult{
			Name:           example.Name,
			Description:    example.Description,
			Winner:         winner,
			ExpectedWinner: example.ExpectedWinner,
			OK:             winner == example.ExpectedWinner,
		})
	}
	return results, nil
}

// exampleRequest builds the canonical request an example decides for.
func exampleRequest(scope Scope) Request {
	return Request{
		ID:            "example-request",
		Text:          "hand-worked example question",
		Namespace:     scope.Namespace,
		ModelVersion:  scope.ModelVersion,
		Context:       scope.Context,
		Timestamp:     exampleNow,
		CacheEligible: true,
	}
}

// exampleEntry builds a valid live entry inserted ten minutes before the request.
func exampleEntry(id string, embedding []float64, scope Scope) Entry {
	return exampleEntryAt(id, embedding, scope, exampleNow.Add(-10*time.Minute))
}

// exampleEntryAt builds a valid entry with an explicit insertion time and the
// policy TTL (3,600 logical seconds) expiry.
func exampleEntryAt(id string, embedding []float64, scope Scope, insertedAt time.Time) Entry {
	return Entry{
		ID:         id,
		Embedding:  embedding,
		Answer:     "canned answer for " + id,
		Scope:      scope,
		InsertedAt: insertedAt,
		ExpiresAt:  insertedAt.Add(PolicyV1TTLSeconds * time.Second),
		Validity:   ValidityMetadata{Reusable: true, PolicyVersion: PolicyV1ID},
	}
}

// AllPolicyExamples returns the canonical example set of docs/cache_policy.md.
// Example names are part of the documented contract; renaming one is a
// documentation change. Hand-vector pairs and their exact rational cosines:
//
//	(3,4) dot (4,3)  = 24/25 = 0.96     (hit)
//	(3,4) dot (5,12) = 63/65 = 0.9692   (hit)
//	(3,4) dot (12,5) = 56/65 = 0.8615   (near-match rejection)
func AllPolicyExamples() []PolicyExample {
	// Threshold boundary probes for the request vector (1,0): entry
	// embeddings (0.90 +/- 1e-10, sqrt(1 - x^2)) give similarities 1e-10 above
	// and below the threshold, far outside float rounding (~1e-16).
	above := 0.90 + 1e-10
	below := 0.90 - 1e-10
	aboveVector := []float64{above, math.Sqrt(1 - above*above)}
	belowVector := []float64{below, math.Sqrt(1 - below*below)}

	modelMismatch := exampleEntry("entry-000001", []float64{4, 3}, exampleScopeA)
	modelMismatch.Scope.ModelVersion = "different-serving-model"

	invalidEntry := exampleEntry("entry-000001", []float64{4, 3}, exampleScopeA)
	invalidEntry.Validity.Reusable = false

	zeroVectorEntry := exampleEntryAt("entry-000001", []float64{0, 0}, exampleScopeA, exampleNow.Add(-1*time.Hour))
	dimensionMismatchEntry := exampleEntryAt("entry-000001", []float64{4, 3, 0}, exampleScopeA, exampleNow.Add(-1*time.Hour))

	return []PolicyExample{
		{
			Name:             "scoped_hit",
			Description:      "Same scope, cosine 24/25 = 0.96 >= 0.90.",
			Request:          exampleRequest(exampleScopeA),
			QueryEmbedding:   []float64{3, 4},
			EmbeddingModelID: exampleEmbeddingModel,
			Entries: []Entry{
				exampleEntry("entry-000001", []float64{4, 3}, exampleScopeA),
			},
			ExpectedWinner: "entry-000001",
		},
		{
			Name:             "similarity_beats_insertion_order",
			Description:      "Older entry at 56/65 = 0.8615 is below threshold; newer entry at 0.96 wins.",
			Request:          exampleRequest(exampleScopeA),
			QueryEmbedding:   []float64{3, 4},
			EmbeddingModelID: exampleEmbeddingModel,
			Entries: []Entry{
				exampleEntryAt("entry-000001", []float64{12, 5}, exampleScopeA, exampleNow.Add(-20*time.Minute)),
				exampleEntryAt("entry-000002", []float64{4, 3}, exampleScopeA, exampleNow.Add(-10*time.Minute)),
			},
			ExpectedWinner: "entry-000002",
		},
		{
			Name:             "cross_tenant_rejected",
			Description:      "Cosine 0.96 but the entry tenant differs.",
			Request:          exampleRequest(exampleScopeA),
			QueryEmbedding:   []float64{3, 4},
			EmbeddingModelID: exampleEmbeddingModel,
			Entries: []Entry{
				exampleEntry("entry-000001", []float64{4, 3}, exampleScopeB),
			},
			ExpectedWinner: "",
		},
		{
			Name:             "generation_model_rejected",
			Description:      "Cosine 0.96 but the entry belongs to a different generation model.",
			Request:          exampleRequest(exampleScopeA),
			QueryEmbedding:   []float64{3, 4},
			EmbeddingModelID: exampleEmbeddingModel,
			Entries:          []Entry{modelMismatch},
			ExpectedWinner:   "",
		},
		{
			Name:             "expired_at_boundary_rejected",
			Description:      "Cosine 0.96 but the entry expires exactly at the request time (half-open lifetime).",
			Request:          exampleRequest(exampleScopeA),
			QueryEmbedding:   []float64{3, 4},
			EmbeddingModelID: exampleEmbeddingModel,
			Entries: []Entry{
				exampleEntryAt("entry-000001", []float64{4, 3}, exampleScopeA, exampleNow.Add(-time.Hour)),
			},
			ExpectedWinner: "",
		},
		{
			Name:             "live_entry_matches",
			Description:      "Cosine 0.96, inserted ten minutes ago, expires in 50 minutes.",
			Request:          exampleRequest(exampleScopeA),
			QueryEmbedding:   []float64{3, 4},
			EmbeddingModelID: exampleEmbeddingModel,
			Entries: []Entry{
				exampleEntry("entry-000001", []float64{4, 3}, exampleScopeA),
			},
			ExpectedWinner: "entry-000001",
		},
		{
			Name:             "near_match_rejected",
			Description:      "Cosine 56/65 = 0.8615 < 0.90: below threshold.",
			Request:          exampleRequest(exampleScopeA),
			QueryEmbedding:   []float64{3, 4},
			EmbeddingModelID: exampleEmbeddingModel,
			Entries: []Entry{
				exampleEntry("entry-000001", []float64{12, 5}, exampleScopeA),
			},
			ExpectedWinner: "",
		},
		{
			Name:             "just_above_threshold_hit",
			Description:      "Cosine 0.9000000001 >= 0.90: inclusive boundary above.",
			Request:          exampleRequest(exampleScopeA),
			QueryEmbedding:   []float64{1, 0},
			EmbeddingModelID: exampleEmbeddingModel,
			Entries: []Entry{
				exampleEntry("entry-000001", aboveVector, exampleScopeA),
			},
			ExpectedWinner: "entry-000001",
		},
		{
			Name:             "just_below_threshold_rejected",
			Description:      "Cosine 0.8999999999 < 0.90: boundary below.",
			Request:          exampleRequest(exampleScopeA),
			QueryEmbedding:   []float64{1, 0},
			EmbeddingModelID: exampleEmbeddingModel,
			Entries: []Entry{
				exampleEntry("entry-000001", belowVector, exampleScopeA),
			},
			ExpectedWinner: "",
		},
		{
			Name:             "equal_similarity_smaller_id_beats_insertion_time",
			Description:      "Identical vectors, different insertion times: smaller ID wins.",
			Request:          exampleRequest(exampleScopeA),
			QueryEmbedding:   []float64{3, 4},
			EmbeddingModelID: exampleEmbeddingModel,
			Entries: []Entry{
				exampleEntryAt("entry-000001", []float64{5, 12}, exampleScopeA, exampleNow),
				exampleEntryAt("entry-000002", []float64{5, 12}, exampleScopeA, exampleNow.Add(-10*time.Minute)),
			},
			ExpectedWinner: "entry-000001",
		},
		{
			Name:             "equal_similarity_equal_time_smaller_id_wins",
			Description:      "Identical vectors and equal insertion times: smaller entry ID wins (order reversed in the list).",
			Request:          exampleRequest(exampleScopeA),
			QueryEmbedding:   []float64{3, 4},
			EmbeddingModelID: exampleEmbeddingModel,
			Entries: []Entry{
				exampleEntryAt("entry-000002", []float64{5, 12}, exampleScopeA, exampleNow.Add(-10*time.Minute)),
				exampleEntryAt("entry-000001", []float64{5, 12}, exampleScopeA, exampleNow.Add(-10*time.Minute)),
			},
			ExpectedWinner: "entry-000001",
		},
		{
			Name:             "invalid_entry_skipped",
			Description:      "Cosine 0.96 but Reusable=false: insertion-time validity blocks reuse.",
			Request:          exampleRequest(exampleScopeA),
			QueryEmbedding:   []float64{3, 4},
			EmbeddingModelID: exampleEmbeddingModel,
			Entries:          []Entry{invalidEntry},
			ExpectedWinner:   "",
		},
		{
			Name:             "zero_vector_entry_skipped",
			Description:      "Stored embedding (0,0) has no direction; a well-formed sibling entry still matches.",
			Request:          exampleRequest(exampleScopeA),
			QueryEmbedding:   []float64{3, 4},
			EmbeddingModelID: exampleEmbeddingModel,
			Entries: []Entry{
				zeroVectorEntry,
				exampleEntry("entry-000002", []float64{4, 3}, exampleScopeA),
			},
			ExpectedWinner: "entry-000002",
		},
		{
			Name:             "dimension_mismatch_skipped",
			Description:      "Entry embedding has 3 dimensions vs 2 for the request: not comparable.",
			Request:          exampleRequest(exampleScopeA),
			QueryEmbedding:   []float64{3, 4},
			EmbeddingModelID: exampleEmbeddingModel,
			Entries:          []Entry{dimensionMismatchEntry},
			ExpectedWinner:   "",
		},
	}
}
