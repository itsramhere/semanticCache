package semanticcache

import (
	"errors"
	"fmt"
	"math"
)

// ErrInvalidEmbedding marks embeddings with no usable direction: empty,
// containing non-finite components, or with a squared norm below
// MinValidNormSquared.
var ErrInvalidEmbedding = errors.New("invalid embedding")

// MinValidNormSquared is the smallest squared Euclidean norm accepted as a
// usable direction. Normalized vectors have squared norm 1, so this constant
// only rejects degenerate (near-zero) inputs. See docs/cache_policy.md.
const MinValidNormSquared = 1e-18

// Normalize returns a unit-length copy of v. The input slice is never
// modified. Degenerate inputs produce an error wrapping ErrInvalidEmbedding.
func Normalize(v []float64) ([]float64, error) {
	if len(v) == 0 {
		return nil, fmt.Errorf("%w: empty vector", ErrInvalidEmbedding)
	}
	squared := 0.0
	for _, component := range v {
		if math.IsNaN(component) || math.IsInf(component, 0) {
			return nil, fmt.Errorf("%w: non-finite component", ErrInvalidEmbedding)
		}
		squared += component * component
	}
	if math.IsInf(squared, 0) || squared < MinValidNormSquared {
		return nil, fmt.Errorf("%w: zero or near-zero vector (squared norm %g)", ErrInvalidEmbedding, squared)
	}
	inverse := 1 / math.Sqrt(squared)
	out := make([]float64, len(v))
	for i, component := range v {
		out[i] = component * inverse
	}
	return out, nil
}

// CosineSimilarity returns the dot product of two normalized vectors, which
// equals their cosine similarity. Inputs must be normalized with Normalize and
// must have equal length. The result is clamped to [-1, 1] to absorb float
// rounding near the extremes. A dimension mismatch is an error.
func CosineSimilarity(a, b []float64) (float64, error) {
	if len(a) == 0 || len(a) != len(b) {
		return 0, fmt.Errorf("similarity needs matching dimensions: %d vs %d", len(a), len(b))
	}
	sum := 0.0
	for i := range a {
		if !finite(a[i]) || !finite(b[i]) {
			return 0, ErrInvalidEmbedding
		}
		sum += a[i] * b[i]
	}
	if sum > 1 {
		sum = 1
	} else if sum < -1 {
		sum = -1
	}
	return sum, nil
}

// SameScope reports whether two scopes are identical. Every scope field must
// match exactly; an unset (empty) field only equals another empty field, so an
// unset value never matches a populated one.
func SameScope(a, b Scope) bool {
	return a == b
}

// Candidate pairs an entry with its similarity to the request embedding.
type Candidate struct {
	Entry      *Entry
	Similarity float64
}

// BetterCandidate returns whichever of a and b wins the frozen tie-break rule:
//
//  1. the higher similarity wins;
//  2. on exact float64 equality, the lexicographically smaller ID wins.
//
// Callers must supply candidates that already passed scope, validity, model
// identity, liveness, and threshold checks. Entry must be non-nil in both
// arguments.
func BetterCandidate(a, b Candidate) Candidate {
	if a.Similarity != b.Similarity {
		if a.Similarity > b.Similarity {
			return a
		}
		return b
	}
	if a.Entry.ID != b.Entry.ID {
		if a.Entry.ID < b.Entry.ID {
			return a
		}
		return b
	}
	return a
}
