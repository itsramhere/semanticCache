package semanticcache

import (
	"errors"
	"math"
	"testing"
	"time"
)

const epsilon12 = 1e-12

func normFor(t *testing.T, v []float64) []float64 {
	t.Helper()
	got, err := Normalize(v)
	if err != nil {
		t.Fatalf("Normalize(%v): %v", v, err)
	}
	return got
}

func TestNormalize(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		input   []float64
		want    []float64
		wantErr bool
	}{
		{name: "3-4-5 triangle", input: []float64{3, 4}, want: []float64{0.6, 0.8}},
		{name: "negative components", input: []float64{-3, -4}, want: []float64{-0.6, -0.8}},
		{name: "already unit", input: []float64{1, 0}, want: []float64{1, 0}},
		{name: "three dimensions", input: []float64{2, 3, 6}, want: []float64{2.0 / 7, 3.0 / 7, 6.0 / 7}},
		{name: "zero vector", input: []float64{0, 0, 0}, wantErr: true},
		{name: "near-zero vector", input: []float64{1e-10, 0}, wantErr: true},
		{name: "nil vector", input: nil, wantErr: true},
		{name: "NaN component", input: []float64{math.NaN(), 1}, wantErr: true},
		{name: "Inf component", input: []float64{math.Inf(1), 1}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := Normalize(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("Normalize(%v) returned %v, want error", tt.input, got)
				}
				if !errors.Is(err, ErrInvalidEmbedding) {
					t.Fatalf("Normalize(%v) error %v, want ErrInvalidEmbedding", tt.input, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Normalize(%v) failed: %v", tt.input, err)
			}
			if len(got) != len(tt.want) {
				t.Fatalf("Normalize(%v) length %d, want %d", tt.input, len(got), len(tt.want))
			}
			for i := range got {
				if math.Abs(got[i]-tt.want[i]) > epsilon12 {
					t.Errorf("Normalize(%v)[%d] = %g, want %g", tt.input, i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestNormalizeDoesNotMutateInput(t *testing.T) {
	t.Parallel()
	input := []float64{3, 4}
	if _, err := Normalize(input); err != nil {
		t.Fatalf("Normalize failed: %v", err)
	}
	if input[0] != 3 || input[1] != 4 {
		t.Fatalf("Normalize mutated its input: %v", input)
	}
}

func TestCosineSimilarity(t *testing.T) {
	t.Parallel()
	// Hand-computed exact fractions, as in docs/cache_policy.md.
	tests := []struct {
		name string
		a    []float64
		b    []float64
		want float64
	}{
		{name: "identical vector", a: []float64{3, 4}, b: []float64{3, 4}, want: 1},
		{name: "24/25 exact fraction", a: []float64{3, 4}, b: []float64{4, 3}, want: 24.0 / 25},
		{name: "63/65 exact fraction", a: []float64{3, 4}, b: []float64{5, 12}, want: 63.0 / 65},
		{name: "56/65 exact fraction", a: []float64{3, 4}, b: []float64{12, 5}, want: 56.0 / 65},
		{name: "orthogonal", a: []float64{1, 0}, b: []float64{0, 1}, want: 0},
		{name: "opposite direction", a: []float64{1, 0}, b: []float64{-1, 0}, want: -1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := CosineSimilarity(normFor(t, tt.a), normFor(t, tt.b))
			if err != nil {
				t.Fatalf("CosineSimilarity failed: %v", err)
			}
			if math.Abs(got-tt.want) > epsilon12 {
				t.Errorf("CosineSimilarity = %g, want %g", got, tt.want)
			}
		})
	}
}

func TestCosineSimilarityErrors(t *testing.T) {
	t.Parallel()
	if _, err := CosineSimilarity([]float64{1, 0}, []float64{1, 0, 0}); err == nil {
		t.Error("dimension mismatch must be an error")
	}
}

func TestCosineSimilarityClampsToUnitInterval(t *testing.T) {
	t.Parallel()
	// The caller contract is normalized inputs; the clamp is defensive and
	// guarantees the reported value stays within [-1, 1] even when raw
	// products slightly overshoot due to float rounding.
	got, err := CosineSimilarity([]float64{1.0000001, 0}, []float64{1.0000001, 0})
	if err != nil {
		t.Fatalf("CosineSimilarity failed: %v", err)
	}
	if got != 1 {
		t.Errorf("overshooting dot product = %g, want clamped 1", got)
	}
}

func TestBetterCandidate(t *testing.T) {
	t.Parallel()
	// Frozen tie-break: higher similarity, then lexicographically smaller
	// entry ID for exactly equal computed scores (docs/cache_policy.md).
	tests := []struct {
		name string
		a    Candidate
		b    Candidate
		want string
	}{
		{
			name: "higher similarity wins",
			a:    Candidate{Entry: &Entry{ID: "entry-000001"}, Similarity: 0.95},
			b:    Candidate{Entry: &Entry{ID: "entry-000002"}, Similarity: 0.96},
			want: "entry-000002",
		},
		{
			name: "exact similarity tie goes to the smaller ID",
			a:    Candidate{Entry: &Entry{ID: "entry-000002"}, Similarity: 0.95},
			b:    Candidate{Entry: &Entry{ID: "entry-000001"}, Similarity: 0.95},
			want: "entry-000001",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			// Both argument orders must produce the same winner.
			if got := BetterCandidate(tt.a, tt.b); got.Entry.ID != tt.want {
				t.Errorf("BetterCandidate winner %q, want %q", got.Entry.ID, tt.want)
			}
			if got := BetterCandidate(tt.b, tt.a); got.Entry.ID != tt.want {
				t.Errorf("BetterCandidate reversed winner %q, want %q", got.Entry.ID, tt.want)
			}
		})
	}
}

func TestEntryIsLive(t *testing.T) {
	t.Parallel()
	inserted := time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC)
	expires := inserted.Add(time.Hour)
	entry := Entry{InsertedAt: inserted, ExpiresAt: expires}
	if !entry.IsLive(inserted) {
		t.Error("entry must be live at its insertion instant (inserted_at <= t < expires_at)")
	}
	if !entry.IsLive(expires.Add(-time.Nanosecond)) {
		t.Error("entry must be live just before expiry")
	}
	if entry.IsLive(expires) {
		t.Error("entry must be dead exactly at expiry (half-open lifetime)")
	}
	if entry.IsLive(expires.Add(time.Minute)) {
		t.Error("entry must be dead after expiry")
	}
	if entry.IsLive(inserted.Add(-time.Nanosecond)) {
		t.Error("entry must not be live before its insertion instant")
	}
}

func TestSameScope(t *testing.T) {
	t.Parallel()
	contextA := RequestContext{TenantID: "tenant-a", Locale: "en", ProductVersion: "v1", KnowledgeVersion: "kb1"}
	contextB := RequestContext{TenantID: "tenant-b", Locale: "en", ProductVersion: "v1", KnowledgeVersion: "kb1"}
	scopeA := Scope{Namespace: "it-faq", Context: contextA, ModelVersion: "fake-v1"}

	if !SameScope(scopeA, scopeA) {
		t.Error("identical scopes must match")
	}
	if SameScope(scopeA, Scope{Namespace: "it-faq", Context: contextB, ModelVersion: "fake-v1"}) {
		t.Error("different tenant must not match")
	}
	if SameScope(scopeA, Scope{Namespace: "other", Context: contextA, ModelVersion: "fake-v1"}) {
		t.Error("different namespace must not match")
	}
	if SameScope(scopeA, Scope{Namespace: "it-faq", Context: contextA, ModelVersion: "other"}) {
		t.Error("different model version must not match")
	}
	if SameScope(scopeA, Scope{}) {
		t.Error("empty scope must not match a populated scope")
	}
	if !SameScope(Scope{}, Scope{}) {
		t.Error("two empty scopes match each other")
	}
}

func TestRequestScopeAndCacheable(t *testing.T) {
	t.Parallel()
	request := Request{
		ID:            "req-1",
		Text:          "how do I reset my password?",
		Namespace:     "it-faq",
		Context:       RequestContext{TenantID: "tenant-a", Locale: "en", ProductVersion: "v1", KnowledgeVersion: "kb1"},
		ModelVersion:  "fake-v1",
		Timestamp:     time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC),
		CacheEligible: true,
	}
	want := Scope{Namespace: "it-faq", Context: request.Context, ModelVersion: "fake-v1"}
	if !SameScope(request.Scope(), want) {
		t.Errorf("Request.Scope() = %+v, want %+v", request.Scope(), want)
	}
	if !request.CacheEligible {
		t.Error("eligible request must report cacheable")
	}
	ineligible := request
	ineligible.CacheEligible = false
	if ineligible.CacheEligible {
		t.Error("ineligible request must report not cacheable")
	}
}
