package semanticcache

import (
	"context"
	"errors"
	"math"
	"slices"
	"testing"
	"time"
)

func testCache(t *testing.T, capacity int) (*ReferenceCache, *LogicalClock) {
	t.Helper()
	clock := &LogicalClock{Time: exampleNow}
	c, err := NewReferenceCache(PolicyConfig{ID: PolicyV1ID, CosineThreshold: 0.9, TTLSeconds: 3600}, 2, capacity, clock)
	if err != nil {
		t.Fatal(err)
	}
	return c, clock
}

func TestReferenceExpirationAndRejectedWrites(t *testing.T) {
	c, clock := testCache(t, 2)
	putTest(t, c, "z", []float64{1, 0})
	putTest(t, c, "a", []float64{0, 1})
	expires := clock.Time.Add(time.Hour)
	clock.Time = expires.Add(-time.Nanosecond)
	r := exampleRequest(exampleScopeA)
	r.Timestamp = clock.Time
	winnerTest(t, c, r, []float64{1, 0}, "z")

	clock.Time = expires
	r.Timestamp = clock.Time
	winnerTest(t, c, r, []float64{1, 0}, "")
	// A rejected write must not even purge expired entries or change FIFO state.
	sequence := c.sequence
	bad := exampleEntry("a", []float64{0, 0}, exampleScopeA)
	if err := c.Put(context.Background(), bad); !errors.Is(err, ErrInvalidEmbedding) {
		t.Fatalf("malformed replacement: %v", err)
	}
	if len(c.entries) != 2 || c.sequence != sequence {
		t.Fatal("rejected replacement mutated expired entries or insertion order")
	}
	ids, err := c.Expire()
	if err != nil || !slices.Equal(ids, []string{"a", "z"}) {
		t.Fatalf("expiration IDs %v, error %v", ids, err)
	}
	if ids, err := c.Expire(); err != nil || len(ids) != 0 {
		t.Fatalf("repeated expiration %v, error %v", ids, err)
	}
	putTest(t, c, "new", []float64{1, 0})
	winnerTest(t, c, r, []float64{1, 0}, "new")
}

func TestReferenceReplacementAndGlobalCapacity(t *testing.T) {
	c, clock := testCache(t, 2)
	ctx := context.Background()
	putTest(t, c, "oldest", []float64{1, 0})
	other := exampleEntry("other-scope", []float64{1, 0}, exampleScopeB)
	if err := c.Put(ctx, other); err != nil {
		t.Fatal(err)
	}
	if err := c.Revoke("oldest"); err != nil {
		t.Fatal(err)
	}
	winnerTest(t, c, exampleRequest(exampleScopeA), []float64{1, 0}, "")
	clock.Time = clock.Time.Add(time.Second)
	vector := []float64{0, 2}
	putTest(t, c, "oldest", vector)
	vector[1] = 0
	r := exampleRequest(exampleScopeA)
	r.Timestamp = clock.Time
	winnerTest(t, c, r, []float64{1, 0}, "")
	winnerTest(t, c, r, []float64{0, 1}, "oldest")
	putTest(t, c, "newest", []float64{1, 0})
	otherRequest := exampleRequest(exampleScopeB)
	otherRequest.Timestamp = clock.Time
	winnerTest(t, c, otherRequest, []float64{1, 0}, "")
	winnerTest(t, c, r, []float64{0, 1}, "oldest")
	if len(c.entries) != 2 {
		t.Fatal("global capacity was not enforced across scopes")
	}
}
func putTest(t *testing.T, c *ReferenceCache, id string, v []float64) {
	t.Helper()
	if err := c.Put(context.Background(), exampleEntry(id, v, exampleScopeA)); err != nil {
		t.Fatal(err)
	}
}
func winnerTest(t *testing.T, c *ReferenceCache, r Request, v []float64, want string) {
	t.Helper()
	got, err := c.Search(context.Background(), r, v)
	if err != nil {
		t.Fatal(err)
	}
	id := ""
	if got != nil {
		id = got.Entry.ID
	}
	if id != want {
		t.Fatalf("winner %q want %q", id, want)
	}
}

func TestReferenceSearch(t *testing.T) {
	c, clock := testCache(t, 10)
	r := exampleRequest(exampleScopeA)
	ctx := context.Background()
	winnerTest(t, c, r, []float64{1, 0}, "")
	putTest(t, c, "z", []float64{1, 0})
	putTest(t, c, "a", []float64{1, 0})
	winnerTest(t, c, r, []float64{1, 0}, "a")
	got, _ := c.Search(ctx, r, []float64{1, 0})
	got.Entry.Embedding[0] = 0
	got.Entry.Answer = "changed"
	winnerTest(t, c, r, []float64{1, 0}, "a")
	for _, change := range []func(*Request){func(r *Request) { r.Context.TenantID = "other" }, func(r *Request) { r.Namespace = "other" }, func(r *Request) { r.ModelVersion = "other" }, func(r *Request) { r.Context.Locale = "other" }, func(r *Request) { r.Context.ProductVersion = "other" }, func(r *Request) { r.Context.KnowledgeVersion = "other" }, func(r *Request) { r.CacheEligible = false }} {
		other := r
		change(&other)
		winnerTest(t, c, other, []float64{1, 0}, "")
	}
	if err := c.Revoke("a"); err != nil {
		t.Fatal(err)
	}
	winnerTest(t, c, r, []float64{1, 0}, "z")
	clock.Time = clock.Time.Add(time.Hour)
	r.Timestamp = clock.Time
	winnerTest(t, c, r, []float64{1, 0}, "")
	ids, err := c.Expire()
	if err != nil || len(ids) != 2 {
		t.Fatalf("expire %v %v", ids, err)
	}
	clock.Time = clock.Time.Add(-time.Second)
	if _, err := c.Expire(); err == nil {
		t.Fatal("accepted backward clock")
	}
}

func TestReferenceBoundary(t *testing.T) {
	for _, x := range []float64{0.8, 0.9 - 1e-10, 0.9, 0.9 + 1e-10, 1} {
		c, _ := testCache(t, 1)
		v := []float64{x, math.Sqrt(1 - x*x)}
		putTest(t, c, "a", v)
		normalized, _ := Normalize(v)
		want := ""
		if normalized[0] >= 0.9 {
			want = "a"
		}
		winnerTest(t, c, exampleRequest(exampleScopeA), []float64{1, 0}, want)
	}
}

func TestReferenceLifecycle(t *testing.T) {
	c, clock := testCache(t, 2)
	putTest(t, c, "a", []float64{1, 0})
	putTest(t, c, "b", []float64{0, 1})
	// Reads do not refresh FIFO. Equal timestamps still preserve insertion order.
	winnerTest(t, c, exampleRequest(exampleScopeA), []float64{1, 0}, "a")
	putTest(t, c, "c", []float64{-1, 0})
	if _, ok := c.entries["a"]; ok {
		t.Fatal("FIFO did not evict a")
	}
	clock.Time = clock.Time.Add(time.Minute)
	putTest(t, c, "b", []float64{1, 0})
	if len(c.entries) != 2 {
		t.Fatal("replacement evicted another entry")
	}
	if !c.entries["b"].entry.ExpiresAt.Equal(clock.Time.Add(time.Hour)) {
		t.Fatal("replacement did not reset TTL")
	}
	id, err := c.Evict()
	if err != nil || id != "c" {
		t.Fatalf("evicted %q: %v", id, err)
	}
	if err := c.Delete("b"); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete("b"); err != nil {
		t.Fatal(err)
	}
	if len(c.entries) != 0 {
		t.Fatal("delete failed")
	}
}

func TestReferenceRejectsMalformedWithoutMutation(t *testing.T) {
	c, _ := testCache(t, 1)
	putTest(t, c, "good", []float64{1, 0})
	ctx := context.Background()
	for _, v := range [][]float64{nil, {0, 0}, {1}, {math.NaN(), 0}, {math.Inf(1), 0}, {1e308, 1e308}, {1e-12, 0}} {
		if err := c.Put(ctx, exampleEntry("good", v, exampleScopeA)); !errors.Is(err, ErrInvalidEmbedding) {
			t.Fatalf("write: %v", err)
		}
		if _, err := c.Search(ctx, exampleRequest(exampleScopeA), v); !errors.Is(err, ErrInvalidEmbedding) {
			t.Fatalf("search: %v", err)
		}
		winnerTest(t, c, exampleRequest(exampleScopeA), []float64{1, 0}, "good")
	}
	e := exampleEntry("bad", []float64{1, 0}, exampleScopeA)
	e.Validity.Revoked = true
	if err := c.Put(ctx, e); err == nil {
		t.Fatal("accepted revoked entry")
	}
	cancelCtx, cancel := context.WithCancel(ctx)
	cancel()
	if err := c.Put(cancelCtx, e); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := c.Search(cancelCtx, exampleRequest(exampleScopeA), []float64{1, 0}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
