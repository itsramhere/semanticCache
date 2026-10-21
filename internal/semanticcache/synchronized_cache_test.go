package semanticcache

import (
	"context"
	"reflect"
	"testing"
	"time"
)

func TestSynchronizedLifecycle(t *testing.T) {
	cache, clock := testCache(t, 2)
	p := smallProjections(t)
	s, _ := NewLSHSummary(p)
	c, err := NewSynchronizedCache(cache, s, p.Config.Identity)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	entry := func(id string, v []float64) Entry {
		return Entry{ID: id, Embedding: v, Scope: exampleScopeA, Answer: "answer", Validity: ValidityMetadata{Reusable: true, PolicyVersion: PolicyV1ID}}
	}
	check := func() {
		t.Helper()
		fresh, _ := NewLSHSummary(p)
		if err := fresh.Reset(p.Config.Identity, cache.Snapshot()); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(s.buckets, fresh.buckets) {
			t.Fatal("summary differs from rebuild")
		}
	}
	for _, e := range []Entry{entry("a", []float64{1, 0}), entry("b", []float64{1, 0}), entry("c", []float64{0, 1}), entry("b", []float64{-1, 0})} {
		if err := c.Put(ctx, e); err != nil {
			t.Fatal(err)
		}
		check()
	}
	if _, ok := cache.entries["a"]; ok {
		t.Fatal("capacity failed")
	}
	if err := c.Delete("b"); err != nil {
		t.Fatal(err)
	}
	check()
	if err := c.Delete("b"); err != nil {
		t.Fatal(err)
	}
	check()
	if err := c.Revoke("c"); err != nil {
		t.Fatal(err)
	}
	check()
	if err := c.Put(ctx, entry("d", []float64{1, 0})); err != nil {
		t.Fatal(err)
	}
	check()
	if _, err := c.Evict(); err != nil {
		t.Fatal(err)
	}
	check()
	if err := c.Put(ctx, entry("ttl", []float64{1, 0})); err != nil {
		t.Fatal(err)
	}
	clock.Time = cache.entries["ttl"].entry.ExpiresAt
	if _, err := c.Expire(); err != nil {
		t.Fatal(err)
	}
	check()
	if len(s.buckets) != 0 {
		t.Fatal("exact TTL boundary retained keys")
	}
	// Simulate a maintenance error after the authoritative write succeeds.
	s.projections.Config.Identity.Dimension = 3
	if err := c.Put(ctx, entry("committed", []float64{1, 0})); err != nil {
		t.Fatal(err)
	}
	if _, ok := cache.entries["committed"]; !ok || s.Ready() || c.MaintenanceError() == nil {
		t.Fatal("committed mutation did not invalidate summary")
	}
	r := exampleRequest(exampleScopeA)
	if !s.Predict(r, []float64{1, 0}).Search {
		t.Fatal("maintenance failure skipped")
	}
	s.projections.Config.Identity.Dimension = 2
	if err := c.Rebuild(); err != nil || !s.Ready() {
		t.Fatal("rebuild failed", err)
	}
	check()
}

func TestPredictiveServeAndExpectedLSHFalseNegative(t *testing.T) {
	b, clock := testBaseline(t)
	// The x=0 plane separates two vectors with cosine 0.98 (policy >=0.9).
	p := &Projections{LSHConfig{"cosine-hyperplanes-v1", b.Identity, 1001, 1, 1}, [][][]float64{{{1, 0}}}}
	s, err := NewLSHSummary(p)
	if err != nil {
		t.Fatal(err)
	}
	c, err := NewSynchronizedCache(b.Cache.(*ReferenceCache), s, b.Identity)
	if err != nil {
		t.Fatal(err)
	}
	b.Cache = c
	b.Predictor = s
	r := exampleRequest(exampleScopeA)
	r.Timestamp = clock.Time
	b.Embedder = stubEmbedder{identity: b.Identity, vector: []float64{0.1, 1}}
	first := b.Serve(context.Background(), r)
	if !first.Success || first.SearchCount != 0 || first.WriteCount != 1 {
		t.Fatalf("first %+v", first)
	}
	r.ID = "repeat"
	clock.Time = clock.Time.Add(time.Second)
	r.Timestamp = clock.Time
	repeat := b.Serve(context.Background(), r)
	if !repeat.Success || repeat.Source != "cache" || repeat.SearchCount != 1 || repeat.MatchedEntryID != first.RequestID {
		t.Fatalf("repeat %+v", repeat)
	}
	r.ID = "near"
	clock.Time = clock.Time.Add(time.Second)
	r.Timestamp = clock.Time
	v := []float64{-0.1, 1}
	oracle, err := OracleSearch(context.Background(), c.cache.policy, 2, 10, c.cache.Snapshot(), r, v)
	if err != nil || oracle == nil {
		t.Fatal("expected semantic match", err)
	}
	b.Embedder = stubEmbedder{identity: b.Identity, vector: v}
	missed := b.Serve(context.Background(), r)
	if !missed.Success || missed.Source != "generation" || missed.SearchCount != 0 || missed.Decision.Search {
		t.Fatalf("expected LSH FN %+v", missed)
	}
	s.Invalidate()
	r.ID = "fallback"
	fallback := b.Serve(context.Background(), r)
	if fallback.SearchCount != 1 || fallback.Decision.Reason != "summary-unready-fallback" {
		t.Fatalf("fallback %+v", fallback)
	}
}

func TestLSHReplayDiagnosticsAndBypasses(t *testing.T) {
	w := testWorkloads(t)[0]
	for _, mode := range []string{"isolation", "end-to-end"} {
		for _, start := range []string{"warm", "cold"} {
			var reference ReplayRun
			for i, diagnostic := range []struct {
				every int
				log   bool
			}{{1, true}, {0, false}, {7, true}, {1, false}} {
				p, err := NewProjections(LSHConfig{"cosine-hyperplanes-v1", w.Identity, 1001, 2, 8})
				if err != nil {
					t.Fatal(err)
				}
				s, _ := NewLSHSummary(p)
				run := runFixture(t, w, ReplayOptions{mode, start, 100, diagnostic.every, diagnostic.log}, s)
				m, err := Summarize(fixtureConfig(t), run)
				if err != nil {
					t.Fatal(err)
				}
				if m.Bypasses == 0 || m.Failures != 0 || m.MemoryBytes == 0 {
					t.Fatalf("incomplete replay %+v", m)
				}
				if i == 0 {
					reference = stripMeasurements(run)
				} else if !reflect.DeepEqual(reference, stripMeasurements(run)) {
					t.Fatal("oracle altered LSH behavior")
				}
			}
		}
	}
}

func TestPredictiveServeUnsafeSummaryFallsBack(t *testing.T) {
	for _, kind := range []string{"unsynchronized", "different-summary", "different-model", "different-revision"} {
		t.Run(kind, func(t *testing.T) {
			b, _ := testBaseline(t)
			p, err := NewProjections(LSHConfig{"cosine-hyperplanes-v1", b.Identity, 42, 2, 3})
			if err != nil {
				t.Fatal(err)
			}
			if kind == "different-model" {
				p.Config.Identity.Model = "another-model"
			}
			if kind == "different-revision" {
				p.Config.Identity.Revision = "another-revision"
			}
			s, err := NewLSHSummary(p)
			if err != nil {
				t.Fatal(err)
			}
			if err := s.Reset(p.Config.Identity, CacheSnapshot{}); err != nil {
				t.Fatal(err)
			}
			b.Predictor = s
			if kind != "unsynchronized" {
				attached := s
				if kind == "different-summary" {
					attached, err = NewLSHSummary(p)
					if err != nil {
						t.Fatal(err)
					}
				}
				b.Cache, err = NewSynchronizedCache(b.Cache.(*ReferenceCache), attached, p.Config.Identity)
				if err != nil {
					t.Fatal(err)
				}
			}
			r := exampleRequest(exampleScopeA)
			first := b.Serve(context.Background(), r)
			if !first.Success || first.EmbeddingCount != 1 || first.SearchCount != 1 || first.WriteCount != 1 || first.Decision == nil || !first.Decision.Search {
				t.Fatalf("unsafe summary must search and continue serving: %+v", first)
			}
			wantReason := "unsynchronized-summary-fallback"
			if kind == "different-model" || kind == "different-revision" {
				wantReason = "summary-identity-mismatch-fallback"
			}
			if first.Decision.Reason != wantReason || first.Decision.ConfigurationID != s.ID() {
				t.Fatalf("missing fallback evidence: %+v", first.Decision)
			}
			r.ID = "fallback-repeat"
			repeat := b.Serve(context.Background(), r)
			if !repeat.Success || repeat.Source != "cache" || repeat.SearchCount != 1 || repeat.MatchedEntryID != first.RequestID {
				t.Fatalf("fallback must retrieve a valid repeat: %+v", repeat)
			}
		})
	}
}

func TestPredictiveServeExactRepeatAtHashBoundary(t *testing.T) {
	b, _ := testBaseline(t)
	// Repeated normalization changes the last bits of these vectors. The
	// rounded dot product is zero for the query and negative for the stored copy.
	v := []float64{-0.4943748127704828, 1.2440150150762053}
	p := &Projections{LSHConfig{"cosine-hyperplanes-v1", b.Identity, 42, 1, 1}, [][][]float64{{{0.9293066945922853, 0.3693089050997298}}}}
	s, err := NewLSHSummary(p)
	if err != nil {
		t.Fatal(err)
	}
	b.Cache, err = NewSynchronizedCache(b.Cache.(*ReferenceCache), s, b.Identity)
	if err != nil {
		t.Fatal(err)
	}
	b.Predictor = s
	b.Embedder = stubEmbedder{identity: b.Identity, vector: v}
	r := exampleRequest(exampleScopeA)
	first := b.Serve(context.Background(), r)
	if !first.Success || first.WriteCount != 1 {
		t.Fatalf("first: %+v", first)
	}
	r.ID = "boundary-repeat"
	repeat := b.Serve(context.Background(), r)
	if !repeat.Success || repeat.Source != "cache" || repeat.SearchCount != 1 || repeat.MatchedEntryID != first.RequestID {
		t.Fatalf("exact repeat was lost at a floating-point hash boundary: %+v", repeat)
	}
	if repeat.Decision == nil || repeat.Decision.Reason != "hash-boundary-fallback" {
		t.Fatalf("missing boundary fallback evidence: %+v", repeat.Decision)
	}
}

func TestPredictiveServeMaintenanceFailureAndRecovery(t *testing.T) {
	b, _ := testBaseline(t)
	p, err := NewProjections(LSHConfig{"cosine-hyperplanes-v1", b.Identity, 42, 2, 3})
	if err != nil {
		t.Fatal(err)
	}
	s, err := NewLSHSummary(p)
	if err != nil {
		t.Fatal(err)
	}
	c, err := NewSynchronizedCache(b.Cache.(*ReferenceCache), s, b.Identity)
	if err != nil {
		t.Fatal(err)
	}
	b.Cache, b.Predictor = c, s
	// Inject summary failure while leaving authoritative cache writes valid.
	s.projections.Config.Identity.Dimension++
	r := exampleRequest(exampleScopeA)
	first := b.Serve(context.Background(), r)
	if !first.Success || first.Source != "generation" || first.WriteCount != 1 || first.CacheWriteError != "" || first.SummaryError == "" || s.Ready() {
		t.Fatalf("committed write and maintenance failure must be separate: %+v", first)
	}
	if _, ok := c.cache.entries[r.ID]; !ok {
		t.Fatal("maintenance failure discarded committed cache entry")
	}
	s.projections.Config.Identity.Dimension--
	r.ID = "maintenance-fallback"
	repeat := b.Serve(context.Background(), r)
	if !repeat.Success || repeat.Source != "cache" || repeat.SearchCount != 1 || repeat.Decision.Reason != "summary-unready-fallback" || s.Ready() {
		t.Fatalf("maintenance must remain in fallback until rebuild: %+v", repeat)
	}
	if err := c.Rebuild(); err != nil {
		t.Fatal(err)
	}
	r.ID = "after-rebuild"
	recovered := b.Serve(context.Background(), r)
	if !recovered.Success || recovered.Source != "cache" || recovered.Decision.Reason != "occupied-bucket" || !s.Ready() || c.MaintenanceError() != nil || recovered.SummaryError != "" {
		t.Fatalf("rebuild did not restore synchronized routing: %+v", recovered)
	}
}
