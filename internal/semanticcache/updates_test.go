package semanticcache

import (
	"context"
	"math/rand"
	"reflect"
	"testing"
	"time"
)

func assertSummaryRebuild(t *testing.T, s *LSHSummary, state CacheSnapshot) {
	t.Helper()
	fresh, err := NewLSHSummary(s.projections)
	if err != nil {
		t.Fatal(err)
	}
	if err := fresh.Reset(s.projections.Config.Identity, state); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(s.buckets, fresh.buckets) || !reflect.DeepEqual(s.entries, fresh.entries) {
		t.Fatal("incremental state differs from rebuild")
	}
	for _, count := range s.buckets {
		if count == 0 || count > uint64(len(s.entries)) {
			t.Fatal("invalid bucket count", count)
		}
	}
}

func TestVersionedLifecycleSequences(t *testing.T) {
	for _, seed := range []int64{101, 202, 303} {
		c, clock := testCache(t, 3)
		s, _ := NewLSHSummary(smallProjections(t))
		cache, err := NewSynchronizedCache(c, s, s.projections.Config.Identity)
		if err != nil {
			t.Fatal(err)
		}
		rng := rand.New(rand.NewSource(seed))
		for i := 0; i < 200; i++ {
			id := []string{"a", "b", "c", "d"}[rng.Intn(4)]
			switch rng.Intn(8) {
			case 0, 1, 2:
				v := []float64{1, 0}
				if i%2 == 0 {
					v = []float64{-1, 0}
				}
				err = cache.Put(context.Background(), exampleEntry(id, v, exampleScopeA))
			case 3:
				err = cache.Delete(id)
				if err == nil {
					err = cache.Delete(id)
				}
			case 4:
				_, err = cache.Evict()
			case 5:
				err = cache.Revoke(id)
			case 6:
				clock.Time = clock.Time.Add(time.Hour)
				_, err = cache.Expire()
			case 7:
				err = cache.Clear()
			}
			if err != nil || cache.MaintenanceError() != nil || !s.Ready() {
				t.Fatalf("seed %d step %d: %v / %v", seed, i, err, cache.MaintenanceError())
			}
			assertSummaryRebuild(t, s, c.Snapshot())
			if cache.consumer.LastApplied != cache.journal.sequence || !reflect.DeepEqual(cache.consumer.versions, cache.journal.versions) {
				t.Fatal("event versions differ")
			}
		}
	}
}

func TestUpdateDuplicateStaleAndResetBarrier(t *testing.T) {
	c, _ := testCache(t, 2)
	j, _ := NewUpdateJournal("test-stream", smallProjections(t).Config.Identity, c.Snapshot())
	s, _ := NewLSHSummary(smallProjections(t))
	consumer, err := NewEventSummary(s, j.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	putTest(t, c, "a", []float64{1, 0})
	first := j.Capture(c.Snapshot())[0]
	if err := consumer.Apply(first); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete("a"); err != nil {
		t.Fatal(err)
	}
	removed := j.Capture(c.Snapshot())[0]
	if err := consumer.Apply(removed); err != nil {
		t.Fatal(err)
	}
	putTest(t, c, "a", []float64{-1, 0})
	replacement := j.Capture(c.Snapshot())[0]
	if err := consumer.Apply(replacement); err != nil {
		t.Fatal(err)
	}
	for _, stale := range []UpdateEvent{first, removed, replacement, removed} {
		if err := consumer.Apply(stale); err != nil {
			t.Fatal(err)
		}
	}
	assertSummaryRebuild(t, s, c.Snapshot())
	// Same payload/time replacement still advances the entry version and FIFO.
	putTest(t, c, "a", []float64{-1, 0})
	events := j.Capture(c.Snapshot())
	if len(events) != 1 || events[0].PreviousVersion != replacement.Sequence {
		t.Fatal("identical replacement lost its version")
	}
	if err := consumer.Apply(events[0]); err != nil {
		t.Fatal(err)
	}
	if err := c.Clear(); err != nil {
		t.Fatal(err)
	}
	reset := j.Reset(c.Snapshot())
	if err := consumer.Apply(reset); err != nil {
		t.Fatal(err)
	}
	if err := consumer.Apply(replacement); err != nil {
		t.Fatal(err)
	}
	assertSummaryRebuild(t, s, c.Snapshot())
	// A newly numbered removal naming a previous incarnation must fail closed
	// for skipping, without decrementing the new entry's buckets.
	putTest(t, c, "a", []float64{1, 0})
	latest := j.Capture(c.Snapshot())[0]
	if err := consumer.Apply(latest); err != nil {
		t.Fatal(err)
	}
	bad := removed
	bad.Sequence = latest.Sequence + 1
	bad.ID = bad.payloadID()
	if consumer.Apply(bad) == nil || s.Ready() {
		t.Fatal("stale incarnation accepted")
	}
	assertSummaryRebuild(t, s, c.Snapshot())
}

func TestUpdateIDsStableAndOwned(t *testing.T) {
	build := func() []UpdateEvent {
		c, _ := testCache(t, 2)
		j, _ := NewUpdateJournal("stable", smallProjections(t).Config.Identity, c.Snapshot())
		putTest(t, c, "z", []float64{1, 0})
		putTest(t, c, "a", []float64{1, 0})
		return j.Capture(c.Snapshot())
	}
	a, b := build(), build()
	if !reflect.DeepEqual(a, b) || a[0].EntryID != "a" || a[1].EntryID != "z" || a[0].ID == a[1].ID {
		t.Fatal("unstable event identity/order")
	}
	a[0].Entry.Embedding[0] = 99
	if b[0].Entry.Embedding[0] == 99 {
		t.Fatal("event aliases another capture")
	}
}

func TestLifecycleRequestAtExpiration(t *testing.T) {
	b, clock := testBaseline(t)
	p := smallProjections(t)
	p.Config.Identity = b.Identity
	s, _ := NewLSHSummary(p)
	c, err := NewSynchronizedCache(b.Cache.(*ReferenceCache), s, b.Identity)
	if err != nil {
		t.Fatal(err)
	}
	b.Cache, b.Predictor = c, s
	r := exampleRequest(exampleScopeA)
	first := b.Serve(context.Background(), r)
	if !first.Success || first.WriteCount != 1 {
		t.Fatal(first)
	}
	clock.Time = c.cache.entries[r.ID].entry.ExpiresAt
	r.ID = "at-expiration"
	r.Timestamp = clock.Time
	result := b.Serve(context.Background(), r)
	if !result.Success || result.Source != "generation" || result.GenerationCount != 1 {
		t.Fatalf("expired entry reused: %+v", result)
	}
	assertSummaryRebuild(t, s, c.cache.Snapshot())
}
