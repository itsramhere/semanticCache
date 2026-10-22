package semanticcache

import (
	"context"
	"reflect"
	"testing"
	"time"
)

func delayedFixture(t *testing.T) (*ReferenceCache, *LogicalClock, *UpdateJournal, *DelayedSummary) {
	t.Helper()
	c, clock := testCache(t, 3)
	j, err := NewUpdateJournal("delayed-test", smallProjections(t).Config.Identity, c.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	d, err := NewDelayedSummary(smallProjections(t), j.Snapshot(), clock.Time, 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return c, clock, j, d
}

func TestSchedulerReproducibleFaults(t *testing.T) {
	build := func() ([]ScheduledUpdate, []ScheduledUpdate) {
		c, clock := testCache(t, 50)
		j, _ := NewUpdateJournal("scheduler", smallProjections(t).Config.Identity, c.Snapshot())
		s, err := NewEventScheduler(FaultConfig{101, 6000, .7, .2})
		if err != nil {
			t.Fatal(err)
		}
		var scheduled []ScheduledUpdate
		for i := 0; i < 30; i++ {
			clock.Time = clock.Time.Add(time.Millisecond)
			putTest(t, c, "same", []float64{1, 0})
			scheduled = append(scheduled, s.Schedule(j.Capture(c.Snapshot())[0])...)
		}
		return scheduled, s.Due(clock.Time.Add(time.Hour))
	}
	a, due := build()
	b, again := build()
	if !reflect.DeepEqual(a, b) || !reflect.DeepEqual(due, again) {
		t.Fatal("seed changed transport")
	}
	drops, duplicates, reordered := 0, 0, false
	for _, e := range a {
		if e.Dropped {
			drops++
		}
		if e.Duplicate {
			duplicates++
		}
	}
	for i := 1; i < len(due); i++ {
		if due[i].Event.Sequence < due[i-1].Event.Sequence {
			reordered = true
		}
	}
	if drops == 0 || duplicates == 0 || !reordered {
		t.Fatalf("missing faults: drops=%d duplicates=%d reordered=%v", drops, duplicates, reordered)
	}
	for _, c := range []FaultConfig{{MaxDelayMS: -1}, {DropProbability: 1.1}, {DuplicateProbability: -.1}} {
		if _, err := NewEventScheduler(c); err == nil {
			t.Fatal("invalid fault settings accepted")
		}
	}
}

func TestUnseenTailInsertionAndFreshnessFallback(t *testing.T) {
	for _, heartbeat := range []bool{false, true} {
		c, clock, j, d := delayedFixture(t)
		putTest(t, c, "unseen", []float64{1, 0})
		j.Capture(c.Snapshot()) // dropped tail, no delivery
		clock.Time = clock.Time.Add(time.Second)
		if heartbeat {
			if err := d.ObserveWatermark(j.sequence, clock.Time); err != nil {
				t.Fatal(err)
			}
		}
		r := exampleRequest(exampleScopeA)
		r.Timestamp = clock.Time
		oracle, err := c.Search(context.Background(), r, []float64{1, 0})
		if err != nil || oracle == nil {
			t.Fatal("expected oracle hit", err)
		}
		if decision := d.Predict(r, []float64{1, 0}); decision.Search != heartbeat {
			t.Fatal("heartbeat must expose a dropped tail; absent heartbeat must leave the blind window visible", decision)
		}
		status := d.Status(clock.Time)
		if !heartbeat && (status.KnownUnapplied != 0 || status.OldestUnappliedAgeMS != nil) {
			t.Fatal("receiver invented knowledge of a dropped tail")
		}
		clock.Time = clock.Time.Add(3 * time.Second)
		r.Timestamp = clock.Time
		if decision := d.Predict(r, []float64{1, 0}); !decision.Search {
			t.Fatal("freshness limit failed", decision)
		}
		if err := d.BeginRebuild(j.Snapshot()); err != nil {
			t.Fatal(err)
		}
		if _, err := d.FinishRebuild(nil, j.sequence, clock.Time); err != nil {
			t.Fatal(err)
		}
		assertSummaryRebuild(t, d.active.Summary, c.Snapshot())
		if !d.Status(clock.Time).Ready {
			t.Fatal("rebuild did not restore readiness")
		}
	}
}

func TestDelayedRemovalWastesSearchAndGapLatches(t *testing.T) {
	c, clock, j, d := delayedFixture(t)
	putTest(t, c, "gone", []float64{1, 0})
	inserted := j.Capture(c.Snapshot())[0]
	if err := d.Receive(inserted, clock.Time); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete("gone"); err != nil {
		t.Fatal(err)
	}
	removed := j.Capture(c.Snapshot())[0]
	r := exampleRequest(exampleScopeA)
	if !d.Predict(r, []float64{1, 0}).Search {
		t.Fatal("stale removal not observable")
	}
	fresh, _ := NewLSHSummary(d.active.Summary.projections)
	if err := fresh.Reset(fresh.projections.Config.Identity, c.Snapshot()); err != nil {
		t.Fatal(err)
	}
	if fresh.Predict(r, []float64{1, 0}).Search {
		t.Fatal("fresh summary should avoid empty search")
	}
	putTest(t, c, "new", []float64{-1, 0})
	next := j.Capture(c.Snapshot())[0]
	if err := d.Receive(next, clock.Time); err != nil {
		t.Fatal(err)
	}
	if status := d.Status(clock.Time); status.Ready || status.FallbackReason != "sequence-gap-fallback" || status.PendingEvents != 1 {
		t.Fatalf("gap invisible: %+v", status)
	}
	if err := d.Receive(removed, clock.Time); err != nil {
		t.Fatal(err)
	}
	if d.active.LastApplied != next.Sequence || d.Status(clock.Time).Ready {
		t.Fatal("catch-up bypassed latched reconciliation")
	}
	if err := d.Receive(inserted, clock.Time); err != nil {
		t.Fatal(err)
	}
	assertSummaryRebuild(t, d.active.Summary, c.Snapshot())
}

func TestRebuildReplaysMutationsBeforeSwap(t *testing.T) {
	c, clock, j, d := delayedFixture(t)
	putTest(t, c, "a", []float64{1, 0})
	j.Capture(c.Snapshot())
	if err := d.BeginRebuild(j.Snapshot()); err != nil {
		t.Fatal(err)
	}
	old := d.active
	putTest(t, c, "a", []float64{-1, 0})
	newer := j.Capture(c.Snapshot())
	if err := c.Delete("a"); err != nil {
		t.Fatal(err)
	}
	newer = append(newer, j.Capture(c.Snapshot())...)
	putTest(t, c, "b", []float64{1, 0})
	newer = append(newer, j.Capture(c.Snapshot())...)
	if _, err := d.FinishRebuild(newer[1:], j.sequence, clock.Time); err == nil || d.active != old || d.Status(clock.Time).Ready {
		t.Fatal("incomplete catch-up enabled replacement")
	}
	// Restart from a current snapshot after the failed catch-up.
	if err := d.BeginRebuild(j.Snapshot()); err != nil {
		t.Fatal(err)
	}
	putTest(t, c, "c", []float64{-1, 0})
	tail := j.Capture(c.Snapshot())
	if err := c.Clear(); err != nil {
		t.Fatal(err)
	}
	tail = append(tail, j.Reset(c.Snapshot()))
	putTest(t, c, "after-reset", []float64{1, 0})
	tail = append(tail, j.Capture(c.Snapshot())...)
	report, err := d.FinishRebuild(tail, j.sequence, clock.Time)
	if err != nil {
		t.Fatal(err)
	}
	if report.EventsReplayed != 3 || report.Backlog != 3 || report.SnapshotWatermark+3 != report.CatchupWatermark || report.PeakOwnedBytes == 0 || report.SampledHeapPeakBytes == 0 {
		t.Fatalf("missing rebuild evidence: %+v", report)
	}
	if d.active == old || !d.Status(clock.Time).Ready {
		t.Fatal("replacement was not enabled")
	}
	assertSummaryRebuild(t, d.active.Summary, c.Snapshot())
	// Mutations on the source after snapshot construction cannot alias rebuild.
	state := j.Snapshot()
	state.Cache.Entries[0].Entry.Embedding[0] = 99
	assertSummaryRebuild(t, d.active.Summary, c.Snapshot())
}

func TestDelayedSummaryRejectsInvalidSourceState(t *testing.T) {
	c, clock, j, d := delayedFixture(t)
	state := j.Snapshot()
	state.Identity.Revision = "different-model-revision"
	if _, err := NewDelayedSummary(d.active.Summary.projections, state, clock.Time, time.Second); err == nil {
		t.Fatal("incompatible snapshot identity accepted")
	}
	if err := d.BeginRebuild(state); err == nil || d.Status(clock.Time).Ready {
		t.Fatal("incompatible replacement enabled")
	}
	putTest(t, c, "a", []float64{1, 0})
	event := j.Capture(c.Snapshot())[0]
	for _, kind := range []string{"payload", "stream", "future"} {
		_, clock, _, receiver := delayedFixture(t)
		bad := event
		switch kind {
		case "payload":
			bad.Kind = "remove"
		case "stream":
			bad.StreamID = "other"
			bad.ID = bad.payloadID()
		case "future":
			bad.Time = clock.Time.Add(time.Second)
			bad.ID = bad.payloadID()
		}
		if receiver.Receive(bad, clock.Time) == nil || receiver.Status(clock.Time).Ready {
			t.Fatal("invalid delivery enabled skipping", kind)
		}
	}
}

func TestFreshnessBoundaryAndLateContact(t *testing.T) {
	_, clock, _, d := delayedFixture(t)
	r := exampleRequest(exampleScopeA)
	r.Timestamp = clock.Time.Add(3*time.Second - time.Nanosecond)
	if d.Predict(r, []float64{1, 0}).Search {
		t.Fatal("fresh empty summary unexpectedly disabled")
	}
	r.Timestamp = clock.Time.Add(3 * time.Second)
	if !d.Predict(r, []float64{1, 0}).Search {
		t.Fatal("inclusive freshness boundary failed")
	}
	if err := d.ObserveWatermark(0, r.Timestamp); err != nil {
		t.Fatal(err)
	}
	if d.Status(r.Timestamp).Ready {
		t.Fatal("late heartbeat cleared required reconciliation")
	}
}

func TestDelayedMemoryRetainsBufferCapacityAllowance(t *testing.T) {
	c, clock, j, reordered := delayedFixture(t)
	_, _, _, ordered := delayedFixture(t)
	var events []UpdateEvent
	for _, id := range []string{"a", "b", "c"} {
		putTest(t, c, id, []float64{1, 0})
		events = append(events, j.Capture(c.Snapshot())...)
	}
	for _, e := range events {
		if err := ordered.Receive(e, clock.Time); err != nil {
			t.Fatal(err)
		}
	}
	for i := len(events) - 1; i >= 0; i-- {
		if err := reordered.Receive(events[i], clock.Time); err != nil {
			t.Fatal(err)
		}
	}
	assertSummaryRebuild(t, reordered.active.Summary, c.Snapshot())
	assertSummaryRebuild(t, ordered.active.Summary, c.Snapshot())
	if len(reordered.pending) != 0 || len(ordered.pending) != 0 || reordered.MemoryBytes() <= ordered.MemoryBytes() {
		t.Fatal("drained larger buffer lost its retained map-capacity allowance")
	}
}

func TestDuplicateUpdatesDoNotRefreshSourceContact(t *testing.T) {
	c, clock, j, d := delayedFixture(t)
	putTest(t, c, "known", []float64{1, 0})
	first := j.Capture(c.Snapshot())[0]
	if err := d.Receive(first, clock.Time); err != nil {
		t.Fatal(err)
	}
	// A final insertion is lost while the transport repeatedly replays its last
	// acknowledged event. Replays provide no new evidence of source freshness.
	putTest(t, c, "unseen", []float64{-1, 0})
	j.Capture(c.Snapshot())
	for i := 1; i <= 3; i++ {
		if err := d.Receive(first, clock.Time.Add(time.Duration(i)*time.Second)); err != nil {
			t.Fatal(err)
		}
	}
	now := clock.Time.Add(3 * time.Second)
	status := d.Status(now)
	if status.Ready || status.FallbackReason != "source-silence-fallback" || status.ContactAgeMS != 3000 {
		t.Fatalf("duplicate updates concealed source silence: %+v", status)
	}
	r := exampleRequest(exampleScopeA)
	r.Timestamp = now
	if !d.Predict(r, []float64{-1, 0}).Search {
		t.Fatal("duplicate deliveries allowed skipping an unseen hit after freshness limit")
	}
}

func TestDelayedPublicationDoesNotRefreshSourceContact(t *testing.T) {
	c, clock, j, d := delayedFixture(t)
	putTest(t, c, "delayed", []float64{1, 0})
	event := j.Capture(c.Snapshot())[0]
	if err := d.Receive(event, clock.Time.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if status := d.Status(clock.Time.Add(time.Second)); !status.Ready || status.ContactAgeMS != 1000 {
		t.Fatalf("delivery time replaced source publication time: %+v", status)
	}
	if status := d.Status(clock.Time.Add(3 * time.Second)); status.Ready || status.FallbackReason != "source-silence-fallback" {
		t.Fatalf("delayed publication extended freshness: %+v", status)
	}
}

func TestRebuildRejectsFutureSourceData(t *testing.T) {
	t.Run("snapshot", func(t *testing.T) {
		c, clock, j, d := delayedFixture(t)
		clock.Time = clock.Time.Add(time.Second)
		putTest(t, c, "future", []float64{1, 0})
		j.Capture(c.Snapshot())
		state := j.Snapshot()
		now := clock.Time.Add(-time.Second)
		if _, err := NewDelayedSummary(d.active.Summary.projections, state, now, time.Second); err == nil {
			t.Fatal("future snapshot accepted as current source state")
		}
		if err := d.BeginRebuild(state); err != nil {
			t.Fatal(err)
		}
		old := d.active
		if _, err := d.FinishRebuild(nil, state.Watermark, now); err == nil || d.active != old || d.Status(now).Ready {
			t.Fatal("future snapshot enabled during rebuild")
		}
		if _, err := d.FinishRebuild(nil, state.Watermark, clock.Time); err != nil || !d.Status(clock.Time).Ready {
			t.Fatal("rebuild did not recover at source time", err)
		}
		assertSummaryRebuild(t, d.active.Summary, c.Snapshot())
	})
	t.Run("catch-up", func(t *testing.T) {
		c, clock, j, d := delayedFixture(t)
		now := clock.Time
		if err := d.BeginRebuild(j.Snapshot()); err != nil {
			t.Fatal(err)
		}
		clock.Time = clock.Time.Add(time.Second)
		putTest(t, c, "future", []float64{1, 0})
		newer := j.Capture(c.Snapshot())
		old := d.active
		if _, err := d.FinishRebuild(newer, j.sequence, now); err == nil || d.active != old || d.Status(now).Ready {
			t.Fatal("future catch-up event enabled during rebuild")
		}
		if _, err := d.FinishRebuild(newer, j.sequence, clock.Time); err != nil || !d.Status(clock.Time).Ready {
			t.Fatal("catch-up did not recover at source time", err)
		}
		assertSummaryRebuild(t, d.active.Summary, c.Snapshot())
	})
}

func TestSchedulerOwnsQueuedUpdates(t *testing.T) {
	c, clock, j, _ := delayedFixture(t)
	putTest(t, c, "queued", []float64{1, 0})
	event := j.Capture(c.Snapshot())[0]
	s, err := NewEventScheduler(FaultConfig{Seed: 101, DuplicateProbability: 1})
	if err != nil {
		t.Fatal(err)
	}
	scheduled := s.Schedule(event)
	event.Entry.Embedding[0] = 99
	for i := range scheduled {
		scheduled[i].Event.Entry.Embedding[0] = -99
	}
	due := s.Due(clock.Time)
	if len(due) != 2 || s.Pending() != 0 {
		t.Fatal("expected original and duplicate deliveries")
	}
	for _, delivery := range due {
		if delivery.Event.Entry.Embedding[0] != 1 || delivery.Event.ID != delivery.Event.payloadID() {
			t.Fatal("mutating schedule evidence corrupted an owned queued delivery")
		}
	}
}
