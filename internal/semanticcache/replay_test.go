package semanticcache

import (
	"context"
	"os"
	"reflect"
	"testing"
	"time"
)

func fixtureConfig(t *testing.T) Config {
	t.Helper()
	f, err := os.Open("../../configs/phase2_fixture.json")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	c, err := LoadConfig(f)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func runFixture(t *testing.T, w Workload, o ReplayOptions, p ReplayPredictor) ReplayRun {
	t.Helper()
	run, err := Replay(context.Background(), fixtureConfig(t), w, o, p)
	if err != nil {
		t.Fatal(err)
	}
	return run
}

func stripMeasurements(run ReplayRun) ReplayRun {
	run.BuildNS = 0
	run.DiagnosticSearches = 0
	run.Options.OracleEvery = 0
	run.Options.OracleLogging = false
	for i := range run.Rows {
		r := &run.Rows[i]
		r.OracleHit = nil
		r.OracleEntryID = ""
		r.Result.Timings = StageTimings{}
	}
	return run
}

func TestOracleHasNoSideEffects(t *testing.T) {
	c, clock := testCache(t, 2)
	ctx := context.Background()
	r := exampleRequest(exampleScopeA)
	r.Timestamp = clock.Time
	if err := c.Put(ctx, Entry{ID: "one", Embedding: []float64{1, 0}, Answer: "answer", Scope: r.Scope(), Validity: ValidityMetadata{Reusable: true, PolicyVersion: PolicyV1ID}}); err != nil {
		t.Fatal(err)
	}
	before := c.Snapshot()
	hit, err := OracleSearch(ctx, c.policy, 2, 2, before, r, []float64{1, 0})
	if err != nil || hit == nil {
		t.Fatalf("oracle %v %v", hit, err)
	}
	hit.Entry.Embedding[0] = 0
	if !reflect.DeepEqual(before, c.Snapshot()) {
		t.Fatal("oracle changed state")
	}
	// Exact expiration boundary is a miss without deleting the expired entry.
	r.Timestamp = before.Entries[0].Entry.ExpiresAt
	hit, err = OracleSearch(ctx, c.policy, 2, 2, c.Snapshot(), r, []float64{1, 0})
	if err != nil || hit != nil {
		t.Fatalf("expiry oracle %v %v", hit, err)
	}
	if !reflect.DeepEqual(before, c.Snapshot()) {
		t.Fatal("oracle purged expired state")
	}
	restored, err := RestoreReferenceCache(c.policy, 2, 2, clock, before)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, restored.Snapshot()) {
		t.Fatal("restore changed FIFO, TTL, or sequence")
	}
}

func TestOracleSamplingDoesNotAffectServing(t *testing.T) {
	w := testWorkloads(t)[0]
	for _, mode := range []string{"isolation", "end-to-end"} {
		var reference ReplayRun
		for i, o := range []ReplayOptions{{mode, "warm", 100, 1, true}, {mode, "warm", 100, 7, true}, {mode, "warm", 100, 1, false}, {mode, "warm", 100, 0, false}} {
			run := runFixture(t, w, o, &ConstantPredictor{false})
			if i == 0 {
				reference = stripMeasurements(run)
			} else if !reflect.DeepEqual(reference, stripMeasurements(run)) {
				t.Fatalf("%s: oracle sampling/logging changed serving", mode)
			}
		}
	}
}

func TestComparisonModes(t *testing.T) {
	w := testWorkloads(t)[0]
	ctx := context.Background()
	c := fixtureConfig(t)
	o := ReplayOptions{"isolation", "warm", 100, 1, true}
	a := runFixture(t, w, o, &ConstantPredictor{true})
	skip := runFixture(t, w, o, &ConstantPredictor{false})
	for i := range a.Rows {
		if !reflect.DeepEqual(a.Rows[i].PostState, skip.Rows[i].PostState) || !reflect.DeepEqual(a.Rows[i].OracleHit, skip.Rows[i].OracleHit) {
			t.Fatal("isolation cache diverged")
		}
		if a.Rows[i].Result.Source != skip.Rows[i].Result.Source {
			t.Fatal("isolation changed authoritative responses")
		}
	}
	o.Mode = "end-to-end"
	b := runFixture(t, w, o, &ConstantPredictor{true})
	s := runFixture(t, w, o, &ConstantPredictor{false})
	if reflect.DeepEqual(b.FinalState, s.FinalState) {
		t.Fatal("end-to-end caches did not diverge")
	}
	for _, row := range s.Rows {
		if row.Result.SearchCount != 0 || row.Result.GenerationCount != 1 {
			t.Fatal("always skip did not control serving")
		}
	}
	// Every B oracle label agrees with that run's own pre-decision state.
	state := CacheSnapshot{Entries: []SnapshotEntry{}}
	for _, e := range w.Warm {
		state.Sequence++
		state.Entries = append(state.Entries, SnapshotEntry{cloneEntry(e.Entry), state.Sequence})
	}
	for i, row := range s.Rows {
		if row.Eligible {
			hit, err := OracleSearch(ctx, c.Policy, w.Identity.Dimension, 100, state, w.Requests[i].Request, w.Requests[i].Vector)
			if err != nil {
				t.Fatal(err)
			}
			if row.OracleHit == nil || *row.OracleHit != (hit != nil) {
				t.Fatal("label from wrong state")
			}
		}
		state = row.PostState
	}
	// Reusing a predictor object must reset all run state.
	p := &ConstantPredictor{true}
	first := runFixture(t, w, o, p)
	second := runFixture(t, w, o, p)
	if !reflect.DeepEqual(stripMeasurements(first), stripMeasurements(second)) {
		t.Fatal("repeat run changed state")
	}
}

type brokenPredictor struct {
	ConstantPredictor
	invalid bool
}

func (p *brokenPredictor) Reset(EmbeddingIdentity, CacheSnapshot) error {
	if !p.invalid {
		return context.Canceled
	}
	return nil
}
func (p *brokenPredictor) Predict(Request, []float64) Decision {
	return Decision{false, "", "wrong-id"}
}

func TestReplayFallbackAndCancellation(t *testing.T) {
	w := testWorkloads(t)[0]
	o := ReplayOptions{"end-to-end", "cold", 100, 1, true}
	for _, invalid := range []bool{false, true} {
		run := runFixture(t, w, o, &brokenPredictor{invalid: invalid})
		for _, r := range run.Rows {
			if r.Eligible && (!r.Decision.Search || r.Result.SearchCount != 1) {
				t.Fatal("unsafe predictor did not fall back")
			}
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Replay(ctx, fixtureConfig(t), w, o, &ConstantPredictor{}); err == nil {
		t.Fatal("ignored cancellation")
	}
	// Real MiniLM configuration must reject simulated identity.
	c := fixtureConfig(t)
	c.Embedding.Model = MiniLMIdentity().Model
	if _, err := Replay(context.Background(), c, w, o, &ConstantPredictor{}); err == nil {
		t.Fatal("mixed simulated and real embedding identities")
	}
}

func TestSnapshotOwnershipAndFIFO(t *testing.T) {
	c, clock := testCache(t, 2)
	ctx := context.Background()
	r := exampleRequest(exampleScopeA)
	put := func(id string) {
		t.Helper()
		if err := c.Put(ctx, Entry{ID: id, Embedding: []float64{1, 0}, Answer: "a", Scope: r.Scope(), Validity: ValidityMetadata{Reusable: true, PolicyVersion: PolicyV1ID}}); err != nil {
			t.Fatal(err)
		}
	}
	put("z")
	clock.Time = clock.Time.Add(time.Second)
	put("a")
	s := c.Snapshot()
	restored, err := RestoreReferenceCache(c.policy, 2, 2, clock, s)
	if err != nil {
		t.Fatal(err)
	}
	s.Entries[0].Entry.Embedding[0] = 0
	if c.Snapshot().Entries[0].Entry.Embedding[0] != 1 || restored.Snapshot().Entries[0].Entry.Embedding[0] != 1 {
		t.Fatal("snapshot vector alias")
	}
	if id, err := restored.Evict(); err != nil || id != "z" {
		t.Fatalf("FIFO %q %v", id, err)
	}
}

// Predictors receive owned input copies, so update bookkeeping cannot corrupt
// cache entries, the saved request vectors, or another predictor's evidence.
type mutatingReplayPredictor struct{ ConstantPredictor }

func (p *mutatingReplayPredictor) Reset(_ EmbeddingIdentity, state CacheSnapshot) error {
	return p.Observe(state)
}
func (p *mutatingReplayPredictor) Observe(state CacheSnapshot) error {
	for i := range state.Entries {
		state.Entries[i].Entry.Embedding[0] = 100
		state.Entries[i].Entry.Answer = "changed"
	}
	return nil
}
func (p *mutatingReplayPredictor) Predict(r Request, v []float64) Decision {
	for i := range v {
		v[i] = 0
	}
	return p.ConstantPredictor.Predict(r, v)
}

func TestReplayPredictorInputOwnership(t *testing.T) {
	w := fourRequestWorkload(t)
	before := fourRequestWorkload(t)
	for _, mode := range []string{"isolation", "end-to-end"} {
		o := ReplayOptions{mode, "warm", 100, 1, true}
		want := runFixture(t, w, o, &ConstantPredictor{true})
		got := runFixture(t, w, o, &mutatingReplayPredictor{ConstantPredictor{true}})
		if !reflect.DeepEqual(stripMeasurements(got), stripMeasurements(want)) {
			t.Fatalf("%s: predictor mutated serving state or evidence", mode)
		}
	}
	if !reflect.DeepEqual(w, before) {
		t.Fatal("predictor changed the source workload")
	}
}

type updateFailurePredictor struct{ ConstantPredictor }

func (p *updateFailurePredictor) Observe(CacheSnapshot) error { return context.Canceled }

func TestReplayUpdateFailureFallback(t *testing.T) {
	w := fourRequestWorkload(t)
	run := runFixture(t, w, ReplayOptions{"end-to-end", "warm", 100, 1, true}, &updateFailurePredictor{})
	if run.Rows[0].Decision.Search || run.Rows[0].Result.SearchCount != 0 {
		t.Fatal("ready predictor did not bypass the first request")
	}
	for _, row := range run.Rows[1:] {
		if !row.Decision.Search || row.Decision.Reason != "predictor-unready-fallback" || row.Result.SearchCount != 1 {
			t.Fatal("failed update did not activate always-search fallback")
		}
	}
}
