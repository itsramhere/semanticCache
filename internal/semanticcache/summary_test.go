package semanticcache

import (
	"reflect"
	"testing"
)

func TestCountedSummaryLifecycle(t *testing.T) {
	p := smallProjections(t)
	s, err := NewLSHSummary(p)
	if err != nil {
		t.Fatal(err)
	}
	r := exampleRequest(exampleScopeA)
	if !s.Predict(r, []float64{1, 0}).Search {
		t.Fatal("unready summary skipped")
	}
	if err := s.Reset(p.Config.Identity, CacheSnapshot{}); err != nil {
		t.Fatal(err)
	}
	emptyMemory := s.MemoryBytes()
	e := Entry{ID: "a", Scope: r.Scope(), Embedding: []float64{1, 0}, Validity: ValidityMetadata{Reusable: true}}
	b := cloneEntry(e)
	b.ID = "b"
	snap := CacheSnapshot{Entries: []SnapshotEntry{{Entry: e}, {Entry: b}}}
	if err := s.Observe(snap); err != nil {
		t.Fatal(err)
	}
	for _, count := range s.buckets {
		if count != 2 {
			t.Fatal(count)
		}
	}
	if s.MemoryBytes() <= emptyMemory {
		t.Fatal("missing entry memory")
	}
	snap.Entries = snap.Entries[1:]
	if err := s.Observe(snap); err != nil {
		t.Fatal(err)
	}
	if !s.Predict(r, e.Embedding).Search {
		t.Fatal("shared bucket lost")
	}
	// Replace into another scope; old contributions must disappear.
	snap.Entries[0].Entry.Scope.Context.TenantID = "other"
	if err := s.Observe(snap); err != nil {
		t.Fatal(err)
	}
	if s.Predict(r, e.Embedding).Search {
		t.Fatal("replacement retained old scope")
	}
	fresh, _ := NewLSHSummary(p)
	if err := fresh.Reset(p.Config.Identity, snap); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(s.buckets, fresh.buckets) {
		t.Fatal("incremental/rebuild drift")
	}
	if err := s.Observe(CacheSnapshot{}); err != nil {
		t.Fatal(err)
	}
	if len(s.buckets) != 0 || len(s.entries) != 0 {
		t.Fatal("final removal retained buckets")
	}
	if d := s.Predict(r, []float64{0, 0}); !d.Search || d.Reason != "hash-error-fallback" {
		t.Fatal(d)
	}
	snap.Entries[0].Entry.Embedding = []float64{0, 0}
	if s.Observe(snap) == nil || s.Ready() || !s.Predict(r, e.Embedding).Search {
		t.Fatal("maintenance failure not fail-open")
	}
	if err := s.Reset(p.Config.Identity, CacheSnapshot{}); err != nil || !s.Ready() {
		t.Fatal("rebuild failed", err)
	}
	wrong := p.Config.Identity
	wrong.Revision = "other"
	if s.Reset(wrong, CacheSnapshot{}) == nil || s.Ready() {
		t.Fatal("identity mismatch accepted")
	}
}

func TestSharedBucketSurvivesEitherDeletion(t *testing.T) {
	for _, remaining := range []string{"a", "b"} {
		t.Run(remaining, func(t *testing.T) {
			p := smallProjections(t)
			s, _ := NewLSHSummary(p)
			e := Entry{ID: "a", Scope: exampleScopeA, Embedding: []float64{1, 0}, Validity: ValidityMetadata{Reusable: true}}
			b := cloneEntry(e)
			b.ID = "b"
			if err := s.Reset(p.Config.Identity, CacheSnapshot{Entries: []SnapshotEntry{{Entry: e}, {Entry: b}}}); err != nil {
				t.Fatal(err)
			}
			e.ID = remaining
			if err := s.Observe(CacheSnapshot{Entries: []SnapshotEntry{{Entry: e}}}); err != nil {
				t.Fatal(err)
			}
			for _, count := range s.buckets {
				if count != 1 {
					t.Fatal(count)
				}
			}
			if !s.Predict(exampleRequest(exampleScopeA), e.Embedding).Search {
				t.Fatal("shared bucket lost")
			}
		})
	}
}

func TestSummaryOwnsInputsAndLatchesFailure(t *testing.T) {
	p := smallProjections(t)
	s, err := NewLSHSummary(p)
	if err != nil {
		t.Fatal(err)
	}
	e := Entry{ID: "owned", Scope: exampleScopeA, Embedding: []float64{1, 0}, Validity: ValidityMetadata{Reusable: true}}
	state := CacheSnapshot{Entries: []SnapshotEntry{{Entry: e}}}
	if err := s.Reset(p.Config.Identity, state); err != nil {
		t.Fatal(err)
	}
	want := make(map[BucketKey]uint64)
	for k, v := range s.buckets {
		want[k] = v
	}
	p.Planes[0][0][0] = 99
	state.Entries[0].Entry.Embedding[0] = 0
	if s.projections.Planes[0][0][0] == 99 || s.entries[e.ID].Entry.Embedding[0] != 1 || !reflect.DeepEqual(s.buckets, want) {
		t.Fatal("summary retained caller-owned data")
	}
	e.Embedding = []float64{1, 0}
	bad := CacheSnapshot{Entries: []SnapshotEntry{{Entry: e}, {Entry: e}}}
	if err := s.Observe(bad); err == nil || s.Ready() {
		t.Fatal("duplicate IDs must disable skipping")
	}
	if err := s.Observe(CacheSnapshot{}); err != nil || s.Ready() {
		t.Fatal("ordinary updates must not reenable skipping before rebuild", err)
	}
	if d := s.Predict(exampleRequest(exampleScopeA), []float64{1, 0}); !d.Search || d.Reason != "summary-unready-fallback" {
		t.Fatal(d)
	}
	if err := s.Reset(s.projections.Config.Identity, CacheSnapshot{}); err != nil || !s.Ready() {
		t.Fatal("rebuild did not restore readiness", err)
	}
}
