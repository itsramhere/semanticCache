package semanticcache

import "testing"

func TestExactRepeatLifecycleAndSemanticMiss(t *testing.T) {
	p := &ExactRepeat{}
	identity := smallProjections(t).Config.Identity
	r := exampleRequest(exampleScopeA)
	e := Entry{ID: "a", Scope: r.Scope(), Embedding: []float64{1, 0}, Validity: ValidityMetadata{Reusable: true}}
	if !p.Predict(r, e.Embedding).Search {
		t.Fatal("unready must search")
	}
	if err := p.Reset(identity, CacheSnapshot{Entries: []SnapshotEntry{{Entry: e}}}); err != nil {
		t.Fatal(err)
	}
	if !p.Predict(r, []float64{2, 0}).Search {
		t.Fatal("repeat lost")
	}
	if p.Predict(r, []float64{0.99, 0.1}).Search {
		t.Fatal("nearby vector is not an exact repeat")
	}
	r.Context.TenantID = "other"
	if p.Predict(r, e.Embedding).Search {
		t.Fatal("scope leak")
	}
	if err := p.Observe(CacheSnapshot{}); err != nil {
		t.Fatal(err)
	}
	if p.Predict(exampleRequest(exampleScopeA), e.Embedding).Search {
		t.Fatal("stale repeat")
	}
	if err := p.Observe(CacheSnapshot{Entries: []SnapshotEntry{{Entry: e}, {Entry: e}}}); err == nil || !p.Predict(r, e.Embedding).Search {
		t.Fatal("failure must search")
	}
}
