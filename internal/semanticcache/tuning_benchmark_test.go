package semanticcache

import (
	"fmt"
	"math/rand"
	"testing"
)

// CPU/allocations use deterministic populated summaries, separately from the
// retained heap/process measurements made by tune. The 1000-entry cases fill
// that capacity even when the authored workload itself never reaches it.
func BenchmarkLSHTuning(b *testing.B) {
	for _, l := range []int{1, 2, 4} {
		for _, k := range []int{4, 8} {
			for _, n := range []int{100, 1000} {
				b.Run(fmt.Sprintf("L%d/K%d/entries%d", l, k, n), func(b *testing.B) {
					p, err := NewProjections(LSHConfig{Version: "cosine-hyperplanes-v1", Identity: FixtureIdentity(), Seed: 1001, Tables: l, Bits: k})
					if err != nil {
						b.Fatal(err)
					}
					s, _ := NewLSHSummary(p)
					rng := rand.New(rand.NewSource(707))
					state := CacheSnapshot{}
					for i := 0; i < n; i++ {
						v := make([]float64, 64)
						for j := range v {
							v[j] = rng.NormFloat64()
						}
						v, _ = Normalize(v)
						e := Entry{ID: fmt.Sprint(i), Embedding: v, Scope: exampleScopeA, Validity: ValidityMetadata{Reusable: true}}
						state.Entries = append(state.Entries, SnapshotEntry{Entry: e})
					}
					if err := s.Reset(FixtureIdentity(), state); err != nil {
						b.Fatal(err)
					}
					r := exampleRequest(exampleScopeA)
					v := state.Entries[0].Entry.Embedding
					b.Run("hash", func(b *testing.B) {
						b.ReportAllocs()
						for i := 0; i < b.N; i++ {
							if _, _, err := p.hash(r.Scope(), v, s.ID()); err != nil {
								b.Fatal(err)
							}
						}
					})
					b.Run("predict", func(b *testing.B) {
						b.ReportAllocs()
						for i := 0; i < b.N; i++ {
							_ = s.Predict(r, v)
						}
					})
					b.Run("update", func(b *testing.B) {
						b.ReportAllocs()
						for i := 0; i < b.N; i++ {
							state.Entries[0].Entry.Answer = []string{"a", "b"}[i%2]
							if err := s.Observe(state); err != nil {
								b.Fatal(err)
							}
						}
					})
				})
			}
		}
	}
}
