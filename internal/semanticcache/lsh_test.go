package semanticcache

import (
	"bytes"
	"encoding/json"
	"math"
	"reflect"
	"testing"
)

func smallProjections(t *testing.T) *Projections {
	t.Helper()
	p, err := NewProjections(LSHConfig{"cosine-hyperplanes-v1", EmbeddingIdentity{"test", "v1", 2, PreprocessingV1}, 42, 2, 3})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestProjectionRejectsMalformedFiles(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*Projections)
	}{
		{"missing-table", func(p *Projections) { p.Planes = p.Planes[:1] }},
		{"missing-bit", func(p *Projections) { p.Planes[0] = p.Planes[0][:1] }},
		{"wrong-dimension", func(p *Projections) { p.Planes[0][0] = []float64{1} }},
		{"zero-plane", func(p *Projections) { p.Planes[0][0] = []float64{0, 0} }},
		{"invalid-version", func(p *Projections) { p.Config.Version = "other" }},
		{"excessive-tables", func(p *Projections) { p.Config.Tables = 257 }},
		{"excessive-bits", func(p *Projections) { p.Config.Bits = 65 }},
		{"excessive-dimension", func(p *Projections) { p.Config.Identity.Dimension = 65537 }},
		{"coefficient-budget", func(p *Projections) { p.Config.Tables, p.Config.Bits, p.Config.Identity.Dimension = 256, 64, 2048 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := smallProjections(t)
			tc.change(p)
			b, err := json.Marshal(p)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := LoadProjections(bytes.NewReader(b), p.Config); err == nil {
				t.Fatal("accepted malformed projections")
			}
		})
	}
	for _, value := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		p := smallProjections(t)
		p.Planes[0][0][0] = value
		if _, err := NewLSHSummary(p); err == nil {
			t.Fatal("accepted non-finite plane")
		}
	}
	p := smallProjections(t)
	b, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	b = append([]byte(`{"unknown":true,`), b[1:]...)
	if _, err := LoadProjections(bytes.NewReader(b), p.Config); err == nil {
		t.Fatal("accepted unknown projection field")
	}
}

func TestProjectionHighBitAndBoundaryFallback(t *testing.T) {
	p := smallProjections(t)
	p.Config.Tables, p.Config.Bits = 1, 64
	p.Planes = [][][]float64{make([][]float64, 64)}
	for b := range p.Planes[0] {
		p.Planes[0][b] = []float64{1, 0}
	}
	keys, err := p.Keys(exampleScopeA, []float64{1, 0})
	if err != nil || keys[0].Bucket != ^uint64(0) {
		t.Fatalf("lost high bits: %+v %v", keys, err)
	}
	p.Config.Bits = 1
	p.Planes = [][][]float64{{{1, 1}}}
	s, err := NewLSHSummary(p)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Reset(p.Config.Identity, CacheSnapshot{}); err != nil {
		t.Fatal(err)
	}
	r := exampleRequest(exampleScopeA)
	for _, v := range [][]float64{{1, -1}, {1, -1 + 1e-16}, {1, -1 - 2e-16}} {
		if d := s.Predict(r, v); !d.Search || d.Reason != "hash-boundary-fallback" {
			t.Fatalf("ambiguous sign skipped: v=%v decision=%+v", v, d)
		}
	}
	if d := s.Predict(r, []float64{0.1, 1}); d.Search || d.Reason != "empty-buckets" {
		t.Fatalf("unambiguous empty bucket must still bypass: %+v", d)
	}
}

func TestProjectionPersistenceAndIsolation(t *testing.T) {
	p := smallProjections(t)
	b, _ := json.Marshal(p)
	q, err := LoadProjections(bytes.NewReader(b), p.Config)
	if err != nil {
		t.Fatal(err)
	}
	a, err := p.Keys(exampleScopeA, []float64{1, 0})
	if err != nil {
		t.Fatal(err)
	}
	z, _ := q.Keys(exampleScopeA, []float64{1, 0})
	if !reflect.DeepEqual(a, z) || p.ID() != q.ID() {
		t.Fatal("reload changed keys")
	}
	fresh, _ := NewProjections(p.Config)
	if !reflect.DeepEqual(p, fresh) {
		t.Fatal("seed is not deterministic")
	}
	other := exampleScopeA
	other.Context.TenantID = "other"
	z, _ = q.Keys(other, []float64{1, 0})
	if a[0] == z[0] {
		t.Fatal("scope collision")
	}
	for _, change := range []func(*LSHConfig){func(c *LSHConfig) { c.Seed++ }, func(c *LSHConfig) { c.Identity.Model = "other" }, func(c *LSHConfig) { c.Identity.Dimension++ }, func(c *LSHConfig) { c.Identity.Revision = "other" }, func(c *LSHConfig) { c.Identity.Preprocessing = "other" }} {
		c := p.Config
		change(&c)
		if _, err := LoadProjections(bytes.NewReader(b), c); err == nil {
			t.Fatal("accepted incompatible configuration")
		}
	}
	q.Planes[0][0] = []float64{0, 1}
	z, _ = q.Keys(exampleScopeA, []float64{1, 0})
	if z[0].Bucket&1 == 0 {
		t.Fatal("zero boundary must set bit")
	}
	if _, err := q.Keys(exampleScopeA, []float64{0, 0}); err == nil {
		t.Fatal("accepted zero vector")
	}
	if _, err := LoadProjections(bytes.NewReader(append(b, []byte(` {}`)...)), p.Config); err == nil {
		t.Fatal("accepted trailing JSON")
	}
}
