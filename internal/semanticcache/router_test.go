package semanticcache

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type stubEmbedder struct {
	vector   []float64
	err      error
	identity EmbeddingIdentity
}

func TestFakeLLMScopeEncoding(t *testing.T) {
	first := exampleRequest(exampleScopeA)
	first.Context.TenantID = "tenant alpha"
	first.Context.Locale = "en"
	second := first
	second.Context.TenantID = "tenant"
	second.Context.Locale = "alpha en"
	generate := func(r Request) GeneratedAnswer {
		t.Helper()
		answer, err := (FakeLLM{}).Generate(context.Background(), r)
		if err != nil {
			t.Fatal(err)
		}
		return answer
	}
	answer := generate(first)
	if generate(second).Answer == answer.Answer {
		t.Fatal("different scopes produced the same fake answer through ambiguous field encoding")
	}
	first.ID = "another-request"
	first.Timestamp = first.Timestamp.Add(time.Second)
	first.Text = "\u2003" + first.Text + " "
	if generate(first) != answer {
		t.Fatal("equivalent text and scope must produce deterministic answers")
	}
}

func (e stubEmbedder) Identity() EmbeddingIdentity                      { return e.identity }
func (e stubEmbedder) Embed(context.Context, string) ([]float64, error) { return e.vector, e.err }

type stubGenerator struct {
	answer GeneratedAnswer
	err    error
}

func (g stubGenerator) Generate(context.Context, Request) (GeneratedAnswer, error) {
	return g.answer, g.err
}

type stubCache struct {
	candidate           *Candidate
	searchErr, writeErr error
	searches, writes    int
}

func (c *stubCache) Search(context.Context, Request, []float64) (*Candidate, error) {
	c.searches++
	return c.candidate, c.searchErr
}
func (c *stubCache) Put(context.Context, Entry) error { c.writes++; return c.writeErr }
func testBaseline(t *testing.T) (*Baseline, *LogicalClock) {
	c, clock := testCache(t, 10)
	identity := EmbeddingIdentity{"test", "revision", 2, PreprocessingV1}
	return &Baseline{Embedder: stubEmbedder{vector: []float64{1, 0}, identity: identity}, Cache: c, Generator: FakeLLM{}, Clock: clock, PolicyID: PolicyV1ID, Threshold: 0.9, Identity: identity}, clock
}

func TestBaselineMissHitAndEligibility(t *testing.T) {
	b, clock := testBaseline(t)
	r := exampleRequest(exampleScopeA)
	first := b.Serve(context.Background(), r)
	if !first.Success || first.Source != "generation" || first.GenerationCount != 1 || first.WriteCount != 1 || first.SearchCount != 1 || first.Timings.Source != "measured-wall-clock" {
		t.Fatalf("first: %+v", first)
	}
	clock.Time = clock.Time.Add(time.Second)
	r.ID = "repeat"
	r.Timestamp = clock.Time
	second := b.Serve(context.Background(), r)
	if !second.Success || second.Source != "cache" || second.Answer != first.Answer || second.GenerationCount != 0 || second.WriteCount != 0 || second.SearchCount != 1 {
		t.Fatalf("repeat: %+v", second)
	}
	first.Embedding.Vector[0] = 0 // captured vectors do not alias the cache.
	third := b.Serve(context.Background(), r)
	if third.Source != "cache" {
		t.Fatalf("aliased vector: %+v", third)
	}
	r.CacheEligible = false
	noReuse := b.Serve(context.Background(), r)
	if !noReuse.Success || noReuse.Source != "generation" || noReuse.SearchCount != 0 || noReuse.WriteCount != 0 {
		t.Fatalf("ineligible: %+v", noReuse)
	}
}

func TestBaselineFailures(t *testing.T) {
	cases := []struct {
		name                          string
		setup                         func(*Baseline, *Request)
		success                       bool
		contains                      string
		searches, generations, writes int
		writeError                    bool
	}{
		{name: "bad request", setup: func(b *Baseline, r *Request) { r.Context.TenantID = "" }, contains: "scope"},
		{name: "invalid embedding", setup: func(b *Baseline, r *Request) {
			b.Embedder = stubEmbedder{vector: []float64{0, 0}, identity: b.Identity}
		}, contains: "embedding"},
		{name: "embedding outage", setup: func(b *Baseline, r *Request) {
			b.Embedder = stubEmbedder{err: fmt.Errorf("offline"), identity: b.Identity}
		}, contains: "offline"},
		{name: "model mismatch", setup: func(b *Baseline, r *Request) { b.Identity.Revision = "different" }, contains: "identity mismatch"},
		{name: "search outage", setup: func(b *Baseline, r *Request) { b.Cache = &stubCache{searchErr: fmt.Errorf("backend failed")} }, contains: "search", searches: 1},
		{name: "generation failure", setup: func(b *Baseline, r *Request) {
			b.Generator = stubGenerator{answer: GeneratedAnswer{Answer: "must not return"}, err: fmt.Errorf("failed generation")}
		}, contains: "generation", searches: 1, generations: 1},
		{name: "empty generation", setup: func(b *Baseline, r *Request) { b.Generator = stubGenerator{answer: GeneratedAnswer{Answer: " "}} }, contains: "empty answer", searches: 1, generations: 1},
		{name: "write failure", setup: func(b *Baseline, r *Request) { b.Cache = &stubCache{writeErr: fmt.Errorf("disk failed")} }, success: true, searches: 1, generations: 1, writeError: true},
		{name: "nonreusable answer", setup: func(b *Baseline, r *Request) {
			b.Generator = stubGenerator{answer: GeneratedAnswer{Answer: "private", Reusable: false}}
		}, success: true, searches: 1, generations: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b, _ := testBaseline(t)
			r := exampleRequest(exampleScopeA)
			tc.setup(b, &r)
			got := b.Serve(context.Background(), r)
			if got.Success != tc.success || !strings.Contains(got.Error, tc.contains) || got.SearchCount != tc.searches || got.GenerationCount != tc.generations || got.WriteCount != tc.writes || (got.CacheWriteError != "") != tc.writeError {
				t.Fatalf("result %+v", got)
			}
			if !got.Success && got.Answer != "" {
				t.Fatal("failed request returned answer")
			}
			if got.Timings.Source != "measured-wall-clock" || got.Timings.TotalNS < 0 {
				t.Fatal("missing or invalid error timing")
			}
		})
	}
	b, _ := testBaseline(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	got := b.Serve(ctx, exampleRequest(exampleScopeA))
	if got.Success || !strings.Contains(got.Error, "canceled") || got.GenerationCount != 0 {
		t.Fatalf("cancellation: %+v", got)
	}
}

func TestBaselineThresholdAndBackwardTime(t *testing.T) {
	for _, x := range []float64{0.9 - 1e-10, 0.9, 0.9 + 1e-10} {
		b, _ := testBaseline(t)
		putTest(t, b.Cache.(*ReferenceCache), "boundary", []float64{x, math.Sqrt(1 - x*x)})
		r := exampleRequest(exampleScopeA)
		candidate, err := b.Cache.Search(context.Background(), r, []float64{1, 0})
		if err != nil {
			t.Fatal(err)
		}
		got := b.Serve(context.Background(), r)
		if !got.Success || (candidate != nil) != (got.Source == "cache") {
			t.Fatalf("inconsistent threshold at %g: %+v", x, got)
		}
	}
	b, clock := testBaseline(t)
	r := exampleRequest(exampleScopeA)
	r.CacheEligible = false
	if got := b.Serve(context.Background(), r); !got.Success {
		t.Fatal(got.Error)
	}
	clock.Time = clock.Time.Add(-time.Second)
	r.Timestamp = clock.Time
	if got := b.Serve(context.Background(), r); got.Success || !strings.Contains(got.Error, "backward") {
		t.Fatalf("accepted backward ineligible request: %+v", got)
	}
}

func TestBaselineRevalidatesRetrievedAnswers(t *testing.T) {
	for _, mutate := range []func(*Entry){func(e *Entry) { e.Validity.Revoked = true }, func(e *Entry) { e.Scope.Context.TenantID = "other" }, func(e *Entry) { e.ExpiresAt = exampleNow }, func(e *Entry) { e.Validity.PolicyVersion = "old" }, func(e *Entry) { e.Embedding = []float64{0, 1} }, func(e *Entry) { e.Embedding = []float64{0, 0} }, func(e *Entry) { e.Answer = "" }} {
		b, _ := testBaseline(t)
		e := exampleEntry("bad", []float64{1, 0}, exampleScopeA)
		mutate(&e)
		b.Cache = &stubCache{candidate: &Candidate{Entry: &e, Similarity: 1}}
		got := b.Serve(context.Background(), exampleRequest(exampleScopeA))
		if !got.Success || got.Source != "generation" || got.GenerationCount != 1 {
			t.Fatalf("invalid reuse: %+v", got)
		}
	}
}

func TestDemoAndSavedArtifacts(t *testing.T) {
	data, err := os.ReadFile("../../configs/experiment.json")
	if err != nil {
		t.Fatal(err)
	}
	config, err := LoadConfig(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	embedder, err := DemoRecordedEmbedder()
	if err != nil {
		t.Fatal(err)
	}
	first, err := RunDemo(context.Background(), config, embedder)
	if err != nil {
		t.Fatal(err)
	}
	second, err := RunDemo(context.Background(), config, embedder)
	if err != nil {
		t.Fatal(err)
	}
	expected := []string{"generation", "cache", "generation", "generation"}
	for i, r := range first {
		if !r.Success || r.Source != expected[i] || r.SearchCount != 1 || r.Timings.Source != "measured-wall-clock" {
			t.Fatalf("request %d: %+v", i, r)
		}
		if second[i].Source != r.Source || second[i].Answer != r.Answer {
			t.Fatal("nondeterministic replay")
		}
	}
	dir := t.TempDir()
	if err := SaveDemo(dir, "../..", "recorded", "", config, first); err != nil {
		t.Fatal(err)
	}
	var manifest struct {
		Artifacts map[string]string `json:"artifacts_sha256"`
		SourceSHA string            `json:"source_sha256"`
	}
	manifestData, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(manifestData, &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.SourceSHA == "" {
		t.Fatal("no source identity")
	}
	for name, hash := range manifest.Artifacts {
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil || hashBytes(data) != hash {
			t.Fatalf("artifact %s: %v", name, err)
		}
	}
	vectors, err := os.Open(filepath.Join(dir, "embeddings.json"))
	if err != nil {
		t.Fatal(err)
	}
	defer vectors.Close()
	if _, err := LoadRecordedEmbedder(vectors, MiniLMIdentity()); err != nil {
		t.Fatal(err)
	}
}
