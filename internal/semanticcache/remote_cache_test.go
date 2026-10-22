package semanticcache

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestRemoteFailureAndCancellation(t *testing.T) {
	for _, mode := range []string{"http", "malformed", "timeout", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/points") {
					if mode == "timeout" || mode == "cancel" {
						select {
						case <-r.Context().Done():
						case <-time.After(100 * time.Millisecond):
						}
						return
					}
					if mode == "http" {
						http.Error(w, "unavailable", 503)
						return
					}
					fmt.Fprint(w, `{"status":"ok"}`)
					return
				}
				fmt.Fprint(w, `{"status":"ok","result":true}`)
			}))
			defer server.Close()
			local, clock := testCache(t, 2)
			p := smallProjections(t)
			s, _ := NewLSHSummary(p)
			c, err := NewRemoteCache(context.Background(), server.URL, "test", 30*time.Millisecond, local.policy, p.Config.Identity, 2, clock, s)
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			if mode == "cancel" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			before := c.local.Snapshot()
			if err := c.Put(ctx, exampleEntry("a", []float64{1, 0}, exampleScopeA)); err == nil {
				t.Fatal("accepted failed write")
			}
			if mode == "cancel" {
				if !reflect.DeepEqual(before, c.local.Snapshot()) || len(c.Calls) != 1 || !s.Ready() {
					t.Fatal("pre-cancellation changed cache")
				}
				return
			}
			if !reflect.DeepEqual(before, c.local.Snapshot()) || s.Ready() || c.broken == nil {
				t.Fatal("failed write committed or did not latch fallback")
			}
			r := exampleRequest(exampleScopeA)
			n := len(c.Calls)
			if d := c.predict(r, []float64{1, 0}, p.Config.Identity); !d.Search || len(c.Calls) != n {
				t.Fatal("unsafe decision or remote prediction call")
			}
			if _, err := c.Search(context.Background(), r, []float64{1, 0}); err == nil {
				t.Fatal("backend failure became miss")
			}
		})
	}
}

// Run explicitly with SEMCACHE_QDRANT_URL. Normal unit tests stay offline.
func TestRemoteLiveParity(t *testing.T) {
	endpoint := os.Getenv("SEMCACHE_QDRANT_URL")
	if endpoint == "" {
		t.Skip("set SEMCACHE_QDRANT_URL for real Qdrant parity")
	}
	ctx := context.Background()
	ref, clock := testCache(t, 3)
	p := smallProjections(t)
	s, _ := NewLSHSummary(p)
	c, err := NewRemoteCache(ctx, endpoint, fmt.Sprintf("phase61_%d", time.Now().UnixNano()), 3*time.Second, ref.policy, p.Config.Identity, 3, clock, s)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := c.call(ctx, "DELETE", c.path(""), nil, nil); err != nil {
			t.Error(err)
		}
	}()
	put := func(id string, v []float64, scope Scope) {
		t.Helper()
		e := exampleEntry(id, v, scope)
		if err := ref.Put(ctx, e); err != nil {
			t.Fatal(err)
		}
		if err := c.Put(ctx, e); err != nil {
			t.Fatal(err)
		}
	}
	check := func(scope Scope, v []float64) {
		t.Helper()
		r := exampleRequest(scope)
		r.Timestamp = clock.Time
		a, err := ref.Search(ctx, r, v)
		if err != nil {
			t.Fatal(err)
		}
		b, err := c.Search(ctx, r, v)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(a, b) {
			t.Fatalf("remote/reference differ: %#v %#v", a, b)
		}
		n := len(c.Calls)
		s.Predict(r, v)
		if len(c.Calls) != n {
			t.Fatal("prediction called backend")
		}
	}
	check(exampleScopeA, []float64{1, 0})
	put("z", []float64{1, 0}, exampleScopeA)
	put("a", []float64{1, 0}, exampleScopeA)
	// Cosine scores are similarities, with float32 rounding at the backend.
	for _, tc := range []struct {
		v    []float64
		want float64
	}{{[]float64{1, 0}, 1}, {[]float64{0, 1}, 0}, {[]float64{-1, 0}, -1}, {[]float64{.9, math.Sqrt(.19)}, .9}} {
		var result struct {
			Points []struct {
				Score float64 `json:"score"`
			} `json:"points"`
		}
		if err := c.call(ctx, "POST", c.path("/points/query"), map[string]any{"query": tc.v, "params": map[string]any{"exact": true}, "limit": 3}, &result); err != nil {
			t.Fatal(err)
		}
		if len(result.Points) != 2 {
			t.Fatal("known-vector candidate count")
		}
		for _, point := range result.Points {
			if math.Abs(point.Score-tc.want) > 1e-6 {
				t.Fatalf("backend cosine %g want %g", point.Score, tc.want)
			}
		}
	}
	check(exampleScopeA, []float64{1, 0})
	check(exampleScopeB, []float64{1, 0})
	// Known cosine values 1, 0, -1 and an inclusive near-threshold vector.
	for _, v := range [][]float64{{1, 0}, {0, 1}, {-1, 0}, {.9, math.Sqrt(.19)}, {math.Nextafter(.9, 0), math.Sqrt(.19)}} {
		check(exampleScopeA, v)
	}
	put("other", []float64{1, 0}, exampleScopeB)
	check(exampleScopeB, []float64{1, 0})
	put("a", []float64{0, 1}, exampleScopeA)
	check(exampleScopeA, []float64{0, 1})
	put("evict", []float64{-1, 0}, exampleScopeA)
	check(exampleScopeA, []float64{1, 0})
	clock.Time = clock.Time.Add(time.Hour)
	check(exampleScopeA, []float64{-1, 0})
	b, _ := json.MarshalIndent(c.Calls, "", "  ")
	t.Logf("real HTTP calls: %s", b)
}

func TestRemoteRoutersExposeSearchErrors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/query") {
			http.Error(w, "outage", 503)
			return
		}
		fmt.Fprint(w, `{"status":"ok","result":true}`)
	}))
	defer server.Close()
	for _, predict := range []bool{false, true} {
		identity := MiniLMIdentity()
		projections, err := NewProjections(LSHConfig{"cosine-hyperplanes-v1", identity, 1001, 2, 3})
		if err != nil {
			t.Fatal(err)
		}
		s, _ := NewLSHSummary(projections)
		clock := &LogicalClock{Time: DemoRequests()[0].Timestamp}
		policy := PolicyConfig{ID: PolicyV1ID, CosineThreshold: .9, TTLSeconds: 3600}
		c, err := NewRemoteCache(context.Background(), server.URL, "outage", time.Second, policy, identity, 3, clock, s)
		if err != nil {
			t.Fatal(err)
		}
		s.Invalidate()
		embedder, _ := DemoRecordedEmbedder()
		b := Baseline{Embedder: embedder, Cache: c, Generator: FakeLLM{}, Clock: clock, PolicyID: policy.ID, Threshold: .9, Identity: identity}
		if predict {
			b.Predictor = s
		}
		r := b.Serve(context.Background(), DemoRequests()[0])
		if r.Success || r.SearchCount != 1 || r.GenerationCount != 0 || !strings.Contains(r.Error, "503") {
			t.Fatalf("outage treated as miss: %+v", r)
		}
	}
}

func TestRemoteLiveMeasuredReplay(t *testing.T) {
	endpoint := os.Getenv("SEMCACHE_QDRANT_URL")
	if endpoint == "" {
		t.Skip("live Qdrant not enabled")
	}
	runs, err := RunRemoteMeasured(context.Background(), endpoint, 2, 4)
	if err != nil {
		t.Fatal(err)
	}
	config, _ := accountingFixture(t)
	c, err := AccountRemote(config, runs)
	if err != nil {
		t.Fatal(err)
	}
	if c.SavedSearches != 2 || c.GenerationDelta != 0 || len(runs[1].Rebuilds) != 2 {
		t.Fatalf("unexpected paired counters: %+v", c)
	}
	for _, m := range c.Runs {
		if m.BackendMismatches != 0 || m.FailedHTTP != 0 || m.Failures != 0 {
			t.Fatalf("live serving failure: %+v", m)
		}
	}
}
