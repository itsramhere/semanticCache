package semanticcache

import (
	"bytes"
	"encoding/json"
	"math"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestExperimentConfig(t *testing.T) {
	b, err := os.ReadFile("../../configs/experiment.json")
	if err != nil {
		t.Fatal(err)
	}
	c, err := LoadConfig(bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	if c.Policy.CosineThreshold != 0.9 || c.MinHitRecall != 0.99 || c.MaxP95LatencyMS != 1200 || c.MaxSummaryBytes != 8388608 {
		t.Fatal("frozen Phase 0 targets changed; version the protocol before changing them")
	}
	if c.Policy.CosineThreshold != PolicyV1CosineThreshold || c.Policy.TTLSeconds != PolicyV1TTLSeconds {
		t.Fatalf("configuration drifted from the frozen policy constants: threshold %v, ttl %d s",
			c.Policy.CosineThreshold, c.Policy.TTLSeconds)
	}
	for _, suffix := range []string{` {}`, ` trailing`} {
		if _, err := LoadConfig(strings.NewReader(string(b) + suffix)); err == nil {
			t.Fatal("accepted trailing content")
		}
	}
	if _, err := LoadConfig(strings.NewReader(strings.Replace(string(b), `"id":`, `"typo":`, 1))); err == nil {
		t.Fatal("accepted unknown field")
	}
	if _, err := LoadConfig(strings.NewReader(`{}`)); err == nil {
		t.Fatal("accepted missing settings")
	}
	cases := []struct {
		name   string
		change func(*Config)
	}{
		{"threshold", func(c *Config) { c.Policy.CosineThreshold = 1.1 }},
		{"nonfinite threshold", func(c *Config) { c.Policy.CosineThreshold = math.NaN() }},
		{"ttl", func(c *Config) { c.Policy.TTLSeconds = 0 }},
		{"capacity", func(c *Config) { c.Policy.CapacityEntries = []int{0} }},
		{"duplicate capacity", func(c *Config) { c.Policy.CapacityEntries = []int{100, 100} }},
		{"identity", func(c *Config) { c.Embedding.Model = "" }},
		{"dimension", func(c *Config) { c.Embedding.Dimension = 0 }},
		{"workload", func(c *Config) { c.RequestsPerSplit = 0 }},
		{"seeds", func(c *Config) { c.ProjectionSeeds = []int64{1, 1, 1} }},
		{"recall", func(c *Config) { c.MinHitRecall = 0 }},
		{"p95", func(c *Config) { c.MaxP95LatencyMS = -1 }},
		{"memory", func(c *Config) { c.MaxSummaryBytes = 0 }},
		{"cost", func(c *Config) { a := c.Assumptions["search"]; a.CostUSD = -1; c.Assumptions["search"] = a }},
		{"source", func(c *Config) {
			a := c.Assumptions["generation"]
			a.Source = "measured"
			c.Assumptions["generation"] = a
		}},
		{"missing assumption", func(c *Config) { delete(c.Assumptions, "embedding") }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var changed Config
			if err := json.Unmarshal(b, &changed); err != nil {
				t.Fatal(err)
			}
			tc.change(&changed)
			if err := changed.Validate(); err == nil {
				t.Fatal("accepted invalid configuration")
			}
		})
	}
}

func TestTTLDurationBounds(t *testing.T) {
	data, err := os.ReadFile("../../configs/experiment.json")
	if err != nil {
		t.Fatal(err)
	}
	config, err := LoadConfig(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	maxSeconds := int64((1<<63 - 1) / time.Second)
	if strconv.IntSize < 64 {
		maxSeconds = int64(1<<31 - 1)
	}
	for _, tc := range []struct {
		name    string
		seconds int64
		valid   bool
	}{
		{"one second", 1, true},
		{"maximum duration", maxSeconds, true},
		{"overflow duration", maxSeconds + 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if strconv.IntSize < 64 && !tc.valid {
				t.Skip("all positive int TTLs fit time.Duration on 32-bit platforms")
			}
			config.Policy.TTLSeconds = int(tc.seconds)
			if err := config.Validate(); (err == nil) != tc.valid {
				t.Errorf("configuration TTL %d: %v; valid=%t", tc.seconds, err, tc.valid)
			}
			clock := &LogicalClock{Time: exampleNow}
			cache, err := NewReferenceCache(config.Policy, 2, 1, clock)
			if (err == nil) != tc.valid {
				t.Fatalf("cache TTL %d: %v; valid=%t", tc.seconds, err, tc.valid)
			}
			if tc.valid {
				putTest(t, cache, "live", []float64{1, 0})
				winnerTest(t, cache, exampleRequest(exampleScopeA), []float64{1, 0}, "live")
			}
		})
	}
}
