package semanticcache

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"time"
)

// The frozen policy constants below must stay consistent with
// configs/experiment.json, docs/cache_policy.md and docs/experiment_protocol.md.
// Tests cross-check the JSON configuration against them.
const (
	// PolicyV1ID identifies the matching policy frozen in Phase 0.1.
	PolicyV1ID = "faq-cosine-v1"
	// PolicyV1CosineThreshold is the inclusive minimum cosine similarity for a
	// hit, decided on the computed float64 value with no epsilon.
	PolicyV1CosineThreshold = 0.90
	// PolicyV1TTLSeconds is the fixed entry lifetime in logical seconds.
	PolicyV1TTLSeconds = 3600
	// TTLs are converted to nanoseconds before computing expiration times.
	maxTTLSeconds = int64((1<<63 - 1) / time.Second)
)

type PolicyConfig struct {
	ID              string  `json:"id"`
	CosineThreshold float64 `json:"cosine_threshold"`
	TTLSeconds      int     `json:"ttl_seconds"`
	CapacityEntries []int   `json:"capacity_entries"`
}

type EmbeddingConfig struct {
	Model         string `json:"model"`
	Dimension     int    `json:"dimension"`
	Preprocessing string `json:"preprocessing"`
}

// Assumption is an explicitly simulated per-operation input, not a measurement.
type Assumption struct {
	Source    string  `json:"source"`
	CostUSD   float64 `json:"cost_usd"`
	LatencyMS float64 `json:"latency_ms"`
}

type Config struct {
	ID               string                `json:"id"`
	Policy           PolicyConfig          `json:"policy"`
	Embedding        EmbeddingConfig       `json:"embedding"`
	WorkloadID       string                `json:"workload_id"`
	RequestsPerSplit int                   `json:"requests_per_split"`
	WorkloadSeeds    []int64               `json:"workload_seeds"`
	ProjectionSeeds  []int64               `json:"projection_seeds"`
	MinHitRecall     float64               `json:"min_hit_recall"`
	MaxP95LatencyMS  float64               `json:"max_p95_latency_ms"`
	MaxSummaryBytes  int64                 `json:"max_summary_bytes"`
	Assumptions      map[string]Assumption `json:"assumptions"`
}

// LoadConfig rejects unknown fields and trailing values to catch configuration typos.
func LoadConfig(r io.Reader) (Config, error) {
	var c Config
	d := json.NewDecoder(r)
	d.DisallowUnknownFields()
	if err := d.Decode(&c); err != nil {
		return c, err
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return c, fmt.Errorf("expected exactly one JSON configuration")
	}
	return c, c.Validate()
}

func finite(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) }

func (c Config) Validate() error {
	if c.ID == "" || c.Policy.ID == "" || c.WorkloadID == "" {
		return fmt.Errorf("configuration, policy and workload IDs are required")
	}
	if !finite(c.Policy.CosineThreshold) || c.Policy.CosineThreshold < -1 || c.Policy.CosineThreshold > 1 {
		return fmt.Errorf("cosine threshold must be finite and in [-1,1]")
	}
	if c.Policy.TTLSeconds <= 0 || int64(c.Policy.TTLSeconds) > maxTTLSeconds || len(c.Policy.CapacityEntries) == 0 {
		return fmt.Errorf("TTL must fit a positive time.Duration and cache capacities are required")
	}
	seen := map[int]bool{}
	for _, n := range c.Policy.CapacityEntries {
		if n <= 0 || seen[n] {
			return fmt.Errorf("cache capacities must be positive and distinct")
		}
		seen[n] = true
	}
	if c.Embedding.Model == "" || c.Embedding.Dimension <= 0 || c.Embedding.Preprocessing == "" {
		return fmt.Errorf("embedding identity, dimension and preprocessing are required")
	}
	if c.RequestsPerSplit <= 0 {
		return fmt.Errorf("requests per split must be positive")
	}
	for _, seeds := range [][]int64{c.WorkloadSeeds, c.ProjectionSeeds} {
		if len(seeds) < 3 {
			return fmt.Errorf("at least three workload and projection seeds are required")
		}
		seen := map[int64]bool{}
		for _, seed := range seeds {
			if seen[seed] {
				return fmt.Errorf("seeds within each stream must be distinct")
			}
			seen[seed] = true
		}
	}
	if !finite(c.MinHitRecall) || c.MinHitRecall <= 0 || c.MinHitRecall > 1 || !finite(c.MaxP95LatencyMS) || c.MaxP95LatencyMS <= 0 || c.MaxSummaryBytes <= 0 {
		return fmt.Errorf("positive latency/memory budgets and recall in (0,1] are required")
	}
	names := []string{"embedding", "search", "generation", "cache_write", "local_cpu_ms"}
	if len(c.Assumptions) != len(names) {
		return fmt.Errorf("exactly five documented cost/latency assumptions are required")
	}
	for _, name := range names {
		a, ok := c.Assumptions[name]
		if !ok || a.Source != "simulated" || !finite(a.CostUSD) || a.CostUSD < 0 || !finite(a.LatencyMS) || a.LatencyMS < 0 {
			return fmt.Errorf("%s requires nonnegative finite simulated cost/latency inputs", name)
		}
	}
	return nil
}
