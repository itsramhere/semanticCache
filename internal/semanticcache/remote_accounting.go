package semanticcache

import (
	"fmt"
	"strings"
)

type RemoteAccounting struct {
	Router                                                                                       string `json:"router"`
	Requests, Searches, Generations, Writes, AdminCalls, FailedHTTP, Failures, BackendMismatches int
	Confusion                                                                                    Confusion    `json:"confusion"`
	Recall                                                                                       Metric       `json:"recall"`
	LookupReduction                                                                              Metric       `json:"lookup_reduction"`
	Stages                                                                                       StageTimings `json:"measured_stage_totals"`
	MeasuredP50MS                                                                                Metric       `json:"measured_routing_p50_ms"`
	MeasuredP95MS                                                                                Metric       `json:"measured_routing_p95_ms"`
	EstimatedP50MS                                                                               Metric       `json:"mixed_end_to_end_p50_ms"`
	EstimatedP95MS                                                                               Metric       `json:"mixed_end_to_end_p95_ms"`
	MeasuredRoutingTotalMS                                                                       float64      `json:"measured_routing_total_ms_including_build_rebuild"`
	MixedTotalMS                                                                                 float64      `json:"mixed_end_to_end_total_ms_including_build_rebuild"`
	RebuildNS                                                                                    int64        `json:"measured_rebuild_ns"`
	AmortizedMaintenanceNS                                                                       float64      `json:"amortized_build_update_rebuild_ns_per_request"`
	PeakRebuildBytes                                                                             int64        `json:"estimated_peak_rebuild_overlap_bytes"`
	SampledProcessHeapBytes                                                                      uint64       `json:"sampled_process_heap_peak_bytes"`
	SummaryBytes                                                                                 int64        `json:"structural_summary_bytes"`
	LocalCPUMS                                                                                   float64      `json:"measured_local_routing_cpu_proxy_ms"`
	NonSearchCostUSD                                                                             float64      `json:"simulated_nonsearch_cost_usd"`
	ServingCostUSD                                                                               float64      `json:"simulated_serving_cost_usd_including_admin_build_rebuild"`
}

type RemoteComparison struct {
	Boundary                 string              `json:"boundary"`
	CostInputs               Config              `json:"cost_inputs"`
	Runs                     []RemoteAccounting  `json:"runs"`
	SavedSearches            int                 `json:"saved_searches"`
	GenerationDelta          int                 `json:"generation_delta"`
	CostDeltaUSD             float64             `json:"simulated_cost_delta_usd"`
	MixedLatencyDeltaMS      float64             `json:"mixed_total_latency_delta_ms"`
	BreakEvenLookupCostUSD   Metric              `json:"break_even_simulated_lookup_cost_usd"`
	BreakEvenLookupLatencyMS Metric              `json:"break_even_uniform_simulated_lookup_latency_ms"`
	Sensitivity              []RemoteSensitivity `json:"simulated_sensitivity"`
}

type RemoteSensitivity struct {
	LookupCostUSD       float64 `json:"lookup_cost_usd"`
	LookupLatencyMS     float64 `json:"lookup_latency_ms"`
	CostDeltaUSD        float64 `json:"paired_cost_delta_usd"`
	TotalLatencyDeltaMS float64 `json:"paired_total_latency_delta_ms"`
}

func accountRemote(config Config, run RemoteRun) (RemoteAccounting, error) {
	m := RemoteAccounting{Router: run.Router, Requests: len(run.Rows), SummaryBytes: run.MemoryBytes}
	if len(run.Rows) == 0 || run.BuildNS < 0 || run.MemoryBytes < 0 {
		return m, fmt.Errorf("empty or invalid remote run")
	}
	var measured, mixed []float64
	var httpNS int64
	for _, call := range append(append([]RemoteCall(nil), run.SetupCalls...), run.CleanupCalls...) {
		if call.DurationNS < 0 {
			return m, fmt.Errorf("negative admin HTTP duration")
		}
		m.AdminCalls++
		if call.Error != "" {
			m.FailedHTTP++
		}
	}
	seen := make(map[string]bool, len(run.Rows))
	for _, row := range run.Rows {
		r := row.Result
		s := r.Timings
		if row.Request.ID == "" || seen[row.Request.ID] || r.RequestID != row.Request.ID || s.TotalNS < 0 || r.Decision == nil {
			return m, fmt.Errorf("invalid remote trace")
		}
		seen[row.Request.ID] = true
		remainingNS := s.TotalNS
		for _, duration := range []int64{s.EmbeddingNS, s.SearchNS, s.ValidationNS, s.GenerationNS, s.CacheWriteNS, s.PredictorNS, s.UpdateNS} {
			if duration < 0 || duration > remainingNS {
				return m, fmt.Errorf("invalid remote stage durations")
			}
			remainingNS -= duration
		}
		for _, count := range []int{r.EmbeddingCount, r.SearchCount, r.GenerationCount, r.WriteCount} {
			if count < 0 || count > 1 {
				return m, fmt.Errorf("invalid remote operation count")
			}
		}
		if r.GenerationCount > r.EmbeddingCount || r.WriteCount > r.GenerationCount || (!row.Request.CacheEligible && (r.SearchCount > 0 || r.WriteCount > 0)) {
			return m, fmt.Errorf("inconsistent remote operation counts")
		}
		if row.Request.CacheEligible && r.SearchCount != 0 && !r.Decision.Search {
			return m, fmt.Errorf("skipped decision performed a search")
		}
		if run.Router == "always-search" && row.Request.CacheEligible && !r.Decision.Search {
			return m, fmt.Errorf("always-search baseline bypassed")
		}
		m.Stages.EmbeddingNS += s.EmbeddingNS
		m.Stages.SearchNS += s.SearchNS
		m.Stages.ValidationNS += s.ValidationNS
		m.Stages.GenerationNS += s.GenerationNS
		m.Stages.CacheWriteNS += s.CacheWriteNS
		m.Stages.PredictorNS += s.PredictorNS
		m.Stages.UpdateNS += s.UpdateNS
		m.Stages.TotalNS += s.TotalNS
		if !r.Success || r.CacheWriteError != "" {
			m.Failures++
		}
		if row.BackendMismatch {
			m.BackendMismatches++
		}
		m.Generations += r.GenerationCount
		searches := 0
		for _, call := range row.Calls {
			if call.DurationNS < 0 {
				return m, fmt.Errorf("negative HTTP duration")
			}
			httpNS += call.DurationNS
			if call.Error != "" {
				m.FailedHTTP++
			}
			if strings.HasSuffix(call.Path, "/points/query") {
				searches++
				m.Searches++
			} else {
				m.Writes++
			}
		}
		if searches != r.SearchCount {
			return m, fmt.Errorf("HTTP/search counters disagree")
		}
		if row.Request.CacheEligible {
			m.Confusion.N++
			if r.Decision.Search {
				if row.OracleHit {
					m.Confusion.TP++
				} else {
					m.Confusion.FP++
				}
			} else {
				if row.OracleHit {
					m.Confusion.FN++
				} else {
					m.Confusion.TN++
				}
			}
		}
		ms := float64(s.TotalNS) / 1e6
		// Replace the precomputed embedding/fake generation stage; never add
		// model delay on top of an already measured live-model stage.
		estimate := ms - float64(s.EmbeddingNS+s.GenerationNS)/1e6 + float64(r.EmbeddingCount)*config.Assumptions["embedding"].LatencyMS + float64(r.GenerationCount)*config.Assumptions["generation"].LatencyMS
		measured = append(measured, ms)
		mixed = append(mixed, estimate)
		m.MixedTotalMS += estimate
	}
	for _, r := range run.Rebuilds {
		if r.DurationNS < 0 || r.PeakStructuralBytes < 0 || r.AfterRequest <= 0 || r.AfterRequest > m.Requests {
			return m, fmt.Errorf("invalid rebuild evidence")
		}
		m.RebuildNS += r.DurationNS
		if r.PeakStructuralBytes > m.PeakRebuildBytes {
			m.PeakRebuildBytes = r.PeakStructuralBytes
		}
		if r.SampledHeapBytes > m.SampledProcessHeapBytes {
			m.SampledProcessHeapBytes = r.SampledHeapBytes
		}
		// Charge the synchronous rebuild to its triggering request for tail
		// estimates; initial build is amortized in totals, not request tails.
		measured[r.AfterRequest-1] += float64(r.DurationNS) / 1e6
		mixed[r.AfterRequest-1] += float64(r.DurationNS) / 1e6
	}
	m.Stages.Source = "measured routing with precomputed embeddings and simulated fake generation"
	m.Recall = ratio(m.Confusion.TP, m.Confusion.TP+m.Confusion.FN)
	m.LookupReduction = ratio(m.Confusion.FN+m.Confusion.TN, m.Confusion.N)
	m.MeasuredP50MS = quantile(measured, .5)
	m.MeasuredP95MS = quantile(measured, .95)
	m.EstimatedP50MS = quantile(mixed, .5)
	m.EstimatedP95MS = quantile(mixed, .95)
	m.AmortizedMaintenanceNS = float64(run.BuildNS+m.RebuildNS+m.Stages.UpdateNS) / float64(m.Requests)
	m.MeasuredRoutingTotalMS = float64(m.Stages.TotalNS+run.BuildNS+m.RebuildNS) / 1e6
	m.MixedTotalMS += float64(run.BuildNS+m.RebuildNS) / 1e6
	// A wall-clock residual, not a profiler CPU measurement. Serialization,
	// validation, prediction and maintenance are included, HTTP wait excluded.
	localNS := m.Stages.TotalNS - httpNS - m.Stages.EmbeddingNS - m.Stages.GenerationNS
	if localNS < 0 {
		return m, fmt.Errorf("HTTP/stage durations exceed request timing")
	}
	m.LocalCPUMS = float64(localNS+run.BuildNS+m.RebuildNS) / 1e6
	embeddings := 0
	for _, row := range run.Rows {
		embeddings += row.Result.EmbeddingCount
	}
	m.NonSearchCostUSD = float64(embeddings)*config.Assumptions["embedding"].CostUSD + float64(m.Generations)*config.Assumptions["generation"].CostUSD + float64(m.Writes+m.AdminCalls)*config.Assumptions["cache_write"].CostUSD + m.LocalCPUMS*config.Assumptions["local_cpu_ms"].CostUSD
	m.ServingCostUSD = m.NonSearchCostUSD + float64(m.Searches)*config.Assumptions["search"].CostUSD
	return m, nil
}

// AccountRemote compares observed paired counts, never infers extra generation
// from FN alone. Break-even is for total cost/time, not a p95 threshold.
func AccountRemote(config Config, runs []RemoteRun) (RemoteComparison, error) {
	c := RemoteComparison{CostInputs: config, Boundary: "mixed experiment: measured loopback HTTP/local routing; precomputed real MiniLM embeddings; simulated embedding/generation prices and latencies; simulated per-HTTP search/write prices; measured wall-clock local residual priced by simulated CPU rate; no added network delay. Build/rebuild charged once per run; collection setup/cleanup charged as write attempts. Heap samples are process-wide lower bounds, structural overlap is an estimate. No final evaluation or production benefit claim."}
	if err := config.Validate(); err != nil {
		return c, err
	}
	if len(runs) != 2 || runs[0].Router != "always-search" || runs[1].Router != "exploratory-lsh" || len(runs[0].Rows) != len(runs[1].Rows) {
		return c, fmt.Errorf("expected paired baseline and predictor")
	}
	for i := range runs[0].Rows {
		if runs[0].Rows[i].Request != runs[1].Rows[i].Request {
			return c, fmt.Errorf("unpaired remote requests")
		}
	}
	for _, run := range runs {
		m, err := accountRemote(config, run)
		if err != nil {
			return c, err
		}
		c.Runs = append(c.Runs, m)
	}
	a, b := c.Runs[0], c.Runs[1]
	c.SavedSearches = a.Searches - b.Searches
	c.GenerationDelta = b.Generations - a.Generations
	c.CostDeltaUSD = b.ServingCostUSD - a.ServingCostUSD
	c.MixedLatencyDeltaMS = b.MixedTotalMS - a.MixedTotalMS
	nonsearch := b.NonSearchCostUSD - a.NonSearchCostUSD
	latencyResidual := (b.MixedTotalMS - float64(b.Stages.SearchNS)/1e6) - (a.MixedTotalMS - float64(a.Stages.SearchNS)/1e6)
	if c.SavedSearches > 0 {
		c.BreakEvenLookupCostUSD = Metric{nonsearch / float64(c.SavedSearches), true}
		c.BreakEvenLookupLatencyMS = Metric{latencyResidual / float64(c.SavedSearches), true}
	}
	for i, latency := range []float64{0, .01, .1, 1, 5, 20, 100, 1000} {
		price := []float64{0, .0000001, .000001, .00001, .0001, .001, .01, .1}[i]
		c.Sensitivity = append(c.Sensitivity, RemoteSensitivity{price, latency, nonsearch - float64(c.SavedSearches)*price, latencyResidual - float64(c.SavedSearches)*latency})
	}
	return c, nil
}
