package semanticcache

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
)

// Metric encodes an undefined ratio or quantile as literal "N/A" in JSON/CSV.
type Metric struct {
	Value   float64
	Defined bool
}

func (m Metric) MarshalJSON() ([]byte, error) {
	if !m.Defined {
		return []byte(`"N/A"`), nil
	}
	return json.Marshal(m.Value)
}
func (m Metric) String() string {
	if !m.Defined {
		return "N/A"
	}
	return strconv.FormatFloat(m.Value, 'g', -1, 64)
}
func ratio(n, d int) Metric {
	if d == 0 {
		return Metric{}
	}
	return Metric{float64(n) / float64(d), true}
}
func quantile(xs []float64, p float64) Metric {
	if len(xs) == 0 {
		return Metric{}
	}
	v := append([]float64(nil), xs...)
	sort.Float64s(v)
	return Metric{v[int(math.Ceil(p*float64(len(v))))-1], true}
}

type Confusion struct {
	TP int `json:"tp"`
	FP int `json:"fp"`
	FN int `json:"fn"`
	TN int `json:"tn"`
	N  int `json:"n"`
}
type Metrics struct {
	PredictorID                  string             `json:"predictor_id"`
	Mode                         string             `json:"mode"`
	Requests                     int                `json:"requests"`
	EligibleRequests             int                `json:"eligible_requests"`
	NoneligibleRequests          int                `json:"noneligible_requests"`
	LabeledRequests              int                `json:"labeled_requests"`
	Confusion                    Confusion          `json:"confusion"`
	LookupReduction              Metric             `json:"lookup_reduction"`
	CacheHitRecall               Metric             `json:"cache_hit_recall"`
	MissedHitFraction            Metric             `json:"missed_hit_fraction"`
	UnsuccessfulRoutedLookups    int                `json:"unsuccessful_routed_lookups"`
	UnsuccessfulRoutedFraction   Metric             `json:"unsuccessful_routed_fraction"`
	CorrectlyAvoidedMisses       int                `json:"correctly_avoided_misses"`
	CorrectlyAvoidedMissFraction Metric             `json:"correctly_avoided_miss_fraction"`
	ProductionEmbeddings         int                `json:"production_embedding_attempts"`
	ProductionSearches           int                `json:"production_searches"`
	ProductionGenerations        int                `json:"production_generation_attempts"`
	ProductionWrites             int                `json:"production_successful_writes"`
	DiagnosticSearches           int                `json:"diagnostic_searches"`
	Failures                     int                `json:"failed_requests"`
	WriteFailures                int                `json:"cache_write_failures"`
	Bypasses                     int                `json:"eligible_decision_bypasses"`
	CacheAnswers                 int                `json:"cache_answers"`
	ReuseLabeled                 int                `json:"answer_reuse_labeled"`
	CorrectReuse                 int                `json:"correct_answer_reuses"`
	ReuseAccuracy                Metric             `json:"answer_reuse_accuracy"`
	MeasuredP50MS                Metric             `json:"measured_routing_p50_ms"`
	MeasuredP95MS                Metric             `json:"measured_routing_p95_ms"`
	FailedP95MS                  Metric             `json:"measured_failed_request_p95_ms"`
	SimulatedP50MS               Metric             `json:"simulated_end_to_end_p50_ms"`
	SimulatedP95MS               Metric             `json:"simulated_end_to_end_p95_ms"`
	PredictorNS                  int64              `json:"measured_predictor_total_ns"`
	PredictorP95MS               Metric             `json:"measured_predictor_p95_ms"`
	UpdateNS                     int64              `json:"measured_update_total_ns"`
	BuildNS                      int64              `json:"measured_build_ns"`
	BuildAmortizationRequests    int                `json:"build_amortization_requests"`
	MemoryBytes                  int64              `json:"predictor_memory_bytes"`
	PeakRebuildMemory            string             `json:"peak_rebuild_memory"`
	ProcessMemory                string             `json:"process_memory"`
	MeasuredServingNS            int64              `json:"measured_serving_total_ns"`
	SimulatedCostUSD             float64            `json:"simulated_total_serving_cost_usd"`
	CostComponentsUSD            map[string]float64 `json:"simulated_cost_components_usd"`
	CostBoundary                 string             `json:"cost_boundary"`
	GenerationDelta              int                `json:"generation_delta_vs_always_search"`
	CostDeltaUSD                 float64            `json:"simulated_cost_delta_vs_always_search_usd"`
}

func Summarize(c Config, run ReplayRun) (Metrics, error) {
	m := Metrics{PredictorID: run.PredictorID, Mode: run.Options.Mode, Requests: len(run.Rows), DiagnosticSearches: run.DiagnosticSearches, BuildNS: run.BuildNS, BuildAmortizationRequests: len(run.Rows), MemoryBytes: run.MemoryBytes, PeakRebuildMemory: "N/A; no rebuild implementation in Phase 2", ProcessMemory: "N/A; not measured", CostComponentsUSD: map[string]float64{}, CostBoundary: "simulated per-operation assumptions plus measured local serving/build wall time converted using simulated CPU rate; excludes diagnostics, snapshots and logging; Phase 2 local fixture only"}
	if err := c.Validate(); err != nil {
		return m, err
	}
	if run.BuildNS < 0 || run.MemoryBytes < 0 {
		return m, fmt.Errorf("negative build time or memory")
	}
	var measured, simulated, failed, prediction []float64
	amortizedMS := 0.0
	if m.Requests > 0 {
		amortizedMS = float64(run.BuildNS) / 1e6 / float64(m.Requests)
	}
	for _, row := range run.Rows {
		r := row.Result
		t := r.Timings
		if r.EmbeddingCount < 0 || r.SearchCount < 0 || r.GenerationCount < 0 || r.WriteCount < 0 {
			return m, fmt.Errorf("negative production count")
		}
		for _, ns := range []int64{t.TotalNS, t.EmbeddingNS, t.SearchNS, t.ValidationNS, t.GenerationNS, t.CacheWriteNS, t.PredictorNS, t.UpdateNS} {
			if ns < 0 {
				return m, fmt.Errorf("negative stage timing")
			}
		}
		if row.Eligible {
			m.EligibleRequests++
			if !row.Decision.Search {
				m.Bypasses++
			}
		} else {
			m.NoneligibleRequests++
		}
		if row.Eligible && row.OracleHit != nil {
			m.Confusion.N++
			if row.Decision.Search {
				if *row.OracleHit {
					m.Confusion.TP++
				} else {
					m.Confusion.FP++
				}
			} else {
				if *row.OracleHit {
					m.Confusion.FN++
				} else {
					m.Confusion.TN++
				}
			}
		}
		m.ProductionEmbeddings += r.EmbeddingCount
		m.ProductionSearches += r.SearchCount
		m.ProductionGenerations += r.GenerationCount
		m.ProductionWrites += r.WriteCount
		if !r.Success {
			m.Failures++
			failed = append(failed, float64(t.TotalNS)/1e6)
		}
		if r.CacheWriteError != "" {
			m.WriteFailures++
		}
		if r.Source == "cache" {
			m.CacheAnswers++
			if row.AnswerReuseCorrect != nil {
				m.ReuseLabeled++
				if *row.AnswerReuseCorrect {
					m.CorrectReuse++
				}
			}
		}
		m.MeasuredServingNS += t.TotalNS
		m.PredictorNS += t.PredictorNS
		m.UpdateNS += t.UpdateNS
		ms := float64(t.TotalNS) / 1e6
		measured = append(measured, ms)
		if row.Eligible {
			prediction = append(prediction, float64(t.PredictorNS)/1e6)
		}
		estimate := ms + amortizedMS + float64(r.EmbeddingCount)*c.Assumptions["embedding"].LatencyMS + float64(r.SearchCount)*c.Assumptions["search"].LatencyMS + float64(r.GenerationCount)*c.Assumptions["generation"].LatencyMS + float64(r.WriteCount)*c.Assumptions["cache_write"].LatencyMS
		simulated = append(simulated, estimate)
	}
	x := m.Confusion
	m.LabeledRequests = x.N
	m.LookupReduction = ratio(x.FN+x.TN, x.N)
	m.CacheHitRecall = ratio(x.TP, x.TP+x.FN)
	m.MissedHitFraction = ratio(x.FN, x.TP+x.FN)
	m.UnsuccessfulRoutedLookups = x.FP
	m.UnsuccessfulRoutedFraction = ratio(x.FP, x.TP+x.FP)
	m.CorrectlyAvoidedMisses = x.TN
	m.CorrectlyAvoidedMissFraction = ratio(x.TN, x.TN+x.FP)
	m.ReuseAccuracy = ratio(m.CorrectReuse, m.ReuseLabeled)
	m.MeasuredP50MS = quantile(measured, 0.5)
	m.MeasuredP95MS = quantile(measured, 0.95)
	m.FailedP95MS = quantile(failed, 0.95)
	m.PredictorP95MS = quantile(prediction, 0.95)
	m.SimulatedP50MS = quantile(simulated, 0.5)
	m.SimulatedP95MS = quantile(simulated, 0.95)
	for name, count := range map[string]int{"embedding": m.ProductionEmbeddings, "search": m.ProductionSearches, "generation": m.ProductionGenerations, "cache_write": m.ProductionWrites} {
		m.CostComponentsUSD[name] = float64(count) * c.Assumptions[name].CostUSD
	}
	m.CostComponentsUSD["local_cpu_ms"] = (float64(m.MeasuredServingNS) + float64(m.BuildNS)) / 1e6 * c.Assumptions["local_cpu_ms"].CostUSD
	// Stable addition order makes cost output reproducible for saved timings.
	for _, name := range []string{"embedding", "search", "generation", "cache_write", "local_cpu_ms"} {
		m.SimulatedCostUSD += m.CostComponentsUSD[name]
	}
	return m, nil
}
