package semanticcache

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"time"
)

// FinalCandidate freezes a seed family and its exact embedding/policy/budget
// identity. An unsuccessful tuning result explicitly freezes always-search.
type FinalCandidate struct {
	Version              string              `json:"version"`
	Router               string              `json:"router"`
	FallbackRouter       string              `json:"fallback_router"`
	Status               string              `json:"status"`
	SelectedVariant      string              `json:"selected_variant"`
	Experiment           Config              `json:"experiment"`
	Identity             EmbeddingIdentity   `json:"embedding_identity"`
	Projections          []LSHConfig         `json:"projections"`
	ProjectionIDs        []string            `json:"projection_ids"`
	Assessments          []VariantAssessment `json:"assessments"`
	Reason               string              `json:"reason"`
	TuningDirectory      string              `json:"tuning_directory"`
	TuningManifestSHA256 string              `json:"tuning_manifest_sha256"`
	SweepSHA256          string              `json:"sweep_sha256"`
	FrozenAt             time.Time           `json:"frozen_at_utc"`
}

func SelectCandidate(result TuningResult, identity EmbeddingIdentity) (FinalCandidate, error) {
	f := FinalCandidate{Version: "phase4-candidate-v1", Router: "always-search", FallbackRouter: "always-search", Status: "no-qualifying-lsh", SelectedVariant: "always-search", Experiment: result.Config, Identity: identity}
	gridBytes, err := json.Marshal(result.Grid)
	if err != nil {
		return f, err
	}
	if _, err := LoadTuningGrid(bytes.NewReader(gridBytes)); err != nil {
		return f, err
	}
	assessments, err := AssessTuning(result)
	if err != nil {
		return f, err
	}
	f.Assessments = assessments
	var best *VariantAssessment
	for i := range assessments {
		a := &assessments[i]
		if a.Tables == 0 || a.PassingConditions != a.Conditions {
			continue
		}
		if best == nil || a.TotalCostUSD < best.TotalCostUSD {
			best = a
		}
	}
	if best == nil {
		f.Reason = "No LSH family meets every frozen tuning constraint. Retain always-search; continue later robustness and break-even analysis without a benefit claim."
		return f, nil
	}
	f.Router, f.Status, f.SelectedVariant = "lsh", "qualified-lsh", best.Variant
	f.Reason = "Lowest total simulated Mode B serving cost among LSH families passing every declared tuning condition; projection seeds are assessed together. Equal costs use lexical variant order."
	for _, seed := range result.Config.ProjectionSeeds {
		config := LSHConfig{Version: "cosine-hyperplanes-v1", Identity: identity, Seed: seed, Tables: best.Tables, Bits: best.Bits}
		p, err := NewProjections(config)
		if err != nil {
			return f, err
		}
		for _, r := range result.Rows {
			if r.Variant == best.Variant && r.ProjectionSeed == seed && r.Metrics.PredictorID != p.ID() {
				return f, fmt.Errorf("selected projection identity differs from tuning evidence")
			}
		}
		f.Projections = append(f.Projections, config)
		f.ProjectionIDs = append(f.ProjectionIDs, p.ID())
	}
	return f, nil
}

func LoadCandidate(r io.Reader) (FinalCandidate, error) {
	var f FinalCandidate
	if err := decodeStrict(r, &f); err != nil {
		return f, err
	}
	return f, f.Validate()
}

func (f FinalCandidate) Validate() error {
	if err := f.Experiment.Validate(); err != nil {
		return err
	}
	if err := f.Identity.Validate(); err != nil {
		return err
	}
	if f.Identity.Model != f.Experiment.Embedding.Model || f.Identity.Dimension != f.Experiment.Embedding.Dimension || f.Identity.Preprocessing != f.Experiment.Embedding.Preprocessing {
		return fmt.Errorf("candidate embedding mismatch")
	}
	if f.Version != "phase4-candidate-v1" || f.FallbackRouter != "always-search" || f.Reason == "" || f.TuningDirectory == "" || f.FrozenAt.IsZero() {
		return fmt.Errorf("incomplete frozen candidate")
	}
	for _, s := range []string{f.TuningManifestSHA256, f.SweepSHA256} {
		b, err := hex.DecodeString(s)
		if err != nil || len(b) != 32 {
			return fmt.Errorf("invalid candidate evidence hash")
		}
	}
	if f.Router == "always-search" {
		if f.Status != "no-qualifying-lsh" || f.SelectedVariant != "always-search" || len(f.Projections) != 0 || len(f.ProjectionIDs) != 0 {
			return fmt.Errorf("invalid fallback configuration")
		}
		return nil
	}
	if f.Router != "lsh" || f.Status != "qualified-lsh" || len(f.Projections) != len(f.Experiment.ProjectionSeeds) || len(f.ProjectionIDs) != len(f.Projections) {
		return fmt.Errorf("invalid candidate router")
	}
	for i, c := range f.Projections {
		if c.Identity != f.Identity || c.Seed != f.Experiment.ProjectionSeeds[i] || f.SelectedVariant != fmt.Sprintf("lsh-L%d-K%d", c.Tables, c.Bits) {
			return fmt.Errorf("candidate projection metadata mismatch")
		}
		p, err := NewProjections(c)
		if err != nil {
			return err
		}
		if p.ID() != f.ProjectionIDs[i] {
			return fmt.Errorf("candidate projection coefficients mismatch")
		}
	}
	return nil
}

func (f FinalCandidate) Predictors() ([]ReplayPredictor, error) {
	if err := f.Validate(); err != nil {
		return nil, err
	}
	ps := []ReplayPredictor{&ConstantPredictor{Search: true}}
	for _, c := range f.Projections {
		p, err := NewProjections(c)
		if err != nil {
			return nil, err
		}
		s, err := NewLSHSummary(p)
		if err != nil {
			return nil, err
		}
		ps = append(ps, s)
	}
	return ps, nil
}
