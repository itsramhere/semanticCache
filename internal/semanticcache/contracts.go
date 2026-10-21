// Package semanticcache implements the reference semantic-cache baseline.
package semanticcache

import "time"

// RequestContext contains every answer-affecting context field in the initial domain.
// Empty fields are invalid; callers must supply explicit values.
type RequestContext struct {
	TenantID         string `json:"tenant_id"`
	Locale           string `json:"locale"`
	ProductVersion   string `json:"product_version"`
	KnowledgeVersion string `json:"knowledge_version"`
}

type Request struct {
	ID            string         `json:"id"`
	Text          string         `json:"text"`
	Namespace     string         `json:"namespace"`
	Context       RequestContext `json:"context"`
	ModelVersion  string         `json:"model_version"`
	Timestamp     time.Time      `json:"timestamp"`
	CacheEligible bool           `json:"cache_eligible"`
}

// Scope uses exact, case-sensitive equality across all fields.
type Scope struct {
	Namespace    string         `json:"namespace"`
	Context      RequestContext `json:"context"`
	ModelVersion string         `json:"model_version"`
}

type ValidityMetadata struct {
	Reusable      bool   `json:"reusable"`
	Revoked       bool   `json:"revoked"`
	PolicyVersion string `json:"policy_version"`
}

// Entry embeddings will be normalized float64 vectors. Their dimension,
// embedding identity and preprocessing are fixed by the experiment config.
type Entry struct {
	ID         string           `json:"id"`
	Embedding  []float64        `json:"embedding"`
	Answer     string           `json:"answer"`
	Scope      Scope            `json:"scope"`
	InsertedAt time.Time        `json:"inserted_at"`
	ExpiresAt  time.Time        `json:"expires_at"`
	Validity   ValidityMetadata `json:"validity"`
}

func (r Request) Scope() Scope {
	return Scope{Namespace: r.Namespace, Context: r.Context, ModelVersion: r.ModelVersion}
}

func (e Entry) IsLive(t time.Time) bool {
	return !t.Before(e.InsertedAt) && t.Before(e.ExpiresAt)
}
