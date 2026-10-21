package semanticcache

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const MiniLMRevision = "1110a243fdf4706b3f48f1d95db1a4f5529b4d41"
const PreprocessingV1 = "trim-unicode-preserve-case-whitespace-l2-v1"

type EmbeddingIdentity struct {
	Model         string `json:"model"`
	Revision      string `json:"revision"`
	Dimension     int    `json:"dimension"`
	Preprocessing string `json:"preprocessing"`
}

func MiniLMIdentity() EmbeddingIdentity {
	return EmbeddingIdentity{"sentence-transformers/all-MiniLM-L6-v2", MiniLMRevision, 384, PreprocessingV1}
}
func (i EmbeddingIdentity) Validate() error {
	if i.Model == "" || i.Revision == "" || i.Dimension <= 0 || i.Preprocessing != PreprocessingV1 {
		return fmt.Errorf("unsupported or incomplete embedding identity")
	}
	return nil
}

type Embedder interface {
	Identity() EmbeddingIdentity
	Embed(context.Context, string) ([]float64, error)
}

// TEIEmbedder calls a real Text Embeddings Inference server, verifies its model
// identity on every call, and applies the project's preprocessing/normalization.
// The HTTP client has a deadline; caller cancellation propagates to both calls.
type TEIEmbedder struct {
	endpoint string
	identity EmbeddingIdentity
	client   *http.Client
}

func NewTEIEmbedder(endpoint string, identity EmbeddingIdentity) (*TEIEmbedder, error) {
	u, err := url.Parse(endpoint)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.RawQuery != "" || u.Fragment != "" || u.User != nil {
		return nil, fmt.Errorf("invalid TEI base URL")
	}
	if err := identity.Validate(); err != nil {
		return nil, err
	}
	return &TEIEmbedder{strings.TrimRight(endpoint, "/"), identity, &http.Client{Timeout: 30 * time.Second}}, nil
}
func (e *TEIEmbedder) Identity() EmbeddingIdentity { return e.identity }
func (e *TEIEmbedder) call(ctx context.Context, method, path string, body []byte, out any) error {
	req, err := http.NewRequestWithContext(ctx, method, e.endpoint+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := e.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("TEI %s: HTTP %d", path, resp.StatusCode)
	}
	// Read one byte beyond the budget so truncation cannot masquerade as EOF
	// after a valid JSON value followed by excessive whitespace or hidden data.
	limited := &io.LimitedReader{R: resp.Body, N: (4 << 20) + 1}
	d := json.NewDecoder(limited)
	if err := d.Decode(out); err != nil {
		return err
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF || limited.N == 0 {
		return fmt.Errorf("TEI returned trailing or oversized JSON")
	}
	return nil
}
func (e *TEIEmbedder) Embed(ctx context.Context, text string) ([]float64, error) {
	text = strings.TrimSpace(text)
	if text == "" {
		return nil, fmt.Errorf("empty embedding input")
	}
	var info struct {
		ModelID  string `json:"model_id"`
		ModelSHA string `json:"model_sha"`
	}
	if err := e.call(ctx, http.MethodGet, "/info", nil, &info); err != nil {
		return nil, err
	}
	if info.ModelID != e.identity.Model || info.ModelSHA != e.identity.Revision {
		return nil, fmt.Errorf("TEI model identity mismatch")
	}
	body, _ := json.Marshal(struct {
		Inputs   string `json:"inputs"`
		Truncate bool   `json:"truncate"`
	}{text, true})
	var vectors [][]float64
	if err := e.call(ctx, http.MethodPost, "/embed", body, &vectors); err != nil {
		return nil, err
	}
	if len(vectors) != 1 {
		return nil, fmt.Errorf("TEI returned %d embeddings, want one", len(vectors))
	}
	return normalizeDimension(vectors[0], e.identity.Dimension)
}

type EmbeddingRecord struct {
	RequestID string            `json:"request_id"`
	Text      string            `json:"text"`
	Identity  EmbeddingIdentity `json:"identity"`
	Vector    []float64         `json:"vector"`
}
type EmbeddingFile struct {
	Identity EmbeddingIdentity `json:"identity"`
	Origin   string            `json:"origin"`
	Records  []EmbeddingRecord `json:"records"`
}

type RecordedEmbedder struct {
	identity EmbeddingIdentity
	vectors  map[string][]float64
}

func LoadRecordedEmbedder(r io.Reader, expected EmbeddingIdentity) (*RecordedEmbedder, error) {
	if err := expected.Validate(); err != nil {
		return nil, err
	}
	var file EmbeddingFile
	d := json.NewDecoder(r)
	d.DisallowUnknownFields()
	if err := d.Decode(&file); err != nil {
		return nil, err
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return nil, fmt.Errorf("trailing embeddings JSON")
	}
	if file.Identity != expected || file.Origin == "" || len(file.Records) == 0 {
		return nil, fmt.Errorf("missing records, origin or incompatible embedding identity")
	}
	e := &RecordedEmbedder{expected, make(map[string][]float64)}
	ids := map[string]bool{}
	for _, record := range file.Records {
		text := strings.TrimSpace(record.Text)
		if record.Identity != expected || text == "" || record.RequestID == "" || ids[record.RequestID] {
			return nil, fmt.Errorf("invalid embedding record metadata")
		}
		if _, ok := e.vectors[text]; ok {
			return nil, fmt.Errorf("duplicate embedding text")
		}
		ids[record.RequestID] = true
		v, err := normalizeDimension(record.Vector, expected.Dimension)
		if err != nil {
			return nil, err
		}
		e.vectors[text] = v
	}
	return e, nil
}
func (e *RecordedEmbedder) Identity() EmbeddingIdentity { return e.identity }
func (e *RecordedEmbedder) Embed(ctx context.Context, text string) ([]float64, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	v, ok := e.vectors[strings.TrimSpace(text)]
	if !ok {
		return nil, fmt.Errorf("no recorded real embedding for input")
	}
	return append([]float64(nil), v...), nil
}
