package semanticcache

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"regexp"
	"time"
)

// RemoteCall counts actual HTTP attempts, including failures, separately from
// logical searches/writes. Calls and mutations are serialized by the owner.
type RemoteCall struct {
	Method     string `json:"method"`
	Path       string `json:"path"`
	DurationNS int64  `json:"duration_ns"`
	Error      string `json:"error,omitempty"`
}

// RemoteCache is a single-owner, initially empty Qdrant collection. The local
// ledger supplies FIFO/TTL and summary updates, never serving search results.
// External writers, reopening collections, and distributed serving are unsupported.
type RemoteCache struct {
	local            *ReferenceCache
	summary          *LSHSummary
	base, collection string
	client           *http.Client
	broken           error
	maintenanceError error
	lastUpdateNS     int64
	Calls            []RemoteCall
}

func NewRemoteCache(ctx context.Context, endpoint, collection string, timeout time.Duration, p PolicyConfig, identity EmbeddingIdentity, capacity int, clock Clock, summary *LSHSummary) (*RemoteCache, error) {
	u, err := url.Parse(endpoint)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "" || !regexp.MustCompile(`^[a-zA-Z0-9_-]+$`).MatchString(collection) || timeout <= 0 {
		return nil, fmt.Errorf("invalid remote endpoint, collection or timeout")
	}
	if err := identity.Validate(); err != nil {
		return nil, err
	}
	if summary != nil && summary.projections.Config.Identity != identity {
		return nil, fmt.Errorf("remote summary identity mismatch")
	}
	l, err := NewReferenceCache(p, identity.Dimension, capacity, clock)
	if err != nil {
		return nil, err
	}
	c := &RemoteCache{local: l, summary: summary, base: endpoint, collection: collection, client: &http.Client{Timeout: timeout}}
	// Create must fail if the collection already exists: never take ownership of
	// unrelated data or enable an empty summary against an existing collection.
	err = c.call(ctx, "PUT", c.path(""), map[string]any{"vectors": map[string]any{"size": identity.Dimension, "distance": "Cosine"}}, nil)
	if err != nil {
		return nil, err
	}
	if summary != nil {
		start := time.Now()
		if err := summary.Reset(identity, l.Snapshot()); err != nil {
			return nil, err
		}
		c.lastUpdateNS = time.Since(start).Nanoseconds()
	}
	return c, nil
}

func (c *RemoteCache) path(s string) string { return "/collections/" + c.collection + s }

func (c *RemoteCache) call(ctx context.Context, method, path string, body any, result any) (err error) {
	start := time.Now()
	defer func() {
		r := RemoteCall{Method: method, Path: path, DurationNS: time.Since(start).Nanoseconds()}
		if err != nil {
			r.Error = err.Error()
		}
		c.Calls = append(c.Calls, r)
	}()
	b, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, (64<<20)+1))
	if err != nil {
		return err
	}
	if len(data) > 64<<20 {
		return fmt.Errorf("qdrant response exceeds 64 MiB")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("qdrant %s %s: HTTP %d: %.300s", method, path, resp.StatusCode, data)
	}
	var envelope struct {
		Result json.RawMessage `json:"result"`
		Status json.RawMessage `json:"status"`
	}
	if err := json.Unmarshal(data, &envelope); err != nil {
		return fmt.Errorf("qdrant response: %w", err)
	}
	if string(envelope.Status) != `"ok"` || len(envelope.Result) == 0 {
		return fmt.Errorf("invalid qdrant success envelope")
	}
	// HTTP success alone does not confirm collection ownership, completed
	// mutations, or an actual (possibly empty) query result. Validate inside
	// call so protocol failures are also recorded in the HTTP attempt log.
	switch path {
	case c.path(""):
		var confirmed bool
		if err := json.Unmarshal(envelope.Result, &confirmed); err != nil || !confirmed {
			return fmt.Errorf("qdrant collection operation was not confirmed")
		}
	case c.path("/points?wait=true"), c.path("/points/delete?wait=true"):
		var operation struct {
			Status string `json:"status"`
		}
		if err := json.Unmarshal(envelope.Result, &operation); err != nil || operation.Status != "completed" {
			return fmt.Errorf("qdrant wait=true mutation was not completed")
		}
	case c.path("/points/query"):
		var query struct {
			Points []json.RawMessage `json:"points"`
		}
		if err := json.Unmarshal(envelope.Result, &query); err != nil || query.Points == nil {
			return fmt.Errorf("invalid qdrant query result: points array required")
		}
	}
	if result != nil {
		return json.Unmarshal(envelope.Result, result)
	}
	return nil
}

func remoteID(id string) string {
	s := sha256.Sum256([]byte(id))
	h := hex.EncodeToString(s[:16])
	return h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
}

func scopeToken(s Scope) string { b, _ := json.Marshal(s); return string(b) }

// Each mutation is staged locally, published with wait=true, then committed.
// An ambiguous/partial HTTP failure permanently disables serving and skipping.
// Recreate a new collection to recover; retrying against uncertain state is unsafe.
func (c *RemoteCache) Put(ctx context.Context, e Entry) error {
	c.lastUpdateNS = 0
	if c.broken != nil {
		return c.broken
	}
	next, err := RestoreReferenceCache(c.local.policy, c.local.dimension, c.local.capacity, c.local.clock, c.local.Snapshot())
	if err != nil {
		return err
	}
	if err := next.Put(ctx, e); err != nil {
		return err
	}
	stored := next.entries[e.ID].entry
	vector := make([]float32, len(stored.Embedding))
	for i, v := range stored.Embedding {
		vector[i] = float32(v)
	}
	encoded, err := json.Marshal(stored)
	if err != nil {
		return err
	}
	point := map[string]any{"id": remoteID(e.ID), "vector": vector, "payload": map[string]any{"scope": scopeToken(e.Scope), "entry_json": string(encoded)}}
	if err = c.call(ctx, "PUT", c.path("/points?wait=true"), map[string]any{"points": []any{point}}, nil); err != nil {
		return c.poison(err)
	}
	var removed []string
	for _, old := range c.local.Snapshot().Entries {
		if _, ok := next.entries[old.Entry.ID]; !ok {
			removed = append(removed, remoteID(old.Entry.ID))
		}
	}
	if len(removed) > 0 {
		if err = c.call(ctx, "POST", c.path("/points/delete?wait=true"), map[string]any{"points": removed}, nil); err != nil {
			return c.poison(err)
		}
	}
	c.local = next
	if c.summary != nil {
		start := time.Now()
		err = c.summary.Observe(next.Snapshot())
		c.lastUpdateNS = time.Since(start).Nanoseconds()
		if err != nil {
			c.maintenanceError = err
			c.summary.Invalidate()
		}
	}
	return nil
}

func (c *RemoteCache) poison(err error) error {
	c.broken = fmt.Errorf("remote cache unavailable; recreate collection: %w", err)
	if c.summary != nil {
		c.summary.Invalidate()
	}
	return c.broken
}

func (c *RemoteCache) predict(r Request, v []float64, identity EmbeddingIdentity) Decision {
	if c.summary == nil || c.broken != nil || c.summary.projections.Config.Identity != identity {
		return Decision{true, "remote-unsynchronized-fallback", ""}
	}
	return c.summary.Predict(r, v)
}

func (c *RemoteCache) Search(ctx context.Context, r Request, v []float64) (*Candidate, error) {
	if c.broken != nil {
		return nil, c.broken
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := ValidateRequest(r); err != nil {
		return nil, err
	}
	q, err := normalizeDimension(v, c.local.dimension)
	if err != nil {
		return nil, err
	}
	now, err := c.local.now()
	if err != nil {
		return nil, err
	}
	if !now.Equal(r.Timestamp) {
		return nil, fmt.Errorf("request timestamp must equal cache clock")
	}
	if !r.CacheEligible {
		return nil, nil
	}
	var response struct {
		Points []struct {
			Score   float64 `json:"score"`
			Payload struct {
				EntryJSON string `json:"entry_json"`
			} `json:"payload"`
		} `json:"points"`
	}
	// Return every scoped candidate (bounded by capacity), then rescore original
	// float64 payload vectors. This preserves inclusive thresholds and all ties
	// despite Qdrant float32 storage. No backend score cutoff is applied.
	body := map[string]any{"query": q, "filter": map[string]any{"must": []any{map[string]any{"key": "scope", "match": map[string]any{"value": scopeToken(r.Scope())}}}}, "params": map[string]any{"exact": true}, "limit": c.local.capacity, "with_payload": true}
	if err = c.call(ctx, "POST", c.path("/points/query"), body, &response); err != nil {
		return nil, err
	}
	var best *Candidate
	for _, point := range response.Points {
		var e Entry
		if err := json.Unmarshal([]byte(point.Payload.EntryJSON), &e); err != nil {
			return nil, fmt.Errorf("invalid remote entry JSON: %w", err)
		}
		if e.ID == "" || !finite(point.Score) {
			return nil, fmt.Errorf("invalid remote candidate")
		}
		// Ledger mismatch is corruption/external mutation, not an ordinary miss.
		owned, ok := c.local.entries[e.ID]
		if !ok || !reflect.DeepEqual(e, owned.entry) {
			return nil, c.poison(fmt.Errorf("remote payload differs from owned ledger"))
		}
		if !ValidAnswer(e, r, now, c.local.policy.ID) {
			continue
		}
		stored, err := normalizeDimension(e.Embedding, c.local.dimension)
		if err != nil {
			return nil, err
		}
		score, err := CosineSimilarity(q, stored)
		if err != nil {
			return nil, err
		}
		if score < c.local.policy.CosineThreshold {
			continue
		}
		candidate := Candidate{Entry: &e, Similarity: score}
		if best == nil || BetterCandidate(candidate, *best) == candidate {
			best = &candidate
		}
	}
	return best, nil
}
