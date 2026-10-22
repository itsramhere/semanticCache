package semanticcache

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"
)

type Clock interface{ Now() time.Time }
type WallClock struct{}

func (WallClock) Now() time.Time { return time.Now().UTC() }

// LogicalClock is advanced explicitly during sequential replay; it is not concurrent.
type LogicalClock struct{ Time time.Time }

func (c *LogicalClock) Now() time.Time { return c.Time }

type Cache interface {
	Search(context.Context, Request, []float64) (*Candidate, error)
	Put(context.Context, Entry) error
}

type storedEntry struct {
	entry    Entry
	sequence uint64
}

// ReferenceCache owns its vectors and returns copies. Calls must be serialized.
// Its embedding identity is fixed by the caller's experiment configuration.
type ReferenceCache struct {
	policy              PolicyConfig
	dimension, capacity int
	clock               Clock
	last                time.Time
	sequence            uint64
	entries             map[string]storedEntry
}

func NewReferenceCache(p PolicyConfig, dimension, capacity int, clock Clock) (*ReferenceCache, error) {
	if p.ID == "" || !finite(p.CosineThreshold) || p.CosineThreshold < -1 || p.CosineThreshold > 1 || p.TTLSeconds <= 0 || int64(p.TTLSeconds) > maxTTLSeconds || dimension <= 0 || capacity <= 0 || clock == nil {
		return nil, fmt.Errorf("invalid cache configuration")
	}
	return &ReferenceCache{policy: p, dimension: dimension, capacity: capacity, clock: clock, entries: make(map[string]storedEntry)}, nil
}

func validateScope(s Scope) error {
	for _, v := range []string{s.Namespace, s.ModelVersion, s.Context.TenantID, s.Context.Locale, s.Context.ProductVersion, s.Context.KnowledgeVersion} {
		if strings.TrimSpace(v) == "" {
			return fmt.Errorf("all scope fields are required")
		}
	}
	return nil
}

func ValidateRequest(r Request) error {
	if strings.TrimSpace(r.ID) == "" || strings.TrimSpace(r.Text) == "" || r.Timestamp.IsZero() || r.Timestamp.Location() != time.UTC {
		return fmt.Errorf("request ID, text and UTC timestamp are required")
	}
	return validateScope(r.Scope())
}

func normalizeDimension(v []float64, dimension int) ([]float64, error) {
	if len(v) != dimension {
		return nil, fmt.Errorf("%w: dimension %d, want %d", ErrInvalidEmbedding, len(v), dimension)
	}
	return Normalize(v)
}

func (c *ReferenceCache) now() (time.Time, error) {
	now := c.clock.Now()
	if now.IsZero() || now.Before(c.last) {
		return time.Time{}, fmt.Errorf("cache clock is zero or moved backward")
	}
	c.last = now
	return now, nil
}

func cloneEntry(e Entry) Entry { e.Embedding = append([]float64(nil), e.Embedding...); return e }

// ValidAnswer is repeated by the router after retrieval.
func ValidAnswer(e Entry, r Request, now time.Time, policyID string) bool {
	return r.CacheEligible && e.IsLive(now) && e.Scope == r.Scope() && e.Validity.Reusable && !e.Validity.Revoked && e.Validity.PolicyVersion == policyID && strings.TrimSpace(e.Answer) != ""
}

func (c *ReferenceCache) Search(ctx context.Context, r Request, v []float64) (*Candidate, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := ValidateRequest(r); err != nil {
		return nil, err
	}
	query, err := normalizeDimension(v, c.dimension)
	if err != nil {
		return nil, err
	}
	now, err := c.now()
	if err != nil {
		return nil, err
	}
	if !r.Timestamp.Equal(now) {
		return nil, fmt.Errorf("request timestamp must equal cache clock")
	}
	var best *Candidate
	for _, stored := range c.entries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		e := stored.entry
		if !ValidAnswer(e, r, now, c.policy.ID) {
			continue
		}
		storedVector, err := normalizeDimension(e.Embedding, c.dimension)
		if err != nil {
			return nil, err
		}
		score, err := CosineSimilarity(query, storedVector)
		if err != nil {
			return nil, err
		}
		if score < c.policy.CosineThreshold {
			continue
		}
		candidate := Candidate{Entry: &e, Similarity: score}
		if best == nil || BetterCandidate(candidate, *best) == candidate {
			best = &candidate
		}
	}
	if best != nil {
		e := cloneEntry(*best.Entry)
		best.Entry = &e
	}
	return best, nil
}

// Put atomically inserts or replaces; timestamps are assigned by this cache.
// A rejected write never expires, replaces or evicts existing entries.
func (c *ReferenceCache) Put(ctx context.Context, e Entry) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if strings.TrimSpace(e.ID) == "" || strings.TrimSpace(e.Answer) == "" || !e.Validity.Reusable || e.Validity.Revoked || e.Validity.PolicyVersion != c.policy.ID {
		return fmt.Errorf("invalid entry identity, answer or validity")
	}
	if err := validateScope(e.Scope); err != nil {
		return err
	}
	v, err := normalizeDimension(e.Embedding, c.dimension)
	if err != nil {
		return err
	}
	now, err := c.now()
	if err != nil {
		return err
	}
	c.purge(now)
	e.Embedding = v
	e.InsertedAt = now
	e.ExpiresAt = now.Add(time.Duration(c.policy.TTLSeconds) * time.Second)
	c.sequence++
	c.entries[e.ID] = storedEntry{entry: e, sequence: c.sequence}
	for len(c.entries) > c.capacity {
		c.evict()
	}
	return nil
}

func (c *ReferenceCache) purge(now time.Time) []string {
	var ids []string
	for id, e := range c.entries {
		if !now.Before(e.entry.ExpiresAt) {
			ids = append(ids, id)
			delete(c.entries, id)
		}
	}
	sort.Strings(ids)
	return ids
}

func (c *ReferenceCache) Expire() ([]string, error) {
	now, err := c.now()
	if err != nil {
		return nil, err
	}
	return c.purge(now), nil
}
func (c *ReferenceCache) Delete(id string) error {
	now, err := c.now()
	if err != nil {
		return err
	}
	c.purge(now)
	delete(c.entries, id)
	return nil
}
func (c *ReferenceCache) Revoke(id string) error {
	now, err := c.now()
	if err != nil {
		return err
	}
	c.purge(now)
	if e, ok := c.entries[id]; ok {
		e.entry.Validity.Revoked = true
		c.entries[id] = e
	}
	return nil
}
func (c *ReferenceCache) evict() string {
	id := ""
	var oldest uint64
	for key, e := range c.entries {
		if id == "" || e.sequence < oldest {
			id = key
			oldest = e.sequence
		}
	}
	if id != "" {
		delete(c.entries, id)
	}
	return id
}

// Evict removes the FIFO head after purging expired entries.
func (c *ReferenceCache) Evict() (string, error) {
	now, err := c.now()
	if err != nil {
		return "", err
	}
	c.purge(now)
	return c.evict(), nil
}

// Clear empties the cache without reusing FIFO insertion versions.
func (c *ReferenceCache) Clear() error {
	if _, err := c.now(); err != nil {
		return err
	}
	c.entries = make(map[string]storedEntry)
	return nil
}
