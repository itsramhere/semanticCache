package semanticcache

import (
	"context"
	"fmt"
	"time"
)

// SynchronizedCache is the single serialized mutation path for a reference
// cache and summary. The caller transfers ownership of the underlying cache;
// direct mutations through another retained reference are unsupported.
// No goroutines are introduced. Observe/snapshot work is production maintenance.
type SynchronizedCache struct {
	cache            *ReferenceCache
	summary          *LSHSummary
	maintenanceError error
	lastUpdateNS     int64
}

func NewSynchronizedCache(c *ReferenceCache, s *LSHSummary, identity EmbeddingIdentity) (*SynchronizedCache, error) {
	if c == nil || s == nil || c.dimension != identity.Dimension {
		return nil, fmt.Errorf("invalid synchronized cache")
	}
	if err := s.Reset(identity, c.Snapshot()); err != nil {
		return nil, err
	}
	return &SynchronizedCache{cache: c, summary: s}, nil
}
func (c *SynchronizedCache) sync() {
	start := time.Now()
	defer func() { c.lastUpdateNS = time.Since(start).Nanoseconds() }()
	c.maintenanceError = c.summary.Observe(c.cache.Snapshot())
	if c.maintenanceError != nil {
		c.summary.Invalidate()
	}
}
func (c *SynchronizedCache) MaintenanceError() error { return c.maintenanceError }
func (c *SynchronizedCache) Rebuild() error {
	c.maintenanceError = c.summary.Reset(c.summary.projections.Config.Identity, c.cache.Snapshot())
	return c.maintenanceError
}
func (c *SynchronizedCache) Search(ctx context.Context, r Request, v []float64) (*Candidate, error) {
	return c.cache.Search(ctx, r, v)
}
func (c *SynchronizedCache) Put(ctx context.Context, e Entry) error {
	c.lastUpdateNS = 0
	if err := c.cache.Put(ctx, e); err != nil {
		return err
	}
	c.sync()
	return nil
}
func (c *SynchronizedCache) Delete(id string) error {
	if err := c.cache.Delete(id); err != nil {
		return err
	}
	c.sync()
	return nil
}
func (c *SynchronizedCache) Revoke(id string) error {
	if err := c.cache.Revoke(id); err != nil {
		return err
	}
	c.sync()
	return nil
}
func (c *SynchronizedCache) Expire() ([]string, error) {
	ids, err := c.cache.Expire()
	if err == nil {
		c.sync()
	}
	return ids, err
}
func (c *SynchronizedCache) Evict() (string, error) {
	id, err := c.cache.Evict()
	if err == nil {
		c.sync()
	}
	return id, err
}
