package semanticcache

import (
	"context"
	"fmt"
	"time"
	"unsafe"
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
	journal          *UpdateJournal
	consumer         *EventSummary
}

func NewSynchronizedCache(c *ReferenceCache, s *LSHSummary, identity EmbeddingIdentity) (*SynchronizedCache, error) {
	if c == nil || s == nil || c.dimension != identity.Dimension {
		return nil, fmt.Errorf("invalid synchronized cache")
	}
	if identity != s.projections.Config.Identity {
		return nil, fmt.Errorf("summary embedding identity mismatch")
	}
	j, err := NewUpdateJournal("local:"+s.ID(), identity, c.Snapshot())
	if err != nil {
		return nil, err
	}
	consumer, err := NewEventSummary(s, j.Snapshot())
	if err != nil {
		return nil, err
	}
	return &SynchronizedCache{cache: c, summary: s, journal: j, consumer: consumer}, nil
}
func (c *SynchronizedCache) sync() {
	start := time.Now()
	defer func() { c.lastUpdateNS = time.Since(start).Nanoseconds() }()
	for _, e := range c.journal.Capture(c.cache.Snapshot()) {
		if err := c.consumer.Apply(e); err != nil {
			c.maintenanceError = err
			c.summary.Invalidate()
		}
	}
}
func (c *SynchronizedCache) MaintenanceError() error { return c.maintenanceError }
func (c *SynchronizedCache) MemoryBytes() int64 {
	state := UpdateSnapshot{c.journal.identity, c.journal.streamID, c.journal.sequence, c.journal.versions, c.journal.state}
	n := c.summary.MemoryBytes() + snapshotMemoryBytes(state) + int64(c.journal.peakVersions-len(c.journal.versions))*112 + int64(c.consumer.peakVersions)*112 + 96
	n += int64(unsafe.Sizeof(*c)+unsafe.Sizeof(*c.journal)+unsafe.Sizeof(*c.consumer)) + int64(len(c.consumer.StreamID))
	for id := range c.consumer.versions {
		n += int64(len(id))
	}
	return n
}
func (c *SynchronizedCache) Rebuild() error {
	consumer, err := NewEventSummary(c.summary, c.journal.Snapshot())
	c.maintenanceError = err
	if err == nil {
		c.consumer = consumer
	}
	return c.maintenanceError
}

func (c *SynchronizedCache) Clear() error {
	if err := c.cache.Clear(); err != nil {
		return err
	}
	e := c.journal.Reset(c.cache.Snapshot())
	if err := c.consumer.Apply(e); err != nil {
		c.maintenanceError = err
		c.summary.Invalidate()
	}
	return nil
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
