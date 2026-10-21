package semanticcache

import (
	"context"
	"fmt"
	"sort"
	"time"
)

type SnapshotEntry struct {
	Entry    Entry  `json:"entry"`
	Sequence uint64 `json:"sequence"`
}

type CacheSnapshot struct {
	Entries  []SnapshotEntry `json:"entries"`
	Sequence uint64          `json:"sequence"`
	LastTime time.Time       `json:"last_time"`
}

// Snapshot is a deep, read-only copy; it neither advances the clock nor purges.
func (c *ReferenceCache) Snapshot() CacheSnapshot {
	s := CacheSnapshot{Sequence: c.sequence, LastTime: c.last, Entries: []SnapshotEntry{}}
	for _, e := range c.entries {
		s.Entries = append(s.Entries, SnapshotEntry{cloneEntry(e.entry), e.sequence})
	}
	sort.Slice(s.Entries, func(i, j int) bool { return s.Entries[i].Sequence < s.Entries[j].Sequence })
	return s
}

// Restore preserves insertion times, expiration and FIFO ordering. It owns all
// restored vectors. It never reinserts entries through Put (which resets TTL).
func RestoreReferenceCache(p PolicyConfig, dimension, capacity int, clock Clock, s CacheSnapshot) (*ReferenceCache, error) {
	c, err := NewReferenceCache(p, dimension, capacity, clock)
	if err != nil {
		return nil, err
	}
	if len(s.Entries) > capacity || s.LastTime.After(clock.Now()) {
		return nil, fmt.Errorf("invalid snapshot capacity or time")
	}
	seenSeq := map[uint64]bool{}
	for _, x := range s.Entries {
		e := x.Entry
		if e.ID == "" || e.Answer == "" || e.InsertedAt.IsZero() || !e.ExpiresAt.After(e.InsertedAt) || e.InsertedAt.After(clock.Now()) || x.Sequence == 0 || x.Sequence > s.Sequence || seenSeq[x.Sequence] {
			return nil, fmt.Errorf("invalid snapshot entry")
		}
		if _, ok := c.entries[e.ID]; ok {
			return nil, fmt.Errorf("duplicate snapshot entry")
		}
		if err := validateScope(e.Scope); err != nil {
			return nil, err
		}
		if _, err := normalizeDimension(e.Embedding, dimension); err != nil {
			return nil, err
		}
		seenSeq[x.Sequence] = true
		c.entries[e.ID] = storedEntry{cloneEntry(e), x.Sequence}
	}
	c.sequence = s.Sequence
	c.last = s.LastTime
	return c, nil
}

// OracleSearch operates only on an owned snapshot. Search clock bookkeeping is
// confined to the disposable view. Diagnostic work has no production counters.
func OracleSearch(ctx context.Context, p PolicyConfig, dimension, capacity int, s CacheSnapshot, r Request, v []float64) (*Candidate, error) {
	view, err := RestoreReferenceCache(p, dimension, capacity, &LogicalClock{Time: r.Timestamp}, s)
	if err != nil {
		return nil, err
	}
	return view.Search(ctx, r, v)
}
