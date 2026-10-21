package semanticcache

import (
	"encoding/json"
	"fmt"
	"reflect"
	"unsafe"
)

type summaryEntry struct {
	Entry Entry
	Keys  []BucketKey
}

// LSHSummary owns immutable projections and a counted entry ledger. Calls are
// serialized. Predict reads only this local summary, never the cache.
type LSHSummary struct {
	projections              *Projections
	id                       string
	ready                    bool
	buckets                  map[BucketKey]uint64
	entries                  map[string]summaryEntry
	peakBuckets, peakEntries int
}

func NewLSHSummary(p *Projections) (*LSHSummary, error) {
	if err := p.Validate(); err != nil {
		return nil, err
	}
	b, _ := json.Marshal(p)
	var owned Projections
	if err := json.Unmarshal(b, &owned); err != nil {
		return nil, err
	}
	return &LSHSummary{projections: &owned, id: owned.ID()}, nil
}
func (s *LSHSummary) ID() string  { return s.id }
func (s *LSHSummary) Ready() bool { return s.ready }
func (s *LSHSummary) Invalidate() { s.ready = false }
func (s *LSHSummary) Reset(identity EmbeddingIdentity, snapshot CacheSnapshot) error {
	s.ready = false
	s.buckets = make(map[BucketKey]uint64)
	s.entries = make(map[string]summaryEntry)
	s.peakBuckets = 0
	s.peakEntries = 0
	if identity != s.projections.Config.Identity {
		return fmt.Errorf("summary embedding identity mismatch")
	}
	if err := s.Observe(snapshot); err != nil {
		return err
	}
	s.ready = true
	return nil
}
func (s *LSHSummary) remove(id string) {
	old, ok := s.entries[id]
	if !ok {
		return
	}
	for _, key := range old.Keys {
		if s.buckets[key] <= 1 {
			delete(s.buckets, key)
		} else {
			s.buckets[key]--
		}
	}
	delete(s.entries, id)
}

// Observe reconciles a serialized authoritative snapshot incrementally. Unchanged
// entries are not rehashed. Failure immediately disables skipping until Reset.
func (s *LSHSummary) Observe(snapshot CacheSnapshot) (err error) {
	defer func() {
		if err != nil {
			s.ready = false
		}
	}()
	if s.buckets == nil || s.entries == nil {
		return fmt.Errorf("summary requires Reset")
	}
	seen := make(map[string]bool, len(snapshot.Entries))
	for _, x := range snapshot.Entries {
		e := x.Entry
		if e.ID == "" || seen[e.ID] {
			return fmt.Errorf("invalid or duplicate summary entry")
		}
		seen[e.ID] = true
		if !e.Validity.Reusable || e.Validity.Revoked {
			s.remove(e.ID)
			continue
		}
		if old, ok := s.entries[e.ID]; ok && reflect.DeepEqual(old.Entry, e) {
			continue
		}
		keys, hashErr := s.projections.keys(e.Scope, e.Embedding, s.id)
		if hashErr != nil {
			return hashErr
		}
		s.remove(e.ID)
		for _, key := range keys {
			s.buckets[key]++
		}
		s.entries[e.ID] = summaryEntry{cloneEntry(e), keys}
		if len(s.buckets) > s.peakBuckets {
			s.peakBuckets = len(s.buckets)
		}
		if len(s.entries) > s.peakEntries {
			s.peakEntries = len(s.entries)
		}
	}
	for id := range s.entries {
		if !seen[id] {
			s.remove(id)
		}
	}
	return nil
}
func (s *LSHSummary) Predict(r Request, v []float64) Decision {
	d := Decision{true, "summary-unready-fallback", s.id}
	if !s.ready {
		return d
	}
	keys, ambiguous, err := s.projections.hash(r.Scope(), v, s.id)
	if err != nil {
		d.Reason = "hash-error-fallback"
		return d
	}
	if ambiguous {
		d.Reason = "hash-boundary-fallback"
		return d
	}
	for _, key := range keys {
		if s.buckets[key] > 0 {
			d.Reason = "occupied-bucket"
			return d
		}
	}
	d.Search = false
	d.Reason = "empty-buckets"
	return d
}

func scopeStringBytes(s Scope) int64 {
	return int64(len(s.Namespace) + len(s.ModelVersion) + len(s.Context.TenantID) + len(s.Context.Locale) + len(s.Context.ProductVersion) + len(s.Context.KnowledgeVersion))
}

// MemoryBytes is a conservative structural estimate, NOT a heap measurement.
// It includes projections, full entry/vector ledger, keys, strings and an
// allowance of 2x payload + 64 bytes per map slot for slack/container overhead.
// Phase 4 will validate retained/process memory with heap profiles.
func (s *LSHSummary) MemoryBytes() int64 {
	n := int64(unsafe.Sizeof(*s)+unsafe.Sizeof(*s.projections)) + int64(len(s.id))
	c := s.projections.Config
	n += int64(len(c.Version) + len(c.Identity.Model) + len(c.Identity.Revision) + len(c.Identity.Preprocessing))
	n += int64(cap(s.projections.Planes)) * int64(unsafe.Sizeof([][]float64{}))
	for _, table := range s.projections.Planes {
		n += int64(cap(table)) * int64(unsafe.Sizeof([]float64{}))
		for _, plane := range table {
			n += int64(cap(plane)) * 8
		}
	}
	n += 96 // two map headers
	n += int64(s.peakBuckets) * (2*(int64(unsafe.Sizeof(BucketKey{}))+8) + 64)
	n += int64(s.peakEntries) * (2*(int64(unsafe.Sizeof(""))+int64(unsafe.Sizeof(summaryEntry{}))) + 64)
	for key := range s.buckets {
		n += scopeStringBytes(key.Scope) + int64(len(key.ProjectionID))
	}
	for id, x := range s.entries {
		e := x.Entry
		n += int64(len(id)+len(e.ID)+len(e.Answer)+len(e.Validity.PolicyVersion)) + scopeStringBytes(e.Scope) + int64(cap(e.Embedding))*8
		n += int64(cap(x.Keys)) * int64(unsafe.Sizeof(BucketKey{}))
		for _, key := range x.Keys {
			n += scopeStringBytes(key.Scope) + int64(len(key.ProjectionID))
		}
	}
	return n
}
