package semanticcache

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"time"
)

// UpdateEvent belongs to one serialized stream. Sequence is independent of FIFO
// order. An upsert's new entry version is its sequence; removal names the exact
// previous version. Reset is an ordered barrier, so old events cannot resurrect
// entries. ID hashes the complete payload, excluding ID itself.
type UpdateEvent struct {
	ID              string    `json:"id"`
	StreamID        string    `json:"stream_id"`
	Sequence        uint64    `json:"sequence"`
	Time            time.Time `json:"time"`
	Kind            string    `json:"kind"`
	EntryID         string    `json:"entry_id,omitempty"`
	PreviousVersion uint64    `json:"previous_version,omitempty"`
	Entry           *Entry    `json:"entry,omitempty"`
}

func (e UpdateEvent) payloadID() string {
	e.ID = ""
	b, err := json.Marshal(e)
	if err != nil {
		return ""
	}
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

// UpdateSnapshot binds an authoritative cache copy and entry versions to an
// event watermark. Snapshot creation and cache mutations must be serialized.
type UpdateSnapshot struct {
	Identity  EmbeddingIdentity `json:"embedding_identity"`
	StreamID  string            `json:"stream_id"`
	Watermark uint64            `json:"watermark"`
	Versions  map[string]uint64 `json:"versions"`
	Cache     CacheSnapshot     `json:"cache"`
}

// UpdateJournal owns the last state, not an unbounded event history. Callers that
// need transport/rebuild replay retain the returned events explicitly.
type UpdateJournal struct {
	identity     EmbeddingIdentity
	streamID     string
	sequence     uint64
	versions     map[string]uint64
	peakVersions int
	state        CacheSnapshot
}

func NewUpdateJournal(streamID string, identity EmbeddingIdentity, state CacheSnapshot) (*UpdateJournal, error) {
	if err := identity.Validate(); err != nil {
		return nil, err
	}
	if streamID == "" {
		return nil, fmt.Errorf("update stream ID is required")
	}
	j := &UpdateJournal{identity: identity, streamID: streamID, sequence: state.Sequence, versions: map[string]uint64{}, state: cloneSnapshot(state)}
	for _, x := range state.Entries {
		if x.Entry.ID == "" || x.Sequence == 0 || x.Sequence > state.Sequence || j.versions[x.Entry.ID] != 0 {
			return nil, fmt.Errorf("invalid initial event versions")
		}
		j.versions[x.Entry.ID] = x.Sequence
	}
	j.peakVersions = len(j.versions)
	return j, nil
}

func cloneSnapshot(s CacheSnapshot) CacheSnapshot {
	s.Entries = append([]SnapshotEntry{}, s.Entries...)
	for i := range s.Entries {
		s.Entries[i].Entry = cloneEntry(s.Entries[i].Entry)
	}
	return s
}

func (j *UpdateJournal) Snapshot() UpdateSnapshot {
	v := make(map[string]uint64, len(j.versions))
	for id, version := range j.versions {
		v[id] = version
	}
	return UpdateSnapshot{j.identity, j.streamID, j.sequence, v, cloneSnapshot(j.state)}
}

func (j *UpdateJournal) event(kind, id string, entry *Entry, at time.Time) UpdateEvent {
	j.sequence++
	e := UpdateEvent{StreamID: j.streamID, Sequence: j.sequence, Time: at, Kind: kind, EntryID: id, PreviousVersion: j.versions[id]}
	if entry != nil {
		owned := cloneEntry(*entry)
		e.Entry = &owned
	}
	if kind == "upsert" {
		j.versions[id] = e.Sequence
		if len(j.versions) > j.peakVersions {
			j.peakVersions = len(j.versions)
		}
	} else if kind == "remove" {
		delete(j.versions, id)
	} else {
		j.versions = map[string]uint64{}
		j.peakVersions = 0
	}
	e.ID = e.payloadID()
	return e
}

// Capture emits removals first, then upserts, each sorted by entry ID. A failed
// cache mutation must not call Capture. Repeated deletion emits nothing.
func (j *UpdateJournal) Capture(state CacheSnapshot) []UpdateEvent {
	old, next := map[string]SnapshotEntry{}, map[string]SnapshotEntry{}
	for _, x := range j.state.Entries {
		old[x.Entry.ID] = x
	}
	for _, x := range state.Entries {
		next[x.Entry.ID] = x
	}
	var removed, changed []string
	for id := range old {
		if _, ok := next[id]; !ok {
			removed = append(removed, id)
		}
	}
	for id, e := range next {
		if previous, ok := old[id]; !ok || !reflect.DeepEqual(previous, e) {
			changed = append(changed, id)
		}
	}
	sort.Strings(removed)
	sort.Strings(changed)
	events := []UpdateEvent{}
	for _, id := range removed {
		events = append(events, j.event("remove", id, nil, state.LastTime))
	}
	for _, id := range changed {
		e := next[id].Entry
		events = append(events, j.event("upsert", id, &e, state.LastTime))
	}
	j.state = cloneSnapshot(state)
	return events
}

func (j *UpdateJournal) Reset(state CacheSnapshot) UpdateEvent {
	e := j.event("reset", "", nil, state.LastTime)
	j.state = cloneSnapshot(state)
	return e
}

// EventSummary consumes only contiguous events. Duplicates/older deliveries are
// harmless; gaps or wrong previous versions latch fallback until a rebuild.
type EventSummary struct {
	Summary      *LSHSummary
	StreamID     string
	LastApplied  uint64
	versions     map[string]uint64
	peakVersions int
}

func NewEventSummary(s *LSHSummary, state UpdateSnapshot) (*EventSummary, error) {
	if s == nil || state.StreamID == "" || len(state.Versions) != len(state.Cache.Entries) {
		return nil, fmt.Errorf("invalid update snapshot")
	}
	v := make(map[string]uint64, len(state.Versions))
	for _, x := range state.Cache.Entries {
		version := state.Versions[x.Entry.ID]
		if version == 0 || version > state.Watermark {
			return nil, fmt.Errorf("invalid snapshot entry version")
		}
		v[x.Entry.ID] = version
	}
	if err := s.Reset(state.Identity, state.Cache); err != nil {
		return nil, err
	}
	return &EventSummary{Summary: s, StreamID: state.StreamID, LastApplied: state.Watermark, versions: v, peakVersions: len(v)}, nil
}

func (s *EventSummary) Apply(e UpdateEvent) (err error) {
	defer func() {
		if err != nil {
			s.Summary.Invalidate()
		}
	}()
	if e.ID == "" || e.ID != e.payloadID() || e.StreamID != s.StreamID || e.Sequence == 0 || e.Time.IsZero() {
		return fmt.Errorf("invalid event identity")
	}
	if e.Sequence <= s.LastApplied {
		return nil
	}
	if e.Sequence != s.LastApplied+1 {
		return fmt.Errorf("event gap: have %d, received %d", s.LastApplied, e.Sequence)
	}
	if e.PreviousVersion != s.versions[e.EntryID] {
		return fmt.Errorf("stale entry version for %q", e.EntryID)
	}
	switch e.Kind {
	case "upsert":
		if e.Entry == nil || e.EntryID == "" || e.Entry.ID != e.EntryID {
			return fmt.Errorf("invalid upsert")
		}
		if err := s.Summary.upsert(*e.Entry); err != nil {
			return err
		}
		s.versions[e.EntryID] = e.Sequence
		if len(s.versions) > s.peakVersions {
			s.peakVersions = len(s.versions)
		}
	case "remove":
		if e.EntryID == "" || e.Entry != nil || e.PreviousVersion == 0 {
			return fmt.Errorf("invalid removal")
		}
		s.Summary.remove(e.EntryID)
		delete(s.versions, e.EntryID)
	case "reset":
		if e.EntryID != "" || e.Entry != nil || e.PreviousVersion != 0 {
			return fmt.Errorf("invalid reset")
		}
		s.Summary.buckets = map[BucketKey]uint64{}
		s.Summary.entries = map[string]summaryEntry{}
		s.versions = map[string]uint64{}
		s.peakVersions = 0
	default:
		return fmt.Errorf("unknown event kind %q", e.Kind)
	}
	s.LastApplied = e.Sequence
	return nil
}
