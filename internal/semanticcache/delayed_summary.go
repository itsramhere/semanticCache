package semanticcache

import (
	"fmt"
	"runtime"
	"sort"
	"time"
	"unsafe"
)

type FreshnessStatus struct {
	LastApplied          uint64 `json:"last_applied_sequence"`
	ObservedWatermark    uint64 `json:"observed_watermark"`
	PendingEvents        int    `json:"received_pending_events"`
	KnownUnapplied       uint64 `json:"known_unapplied_events"`
	OldestUnappliedAgeMS *int64 `json:"oldest_known_unapplied_age_lower_bound_ms"`
	ContactAgeMS         int64  `json:"last_source_contact_age_ms"`
	Ready                bool   `json:"ready"`
	Reconciliation       string `json:"reconciliation"`
	FallbackReason       string `json:"fallback_reason,omitempty"`
}

type RebuildMeasurement struct {
	SnapshotWatermark      uint64 `json:"snapshot_watermark"`
	CatchupWatermark       uint64 `json:"catchup_watermark"`
	EventsReplayed         int    `json:"events_replayed"`
	BuildNS                int64  `json:"measured_build_ns"`
	CatchupNS              int64  `json:"measured_catchup_ns"`
	PeakOwnedBytes         int64  `json:"estimated_peak_owned_bytes"`
	SampledHeapPeakBytes   uint64 `json:"measured_sampled_process_heap_peak_bytes"`
	SampledHeapGrowthBytes uint64 `json:"measured_sampled_process_heap_growth_bytes"`
	Backlog                uint64 `json:"catchup_backlog"`
}

type summaryRebuild struct {
	consumer      *EventSummary
	measurement   RebuildMeasurement
	heapBefore    uint64
	snapshotBytes int64
	snapshotTime  time.Time
}

// DelayedSummary is a serialized, local-only predictor. Watermarks arrive from
// the update source, not from cache reads during Predict. A gap or freshness
// breach latches search fallback until a successful watermarked rebuild.
type DelayedSummary struct {
	active      *EventSummary
	pending     map[uint64]UpdateEvent
	peakPending int
	knownHigh   uint64
	oldestKnown time.Time
	lastContact time.Time
	limit       time.Duration
	reason      string
	build       *summaryRebuild
}

func NewDelayedSummary(p *Projections, state UpdateSnapshot, now time.Time, freshnessLimit time.Duration) (*DelayedSummary, error) {
	if now.IsZero() || freshnessLimit <= 0 || state.Cache.LastTime.After(now) {
		return nil, fmt.Errorf("invalid freshness settings")
	}
	s, err := NewLSHSummary(p)
	if err != nil {
		return nil, err
	}
	c, err := NewEventSummary(s, state)
	if err != nil {
		return nil, err
	}
	return &DelayedSummary{active: c, pending: map[uint64]UpdateEvent{}, knownHigh: state.Watermark, lastContact: now, limit: freshnessLimit}, nil
}

func (d *DelayedSummary) unsafe(reason string) {
	if d.reason == "" {
		d.reason = reason
	}
	d.active.Summary.Invalidate()
}

func (d *DelayedSummary) ObserveWatermark(sequence uint64, now time.Time) error {
	if err := d.observeContact(sequence, now, now); err != nil {
		return err
	}
	if sequence > d.active.LastApplied {
		d.unsafe("watermark-gap-fallback")
	}
	return nil
}

func (d *DelayedSummary) observeContact(sequence uint64, now, sourceTime time.Time) error {
	d.Status(now) // Check freshness before a new contact resets its age.
	if now.Before(d.lastContact) {
		d.unsafe("update-clock-fallback")
		return fmt.Errorf("update clock moved backward")
	}
	if sequence < d.active.LastApplied {
		return nil
	} // delayed heartbeat
	// Receipt of a delayed event or duplicate is not fresh source activity.
	// Only its publication time (or a current heartbeat) advances freshness.
	if sourceTime.After(d.lastContact) {
		d.lastContact = sourceTime
	}
	if sequence > d.knownHigh {
		d.knownHigh = sequence
	}
	if d.knownHigh > d.active.LastApplied && d.oldestKnown.IsZero() {
		d.oldestKnown = now
	}
	return nil
}

func (d *DelayedSummary) Receive(e UpdateEvent, now time.Time) error {
	if e.StreamID != d.active.StreamID || e.ID == "" || e.ID != e.payloadID() || e.Sequence == 0 || e.Time.IsZero() || e.Time.After(now) {
		d.unsafe("invalid-update-fallback")
		return fmt.Errorf("invalid delivered event")
	}
	if err := d.observeContact(e.Sequence, now, e.Time); err != nil {
		return err
	}
	if e.Sequence <= d.active.LastApplied {
		return nil
	}
	if old, ok := d.pending[e.Sequence]; ok && old.ID != e.ID {
		d.unsafe("conflicting-update-fallback")
		return fmt.Errorf("conflicting event sequence")
	}
	owned := e
	if e.Entry != nil {
		x := cloneEntry(*e.Entry)
		owned.Entry = &x
	}
	d.pending[e.Sequence] = owned
	if len(d.pending) > d.peakPending {
		d.peakPending = len(d.pending)
	}
	if d.oldestKnown.IsZero() || e.Time.Before(d.oldestKnown) {
		d.oldestKnown = e.Time
	}
	if e.Sequence > d.active.LastApplied+1 {
		d.unsafe("sequence-gap-fallback")
	}
	for {
		next, ok := d.pending[d.active.LastApplied+1]
		if !ok {
			break
		}
		if err := d.active.Apply(next); err != nil {
			d.unsafe("update-error-fallback")
			return err
		}
		delete(d.pending, next.Sequence)
	}
	if d.active.LastApplied == d.knownHigh {
		d.oldestKnown = time.Time{}
	}
	return nil
}

func (d *DelayedSummary) Status(now time.Time) FreshnessStatus {
	if now.Before(d.lastContact) {
		d.unsafe("update-clock-fallback")
	}
	age := now.Sub(d.lastContact)
	var oldest *int64
	if d.knownHigh > d.active.LastApplied && !d.oldestKnown.IsZero() {
		x := now.Sub(d.oldestKnown).Milliseconds()
		oldest = &x
		if now.Sub(d.oldestKnown) >= d.limit {
			d.unsafe("freshness-limit-fallback")
		}
	}
	if age >= d.limit {
		d.unsafe("source-silence-fallback")
	}
	state := "synchronized"
	if d.knownHigh > d.active.LastApplied {
		state = "lagging"
	}
	if d.reason != "" {
		state = "required"
	}
	if d.build != nil {
		state = "rebuilding"
	}
	return FreshnessStatus{d.active.LastApplied, d.knownHigh, len(d.pending), d.knownHigh - d.active.LastApplied, oldest, age.Milliseconds(), d.active.Summary.Ready() && d.reason == "" && d.build == nil, state, d.reason}
}

func (d *DelayedSummary) Predict(r Request, v []float64) Decision {
	status := d.Status(r.Timestamp)
	if !status.Ready {
		reason := status.FallbackReason
		if d.build != nil {
			reason = "rebuild-in-progress-fallback"
		}
		if reason == "" {
			reason = "summary-unready-fallback"
		}
		return Decision{true, reason, d.active.Summary.ID()}
	}
	return d.active.Summary.Predict(r, v)
}

func snapshotMemoryBytes(s UpdateSnapshot) int64 {
	n := int64(unsafe.Sizeof(s)) + int64(len(s.StreamID)) + 96
	n += int64(len(s.Identity.Model) + len(s.Identity.Revision) + len(s.Identity.Preprocessing))
	n += int64(len(s.Versions)) * 112
	for id := range s.Versions {
		n += int64(len(id))
	}
	n += int64(cap(s.Cache.Entries)) * int64(unsafe.Sizeof(SnapshotEntry{}))
	for _, x := range s.Cache.Entries {
		e := x.Entry
		n += int64(len(e.ID)+len(e.Answer)+len(e.Validity.PolicyVersion)+cap(e.Embedding)*8) + scopeStringBytes(e.Scope)
	}
	return n
}

func eventMemoryBytes(e UpdateEvent) int64 {
	n := int64(unsafe.Sizeof(e)) + int64(len(e.ID)+len(e.StreamID)+len(e.EntryID))
	if e.Entry != nil {
		x := e.Entry
		n += int64(unsafe.Sizeof(*x)) + int64(len(x.ID)+len(x.Answer)+len(x.Validity.PolicyVersion)+cap(x.Embedding)*8) + scopeStringBytes(x.Scope)
	}
	return n
}

func (d *DelayedSummary) MemoryBytes() int64 {
	n := d.active.Summary.MemoryBytes() + int64(unsafe.Sizeof(*d)+unsafe.Sizeof(*d.active)) + int64(len(d.active.StreamID)) + int64(d.active.peakVersions)*112 + 96
	n += int64(d.peakPending) * (2*(8+int64(unsafe.Sizeof(UpdateEvent{}))) + 64)
	for id := range d.active.versions {
		n += int64(len(id))
	}
	for _, e := range d.pending {
		n += 2 * (eventMemoryBytes(e) - int64(unsafe.Sizeof(e)))
	}
	return n
}

// BeginRebuild builds off to the side. Serving continues with search fallback;
// source mutations may occur before FinishRebuild in the same serialized loop.
func (d *DelayedSummary) BeginRebuild(state UpdateSnapshot) error {
	d.unsafe("rebuild-in-progress-fallback")
	if state.StreamID != d.active.StreamID || state.Watermark < d.active.LastApplied {
		return fmt.Errorf("incompatible rebuild snapshot")
	}
	var heap runtime.MemStats
	runtime.ReadMemStats(&heap)
	start := time.Now()
	s, err := NewLSHSummary(d.active.Summary.projections)
	if err != nil {
		return err
	}
	c, err := NewEventSummary(s, state)
	if err != nil {
		return err
	}
	b := &summaryRebuild{consumer: c, heapBefore: heap.HeapAlloc, snapshotBytes: snapshotMemoryBytes(state), snapshotTime: state.Cache.LastTime}
	b.measurement.SnapshotWatermark = state.Watermark
	b.measurement.BuildNS = time.Since(start).Nanoseconds()
	d.build = b
	d.sampleRebuild(nil)
	return nil
}

func (d *DelayedSummary) sampleRebuild(events []UpdateEvent) {
	b := d.build
	n := d.MemoryBytes() + b.consumer.Summary.MemoryBytes() + int64(unsafe.Sizeof(*b)+unsafe.Sizeof(*b.consumer)) + int64(len(b.consumer.StreamID)) + int64(b.consumer.peakVersions)*112 + 48 + b.snapshotBytes
	for id := range b.consumer.versions {
		n += int64(len(id))
	}
	for _, e := range events {
		n += eventMemoryBytes(e)
	}
	if n > b.measurement.PeakOwnedBytes {
		b.measurement.PeakOwnedBytes = n
	}
	var heap runtime.MemStats
	runtime.ReadMemStats(&heap)
	if heap.HeapAlloc > b.measurement.SampledHeapPeakBytes {
		b.measurement.SampledHeapPeakBytes = heap.HeapAlloc
	}
	if b.measurement.SampledHeapPeakBytes > b.heapBefore {
		b.measurement.SampledHeapGrowthBytes = b.measurement.SampledHeapPeakBytes - b.heapBefore
	}
}

// FinishRebuild needs a reliable journal replay and a current source watermark.
// Any missing newer event leaves the old predictor in fallback. The single
// pointer swap occurs only after contiguous catch-up to the verified watermark.
func (d *DelayedSummary) FinishRebuild(newer []UpdateEvent, watermark uint64, now time.Time) (RebuildMeasurement, error) {
	if d.build == nil {
		return RebuildMeasurement{}, fmt.Errorf("no rebuild in progress")
	}
	b := d.build
	if watermark < b.consumer.LastApplied || watermark < d.knownHigh || now.Before(d.lastContact) || b.snapshotTime.After(now) {
		return b.measurement, fmt.Errorf("invalid catch-up watermark/time")
	}
	start := time.Now()
	events := append([]UpdateEvent{}, newer...)
	sort.SliceStable(events, func(i, j int) bool { return events[i].Sequence < events[j].Sequence })
	b.measurement.Backlog = watermark - b.measurement.SnapshotWatermark
	d.sampleRebuild(events)
	for _, e := range events {
		if e.Sequence <= b.measurement.SnapshotWatermark {
			continue
		}
		if e.Sequence > watermark {
			return b.measurement, fmt.Errorf("event beyond catch-up watermark")
		}
		if e.Time.After(now) {
			return b.measurement, fmt.Errorf("catch-up event is in the future")
		}
		before := b.consumer.LastApplied
		if err := b.consumer.Apply(e); err != nil {
			return b.measurement, err
		}
		if b.consumer.LastApplied > before {
			b.measurement.EventsReplayed++
		}
		d.sampleRebuild(events)
	}
	if b.consumer.LastApplied != watermark || !b.consumer.Summary.Ready() {
		return b.measurement, fmt.Errorf("incomplete rebuild catch-up")
	}
	b.measurement.CatchupNS = time.Since(start).Nanoseconds()
	b.measurement.CatchupWatermark = watermark
	d.active = b.consumer
	for seq := range d.pending {
		if seq <= watermark {
			delete(d.pending, seq)
		}
	}
	d.knownHigh = watermark
	d.oldestKnown = time.Time{}
	d.lastContact = now
	d.reason = ""
	d.build = nil
	return b.measurement, nil
}
