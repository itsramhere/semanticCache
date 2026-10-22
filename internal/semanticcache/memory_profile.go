package semanticcache

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"unsafe"
)

type SummaryMemoryMeasurement struct {
	RetainedHeapDelta int64  `json:"measured_retained_heap_delta_bytes"`
	HeapBefore        uint64 `json:"heap_before_bytes"`
	HeapAfter         uint64 `json:"heap_after_bytes"`
	ProcessBefore     int64  `json:"process_working_set_before_bytes"`
	ProcessAfter      int64  `json:"process_working_set_after_bytes"`
	StructuralBytes   int64  `json:"rebuilt_structural_estimate_bytes"`
	Boundary          string `json:"boundary"`
}

// ReferenceStateBytes uses the same conservative map allowance as MemoryBytes.
// It is an estimate of the reference cache at this state, not measured heap.
func ReferenceStateBytes(snapshot CacheSnapshot) int64 {
	n := int64(unsafe.Sizeof(ReferenceCache{})) + 48
	n += int64(len(snapshot.Entries)) * (2*(int64(unsafe.Sizeof(""))+int64(unsafe.Sizeof(storedEntry{}))) + 64)
	for _, x := range snapshot.Entries {
		e := x.Entry
		n += int64(2*len(e.ID)+len(e.Answer)+len(e.Validity.PolicyVersion)+cap(e.Embedding)*8) + scopeStringBytes(e.Scope)
	}
	return n
}

// MeasureSummary rebuilds an independently owned summary of the recorded state.
// Input serialization is kept alive on both sides; decoded strings/vectors and
// projection copies belong only to the measured summary after the second GC.
// Run serially: GC/heap profiles are process-wide. This is retained memory, not
// allocations/op, and not a measurement of peak memory during rebuilding.
func MeasureSummary(dir string, identity EmbeddingIdentity, snapshot CacheSnapshot, factory func() (ReplayPredictor, error)) (SummaryMemoryMeasurement, error) {
	m := SummaryMemoryMeasurement{Boundary: "measured GC HeapAlloc delta for a fresh, independently owned rebuild of the state with maximum incremental structural estimate; before/after sampled heap profiles; process working set includes the evaluation harness and runtime; peak rebuild memory not measured"}
	b, err := json.Marshal(snapshot)
	if err != nil {
		return m, err
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		return m, err
	}
	profile := func(name string) error {
		f, err := os.Create(filepath.Join(dir, name))
		if err != nil {
			return err
		}
		err = pprof.WriteHeapProfile(f)
		closeErr := f.Close()
		if err != nil {
			return err
		}
		return closeErr
	}
	// Warm profile-writing machinery before taking the allocation baseline.
	if err := profile("before.pprof"); err != nil {
		return m, err
	}
	m.ProcessBefore, err = processWorkingSet()
	if err != nil {
		return m, err
	}
	runtime.GC()
	if err := profile("before.pprof"); err != nil {
		return m, err
	}
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	p, err := func() (ReplayPredictor, error) {
		var owned CacheSnapshot
		if err := json.Unmarshal(b, &owned); err != nil {
			return nil, err
		}
		p, err := factory()
		if err != nil {
			return nil, err
		}
		if err := p.Reset(identity, owned); err != nil {
			return nil, err
		}
		return p, nil
	}()
	if err != nil {
		return m, err
	}
	runtime.GC()
	runtime.GC() // heap profiles may lag reclamation by up to two GC cycles
	runtime.ReadMemStats(&after)
	m.HeapBefore, m.HeapAfter = before.HeapAlloc, after.HeapAlloc
	m.RetainedHeapDelta = int64(after.HeapAlloc) - int64(before.HeapAlloc)
	m.StructuralBytes = p.MemoryBytes()
	if err := profile("after.pprof"); err != nil {
		return m, err
	}
	m.ProcessAfter, err = processWorkingSet()
	runtime.KeepAlive(p)
	runtime.KeepAlive(b)
	runtime.KeepAlive(snapshot)
	runtime.KeepAlive(factory)
	if err != nil {
		return m, err
	}
	if m.RetainedHeapDelta <= 0 && m.StructuralBytes > 0 {
		return m, fmt.Errorf("nonpositive retained memory measurement; rerun in a quieter process")
	}
	return m, nil
}
