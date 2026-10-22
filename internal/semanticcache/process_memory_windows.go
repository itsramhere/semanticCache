package semanticcache

import (
	"fmt"
	"syscall"
	"unsafe"
)

func processWorkingSet() (int64, error) {
	// PROCESS_MEMORY_COUNTERS: two DWORDs followed by eight SIZE_Ts.
	var counters struct {
		Size, PageFaultCount                                     uint32
		PeakWorkingSet, WorkingSet, QuotaPeakPaged, QuotaPaged   uintptr
		QuotaPeakNonPaged, QuotaNonPaged, Pagefile, PeakPagefile uintptr
	}
	counters.Size = uint32(unsafe.Sizeof(counters))
	proc := syscall.NewLazyDLL("psapi.dll").NewProc("GetProcessMemoryInfo")
	ok, _, err := proc.Call(^uintptr(0), uintptr(unsafe.Pointer(&counters)), uintptr(counters.Size))
	if ok == 0 {
		return 0, fmt.Errorf("GetProcessMemoryInfo: %w", err)
	}
	return int64(counters.WorkingSet), nil
}
