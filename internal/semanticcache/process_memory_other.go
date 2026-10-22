//go:build !windows

package semanticcache

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
)

func processWorkingSet() (int64, error) {
	if runtime.GOOS != "linux" {
		return 0, fmt.Errorf("process memory measurement unsupported on %s", runtime.GOOS)
	}
	b, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0, err
	}
	fields := strings.Fields(string(b))
	if len(fields) < 2 {
		return 0, fmt.Errorf("invalid process statm")
	}
	n, err := strconv.ParseInt(fields[1], 10, 64)
	return n * int64(os.Getpagesize()), err
}
