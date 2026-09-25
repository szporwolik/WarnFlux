// Package sysinfo samples host CPU and memory utilization for the
// dashboard's System card. It reads the Linux /proc pseudo-filesystem:
// on other platforms every metric reports ok=false so the UI shows
// "n/a" instead of invented numbers.
package sysinfo

import (
	"bufio"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Sampler is a small stateful sampler. The CPU percentage is a delta
// between two snapshots, so the sampler keeps the previous one between
// calls (the dashboard polls every few seconds, which gives the delta a
// meaningful window).
type Sampler struct {
	mu sync.Mutex
	// prevCPU is the baseline for the CPU delta; read at construction so
	// the very first poll already has a meaningful window.
	prevCPU cpuSnapshot
}

type cpuSnapshot struct {
	at    time.Time
	busy  uint64 // jiffies not spent idle
	total uint64 // all jiffies
}

// New builds a sampler with an initial CPU baseline. It never fails:
// unsupported platforms simply keep reporting ok=false.
func New() *Sampler {
	s := &Sampler{}
	s.prevCPU, _ = readCPU()
	return s
}

// cpuJiffies reports the total (and busy) CPU jiffies since boot.
func readCPU() (cpuSnapshot, bool) {
	f, err := os.Open("/proc/stat")
	if err != nil {
		return cpuSnapshot{}, false
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	if !sc.Scan() {
		return cpuSnapshot{}, false
	}
	fields := strings.Fields(sc.Text())
	if len(fields) < 5 || fields[0] != "cpu" {
		return cpuSnapshot{}, false
	}
	var vals [4]uint64 // user, nice, system, idle
	var total uint64
	for i, want := range []int{1, 2, 3, 4} {
		v, err := strconv.ParseUint(fields[want], 10, 64)
		if err != nil {
			return cpuSnapshot{}, false
		}
		vals[i] = v
		total += v
	}
	for _, f := range fields[5:] { // iowait, irq, softirq, steal, guest, guest_nice
		v, err := strconv.ParseUint(f, 10, 64)
		if err == nil {
			total += v
		}
	}
	var idle, iowait uint64
	if len(fields) > 5 {
		iowait, _ = strconv.ParseUint(fields[5], 10, 64)
	}
	idle = vals[3] + iowait
	return cpuSnapshot{at: time.Now(), busy: total - idle, total: total}, true
}

// CPUPercent returns the host CPU utilization between the previous sample
// and now (0 on the first call), or ok=false when unsupported.
func (s *Sampler) CPUPercent() (float64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := readCPU()
	if !ok {
		return 0, false
	}
	prev := s.prevCPU
	s.prevCPU = cur
	dTotal := cur.total - prev.total
	if dTotal == 0 || cur.at.Equal(prev.at) {
		return 0, true
	}
	dbusy := cur.busy - prev.busy
	pct := 100 * float64(dbusy) / float64(dTotal)
	if pct < 0 {
		pct = 0
	}
	if pct > 100 {
		pct = 100
	}
	return pct, true
}

// Memory reports the host memory utilization and the used/total amounts
// in GiB. ok is false on unsupported platforms.
func (s *Sampler) Memory() (pct, usedGiB, totalGiB float64, ok bool) {
	info, err := readMemInfo()
	if err != nil {
		return 0, 0, 0, false
	}
	total := info["MemTotal"]
	available := info["MemAvailable"]
	if available == 0 {
		// Pre-3.14 fallback: free + buffers + cache.
		available = info["MemFree"] + info["Buffers"] + info["Cached"] + info["SReclaimable"]
	}
	if total == 0 {
		return 0, 0, 0, false
	}
	used := total - available
	pct = 100 * float64(used) / float64(total)
	if pct < 0 {
		pct = 0
	}
	if pct > 100 {
		pct = 100
	}
	const giB = 1024 * 1024
	return pct, float64(used) / giB, float64(total) / giB, true
}

// readMemInfo parses /proc/meminfo into a kB map.
func readMemInfo() (map[string]uint64, error) {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return nil, err
	}
	defer f.Close()
	out := make(map[string]uint64, 16)
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) < 2 {
			continue
		}
		name := strings.TrimSuffix(fields[0], ":")
		v, err := strconv.ParseUint(fields[1], 10, 64)
		if err == nil {
			out[name] = v
		}
	}
	return out, sc.Err()
}
