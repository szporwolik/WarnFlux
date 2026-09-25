package sysinfo

import (
	"runtime"
	"testing"
	"time"
)

// TestMemory pins the /proc/meminfo path: on Linux the metric is present,
// the total is positive and the percentage stays within bounds.
func TestMemory(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("memory sampling is Linux-only")
	}
	s := New()
	pct, used, total, ok := s.Memory()
	if !ok {
		t.Fatal("Memory() reported unsupported on Linux")
	}
	if total <= 0 {
		t.Fatalf("total memory = %.2f GiB, want > 0", total)
	}
	if used < 0 || used > total {
		t.Fatalf("used memory = %.2f GiB out of %.2f GiB", used, total)
	}
	if pct < 0 || pct > 100 {
		t.Fatalf("memory percent = %.2f, want 0-100", pct)
	}
}

// TestCPUPercent pins the delta sampler: two samples over a busy window
// stay within 0-100 and a tiny sleep yields a second valid sample.
func TestCPUPercent(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("cpu sampling is Linux-only")
	}
	s := New()
	// Burn some CPU so the delta is non-trivial.
	done := make(chan struct{})
	go func() {
		x := 0
		for {
			select {
			case <-done:
				return
			default:
				x += x ^ 0x5f3759df
			}
		}
	}()
	time.Sleep(40 * time.Millisecond)
	pct, ok := s.CPUPercent()
	close(done)
	if !ok {
		t.Fatal("CPUPercent() reported unsupported on Linux")
	}
	if pct < 0 || pct > 100 {
		t.Fatalf("cpu percent = %.2f, want 0-100", pct)
	}
	// A second, near-zero-window sample is still valid (0 is allowed).
	time.Sleep(5 * time.Millisecond)
	if _, ok := s.CPUPercent(); !ok {
		t.Fatal("second CPUPercent() reported unsupported on Linux")
	}
}
