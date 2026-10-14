package web

import (
	"strings"
	"testing"
)

// TestLogValueTruncatesAndFlattens pins the helper that guards every
// user-controlled value written to a log line or the audit trail.
func TestLogValueTruncatesAndFlattens(t *testing.T) {
	if got := logValue("admin\nINFO forged line"); strings.ContainsAny(got, "\n\r") {
		t.Errorf("logValue kept a line break: %q", got)
	}
	long := strings.Repeat("a", 4096)
	got := logValue(long)
	if len(got) > maxLoggedRunes*2+3 {
		t.Errorf("logValue length = %d, want a bounded value", len(got))
	}
	if !strings.HasSuffix(got, "…") {
		t.Errorf("logValue did not mark the truncation: %q", got)
	}
	if got := logValue("member1"); got != "member1" {
		t.Errorf("logValue changed a short value: %q", got)
	}
}

// TestLogBufferByteBudget pins the memory bounds of the log viewer buffer:
// a line count alone is not a bound, so both the single line and the whole
// buffer are capped.
func TestLogBufferByteBudget(t *testing.T) {
	b := NewLogBuffer(10)

	// One enormous line: kept truncated, with the dropped size reported.
	b.Write([]byte(strings.Repeat("x", 5<<20) + "\n"))
	lines := b.Snapshot(0)
	if len(lines) != 1 {
		t.Fatalf("lines = %d, want 1", len(lines))
	}
	if len(lines[0].Text) > MaxLogLineBytes+64 {
		t.Errorf("single line = %d bytes, want <= %d", len(lines[0].Text), MaxLogLineBytes+64)
	}
	if !strings.Contains(lines[0].Text, "bytes)") {
		t.Errorf("truncated line does not report the dropped bytes: %q", lines[0].Text)
	}

	// An unterminated line must not accumulate either.
	b2 := NewLogBuffer(10)
	for i := 0; i < 64; i++ {
		b2.Write([]byte(strings.Repeat("y", 1<<20)))
	}
	if got := len(b2.partial); got > MaxLogLineBytes {
		t.Errorf("partial buffer = %d bytes, want <= %d", got, MaxLogLineBytes)
	}
	b2.Write([]byte("\n"))
	if lines = b2.Snapshot(0); len(lines) != 1 {
		t.Fatalf("lines after the terminated tail = %d, want 1", len(lines))
	}
	if len(lines[0].Text) > MaxLogLineBytes+64 {
		t.Errorf("terminated line = %d bytes, want <= %d", len(lines[0].Text), MaxLogLineBytes+64)
	}

	// Many lines: the byte budget evicts the oldest ones.
	b3 := NewLogBuffer(100000)
	line := strings.Repeat("z", 1024) + "\n"
	for i := 0; i < 4096; i++ {
		b3.Write([]byte(line))
	}
	if b3.bytes > MaxLogBytes {
		t.Errorf("buffer holds %d bytes, want <= %d", b3.bytes, MaxLogBytes)
	}
	if len(b3.lines) >= 4096 {
		t.Errorf("buffer kept %d lines, want eviction below the byte budget", len(b3.lines))
	}
}
