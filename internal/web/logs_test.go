package web

import (
	"strings"
	"testing"
)

func TestLogBufferRingAndLevels(t *testing.T) {
	b := NewLogBuffer(3)

	// Partial write stays buffered until the newline arrives.
	if _, err := b.Write([]byte("level=INFO hello")); err != nil {
		t.Fatal(err)
	}
	if got := len(b.Snapshot(0)); got != 0 {
		t.Fatalf("snapshot before newline = %d lines, want 0", got)
	}
	if _, err := b.Write([]byte(" world\n")); err != nil {
		t.Fatal(err)
	}

	lines := b.Snapshot(0)
	if len(lines) != 1 || lines[0].Text != "level=INFO hello world" || lines[0].Level != "info" {
		t.Fatalf("lines = %+v", lines)
	}
	if lines[0].Seq != 1 {
		t.Fatalf("seq = %d, want 1", lines[0].Seq)
	}

	// Ring cap: only the last 3 lines survive.
	for _, s := range []string{"level=DEBUG d2", "level=WARN w3", "level=ERROR e4", "plain p5"} {
		if _, err := b.Write([]byte(s + "\n")); err != nil {
			t.Fatal(err)
		}
	}
	lines = b.Snapshot(0)
	if len(lines) != 3 {
		t.Fatalf("buffered = %d lines, want 3 (cap)", len(lines))
	}
	joined := ""
	for _, l := range lines {
		joined += l.Text + "\n"
	}
	for _, want := range []string{"w3", "e4", "p5"} {
		if !strings.Contains(joined, want) {
			t.Errorf("buffer missing %q: %s", want, joined)
		}
	}
	if strings.Contains(joined, "d2") {
		t.Errorf("oldest line must be evicted: %s", joined)
	}
	// Level extraction: warn/error detected, plain text has none.
	if lines[0].Level != "warn" || lines[1].Level != "error" || lines[2].Level != "" {
		t.Errorf("levels = %q/%q/%q, want warn/error/empty", lines[0].Level, lines[1].Level, lines[2].Level)
	}

	// Cursor semantics: only lines after the given seq.
	after := lines[1].Seq
	next := b.Snapshot(after)
	if len(next) != 1 || next[0].Text != "plain p5" {
		t.Errorf("snapshot(after %d) = %+v, want only the last line", after, next)
	}
}
