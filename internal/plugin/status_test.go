package plugin

import (
	"errors"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

// TestTruncateStatusError pins the LastError bound: small errors pass
// through, oversized errors are truncated to a valid-UTF-8 string with an
// explicit marker, never cut mid-rune.
func TestTruncateStatusError(t *testing.T) {
	if got := truncateStatusError("boom"); got != "boom" {
		t.Errorf("short error changed: %q", got)
	}

	exact := strings.Repeat("a", maxStatusErrorBytes)
	if got := truncateStatusError(exact); got != exact {
		t.Error("exact-boundary error must be unchanged")
	}

	over := truncateStatusError(strings.Repeat("a", maxStatusErrorBytes+1))
	if len(over) > maxStatusErrorBytes {
		t.Errorf("len = %d, want <= %d", len(over), maxStatusErrorBytes)
	}
	if !strings.Contains(over, "[... truncated]") {
		t.Errorf("truncation marker missing: %q", over)
	}
	if !strings.HasPrefix(over, strings.Repeat("a", len(over)-len(" [... truncated]"))) {
		t.Errorf("prefix not preserved: %q", over)
	}

	huge := truncateStatusError(strings.Repeat("ą", 10000))
	if len(huge) > maxStatusErrorBytes {
		t.Errorf("huge UTF-8: len = %d, want <= %d", len(huge), maxStatusErrorBytes)
	}
	if !utf8.ValidString(huge) {
		t.Errorf("huge UTF-8 truncation produced invalid UTF-8: %q", huge)
	}

	// A multi-byte rune straddling the cut point must never be split.
	prefix := strings.Repeat("a", maxStatusErrorBytes-len(" [... truncated]")-1) + "ą" + strings.Repeat("b", 100)
	cut := truncateStatusError(prefix)
	if !utf8.ValidString(cut) {
		t.Errorf("rune-boundary truncation produced invalid UTF-8: %q", cut)
	}
	if len(cut) > maxStatusErrorBytes {
		t.Errorf("len = %d, want <= %d", len(cut), maxStatusErrorBytes)
	}
	if strings.Contains(cut, "b") {
		t.Errorf("truncation must not include bytes after the cut: %q", cut)
	}
}

// TestFailureBoundsLastError verifies the bound is applied when the error
// is recorded, so the whole internal status model stays bounded.
func TestFailureBoundsLastError(t *testing.T) {
	tr := newStatusTracker("p1", "mqtt", KindOutput)
	tr.failure(errors.New(strings.Repeat("x", 10000)), 0, time.Now())
	snap := tr.snapshot()
	if len(snap.LastError) > maxStatusErrorBytes {
		t.Fatalf("LastError len = %d, want <= %d", len(snap.LastError), maxStatusErrorBytes)
	}
	if !strings.Contains(snap.LastError, "[... truncated]") {
		t.Errorf("marker missing: %q", snap.LastError)
	}
	if !utf8.ValidString(snap.LastError) {
		t.Errorf("LastError is not valid UTF-8: %q", snap.LastError)
	}
}

// TestStatusSummary pins the source poll summary surface.
func TestStatusSummary(t *testing.T) {
	tr := newStatusTracker("s1", "rso", KindSource)
	tr.setSummary("42 items / 7 filtered")
	snap := tr.snapshot()
	if snap.LastSummary != "42 items / 7 filtered" {
		t.Fatalf("LastSummary = %q", snap.LastSummary)
	}
	tr.setSummary("2 items / 1 filtered")
	if got := tr.snapshot().LastSummary; got != "2 items / 1 filtered" {
		t.Fatalf("LastSummary after update = %q", got)
	}
}
