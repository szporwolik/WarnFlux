package aprs

import (
	"strings"
	"testing"
)

func TestLimitMessageTextTransliterates(t *testing.T) {
	got := LimitMessageText("Pożar w lesie — silny wiatr", 0)
	want := "Pozar w lesie silny wiatr"
	if got != want {
		t.Fatalf("LimitMessageText = %q, want %q", got, want)
	}
	if got := LimitMessageText("Drogi Ćwikliński", 0); got != "Drogi Cwiklinski" {
		t.Fatalf("diacritics = %q", got)
	}
	// Unknown non-ASCII is dropped entirely.
	if got := LimitMessageText("alert 警报 test", 0); got != "alert test" {
		t.Fatalf("non-ascii drop = %q", got)
	}
}

func TestLimitMessageTextWordBoundary(t *testing.T) {
	words := make([]string, 12)
	for i := range words {
		words[i] = "abcdefghij"
	}
	text := strings.Join(words, " ")
	for _, reserve := range []int{0, AckSuffixLen} {
		got := LimitMessageText(text, reserve)
		if len(got) > MaxMessageText-reserve {
			t.Fatalf("reserve %d: len = %d, want <= %d", reserve, len(got), MaxMessageText-reserve)
		}
		// The cut must land on a word boundary: the result is a prefix of
		// complete words.
		if !strings.HasSuffix(text[:len(got)], got) {
			t.Fatalf("reserve %d: cut mid-word: %q", reserve, got)
		}
	}
}

func TestLimitMessageTextHardCutWithoutSpaces(t *testing.T) {
	got := LimitMessageText(strings.Repeat("x", 200), 0)
	if len(got) != MaxMessageText {
		t.Fatalf("len = %d, want %d", len(got), MaxMessageText)
	}
}

func TestSeverityAbbrev(t *testing.T) {
	cases := map[string]string{
		"severe":   "SEV",
		"MODERATE": "MOD",
		"minor":    "MIN",
		"extreme":  "EXT",
		"unknown":  "UNK",
		"":         "",
		"weird":    "",
	}
	for in, want := range cases {
		if got := SeverityAbbrev(in); got != want {
			t.Errorf("SeverityAbbrev(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestBuildAlertMessagePriority(t *testing.T) {
	// Everything fits: richest form wins.
	got := BuildAlertMessage("WarnFlux", "severe", "Storm", "Strong wind", 0)
	if got != "WarnFlux SEV Storm: Strong wind" {
		t.Fatalf("full = %q", got)
	}

	// A long headline forces dropping less important components; the
	// headline itself stays intact.
	long := "Very dangerous storm approaching the southern districts"
	got = BuildAlertMessage("WarnFlux", "severe", "Storm", long, 0)
	want := "SEV Storm: " + long
	if len(want) <= MaxMessageText {
		if got != want {
			t.Fatalf("event kept = %q, want %q", got, want)
		}
	}
	if !strings.HasSuffix(got, long) {
		t.Fatalf("headline was shortened: %q", got)
	}

	// A huge headline is cut at a word boundary, never mid-word.
	huge := strings.Repeat("headline ", 30)
	got = BuildAlertMessage("WarnFlux", "severe", "Storm", huge, 0)
	if len(got) > MaxMessageText || !strings.HasPrefix(got, "headline") {
		t.Fatalf("huge = %q", got)
	}

	// Reserve leaves room for the {id} suffix.
	got = BuildAlertMessage("WarnFlux", "severe", "Storm", "Strong wind", AckSuffixLen)
	if len(got)+AckSuffixLen > MaxMessageText {
		t.Fatalf("reserve violated: %q (%d)", got, len(got))
	}

	// Fallbacks.
	if got := BuildAlertMessage("", "", "", "Pożar", 0); got != "Pozar" {
		t.Fatalf("bare headline = %q", got)
	}
	if got := BuildAlertMessage("", "severe", "Flood", "", 0); !strings.Contains(got, "Flood") {
		t.Fatalf("event fallback = %q", got)
	}
	if got := BuildAlertMessage("", "", "", "", 0); got != "WarnFlux alert" {
		t.Fatalf("empty fallback = %q", got)
	}
}
