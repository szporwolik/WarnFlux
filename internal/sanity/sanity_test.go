package sanity

import (
	"context"
	"strings"
	"testing"
)

func TestNormalizeAPRS(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"  Test   message  ", "Test message"},
		{"Line1\nLine2\t Line3", "Line1 Line2 Line3"},
		{"Ostrzeżenie: śnieg i lód", "Ostrzezenie: snieg i lod"},
		{"Hazard  —  uwaga!", "Hazard uwaga!"}, // em dash is non-ASCII → dropped
		{"Uwaga 🚨 zalanie", "Uwaga zalanie"},   // emoji dropped, spacing normalized
		{"", ""},
		{"OK", "OK"}, // unchanged
	}
	for _, c := range cases {
		got := NormalizeText(context.Background(), ChannelAPRS, c.in)
		if got != c.want {
			t.Errorf("aprs %q = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestNormalizeAPRSNote(t *testing.T) {
	svc := &Service{}
	m := svc.Prepare(context.Background(), Message{Channel: ChannelAPRS, Text: "  Test\r\ną  "})
	if m.Text != "Test a" {
		t.Errorf("text = %q", m.Text)
	}
	if m.Note == "" {
		t.Error("normalization must record a note")
	}
	if !strings.Contains(m.Note, "normalized") {
		t.Errorf("note = %q", m.Note)
	}
	// Unchanged text produces no note.
	m = svc.Prepare(context.Background(), Message{Channel: ChannelAPRS, Text: "CLEAN"})
	if m.Note != "" {
		t.Errorf("unchanged text must not produce a note, got %q", m.Note)
	}
}

func TestNormalizeSubject(t *testing.T) {
	long := strings.Repeat("x", 300)
	got := NormalizeText(context.Background(), ChannelEmailSubject, "  Powódź  \n w gminie ")
	if got != "Powódź w gminie" {
		t.Errorf("subject = %q", got)
	}
	// Polish characters survive (email is UTF-8).
	if !strings.Contains(got, "ó") {
		t.Error("subject must keep Polish characters")
	}
	capped := NormalizeText(context.Background(), ChannelEmailSubject, long)
	if len([]rune(capped)) > maxSubjectRunes {
		t.Errorf("subject cap = %d runes, limit %d", len([]rune(capped)), maxSubjectRunes)
	}
}

func TestNormalizeEmailBody(t *testing.T) {
	in := "\x00Line1\r\n\r\nLine2\tok\x01\n\n\n\n\nLine3\n\n"
	got := NormalizeText(context.Background(), ChannelEmailBody, in)
	if strings.ContainsAny(got, "\x00\x01") {
		t.Errorf("control characters survived: %q", got)
	}
	if strings.Contains(got, "\r") {
		t.Errorf("CR survived: %q", got)
	}
	if strings.Count(got, "\n\n\n") > 0 {
		t.Errorf("blank-line run not collapsed: %q", got)
	}
	if !strings.HasPrefix(got, "Line1") {
		t.Errorf("leading blank lines not trimmed: %q", got)
	}
	if !strings.HasSuffix(got, "Line3") {
		t.Errorf("trailing blank lines not trimmed: %q", got)
	}
}

func TestPrepareUnknownChannel(t *testing.T) {
	m := (&Service{}).Prepare(context.Background(), Message{Channel: "future", Text: "  x  "})
	if m.Text != "  x  " || m.Note != "" {
		t.Errorf("unknown channel must pass through untouched, got %q / %q", m.Text, m.Note)
	}
}

func TestNormalizeTextSubjectRouting(t *testing.T) {
	// NormalizeText must route subject-channel text into the Subject
	// field (regression: a plain Message{Text:...} leaves Subject empty).
	got := NormalizeText(context.Background(), ChannelEmailSubject, "  A  ")
	if got != "A" {
		t.Errorf("subject routing = %q", got)
	}
}
