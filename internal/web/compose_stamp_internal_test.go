package web

import "testing"

// TestComposeStampFormats pins the accepted and displayed shapes of the
// two compose time fields: the form shows ISO with a space and a 24-hour
// clock, the older datetime-local "T" shape and RFC 3339 (hidden
// round-trips, values pasted from elsewhere) still parse, and anything
// else stays as typed so validation can report it.
func TestComposeStampFormats(t *testing.T) {
	for _, in := range []string{"2026-10-12 14:30", "2026-10-12T14:30", "2026-10-12T12:30:00Z"} {
		if parseComposeTime(in) == nil {
			t.Errorf("%q must be accepted", in)
		}
	}
	for _, in := range []string{"", "12.10.2026 14:30", "2026-10-12", "2026-10-12 2:30 PM", "tomorrow"} {
		if parseComposeTime(in) != nil {
			t.Errorf("%q must be rejected", in)
		}
	}
	if got := canonicalComposeStamp("2026-10-12T14:30"); got != "2026-10-12 14:30" {
		t.Errorf("display of a datetime-local value = %q", got)
	}
	if got := canonicalComposeStamp("2026-10-12 14:30"); got != "2026-10-12 14:30" {
		t.Errorf("an already canonical value changed: %q", got)
	}
	if got := canonicalComposeStamp("nonsense"); got != "nonsense" {
		t.Errorf("an unparsable value must come back as typed, got %q", got)
	}
	if got := canonicalComposeStamp(""); got != "" {
		t.Errorf("an empty value must stay empty, got %q", got)
	}
}
