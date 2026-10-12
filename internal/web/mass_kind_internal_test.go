package web

import (
	"strings"
	"testing"
)

// TestMassKindRules pins the composer's single source of truth: the
// character budget per kind, the accepted values and which channel can
// carry which kind. Both the form markup and the server validation derive
// from these helpers, so a change here moves the UI with it.
func TestMassKindRules(t *testing.T) {
	if got := massMessageLimit(massKindShort); got != 50 {
		t.Errorf("short limit = %d, want 50", got)
	}
	if got := massMessageLimit(massKindLong); got != 1000 {
		t.Errorf("long limit = %d, want 1000", got)
	}
	if got := massCharLimits(); got != "short:50 long:1000" {
		t.Errorf("char limits = %q", got)
	}

	// An empty or unknown kind is the short one, so older forms and
	// bookmarks keep working.
	for _, in := range []string{"", "  ", "short", "huge", "LONG"} {
		want := massKindShort
		if in == "LONG" {
			want = massKindShort // kinds are matched case-sensitively
		}
		if got := massNormalizeKind(in); got != want {
			t.Errorf("massNormalizeKind(%q) = %q, want %q", in, got, want)
		}
	}
	if got := massNormalizeKind("long"); got != massKindLong {
		t.Errorf(`massNormalizeKind("long") = %q`, got)
	}

	// Every channel is offered for a short notice; only e-mail and Discord
	// can carry a long one.
	for _, ch := range massChannelOrder {
		if !massChannelAllows(massKindShort, ch) {
			t.Errorf("short notice cannot use %q", ch)
		}
	}
	for _, ch := range []string{"aprs", "sms", "meshtastic"} {
		if massChannelAllows(massKindLong, ch) {
			t.Errorf("long notice must not be offered over %q", ch)
		}
	}
	for _, ch := range []string{"email", "discord"} {
		if !massChannelAllows(massKindLong, ch) {
			t.Errorf("long notice must be offered over %q", ch)
		}
	}
	if got := massChannelKinds("email"); got != "short long" {
		t.Errorf(`massChannelKinds("email") = %q`, got)
	}
	if got := massChannelKinds("aprs"); got != "short" {
		t.Errorf(`massChannelKinds("aprs") = %q`, got)
	}

	// The form offers exactly the channels the rules know about.
	for _, ch := range massChannelOrder {
		if massChannelKinds(ch) == "" {
			t.Errorf("channel %q is offered nowhere", ch)
		}
	}
	if !strings.Contains(massChannelKinds("email"), massKindLong) {
		t.Error("the long kind must reach the form for e-mail")
	}
}
