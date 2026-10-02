package radiocli

import (
	"strings"
	"testing"
)

// TestHandle pins the command interpreter: case-insensitive commands,
// the public/restricted split, the help list per authorization level,
// the debug pass-through, unknown commands and plain (non-command)
// messages.
func TestHandle(t *testing.T) {
	b := New("WarnFlux v1.0 - SOSNA - sosna.sp9moa.pl")

	if res := b.Handle("powodz w krakowie", false); res.Handled {
		t.Fatal("plain text must not be handled as a command")
	}

	// /help is public: everyone gets a list, but only of the commands
	// they may run.
	res := b.Handle("/help", false)
	if !res.Handled || res.Debug {
		t.Fatalf("/help = %+v, want handled without debug", res)
	}
	if res.Reply == "" || len(res.Reply) > 67 {
		t.Fatalf("/help reply = %q, want a short list", res.Reply)
	}
	for _, want := range []string{"/help", "Commands:"} {
		if !strings.Contains(res.Reply, want) {
			t.Fatalf("public help reply = %q, missing %s", res.Reply, want)
		}
	}
	if strings.Contains(res.Reply, "/debug") {
		t.Fatalf("public help reply = %q, must not list restricted commands", res.Reply)
	}

	res = b.Handle("/help", true)
	if !res.Handled || !strings.Contains(res.Reply, "/debug") {
		t.Fatalf("authorized /help = %+v, want the full list including /debug", res)
	}

	// /debug is restricted: authorized senders fire the debug alarm,
	// everyone else gets the public banner.
	res = b.Handle("/DEBUG", true)
	if !res.Handled || !res.Debug || res.Reply == "" {
		t.Fatalf("/DEBUG = %+v, want debug with a confirmation", res)
	}
	res = b.Handle("/debug", false)
	if !res.Handled || res.Debug || res.Reply == "" {
		t.Fatalf("unauthorized /debug = %+v, want the public banner without debug", res)
	}
	if !strings.Contains(res.Reply, HelpHint) {
		t.Fatalf("unauthorized /debug reply = %q, want the banner with the /help hint", res.Reply)
	}

	// Unknown slash messages answer with the installation banner plus
	// the /help hint, not a command list.
	res = b.Handle("/nope", true)
	if !res.Handled || res.Debug {
		t.Fatalf("/nope = %+v, want handled without debug", res)
	}
	if res.Reply != "WarnFlux v1.0 - SOSNA - sosna.sp9moa.pl | type /help for help" {
		t.Fatalf("/nope reply = %q, want the identity banner with the /help hint", res.Reply)
	}
	if b.Identity() != "WarnFlux v1.0 - SOSNA - sosna.sp9moa.pl" {
		t.Fatalf("Identity() = %q, want the banner", b.Identity())
	}
	if b.Banner() != "WarnFlux v1.0 - SOSNA - sosna.sp9moa.pl | type /help for help" {
		t.Fatalf("Banner() = %q, want identity + /help hint", b.Banner())
	}

	// Future topics plug in: public commands through Register, reserved
	// ones through RegisterRestricted.
	called := false
	b.Register("test", "testowa", func(args string) Result {
		called = true
		return Result{Handled: true, Reply: "arg=" + args}
	})
	if res := b.Handle("/test abc", false); !res.Handled || !called || res.Reply != "arg=abc" {
		t.Fatalf("/test = %+v (called=%v), want it to run for everyone", res, called)
	}
	opCalled := false
	b.RegisterRestricted("op", "operator only", func(args string) Result {
		opCalled = true
		return Result{Handled: true, Reply: "op ok"}
	})
	if res := b.Handle("/op", false); !res.Handled || opCalled || !strings.Contains(res.Reply, HelpHint) {
		t.Fatalf("unauthorized /op = %+v (called=%v), want the public banner", res, opCalled)
	}
	if res := b.Handle("/op", true); !res.Handled || !opCalled || res.Reply != "op ok" {
		t.Fatalf("authorized /op = %+v (called=%v), want it to run", res, opCalled)
	}
	if res := b.Handle("/help", false); !strings.Contains(res.Reply, "/test") || strings.Contains(res.Reply, "/op") {
		t.Fatalf("public help reply = %q, want /test but not /op", res.Reply)
	}
	if res := b.Handle("/help", true); !strings.Contains(res.Reply, "/test") || !strings.Contains(res.Reply, "/op") {
		t.Fatalf("authorized help reply = %q, want /test and /op", res.Reply)
	}
}

// TestBannerCapped keeps the banner within the 67-character APRS
// message limit even for a very long installation identity.
func TestBannerCapped(t *testing.T) {
	b := New(strings.Repeat("x", 80))
	if got := b.Banner(); len([]rune(got)) != 67 {
		t.Fatalf("Banner() = %d runes, want 67", len([]rune(got)))
	}
	if !strings.Contains(b.Banner(), HelpHint) {
		t.Fatalf("Banner() = %q, missing the /help hint", b.Banner())
	}
}
