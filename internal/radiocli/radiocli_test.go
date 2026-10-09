package radiocli

import (
	"strings"
	"testing"
	"time"
)

// TestHandle pins the command interpreter: case-insensitive commands,
// the public/restricted split, the help list per authorization level,
// the debug pass-through, unknown commands and plain (non-command)
// messages.
func TestHandle(t *testing.T) {
	b := New("WarnFlux v1.0 - SOSNA - sosna.sp9moa.pl")

	if res := b.Handle("powodz w krakowie", Sender{}); res.Handled {
		t.Fatal("plain text must not be handled as a command")
	}

	// /help is public: everyone gets a list, but only of the commands
	// they may run.
	res := b.Handle("/help", Sender{})
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

	res = b.Handle("/help", Sender{Authorized: true})
	if !res.Handled || !strings.Contains(res.Reply, "/debug") {
		t.Fatalf("authorized /help = %+v, want the full list including /debug", res)
	}
	if len(res.Reply) > 67 {
		t.Fatalf("authorized /help = %q, %d chars — over the APRS limit", res.Reply, len(res.Reply))
	}

	// /debug is restricted: authorized senders fire the debug alarm,
	// everyone else gets an explicit denial.
	res = b.Handle("/DEBUG", Sender{Authorized: true})
	if !res.Handled || !res.Debug || res.Reply == "" {
		t.Fatalf("/DEBUG = %+v, want debug with a confirmation", res)
	}
	res = b.Handle("/debug", Sender{})
	if !res.Handled || res.Debug || res.Reply == "" {
		t.Fatalf("unauthorized /debug = %+v, want a denial without debug", res)
	}
	if !strings.Contains(res.Reply, "WarnFlux v1.0 - SOSNA") || !strings.Contains(res.Reply, DeniedText) {
		t.Fatalf("unauthorized /debug reply = %q, want the banner with the denial", res.Reply)
	}
	if len(res.Reply) > 67 {
		t.Fatalf("unauthorized /debug reply = %q, %d chars — over the APRS limit", res.Reply, len(res.Reply))
	}
	if b.Denied() != "WarnFlux v1.0 - SOSNA - sosna.sp9moa.pl - You are not authorized" {
		t.Fatalf("Denied() = %q, want identity + denial", b.Denied())
	}

	// Unknown slash messages answer with the installation banner plus
	// the /help hint, not a command list.
	res = b.Handle("/nope", Sender{Authorized: true})
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
	b.Register("test", "testowa", func(args string, _ Sender) Result {
		called = true
		return Result{Handled: true, Reply: "arg=" + args}
	})
	if res := b.Handle("/test abc", Sender{}); !res.Handled || !called || res.Reply != "arg=abc" {
		t.Fatalf("/test = %+v (called=%v), want it to run for everyone", res, called)
	}
	opCalled := false
	b.RegisterRestricted("op", "operator only", func(args string, _ Sender) Result {
		opCalled = true
		return Result{Handled: true, Reply: "op ok"}
	})
	if res := b.Handle("/op", Sender{}); !res.Handled || opCalled || !strings.Contains(res.Reply, DeniedText) {
		t.Fatalf("unauthorized /op = %+v (called=%v), want the denial", res, opCalled)
	}
	if res := b.Handle("/op", Sender{Authorized: true}); !res.Handled || !opCalled || res.Reply != "op ok" {
		t.Fatalf("authorized /op = %+v (called=%v), want it to run", res, opCalled)
	}

	// /alert (registered restricted, like main wires it): a missing
	// parameter answers with the usage, a real text carries the
	// AlertSpec back to the channel.
	b.RegisterRestricted("alert", "alert", func(args string, _ Sender) Result {
		headline := strings.TrimSpace(args)
		if headline == "" {
			return Result{Handled: true, Reply: "Missing parameter: /alert <text>"}
		}
		return Result{Handled: true, Alert: &AlertSpec{Headline: headline, TTL: 4 * time.Hour}, Reply: "OK: alert raised"}
	})
	if res := b.Handle("/alert", Sender{}); !res.Handled || res.Alert != nil || !strings.Contains(res.Reply, DeniedText) {
		t.Fatalf("unauthorized /alert = %+v, want the denial without an alert", res)
	}
	if res := b.Handle("/alert", Sender{Authorized: true}); !res.Handled || res.Alert != nil || !strings.Contains(res.Reply, "Missing parameter") {
		t.Fatalf("authorized no-arg /alert = %+v, want the usage reply", res)
	}
	if res := b.Handle("/alert pozar lasu", Sender{Authorized: true}); !res.Handled || res.Alert == nil ||
		res.Alert.Headline != "pozar lasu" || res.Alert.TTL != 4*time.Hour || !strings.Contains(res.Reply, "OK") {
		t.Fatalf("authorized /alert = %+v, want the AlertSpec with the 4h TTL", res)
	}
	if res := b.Handle("/help", Sender{}); !strings.Contains(res.Reply, "/test") || strings.Contains(res.Reply, "/op") {
		t.Fatalf("public help reply = %q, want /test but not /op", res.Reply)
	}
	if res := b.Handle("/help", Sender{Authorized: true}); !strings.Contains(res.Reply, "/test") || !strings.Contains(res.Reply, "/op") || !strings.Contains(res.Reply, "/alert") {
		t.Fatalf("authorized help reply = %q, want /test, /op and /alert", res.Reply)
	}
	if len(res.Reply) > 67 {
		t.Fatalf("authorized help reply = %q, %d chars — over the APRS limit", res.Reply, len(res.Reply))
	}
}

// TestFitProgressive pins the shared channel fitting: the identity
// shortens progressively (domain → installation name → the WarnFlux
// word) and only as the very last resort the payload end is cut.
func TestFitProgressive(t *testing.T) {
	b := New("WarnFlux v1.0 - SOSNA - sosna.sp9moa.pl")

	// Fits as-is: untouched.
	if got := b.Fit(b.Banner(), 67); got != b.Banner() {
		t.Fatalf("Fit(banner,67) = %q, want it unchanged", got)
	}

	// Domain first: a very long address drops, the installation name
	// survives.
	long := New("WarnFlux v1.0 - SOSNA - a.very.long.domain.example.org")
	if got := long.Fit(long.Banner(), 67); got != "WarnFlux v1.0 - SOSNA | type /help for help" {
		t.Fatalf("Fit = %q, want the domain dropped", got)
	}

	// Instance next: a long installation name leaves only the WarnFlux
	// word.
	longInst := New("WarnFlux v1.0 - " + strings.Repeat("x", 60))
	if got := longInst.Fit(longInst.Banner(), 67); got != "WarnFlux v1.0 | type /help for help" {
		t.Fatalf("Fit = %q, want the instance dropped", got)
	}

	// The denial shortens the same way.
	if got := long.Fit(long.Denied(), 40); got != "WarnFlux v1.0 - You are not authorized" {
		t.Fatalf("Fit(denied,40) = %q, want instance+domain dropped, denial kept", got)
	}

	// The WarnFlux word itself drops before the payload is cut.
	if got := b.Fit(b.Banner(), 20); got != "type /help for help" {
		t.Fatalf("Fit(banner,20) = %q, want only the hint", got)
	}

	// Last resort: the payload end is truncated.
	if got := b.Fit(b.Banner(), 10); len([]rune(got)) != 10 || !strings.HasPrefix(got, "type /hel") {
		t.Fatalf("Fit(banner,10) = %q, want the payload truncated at 10 runes", got)
	}

	// Texts without the identity are simply truncated at the end.
	if got := b.Fit("T 7.5C / RH 86% (29 st) | fcst 22/6C", 12); got != "T 7.5C / RH " {
		t.Fatalf("Fit(weather,12) = %q, want the end cut", got)
	}
}

// TestReplyLines pins the multi-line reply splitting used by both radio
// channels for the /hazard list: one message per line, CRLF tolerated,
// blank lines dropped, and the cap closed with a "+N more" marker.
func TestReplyLines(t *testing.T) {
	if got := ReplyLines("header\nline one\nline two", 12); len(got) != 3 ||
		got[0] != "header" || got[1] != "line one" || got[2] != "line two" {
		t.Fatalf("ReplyLines = %v", got)
	}
	if got := ReplyLines("a\r\nb\n\nc\r", 12); len(got) != 3 || got[0] != "a" || got[1] != "b" || got[2] != "c" {
		t.Fatalf("ReplyLines(CRLF) = %v", got)
	}
	got := ReplyLines("h\n1\n2\n3", 2)
	if len(got) != 3 || got[0] != "h" || got[1] != "1" || got[2] != "+2 more" {
		t.Fatalf("ReplyLines(cap) = %v, want h, 1, +2 more", got)
	}
	if got := ReplyLines("only", 12); len(got) != 1 || got[0] != "only" {
		t.Fatalf("ReplyLines(single) = %v", got)
	}
	if got := ReplyLines("", 12); len(got) != 0 {
		t.Fatalf("ReplyLines(empty) = %v, want none", got)
	}
}
