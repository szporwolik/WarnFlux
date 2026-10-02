package radiocli

import (
	"strings"
	"testing"
)

// TestHandle pins the command interpreter: case-insensitive commands,
// the help list, the debug pass-through, unknown commands and plain
// (non-command) messages.
func TestHandle(t *testing.T) {
	b := New("WarnFlux v1.0 - SOSNA - sosna.sp9moa.pl")

	if res := b.Handle("powodz w krakowie"); res.Handled {
		t.Fatal("plain text must not be handled as a command")
	}

	res := b.Handle("/help")
	if !res.Handled || res.Debug {
		t.Fatalf("/help = %+v, want handled without debug", res)
	}
	if res.Reply == "" || len(res.Reply) > 67 {
		t.Fatalf("/help reply = %q, want a short list", res.Reply)
	}
	for _, want := range []string{"/help", "/debug", "Commands:"} {
		if !strings.Contains(res.Reply, want) {
			t.Fatalf("help reply = %q, missing %s", res.Reply, want)
		}
	}

	res = b.Handle("/DEBUG")
	if !res.Handled || !res.Debug || res.Reply == "" {
		t.Fatalf("/DEBUG = %+v, want debug with a confirmation", res)
	}

	// Unknown slash messages answer with the installation banner, not a
	// command hint.
	res = b.Handle("/nope")
	if !res.Handled || res.Debug {
		t.Fatalf("/nope = %+v, want handled without debug", res)
	}
	if res.Reply != "WarnFlux v1.0 - SOSNA - sosna.sp9moa.pl" {
		t.Fatalf("/nope reply = %q, want the identity banner", res.Reply)
	}
	if b.Identity() != "WarnFlux v1.0 - SOSNA - sosna.sp9moa.pl" {
		t.Fatalf("Identity() = %q, want the banner", b.Identity())
	}

	// Future topics plug in through Register.
	called := false
	b.Register("test", "testowa", func(args string) Result {
		called = true
		return Result{Handled: true, Reply: "arg=" + args}
	})
	if res := b.Handle("/test abc"); !res.Handled || !called || res.Reply != "arg=abc" {
		t.Fatalf("/test = %+v (called=%v)", res, called)
	}
	if res := b.Handle("/help"); !strings.Contains(res.Reply, "/test") {
		t.Fatalf("help reply = %q, missing the registered command", res.Reply)
	}
}
