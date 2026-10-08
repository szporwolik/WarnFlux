package gsm

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/dispatch"
	"github.com/szporwolik/WarnFlux/internal/radiocli"
)

// replyHub wires a hub with the shared CLI and a scripted port ready to
// accept n automatic replies (each reply consumes CMGF=1, the prompt,
// the network OK and the CMGF=0 restore).
func replyHub(t *testing.T, n int) (*Hub, *fakePort) {
	t.Helper()
	h, f, _ := newTestHub(t)
	h.SetCLI(radiocli.New("WarnFlux v1 - SOSDEV - sosna.sp9moa.pl"))
	for i := 0; i < n; i++ {
		f.feed("OK\r\n") // AT+CMGF=1
		f.feed("> \r\n") // CMGS prompt
		f.feed("OK\r\n") // network accept
		f.feed("OK\r\n") // AT+CMGF=0 restore
	}
	return h, f
}

// waitForWritten polls the recorded write stream until it contains want
// (replies are fire-and-forget goroutines).
func waitForWritten(t *testing.T, f *fakePort, want string) string {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if w := f.written(); strings.Contains(w, want) {
			return w
		}
		time.Sleep(5 * time.Millisecond)
	}
	return f.written()
}

func TestNumberKey(t *testing.T) {
	cases := map[string]string{
		"+48509558155":    "48509558155",
		"509 558 155":     "509558155",
		"+48 509-558-155": "48509558155",
		"":                "",
	}
	for in, want := range cases {
		if got := NumberKey(in); got != want {
			t.Errorf("NumberKey(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestSMSCommandHelpPublic pins the public command list: /help works for
// an unregistered sender but must not list restricted commands.
func TestSMSCommandHelpPublic(t *testing.T) {
	h, f := replyHub(t, 1)
	h.SetSenderGate(func(string) string { return "" })
	h.routeMessage("+48600111222", "/help")
	w := waitForWritten(t, f, "Commands: /help")
	if !strings.Contains(w, "Commands: /help") {
		t.Fatalf("help reply missing: %q", w)
	}
	if strings.Contains(w, "/debug") || strings.Contains(w, "/alert") {
		t.Fatalf("public help leaked restricted commands: %q", w)
	}
}

// TestSMSCommandHelpAuthorized pins the registered-sender list: the
// restricted commands appear once the phone belongs to a user.
func TestSMSCommandHelpAuthorized(t *testing.T) {
	h, f := replyHub(t, 1)
	h.SetSenderGate(func(string) string { return "sp9kow" })
	h.routeMessage("+48600111222", "/help")
	w := waitForWritten(t, f, "/debug")
	if !strings.Contains(w, "/debug") {
		t.Fatalf("authorized help missing /debug: %q", w)
	}
}

// TestSMSUnknownSlashBanner pins the installation banner for unknown
// slash texts.
func TestSMSUnknownSlashBanner(t *testing.T) {
	h, f := replyHub(t, 1)
	h.SetSenderGate(func(string) string { return "" })
	h.routeMessage("+48600111222", "/wat")
	w := waitForWritten(t, f, radiocli.HelpHint)
	if !strings.Contains(w, "WarnFlux v1") || !strings.Contains(w, "type /help for help") {
		t.Fatalf("banner reply missing: %q", w)
	}
}

// TestSMSPlainBanner pins the plain-text answer: the banner with the
// /help hint, never an alarm.
func TestSMSPlainBanner(t *testing.T) {
	h, f := replyHub(t, 1)
	h.SetSenderGate(func(string) string { return "" })
	h.routeMessage("+48600111222", "hej, co umiesz?")
	w := waitForWritten(t, f, "type /help for help")
	if !strings.Contains(w, "WarnFlux v1") {
		t.Fatalf("plain reply missing the banner: %q", w)
	}
}

// TestSMSDeniedRestricted pins the explicit denial for an unregistered
// sender attempting a restricted command.
func TestSMSDeniedRestricted(t *testing.T) {
	h, f := replyHub(t, 1)
	h.SetSenderGate(func(string) string { return "" })
	h.mu.Lock()
	cli := h.cli
	h.mu.Unlock()
	cli.RegisterRestricted("alert", "alert", func(string) radiocli.Result {
		return radiocli.Result{Handled: true, Reply: "OK: alert raised"}
	})
	h.routeMessage("+48600111222", "/alert test")
	w := waitForWritten(t, f, radiocli.DeniedText)
	if !strings.Contains(w, "You are not authorized") {
		t.Fatalf("denial missing: %q", w)
	}
}

// TestSMSDebugEvent pins the /debug path: an authorized sender's command
// becomes a canonical "gsm" event through the installed acceptor and the
// reply is the handler confirmation.
func TestSMSDebugEvent(t *testing.T) {
	h, f := replyHub(t, 1)
	h.SetSenderGate(func(string) string { return "sp9kow" })
	var mu sync.Mutex
	var payload string
	h.SetEventAcceptor(func(p []byte) dispatch.Acceptance {
		mu.Lock()
		payload = string(p)
		mu.Unlock()
		return dispatch.AcceptedDurable
	})
	h.routeMessage("+48600111222", "/debug")
	w := waitForWritten(t, f, "OK: debug alarm generated")
	mu.Lock()
	p := payload
	mu.Unlock()
	if !strings.Contains(p, `"source":"gsm"`) || !strings.Contains(p, "gsm:+48600111222:msg:") {
		t.Fatalf("debug event payload = %q", p)
	}
	_ = w
}

// TestSMSAlertEvent pins the /alert path for a registered sender: a
// severe hazard with the default TTL flows into the pipeline.
func TestSMSAlertEvent(t *testing.T) {
	h, f := replyHub(t, 1)
	h.SetSenderGate(func(string) string { return "sp9kow" })
	h.mu.Lock()
	cli := h.cli
	h.mu.Unlock()
	cli.RegisterRestricted("alert", "alert", func(args string) radiocli.Result {
		return radiocli.Result{Handled: true, Alert: &radiocli.AlertSpec{Headline: args}, Reply: "OK: alert raised"}
	})
	var mu sync.Mutex
	var payload string
	h.SetEventAcceptor(func(p []byte) dispatch.Acceptance {
		mu.Lock()
		payload = string(p)
		mu.Unlock()
		return dispatch.AcceptedDurable
	})
	h.routeMessage("+48600111222", "/alert Burza nad miastem")
	w := waitForWritten(t, f, "OK: alert raised")
	mu.Lock()
	p := payload
	mu.Unlock()
	if !strings.Contains(p, ":alert:") || !strings.Contains(p, "Burza nad miastem") {
		t.Fatalf("alert event payload = %q", p)
	}
	_ = w
}

// TestSMSReplyFitsOneMessage pins the reply fitting: a long command
// answer is transliterated and hard-capped so it always fits a single
// 160-character text-mode SMS.
func TestSMSReplyFitsOneMessage(t *testing.T) {
	h, f := replyHub(t, 1)
	h.SetSenderGate(func(string) string { return "" })
	h.mu.Lock()
	cli := h.cli
	h.mu.Unlock()
	cli.Register("long", "long reply", func(string) radiocli.Result {
		return radiocli.Result{Handled: true, Reply: "ż" + strings.Repeat("x", 200)}
	})
	h.routeMessage("+48600111222", "/long")
	w := waitForWritten(t, f, "\x1A")
	idx := strings.Index(w, "AT+CMGS=\"+48600111222\"\r")
	if idx < 0 {
		t.Fatalf("reply send missing: %q", w)
	}
	body := w[idx:]
	// The body sits between the prompt and the ctrl-Z.
	start := strings.Index(body, "\r") + 1
	if end := strings.Index(body, "\x1A"); end > start {
		body = body[start:end]
	}
	body = strings.TrimSpace(body)
	if n := len([]rune(body)); n > 160 {
		t.Fatalf("reply length = %d, want <= 160", n)
	}
	if !strings.HasPrefix(body, "z") {
		t.Fatalf("reply was not transliterated: %q", body)
	}
}

// TestSMSReplyBurst pins the shared reply budget: a flood of distinct
// unknown commands produces at most replyBurst automatic answers.
func TestSMSReplyBurst(t *testing.T) {
	h, f := replyHub(t, replyBurst)
	h.SetSenderGate(func(string) string { return "" })
	for i := 0; i < replyBurst+2; i++ {
		h.routeMessage("+48600111222", "/unknown"+string(rune('a'+i)))
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Count(f.written(), "AT+CMGS=") >= replyBurst {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if n := strings.Count(f.written(), "AT+CMGS="); n != replyBurst {
		t.Fatalf("automatic replies after flood = %d, want exactly %d", n, replyBurst)
	}
}

// TestSMSCommandReplay pins the dedup: a repeated command replays the
// previous reply instead of re-executing (one /debug event total).
func TestSMSCommandReplay(t *testing.T) {
	h, f := replyHub(t, 2)
	h.SetSenderGate(func(string) string { return "sp9kow" })
	var mu sync.Mutex
	events := 0
	h.SetEventAcceptor(func(p []byte) dispatch.Acceptance {
		mu.Lock()
		events++
		mu.Unlock()
		return dispatch.AcceptedDurable
	})
	h.routeMessage("+48600111222", "/debug")
	w := waitForWritten(t, f, "OK: debug alarm generated")
	h.routeMessage("+48600111222", "/debug")
	w = waitForWritten(t, f, "\x1A")
	if strings.Count(w, "OK: debug alarm generated") < 1 {
		t.Fatalf("replay reply missing: %q", w)
	}
	mu.Lock()
	n := events
	mu.Unlock()
	if n != 1 {
		t.Fatalf("debug events = %d, want 1 (replay must not re-execute)", n)
	}
}
