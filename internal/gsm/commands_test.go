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
// accept n automatic replies (each reply consumes the CMGS prompt and
// the network OK — the session is permanently in text mode).
func replyHub(t *testing.T, n int) (*Hub, *fakePort) {
	t.Helper()
	h, f, _ := newTestHub(t)
	h.SetCLI(radiocli.New("WarnFlux v1 - SOSDEV - sosna.sp9moa.pl"))
	for i := 0; i < n; i++ {
		f.feed("> \r\n") // CMGS prompt
		f.feed("OK\r\n") // network accept
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

func TestCanonicalNumberKey(t *testing.T) {
	cases := map[string]string{
		"+48509558155":    "48509558155",
		"0048509558155":   "48509558155",
		"+48 509-558-155": "48509558155",
		"0044 7000 00000": "44700000000",
		"509 558 155":     "509558155",
		"":                "",
		"00":              "",
	}
	for in, want := range cases {
		if got := CanonicalNumberKey(in); got != want {
			t.Errorf("CanonicalNumberKey(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestSMSUnknownSenderIgnored pins the spam hygiene: an unregistered
// number is never answered and never reaches the command interpreter —
// not even for /help or a plain text.
func TestSMSUnknownSenderIgnored(t *testing.T) {
	h, f := replyHub(t, 0)
	h.SetSenderGate(func(string) string { return "" })
	ran := false
	h.mu.Lock()
	cli := h.cli
	h.mu.Unlock()
	cli.Register("zzz", "zzz", func(string) radiocli.Result {
		ran = true
		return radiocli.Result{Handled: true, Reply: "ZZZ"}
	})
	for _, msg := range []string{"hej", "/zzz", "/debug"} {
		h.routeMessage("+48600111222", msg)
	}
	time.Sleep(50 * time.Millisecond)
	if w := f.written(); strings.Contains(w, "AT+CMGS") {
		t.Fatalf("unknown sender got an automatic reply: %q", w)
	}
	if ran {
		t.Fatal("unknown sender reached the command interpreter")
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

// TestSMSKnownSlashBanner pins the installation banner for unknown slash
// texts from a registered sender.
func TestSMSKnownSlashBanner(t *testing.T) {
	h, f := replyHub(t, 1)
	h.SetSenderGate(func(string) string { return "sp9kow" })
	h.routeMessage("+48600111222", "/wat")
	w := waitForWritten(t, f, radiocli.HelpHint)
	if !strings.Contains(w, "WarnFlux v1") || !strings.Contains(w, "type /help for help") {
		t.Fatalf("banner reply missing: %q", w)
	}
}

// TestSMSPlainBanner pins the plain-text answer for a REGISTERED sender:
// the banner with the /help hint, never an alarm.
func TestSMSPlainBanner(t *testing.T) {
	h, f := replyHub(t, 1)
	h.SetSenderGate(func(string) string { return "sp9kow" })
	h.routeMessage("+48600111222", "hej, co umiesz?")
	w := waitForWritten(t, f, "type /help for help")
	if !strings.Contains(w, "WarnFlux v1") {
		t.Fatalf("plain reply missing the banner: %q", w)
	}
}

// TestSMSRestrictedRegistered pins the authorized path: a registered
// sender runs a restricted command and gets its confirmation (unknown
// senders never reach this far — see TestSMSUnknownSenderIgnored).
func TestSMSRestrictedRegistered(t *testing.T) {
	h, f := replyHub(t, 1)
	h.SetSenderGate(func(string) string { return "sp9kow" })
	h.mu.Lock()
	cli := h.cli
	h.mu.Unlock()
	cli.RegisterRestricted("alert", "alert", func(string) radiocli.Result {
		return radiocli.Result{Handled: true, Reply: "OK: alert raised"}
	})
	h.routeMessage("+48600111222", "/alert test")
	w := waitForWritten(t, f, "OK: alert raised")
	if !strings.Contains(w, "OK: alert raised") {
		t.Fatalf("authorized restricted reply missing: %q", w)
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

// TestSMSRCBSender pins the broadcast-sender detection that separates
// national Alert RCB dispatches from ordinary messages.
func TestSMSRCBSender(t *testing.T) {
	for in, want := range map[string]bool{
		"ALERT RCB":    true,
		"alert rcb":    true,
		"Alert RCB":    true,
		"ALERT-RCB":    true,
		" alert rcb ":  true,
		"+48509558155": false,
		"":             false,
		"ALERT RCB2":   false,
		"RCB ALERT":    false,
		"alertrcb":     false,
	} {
		if got := isRCBSender(in); got != want {
			t.Errorf("isRCBSender(%q) = %v, want %v", in, got, want)
		}
	}
}

// TestSMSRCBEvent pins the Alert RCB pipeline: the incoming SMS becomes
// a canonical "rcb" event (severe, expiring 24 hours out) through the
// installed acceptor, and the broadcast sender is never answered.
func TestSMSRCBEvent(t *testing.T) {
	h, f := replyHub(t, 0)
	var mu sync.Mutex
	var payload string
	h.SetEventAcceptor(func(p []byte) dispatch.Acceptance {
		mu.Lock()
		payload = string(p)
		mu.Unlock()
		return dispatch.AcceptedDurable
	})
	h.routeMessage("ALERT RCB", "Uwaga! Jutro burze z gradem")
	mu.Lock()
	p := payload
	mu.Unlock()
	if p == "" {
		t.Fatal("no event published for the Alert RCB SMS")
	}
	if !strings.Contains(p, `"source":"rcb"`) {
		t.Fatalf("rcb event missing source: %q", p)
	}
	if !strings.Contains(p, `"severity":"severe"`) || !strings.Contains(p, `"expires_at"`) {
		t.Fatalf("rcb event severity/expiry missing: %q", p)
	}
	if !strings.Contains(p, "Uwaga! Jutro burze z gradem") {
		t.Fatalf("rcb event lost the alert text: %q", p)
	}
	// The broadcast sender must never be answered.
	time.Sleep(50 * time.Millisecond)
	if w := f.written(); strings.Contains(w, "AT+CMGS") {
		t.Fatalf("automatic reply sent to the Alert RCB sender: %q", w)
	}
}

// TestSMSRCBExpiry pins the 24-hour lifecycle window stamped on the
// generated hazard.
func TestSMSRCBExpiry(t *testing.T) {
	h, _ := replyHub(t, 0)
	fixed := time.Date(2025, 6, 1, 12, 0, 0, 0, time.UTC)
	h.now = func() time.Time { return fixed }
	var mu sync.Mutex
	var payload string
	h.SetEventAcceptor(func(p []byte) dispatch.Acceptance {
		mu.Lock()
		payload = string(p)
		mu.Unlock()
		return dispatch.AcceptedDurable
	})
	h.routeMessage("Alert RCB", "Uwaga! Burze")
	mu.Lock()
	p := payload
	mu.Unlock()
	wantEff := fixed.Format(time.RFC3339)
	wantExp := fixed.Add(24 * time.Hour).Format(time.RFC3339)
	if !strings.Contains(p, wantEff) || !strings.Contains(p, wantExp) {
		t.Fatalf("rcb event lifecycle = %q, want effective %s expires %s", p, wantEff, wantExp)
	}
}

// TestSMSRCBEmpty pins the no-op for an empty broadcast body.
func TestSMSRCBEmpty(t *testing.T) {
	h, f := replyHub(t, 0)
	called := false
	h.SetEventAcceptor(func([]byte) dispatch.Acceptance {
		called = true
		return dispatch.AcceptedDurable
	})
	h.routeMessage("ALERT RCB", "   ")
	if called {
		t.Fatal("empty RCB SMS published an event")
	}
	if w := f.written(); strings.Contains(w, "AT+CMGS") {
		t.Fatalf("automatic reply sent for an empty RCB SMS: %q", w)
	}
}

// TestSMSRCBPlainSender pins that an RCB-looking text from an ordinary
// phone neither publishes an rcb event nor gets an answer: unregistered
// numbers are dropped outright.
func TestSMSRCBPlainSender(t *testing.T) {
	h, f := replyHub(t, 0)
	called := false
	h.SetEventAcceptor(func([]byte) dispatch.Acceptance {
		called = true
		return dispatch.AcceptedDurable
	})
	h.routeMessage("+48600111222", "Uwaga! Jutro burze z gradem")
	if called {
		t.Fatal("plain phone message published an rcb event")
	}
	time.Sleep(50 * time.Millisecond)
	if w := f.written(); strings.Contains(w, "AT+CMGS") {
		t.Fatalf("automatic reply sent to an unregistered number: %q", w)
	}
}

// TestSMSReplyFitsOneMessage pins the reply fitting: a long command
// answer is transliterated and hard-capped so it always fits a single
// 160-character text-mode SMS.
func TestSMSReplyFitsOneMessage(t *testing.T) {
	h, f := replyHub(t, 1)
	h.SetSenderGate(func(string) string { return "sp9kow" })
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

// TestSMSReplyBurst pins the shared reply budget: a registered sender's
// flood of distinct unknown commands produces at most replyBurst
// automatic answers.
func TestSMSReplyBurst(t *testing.T) {
	h, f := replyHub(t, replyBurst)
	h.SetSenderGate(func(string) string { return "sp9kow" })
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
