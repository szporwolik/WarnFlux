package web_test

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/gsm"
	"github.com/szporwolik/WarnFlux/internal/storage"
)

// fakeGSMMsgs is an in-memory GSMMessageStore.
type fakeGSMMsgs struct {
	mu   sync.Mutex
	rows []storage.GSMMessage
}

func (f *fakeGSMMsgs) RecordGSMMessage(_ context.Context, direction, from, to, text string, at time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rows = append(f.rows, storage.GSMMessage{Direction: direction, From: from, To: to, Text: text, At: at})
	return nil
}

func (f *fakeGSMMsgs) ListGSMMessages(_ context.Context, direction string, limit, offset int) ([]storage.GSMMessage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if direction == "all" {
		direction = ""
	}
	var out []storage.GSMMessage
	for i := len(f.rows) - 1; i >= 0; i-- {
		if direction != "" && f.rows[i].Direction != direction {
			continue
		}
		out = append(out, f.rows[i])
	}
	if offset >= len(out) {
		return nil, nil
	}
	out = out[offset:]
	if limit >= 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (f *fakeGSMMsgs) CountGSMMessages(_ context.Context, direction string) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if direction == "all" {
		direction = ""
	}
	n := 0
	for _, r := range f.rows {
		if direction == "" || r.Direction == direction {
			n++
		}
	}
	return n, nil
}

// scriptedPort serves one response queue byte-by-byte and records the
// writes, like the modem would.
type scriptedPort struct {
	ch     chan byte
	mu     sync.Mutex
	writes strings.Builder
}

func newScriptedPort(script string) *scriptedPort {
	p := &scriptedPort{ch: make(chan byte, 8192)}
	for i := 0; i < len(script); i++ {
		p.ch <- script[i]
	}
	return p
}

func (p *scriptedPort) Read(b []byte) (int, error) {
	c, ok := <-p.ch
	if !ok {
		return 0, io.EOF
	}
	b[0] = c
	return 1, nil
}

func (p *scriptedPort) Write(b []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.writes.Write(b)
}

func (p *scriptedPort) Close() error { return nil }

// newTestGSMHub builds a hub whose modem answers the init exchange and
// one outgoing send.
func newTestGSMHub(t *testing.T) *gsm.Hub {
	return newTestGSMHubRec(t, &fakeGSMMsgs{})
}

func newTestGSMHubRec(t *testing.T, rec gsm.MessageRecorder) *gsm.Hub {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := gsm.NewHub(gsm.Config{Enabled: true, Device: "/dev/null"}, rec, logger)
	// init: ATE0→OK, AT+CMGF=1→OK, AT+CSCS→OK, AT+CSDH=1→OK,
	// AT+CPMS=?→SM only (no drain/switch); status: AT+COPS=3,0→OK,
	// AT+COPS?→PLAY, AT+CSQ→26; send: prompt then OK.
	h.SetTransport(newScriptedPort(
		"OK\r\nOK\r\n" +
			"OK\r\n" +
			"OK\r\n" +
			"+CPMS: (\"SM\",\"SR\")\r\nOK\r\n" +
			"OK\r\n" +
			"+COPS: 0,0,\"PLAY\",0\r\nOK\r\n" +
			"+CSQ: 26,99\r\nOK\r\n" +
			"> \r\n" +
			"OK\r\n"))
	h.Start(context.Background())
	t.Cleanup(h.Close)
	return h
}

func TestGSMPage(t *testing.T) {
	store := &fakeGSMMsgs{}
	hub := newTestGSMHub(t)
	env := newTestEnvWebUsers(t, defaultTestWebConfig(), nil, nil, nil, nil, nil, nil, nil, hub, store)

	resp, _ := env.get("/gsm")
	if resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/login" {
		t.Fatalf("GET /gsm unauthenticated = %d %q, want 303 /login", resp.StatusCode, resp.Header.Get("Location"))
	}

	if err := store.RecordGSMMessage(context.Background(), "rx", "+48600111222", "self", "witam", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordGSMMessage(context.Background(), "tx", "self", "+48600999888", "odpowiedz", time.Now()); err != nil {
		t.Fatal(err)
	}
	env.login()
	resp2, html := env.get("/gsm")
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("GET /gsm = %d, want 200", resp2.StatusCode)
	}
	if !strings.Contains(html, "GSM / SMS") {
		t.Errorf("gsm page missing title: %s", html)
	}
	if !strings.Contains(html, `action="/gsm/send"`) {
		t.Errorf("gsm page missing send form: %s", html)
	}
	if !strings.Contains(html, "/gsm?dir=rx") || !strings.Contains(html, "/gsm?dir=tx") {
		t.Errorf("gsm page missing direction filters: %s", html)
	}
	if !strings.Contains(html, "48600111222") || !strings.Contains(html, "48600999888") {
		t.Errorf("gsm page missing history rows: %s", html)
	}
	if !strings.Contains(html, "Modem connected and ready") {
		t.Errorf("gsm page missing modem status: %s", html)
	}
	if !strings.Contains(html, "Play") || !strings.Contains(html, "aria-label=\"CSQ 26 · -61 dBm\"") {
		t.Errorf("gsm page missing operator/signal indicator: %s", html)
	}
	if !strings.Contains(html, "class=\"gsm-bar on\"") {
		t.Errorf("gsm page missing signal bars: %s", html)
	}
	if !strings.Contains(html, `<span class="nav-label">GSM</span>`) {
		t.Errorf("gsm page nav should read GSM: %s", html)
	}

	// Direction filter narrows the history.
	_, rxHTML := env.get("/gsm?dir=rx")
	if strings.Contains(rxHTML, "48600999888") {
		t.Errorf("rx filter leaked a tx row: %s", rxHTML)
	}
}

func TestGSMPagePickup(t *testing.T) {
	store := &fakeGSMMsgs{}
	hub := newTestGSMHub(t)
	env := newTestEnvWebUsers(t, defaultTestWebConfig(), nil, nil, nil, nil, nil, nil, nil, hub, store)
	u, err := env.users.CreateUser("sp9kow", "600111222", "", "", "member", "pw1")
	if err != nil {
		t.Fatal(err)
	}
	_ = u
	env.login()
	_, html := env.get("/gsm")
	if !strings.Contains(html, `list="gsm-phones"`) || !strings.Contains(html, `<datalist id="gsm-phones"><option value="600111222">600111222 · sp9kow</option>`) {
		t.Errorf("gsm page missing phone picker: %s", html)
	}
}

func TestGSMSendValidation(t *testing.T) {
	store := &fakeGSMMsgs{}
	hub := newTestGSMHub(t)
	env := newTestEnvWebUsers(t, defaultTestWebConfig(), nil, nil, nil, nil, nil, nil, nil, hub, store)
	env.login()
	_, html := env.get("/gsm")
	csrf := extractCSRF(t, html)

	// Wrong CSRF: hard 403.
	resp, _ := env.postForm("/gsm/send", url.Values{"csrf": {"bogus"}, "number": {"+48600111222"}, "text": {"hello"}})
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("POST gsm send bad csrf = %d, want 403", resp.StatusCode)
	}

	// Invalid number: redirect with an error flash.
	resp, _ = env.postForm("/gsm/send", url.Values{"csrf": {csrf}, "number": {"abc"}, "text": {"hello"}})
	if resp.StatusCode != http.StatusSeeOther || !strings.Contains(resp.Header.Get("Location"), "err=") {
		t.Fatalf("POST invalid number = %d %q, want 303 with err", resp.StatusCode, resp.Header.Get("Location"))
	}
	if n, _ := store.CountGSMMessages(context.Background(), ""); n != 0 {
		t.Fatalf("invalid send recorded %d rows", n)
	}

	// Empty text: redirect with an error flash.
	resp, _ = env.postForm("/gsm/send", url.Values{"csrf": {csrf}, "number": {"+48600111222"}, "text": {"  "}})
	if resp.StatusCode != http.StatusSeeOther || !strings.Contains(resp.Header.Get("Location"), "err=") {
		t.Fatalf("POST empty text = %d %q, want 303 with err", resp.StatusCode, resp.Header.Get("Location"))
	}
}

// TestGSMSendFlow pins the happy path: the message goes through the hub
// and the flash confirms delivery.
func TestGSMSendFlow(t *testing.T) {
	store := &fakeGSMMsgs{}
	hub := newTestGSMHubRec(t, store) // the hub's tx history is this store
	env := newTestEnvWebUsers(t, defaultTestWebConfig(), nil, nil, nil, nil, nil, nil, nil, hub, store)
	env.login()
	_, html := env.get("/gsm")
	csrf := extractCSRF(t, html)

	resp, _ := env.postForm("/gsm/send", url.Values{"csrf": {csrf}, "number": {"+48600111222"}, "text": {"Czesc"}})
	if resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/gsm?sent=1" {
		t.Fatalf("POST send = %d %q, want 303 /gsm?sent=1", resp.StatusCode, resp.Header.Get("Location"))
	}
	rows, _ := store.ListGSMMessages(context.Background(), "tx", 10, 0)
	if len(rows) != 1 || rows[0].To != "+48600111222" || rows[0].Text != "Czesc" {
		t.Fatalf("tx rows = %+v", rows)
	}
}

// TestGSMSendNoModem pins the flash when the hub has no working session.
func TestGSMSendNoModem(t *testing.T) {
	rec := &fakeGSMMsgs{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	hub := gsm.NewHub(gsm.Config{Enabled: true}, rec, logger) // no transport
	env := newTestEnvWebUsers(t, defaultTestWebConfig(), nil, nil, nil, nil, nil, nil, nil, hub, rec)
	env.login()
	_, html := env.get("/gsm")
	csrf := extractCSRF(t, html)

	resp, _ := env.postForm("/gsm/send", url.Values{"csrf": {csrf}, "number": {"+48600111222"}, "text": {"x"}})
	if resp.StatusCode != http.StatusSeeOther || !strings.Contains(resp.Header.Get("Location"), "err=") {
		t.Fatalf("POST send without modem = %d %q, want 303 with err", resp.StatusCode, resp.Header.Get("Location"))
	}
}
