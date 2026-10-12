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
	// init: ATE0→OK, AT+CMGF=0→OK, AT+CPMS=?→SM only (no drain/switch);
	// status: AT+COPS=3,0→OK, AT+COPS?→PLAY, AT+CSQ→26; send: prompt
	// then OK.
	h.SetTransport(newScriptedPort(
		"OK\r\nOK\r\n" +
			"OK\r\n" + // AT+CSCS="IRA"
			"+CPMS: (\"SM\",\"SR\")\r\nOK\r\n" +
			"OK\r\n" +
			"+COPS: 0,0,\"PLAY\",0\r\nOK\r\n" +
			"+CSQ: 26,99\r\nOK\r\n" +
			"OK\r\n" + // AT+CMGF=1 (send)
			"> \r\n" +
			"OK\r\n" +
			"OK\r\n")) // AT+CMGF=0 (restore)
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
	if !strings.Contains(html, `data-char-count="160"`) || !strings.Contains(html, `id="gsm-count"`) {
		t.Errorf("gsm page missing the character counter: %s", html)
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

// TestGSMSendTooLong pins the segment cap: the web handler rejects the
// message before the modem is ever asked.
func TestGSMSendTooLong(t *testing.T) {
	rec := &fakeGSMMsgs{}
	hub := newTestGSMHub(t)
	env := newTestEnvWebUsers(t, defaultTestWebConfig(), nil, nil, nil, nil, nil, nil, nil, hub, rec)
	env.login()
	_, html := env.get("/gsm")
	csrf := extractCSRF(t, html)

	resp, _ := env.postForm("/gsm/send", url.Values{"csrf": {csrf}, "number": {"+48600111222"}, "text": {strings.Repeat("A", 500)}})
	if resp.StatusCode != http.StatusSeeOther || !strings.Contains(resp.Header.Get("Location"), "too+long") {
		t.Fatalf("POST long text = %d %q, want 303 with too_long flash", resp.StatusCode, resp.Header.Get("Location"))
	}
	if n, _ := rec.CountGSMMessages(context.Background(), ""); n != 0 {
		t.Fatalf("long send recorded %d rows", n)
	}
}

// TestGSMMessagesMergeLegacyParts pins the display-time joining of the
// split rows imported by the old text-mode pipeline: consecutive rx
// rows from one sender within the same minute render as ONE message in
// reading order (the list is newest-first, the parts are ascending).
// TestGSMMessagesKeepIndependentMessagesApart pins the reported P2: the
// history used to glue every consecutive rx row from one sender within
// the same minute, so two independent SMSes shown one text. Each stored
// message is shown as it is; the parts of a split message are joined
// once by the v58 migration, not guessed at display time.
func TestGSMMessagesKeepIndependentMessagesApart(t *testing.T) {
	store := &fakeGSMMsgs{}
	hub := newTestGSMHub(t)
	env := newTestEnvWebUsers(t, defaultTestWebConfig(), nil, nil, nil, nil, nil, nil, nil, hub, store)
	base := time.Date(2026, 10, 8, 19, 58, 4, 0, time.Local)
	texts := []string{"Kod weryfikacyjny: 3275.", "Jesli nie wiesz czego dotyczy,", "skontaktuj sie z nami."}
	for i, part := range texts {
		if err := store.RecordGSMMessage(context.Background(), "rx", "80766589", "self", part, base.Add(time.Duration(i)*time.Second)); err != nil {
			t.Fatal(err)
		}
	}
	env.login()
	_, html := env.get("/gsm")

	for _, want := range texts {
		if !strings.Contains(html, want) {
			t.Fatalf("gsm page missing the message %q: %s", want, html)
		}
	}
	// The fuse itself must be gone: no row may carry another one's text.
	// (The old merge concatenated the parts without a separator, so this
	// is exactly what the page showed.)
	if glued := strings.Join(texts, ""); strings.Contains(html, glued) {
		t.Fatalf("gsm page joined independent messages into %q", glued)
	}
}

// TestGSMMessagesKeepSentCopies pins the other half of the old guess:
// only rx rows were candidates for joining, and a tx row never merges
// with anything — the station's own sends always stay separate rows.
func TestGSMMessagesKeepSentCopies(t *testing.T) {
	store := &fakeGSMMsgs{}
	hub := newTestGSMHub(t)
	env := newTestEnvWebUsers(t, defaultTestWebConfig(), nil, nil, nil, nil, nil, nil, nil, hub, store)
	base := time.Date(2026, 10, 8, 19, 58, 4, 0, time.Local)
	if err := store.RecordGSMMessage(context.Background(), "tx", "self", "+48509558155", "Pong", base); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordGSMMessage(context.Background(), "tx", "self", "+48509558155", "Pong", base.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	env.login()
	_, html := env.get("/gsm")
	if strings.Contains(html, "PongPong") {
		t.Fatal("two sent messages were joined into one row")
	}
}

// TestGSMPageNamesDirectoryNumbers pins the From/To naming: a number the
// directory knows is shown next to the username, across the "+48..." and
// "0048..." spellings, and ALSO when the profile carries a human form
// (spaces) that the strict SEND shape rejects — the SEND path keeps
// offering only sendable numbers.
func TestGSMPageNamesDirectoryNumbers(t *testing.T) {
	store := &fakeGSMMsgs{}
	hub := newTestGSMHub(t)
	env := newTestEnvWebUsers(t, defaultTestWebConfig(), nil, nil, nil, nil, nil, nil, nil, hub, store)

	// Registered cleanly, and registered the way people type it.
	if _, err := env.users.CreateUser("sp9abc", "0048600111222", "", "", "member", "pw1"); err != nil {
		t.Fatal(err)
	}
	if _, err := env.users.CreateUser("sp9xyz", "+48 601 333 444", "", "", "member", "pw1"); err != nil {
		t.Fatal(err)
	}
	// A number nobody registered stays bare.
	for _, m := range []struct{ dir, from, to string }{
		{"rx", "+48600111222", "self"},
		{"tx", "self", "0048600111222"},
		{"rx", "48601333444", "self"},
		{"rx", "+48600999888", "self"},
	} {
		if err := store.RecordGSMMessage(context.Background(), m.dir, m.from, m.to, "x", time.Now()); err != nil {
			t.Fatal(err)
		}
	}

	env.login()
	resp, html := env.get("/gsm")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /gsm = %d, want 200", resp.StatusCode)
	}
	for _, want := range []string{
		// rx: the sender column wraps the number in <strong>.
		`<strong>&#43;48600111222</strong> <span class="muted op-name">(sp9abc)</span>`,
		// tx: the addressee column holds the number bare.
		`0048600111222 <span class="muted op-name">(sp9abc)</span>`,
		`<strong>48601333444</strong> <span class="muted op-name">(sp9xyz)</span>`,
	} {
		if !strings.Contains(html, want) {
			t.Errorf("gsm history missing %q", want)
		}
	}

	// The polled fragment renders the list on its own: it must resolve
	// the same names (the page is replaced by it every few seconds, so a
	// name that only lives in the full page would vanish on refresh).
	respPartial, partial := env.get("/partials/gsm")
	if respPartial.StatusCode != http.StatusOK {
		t.Fatalf("GET /partials/gsm = %d, want 200", respPartial.StatusCode)
	}
	for _, want := range []string{
		`<strong>&#43;48600111222</strong> <span class="muted op-name">(sp9abc)</span>`,
		`0048600111222 <span class="muted op-name">(sp9abc)</span>`,
		`<strong>48601333444</strong> <span class="muted op-name">(sp9xyz)</span>`,
	} {
		if !strings.Contains(partial, want) {
			t.Errorf("polled gsm fragment missing %q", want)
		}
	}
	// A number nobody registered stays bare — no name span follows it.
	if strings.Contains(html, `48600999888</strong> <span class="muted op-name"`) {
		t.Errorf("an unregistered number was named: %s", html)
	}
	// The spaced profile number never reaches the send picker (it is not
	// a sendable shape), but it still names the history.
	if strings.Contains(html, `<option value="&#43;48 601 333 444">`) {
		t.Error("the send picker offered a number the modem would reject")
	}
}

// TestGSMPartialRefreshesModemStatus pins the polled fragment: it must
// carry the modem session pill, not only the history. The page used to
// render the session state once, so a modem that dropped and came back
// stayed "down" (or "ok") on screen for as long as the tab lived, which
// reads as auto-refresh being broken.
func TestGSMPartialRefreshesModemStatus(t *testing.T) {
	store := &fakeGSMMsgs{}
	hub := newTestGSMHub(t)
	env := newTestEnvWebUsers(t, defaultTestWebConfig(), nil, nil, nil, nil, nil, nil, nil, hub, store)
	env.login()

	if err := store.RecordGSMMessage(context.Background(), "rx", "+48600111222", "self", "witam", time.Now()); err != nil {
		t.Fatal(err)
	}

	// The page carries both boxes the polled response provides: the modem
	// state keeps its place below the hint, the history lives at the
	// bottom where the table belongs.
	_, page := env.get("/gsm")
	for _, want := range []string{`id="gsm-status"`, `id="gsm-msgs"`} {
		if !strings.Contains(page, want) {
			t.Fatalf("gsm page lacks %s: %s", want, page)
		}
	}
	if strings.Index(page, `id="gsm-status"`) > strings.Index(page, `id="gsm-msgs"`) {
		t.Error("the modem state must stay above the history")
	}

	resp, partial := env.get("/partials/gsm")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /partials/gsm = %d, want 200", resp.StatusCode)
	}
	for _, want := range []string{
		// The session pill: state, network and signal bars.
		"Modem connected and ready",
		"Play",
		`aria-label="CSQ 26 · -61 dBm"`,
		`class="gsm-bar on"`,
		// …and the history it refreshes with, each in its own box so the
		// script can lift them into place.
		`<div id="gsm-status">`,
		`<div id="gsm-msgs">`,
		"48600111222",
	} {
		if !strings.Contains(partial, want) {
			t.Errorf("the polled fragment lacks %q: %s", want, partial)
		}
	}
	// The page and the fragment render the same tree, so the swap cannot
	// drop or duplicate markup.
	if strings.Count(page, `id="gsm-msgs"`) != 1 {
		t.Errorf("the page carries %d history boxes, want 1", strings.Count(page, `id="gsm-msgs"`))
	}
}

// TestGSMPartialStatusFollowsTheHub pins the point of the refresh: the
// fragment reports the hub's state as it is now, so the poll turns a
// reconnected modem back to "connected" (and the signal back on) in the
// browser without a reload.
func TestGSMPartialStatusFollowsTheHub(t *testing.T) {
	store := &fakeGSMMsgs{}
	hub := newTestGSMHub(t)
	env := newTestEnvWebUsers(t, defaultTestWebConfig(), nil, nil, nil, nil, nil, nil, nil, hub, store)
	env.login()

	_, before := env.get("/partials/gsm")
	if !strings.Contains(before, "Modem connected and ready") {
		t.Fatalf("connected hub was not reported: %s", before)
	}

	// The modem link drops: the very next poll must say so.
	hub.Close()
	_, after := env.get("/partials/gsm")
	if strings.Contains(after, "Modem connected and ready") {
		t.Errorf("a dropped modem still reads as connected: %s", after)
	}
	if !strings.Contains(after, "Modem not connected") {
		t.Errorf("the fragment does not report the modem loss: %s", after)
	}
}
