package gsm

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/storage"
)

// fakePort is a scripted serial transport: byte-by-byte delivery from a
// queue and a recorded write stream.
type fakePort struct {
	ch   chan byte
	done chan struct{}

	mu     sync.Mutex
	writes bytes.Buffer
	closed bool
}

func newFakePort() *fakePort {
	return &fakePort{ch: make(chan byte, 8192), done: make(chan struct{})}
}

// feed enqueues a full response chunk.
func (f *fakePort) feed(s string) {
	for i := 0; i < len(s); i++ {
		f.ch <- s[i]
	}
}

func (f *fakePort) Read(p []byte) (int, error) {
	select {
	case b, ok := <-f.ch:
		if !ok {
			return 0, io.EOF
		}
		p[0] = b
		return 1, nil
	case <-f.done:
		return 0, io.EOF
	}
}

func (f *fakePort) Write(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return 0, io.ErrClosedPipe
	}
	return f.writes.Write(p)
}

func (f *fakePort) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return nil
	}
	f.closed = true
	close(f.done)
	return nil
}

func (f *fakePort) written() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.writes.String()
}

// recStub captures the durable-history rows.
type recStub struct {
	mu   sync.Mutex
	rows []storage.GSMMessage
}

func (r *recStub) RecordGSMMessage(_ context.Context, direction, from, to, text string, at time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rows = append(r.rows, storage.GSMMessage{Direction: direction, From: from, To: to, Text: text, At: at})
	return nil
}

func (r *recStub) list() []storage.GSMMessage {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]storage.GSMMessage, len(r.rows))
	copy(out, r.rows)
	return out
}

// newTestHub wires a hub onto a fake port in the connected state (like
// Start does after opening the device), without launching the poll loop.
func newTestHub(t *testing.T) (*Hub, *fakePort, *recStub) {
	t.Helper()
	rec := &recStub{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := NewHub(Config{Enabled: true, Device: "/dev/null"}, rec, logger)
	f := newFakePort()
	h.SetTransport(f)
	h.connected.Store(true)
	t.Cleanup(func() { h.Close() })
	return h, f, rec
}

func TestValidNumber(t *testing.T) {
	cases := []struct {
		in   string
		want bool
	}{
		{"+48600111222", true},
		{"48600111222", true},
		{"1234567", true},
		{"+123456789012345", true},
		{"", false},
		{"abc", false},
		{"+48 600 111 222", false},
		{"123456", false},
		{"+", false},
		{"++48123", false},
	}
	for _, c := range cases {
		if got := ValidNumber(c.in); got != c.want {
			t.Errorf("ValidNumber(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}

func TestInitExchange(t *testing.T) {
	h, f, _ := newTestHub(t)
	f.feed("OK\r\n")                           // ATE0
	f.feed("OK\r\n")                           // AT+CMGF=0
	f.feed("OK\r\n")                           // AT+CSCS="IRA"
	f.feed("+CPMS: (\"SM\",\"SR\")\r\nOK\r\n") // no ME → stay on SIM
	h.mu.Lock()
	ok := h.initLocked()
	storage := h.storage
	h.mu.Unlock()
	if !ok {
		t.Fatalf("init failed")
	}
	if storage != "SM" {
		t.Fatalf("storage = %q, want SM (no ME support)", storage)
	}
	written := f.written()
	for _, cmd := range []string{"ATE0\r", "AT+CMGF=0\r", "AT+CPMS=?\r"} {
		if !strings.Contains(written, cmd) {
			t.Fatalf("init command %q missing: %q", cmd, written)
		}
	}
	if strings.Contains(written, "AT+CPMS=\"ME") {
		t.Fatalf("ME switch attempted without ME support: %q", written)
	}
}

func TestInitPrefersME(t *testing.T) {
	h, f, _ := newTestHub(t)
	f.feed("OK\r\n") // ATE0
	f.feed("OK\r\n") // AT+CMGF=0
	f.feed("OK\r\n") // AT+CSCS="IRA"
	f.feed("+CPMS: (\"ME\",\"MT\",\"SM\",\"SR\"),(\"ME\",\"MT\",\"SM\",\"SR\"),(\"ME\",\"MT\",\"SM\",\"SR\")\r\nOK\r\n")
	f.feed("+CPMS: \"SM\",0,1,\"SM\",0,1,\"SM\",0,1\r\nOK\r\n") // SIM drain: empty → no probing
	f.feed("OK\r\n")                                            // AT+CPMS="ME","ME","ME"
	f.feed("+CPMS: \"ME\",0,23,\"ME\",0,23,\"ME\",0,23\r\nOK\r\n")
	h.mu.Lock()
	ok := h.initLocked()
	storage := h.storage
	h.mu.Unlock()
	if !ok {
		t.Fatalf("init failed")
	}
	if storage != "ME" {
		t.Fatalf("storage = %q, want ME", storage)
	}
	if w := f.written(); !strings.Contains(w, "AT+CPMS=\"ME\",\"ME\",\"ME\"\r") {
		t.Fatalf("ME switch missing: %q", w)
	}
}

func TestInitDrainsSIMBeforeSwitch(t *testing.T) {
	h, f, rec := newTestHub(t)
	f.feed("OK\r\n") // ATE0
	f.feed("OK\r\n") // AT+CMGF=0
	f.feed("OK\r\n") // AT+CSCS="IRA"
	f.feed("+CPMS: (\"ME\",\"MT\",\"SM\",\"SR\"),(\"ME\",\"MT\",\"SM\",\"SR\"),(\"ME\",\"MT\",\"SM\",\"SR\")\r\nOK\r\n")
	f.feed("+CPMS: \"SM\",1,1,\"SM\",1,1,\"SM\",1,1\r\nOK\r\n") // one SIM leftover
	f.feed("+CMGR: 0\r\n" + deliverPDU(t, "+48123456789", "Zalegla wiadomosc") + "\r\nOK\r\n")
	f.feed("OK\r\n") // AT+CMGD=1
	f.feed("OK\r\n") // AT+CPMS="ME","ME","ME"
	f.feed("+CPMS: \"ME\",0,23,\"ME\",0,23,\"ME\",0,23\r\nOK\r\n")
	h.mu.Lock()
	ok := h.initLocked()
	storage := h.storage
	h.mu.Unlock()
	if !ok || storage != "ME" {
		t.Fatalf("init ok=%v storage=%q, want ok + ME", ok, storage)
	}
	rows := rec.list()
	if len(rows) != 1 || rows[0].Text != "Zalegla wiadomosc" {
		t.Fatalf("SIM leftover not drained: %+v", rows)
	}
}

func TestInitFailsOnModemError(t *testing.T) {
	h, f, _ := newTestHub(t)
	f.feed("ERROR\r\n")
	h.mu.Lock()
	ok := h.initLocked()
	h.mu.Unlock()
	if ok {
		t.Fatalf("init succeeded on modem ERROR")
	}
}

// TestPollUnreadParses pins the slot-probing drain (the Huawei firmware
// lists nothing via CMGL AND leaves holes instead of compacting): every
// slot 1..max is read, received messages import, sent copies are deleted
// silently and an out-of-range index ends the scan.
func TestPollUnreadParses(t *testing.T) {
	h, f, rec := newTestHub(t)
	f.feed("+CPMS: \"SM\",4,5,\"SM\",4,5,\"SM\",4,5\r\nOK\r\n")
	f.feed("OK\r\n") // CMGR=1: empty slot (hole)
	f.feed("+CMGR: 0\r\n" + deliverPDU(t, "+48123456789", "Wiadomosc testowa") + "\r\nOK\r\n")
	f.feed("OK\r\n") // AT+CMGD=2
	f.feed("+CMGR: 3\r\n" + submitHex(t, "kopia wysylki") + "\r\nOK\r\n")
	f.feed("OK\r\n") // AT+CMGD=3 (sent copy — deleted, not imported)
	f.feed("+CMGR: 1\r\n" + deliverPDU(t, "+48600999888", "Druga wiadomosc") + "\r\nOK\r\n")
	f.feed("OK\r\n")              // AT+CMGD=4
	f.feed("+CMS ERROR: 321\r\n") // CMGR=5: beyond the real storage

	h.mu.Lock()
	got, ok := h.pollUnreadLocked()
	h.mu.Unlock()
	if !ok {
		t.Fatalf("poll reported a transport failure")
	}
	if len(got) != 2 {
		t.Fatalf("received %d messages, want 2", len(got))
	}
	if got[0].from != "+48123456789" || got[0].text != "Wiadomosc testowa" {
		t.Fatalf("first = %+v", got[0])
	}
	if got[1].from != "+48600999888" || got[1].text != "Druga wiadomosc" {
		t.Fatalf("second = %+v", got[1])
	}
	rows := rec.list()
	if len(rows) != 2 {
		t.Fatalf("recorded %d rows, want 2", len(rows))
	}
	for i, r := range rows {
		if r.Direction != "rx" || r.To != "self" {
			t.Fatalf("row %d = %+v", i, r)
		}
	}
	written := f.written()
	if !strings.Contains(written, "AT+CPMS?\r") {
		t.Fatalf("count command missing: %q", written)
	}
	if !strings.Contains(written, "AT+CMGR=2\r") {
		t.Fatalf("read command missing: %q", written)
	}
	if !strings.Contains(written, "AT+CMGD=2\r") {
		t.Fatalf("delete command missing: %q", written)
	}
	if !strings.Contains(written, "AT+CMGD=3\r") {
		t.Fatalf("sent-copy delete missing: %q", written)
	}
}

// submitHex builds a stored SMS-SUBMIT PDU (a sent copy) — the MTI
// differs from SMS-DELIVER, so the receiver must delete, not import.
func submitHex(t *testing.T, text string) string {
	t.Helper()
	pdus, err := encodePDU("+48600999888", text, 0)
	if err != nil {
		t.Fatalf("submit build: %v", err)
	}
	return pdus[0].Hex
}

// TestPollUnreadReassemblesConcat pins the standards-defined part
// joining: the E173 stores each segment of a long message as its own
// slot and the storage order is NOT guaranteed, so the UDH sequence
// numbers decide the final text.
func TestPollUnreadReassemblesConcat(t *testing.T) {
	h, f, rec := newTestHub(t)
	f.feed("+CPMS: \"ME\",3,3,\"ME\",3,3,\"ME\",3,3\r\nOK\r\n")
	// Slot 1 holds seq 2, slot 2 holds seq 1 — storage order reversed.
	f.feed("+CMGR: 0\r\n" + deliverPDU(t, "+48123456789", "czesc druga", 0x2A, 2, 2) + "\r\nOK\r\n")
	f.feed("OK\r\n") // AT+CMGD=1
	f.feed("+CMGR: 0\r\n" + deliverPDU(t, "+48123456789", "Czesc pierwsza ", 0x2A, 2, 1) + "\r\nOK\r\n")
	f.feed("OK\r\n") // AT+CMGD=2
	f.feed("+CMGR: 0\r\n" + deliverPDU(t, "+48600999888", "osobna wiadomosc") + "\r\nOK\r\n")
	f.feed("OK\r\n") // AT+CMGD=3

	h.mu.Lock()
	got, ok := h.pollUnreadLocked()
	h.mu.Unlock()
	if !ok {
		t.Fatalf("poll reported a transport failure")
	}
	if len(got) != 2 {
		t.Fatalf("received %d messages, want 2: %+v", len(got), got)
	}
	if got[0].text != "Czesc pierwsza czesc druga" {
		t.Fatalf("merged = %q, want the joined parts in UDH order", got[0].text)
	}
	if got[1].text != "osobna wiadomosc" {
		t.Fatalf("second = %q", got[1].text)
	}
	if rows := rec.list(); len(rows) != 2 || rows[0].Text != "Czesc pierwsza czesc druga" {
		t.Fatalf("recorded = %+v", rows)
	}
}

// TestPollSkipsEmptyStorage pins the idle fast path: an empty inbox
// answers only the capacity query — no per-slot probing, which keeps
// the modem chatter minimal over years of uptime.
func TestPollSkipsEmptyStorage(t *testing.T) {
	h, f, rec := newTestHub(t)
	f.feed("+CPMS: \"ME\",0,23,\"ME\",0,23,\"ME\",0,23\r\nOK\r\n")
	h.mu.Lock()
	got, ok := h.pollUnreadLocked()
	h.mu.Unlock()
	if !ok || len(got) != 0 {
		t.Fatalf("poll = %+v ok=%v, want empty + ok", got, ok)
	}
	if w := f.written(); strings.Contains(w, "AT+CMGR=") {
		t.Fatalf("empty storage was probed: %q", w)
	}
	if len(rec.list()) != 0 {
		t.Fatalf("empty storage recorded rows")
	}
}

// TestDeepClean pins the hourly maintenance: the inactive storage is
// drained into the history, the status-report mailbox is bulk-cleared
// and the active storage is restored each time.
func TestDeepClean(t *testing.T) {
	h, f, rec := newTestHub(t)
	h.mu.Lock()
	h.storage = "ME"
	h.mu.Unlock()
	f.feed("OK\r\n") // AT+CPMS="SM","ME","ME"
	f.feed("+CPMS: \"SM\",1,1,\"ME\",1,1,\"ME\",1,1\r\nOK\r\n")
	f.feed("+CMGR: 0\r\n" + deliverPDU(t, "+48123456789", "zapomniana z SIM") + "\r\nOK\r\n")
	f.feed("OK\r\n") // AT+CMGD=1
	f.feed("OK\r\n") // AT+CPMS="ME","ME","ME"
	f.feed("OK\r\n") // AT+CPMS="SR","ME","ME"
	f.feed("OK\r\n") // AT+CMGD=1,4
	f.feed("OK\r\n") // AT+CPMS="ME","ME","ME"

	h.mu.Lock()
	h.deepCleanLocked()
	h.mu.Unlock()
	rows := rec.list()
	if len(rows) != 1 || rows[0].Text != "zapomniana z SIM" {
		t.Fatalf("deep clean did not import the SIM leftover: %+v", rows)
	}
	w := f.written()
	for _, cmd := range []string{"AT+CPMS=\"SM\",\"ME\",\"ME\"\r", "AT+CMGD=1,4\r"} {
		if !strings.Contains(w, cmd) {
			t.Fatalf("deep clean missing %q: %q", cmd, w)
		}
	}
	if n := strings.Count(w, "AT+CPMS=\"ME\",\"ME\",\"ME\"\r"); n != 2 {
		t.Fatalf("storage restore count = %d, want 2: %q", n, w)
	}
}

func TestSendFlow(t *testing.T) {
	h, f, rec := newTestHub(t)
	f.feed("OK\r\n") // AT+CMGF=1
	f.feed("> \r\n") // CMGS prompt
	f.feed("OK\r\n") // network accept
	f.feed("OK\r\n") // AT+CMGF=0 (restore PDU mode)
	if err := h.Send(context.Background(), "+48600111222", "Czesc"); err != nil {
		t.Fatalf("send: %v", err)
	}
	written := f.written()
	for _, cmd := range []string{"AT+CMGF=1\r", "AT+CMGS=\"+48600111222\"\r", "Czesc\x1A", "AT+CMGF=0\r"} {
		if !strings.Contains(written, cmd) {
			t.Fatalf("send sequence missing %q: %q", cmd, written)
		}
	}
	rows := rec.list()
	if len(rows) != 1 {
		t.Fatalf("recorded %d rows, want 1", len(rows))
	}
	r := rows[0]
	if r.Direction != "tx" || r.From != "self" || r.To != "+48600111222" || r.Text != "Czesc" {
		t.Fatalf("tx row = %+v", r)
	}
}

// feedCapture records the last message-feed publication.
type feedCapture struct {
	mu       sync.Mutex
	topic    string
	retained bool
	payload  string
}

func (c *feedCapture) sink(_ context.Context, topic string, retained bool, payload []byte) error {
	c.mu.Lock()
	c.topic, c.retained, c.payload = topic, retained, string(payload)
	c.mu.Unlock()
	return nil
}

// waitDoc polls until a document arrives (the publish is fire-and-forget).
func (c *feedCapture) waitDoc(t *testing.T) (topic string, retained bool, payload string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		topic, retained, payload = c.topic, c.retained, c.payload
		c.mu.Unlock()
		if topic != "" {
			return topic, retained, payload
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("no message feed document published")
	return "", false, ""
}

// TestSendFeedPublishes pins the MQTT message feed for sent SMS: one
// non-retained document on gsm/messages per transmission.
func TestSendFeedPublishes(t *testing.T) {
	h, f, _ := newTestHub(t)
	f.feed("OK\r\n") // AT+CMGF=1
	f.feed("> \r\n")
	f.feed("OK\r\n")
	f.feed("OK\r\n") // AT+CMGF=0
	var feed feedCapture
	h.SetMessageSink(feed.sink)
	if err := h.Send(context.Background(), "+48600111222", "Czesc"); err != nil {
		t.Fatalf("send: %v", err)
	}
	topic, retained, payload := feed.waitDoc(t)
	if topic != "gsm/messages" || retained {
		t.Fatalf("feed publish = %q retained=%v, want non-retained gsm/messages", topic, retained)
	}
	for _, want := range []string{`"direction":"tx"`, `"from":"self"`, `"to":"+48600111222"`, `"text":"Czesc"`} {
		if !strings.Contains(payload, want) {
			t.Fatalf("tx feed payload missing %s: %q", want, payload)
		}
	}
}

// TestReceiveFeedPublishes pins the MQTT message feed for received SMS.
func TestReceiveFeedPublishes(t *testing.T) {
	h, _, _ := newTestHub(t)
	var feed feedCapture
	h.SetMessageSink(feed.sink)
	h.mu.Lock()
	h.recordBatch([]received{{from: "+48600999888", text: "hej"}})
	h.mu.Unlock()
	topic, retained, payload := feed.waitDoc(t)
	if topic != "gsm/messages" || retained {
		t.Fatalf("feed publish = %q retained=%v, want non-retained gsm/messages", topic, retained)
	}
	for _, want := range []string{`"direction":"rx"`, `"from":"+48600999888"`, `"to":"self"`, `"text":"hej"`} {
		if !strings.Contains(payload, want) {
			t.Fatalf("rx feed payload missing %s: %q", want, payload)
		}
	}
}

// TestSendTransliterates pins the text-mode alphabet: Polish diacritics
// get their ASCII equivalents before the modem sees them.
func TestSendTransliterates(t *testing.T) {
	h, f, rec := newTestHub(t)
	f.feed("OK\r\n") // AT+CMGF=1
	f.feed("> \r\n")
	f.feed("OK\r\n")
	f.feed("OK\r\n") // AT+CMGF=0
	if err := h.Send(context.Background(), "+48600111222", "Zażółć gęślą jaźń"); err != nil {
		t.Fatalf("send: %v", err)
	}
	if !strings.Contains(f.written(), "Zazolc gesla jazn\x1A") {
		t.Fatalf("transliterated body missing: %q", f.written())
	}
	rows := rec.list()
	if len(rows) != 1 || rows[0].Text != "Zazolc gesla jazn" {
		t.Fatalf("tx rows = %+v", rows)
	}
}

// TestSendTooLong160 pins the single-segment cap of the text-mode path.
func TestSendTooLong160(t *testing.T) {
	h, f, rec := newTestHub(t)
	if err := h.Send(context.Background(), "+48600111222", strings.Repeat("A", 161)); !errors.Is(err, ErrTooLong) {
		t.Fatalf("send(161) = %v, want ErrTooLong", err)
	}
	if w := f.written(); w != "" {
		t.Fatalf("too-long send touched the modem: %q", w)
	}
	if len(rec.list()) != 0 {
		t.Fatalf("too-long send wrote history rows")
	}
}

func TestSendValidates(t *testing.T) {
	h, _, rec := newTestHub(t)
	if err := h.Send(context.Background(), "abc", "x"); err == nil {
		t.Fatalf("invalid number accepted")
	}
	if err := h.Send(context.Background(), "+48600111222", "  "); err == nil {
		t.Fatalf("empty text accepted")
	}
	if len(rec.list()) != 0 {
		t.Fatalf("validation wrote history rows")
	}
}

func TestSendModemError(t *testing.T) {
	h, f, rec := newTestHub(t)
	f.feed("ERROR\r\n")
	if err := h.Send(context.Background(), "+48600111222", "x"); err == nil {
		t.Fatalf("send succeeded on modem ERROR")
	}
	if len(rec.list()) != 0 {
		t.Fatalf("failed send wrote history rows")
	}
}

func TestSendOnDeadTransport(t *testing.T) {
	h, f, rec := newTestHub(t)
	if err := f.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := h.Send(context.Background(), "+48600111222", "x"); err == nil {
		t.Fatalf("send succeeded on dead transport")
	}
	if len(rec.list()) != 0 {
		t.Fatalf("failed send wrote history rows")
	}
}

func TestNoTransport(t *testing.T) {
	rec := &recStub{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := NewHub(Config{Enabled: true}, rec, logger)
	if err := h.Send(context.Background(), "+48600111222", "x"); err == nil {
		t.Fatalf("send succeeded without a session")
	}
}

// TestSendPromptWithoutNewline pins the Huawei-style prompt: '> ' with
// NO trailing newline (the line-based exchange would time out). The
// modem's accept reply then starts with a fresh CRLF, exactly like the
// real device.
func TestSendPromptWithoutNewline(t *testing.T) {
	h, f, rec := newTestHub(t)
	f.feed("OK\r\n") // AT+CMGF=1
	f.feed("> ")     // prompt only — no CRLF after it
	f.feed("\r\n+CMGS: 1\r\nOK\r\n")
	f.feed("OK\r\n") // AT+CMGF=0
	if err := h.Send(context.Background(), "+48600111222", "Czesc"); err != nil {
		t.Fatalf("send: %v", err)
	}
	if !strings.Contains(f.written(), "Czesc\x1A") {
		t.Fatalf("body+ctrl-z missing: %q", f.written())
	}
	rows := rec.list()
	if len(rows) != 1 || rows[0].Text != "Czesc" {
		t.Fatalf("tx rows = %+v", rows)
	}
}

func TestParseCSQCOPS(t *testing.T) {
	if got := parseCSQ("+CSQ: 26,99"); got != 26 {
		t.Errorf("parseCSQ = %d, want 26", got)
	}
	if got := parseCSQ("+CSQ: 99,99"); got != 99 {
		t.Errorf("parseCSQ(99) = %d, want 99", got)
	}
	if got := parseCSQ("bogus"); got != -1 {
		t.Errorf("parseCSQ(bogus) = %d, want -1", got)
	}
	if got := parseCOPS(`+COPS: 0,0,"PLAY",0`); got != "PLAY" {
		t.Errorf("parseCOPS(PLAY) = %q", got)
	}
	if got := parseCOPS(`+COPS: 0,2,"26006",0`); got != "Play" {
		t.Errorf("parseCOPS(26006) = %q, want Play", got)
	}
	if got := parseCOPS(`+COPS: 0,2,"26001",0`); got != "Plus" {
		t.Errorf("parseCOPS(26001) = %q, want Plus", got)
	}
	if got := parseCOPS("+COPS:"); got != "" {
		t.Errorf("parseCOPS(empty) = %q", got)
	}
}

func TestRefreshStatus(t *testing.T) {
	h, f, _ := newTestHub(t)
	f.feed("OK\r\n") // AT+COPS=3,0
	f.feed("+COPS: 0,0,\"PLAY\",0\r\nOK\r\n")
	f.feed("+CSQ: 26,99\r\nOK\r\n")
	h.mu.Lock()
	h.refreshStatusLocked()
	h.mu.Unlock()
	if got := h.Operator(); got != "PLAY" {
		t.Errorf("Operator = %q, want PLAY", got)
	}
	if got := h.SignalCSQ(); got != 26 {
		t.Errorf("SignalCSQ = %d, want 26", got)
	}
}

// TestDecodeSMSBody pins the UCS-2 hex path: emoji-bearing messages
// arrive as hex code units from the modem and must become readable
// text; plain messages pass through unchanged (idempotent).
func TestDecodeSMSBody(t *testing.T) {
	hex := "004800690020007700610072006E0066006C007500780020D83DDC4B0020"
	decoded := DecodeSMSBody(hex)
	if !strings.Contains(decoded, "Hi warnflux") || !strings.Contains(decoded, "👋") {
		t.Errorf("DecodeSMSBody = %q, want 'Hi warnflux 👋 '", decoded)
	}
	if got := DecodeSMSBody(decoded); got != strings.TrimSpace(decoded) {
		t.Errorf("DecodeSMSBody is not idempotent: %q", got)
	}
	if got := DecodeSMSBody("Hi warnflux"); got != "Hi warnflux" {
		t.Errorf("plain text changed: %q", got)
	}
	if got := DecodeSMSBody("ABC"); got != "ABC" {
		t.Errorf("short hex changed: %q", got)
	}
}

// TestOperatorAndSignal pins the friendly status helpers for the admin
// page: short network aliases map to names and CSQ maps to bar levels.
func TestOperatorAndSignal(t *testing.T) {
	for in, want := range map[string]string{
		"POL":        "Play",
		"PLAY":       "Play",
		"POL ":       "Play",
		"T-MOBILE":   "T-Mobile",
		"Orange PL":  "Orange",
		"some weird": "some weird",
		"":           "",
	} {
		if got := OperatorName(in); got != want {
			t.Errorf("OperatorName(%q) = %q, want %q", in, got, want)
		}
	}
	for csq, want := range map[int]int{0: 0, 3: 1, 8: 2, 13: 3, 18: 4, 26: 5, 31: 5} {
		if got := SignalLevel(csq); got != want {
			t.Errorf("SignalLevel(%d) = %d, want %d", csq, got, want)
		}
	}
}
