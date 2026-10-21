package gsm

import (
	"bytes"
	"context"
	"errors"
	"fmt"
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

// logBuffer is a slog sink safe for a goroutine under test and its reader.
type logBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *logBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *logBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
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

// cmgrText builds a text-mode +CMGR response for one stored message —
// the live E173 header shape (status, sender, empty alpha, service
// center timestamp with a comma inside quotes, TPDU fields).
func cmgrText(status, from, body string) string {
	return fmt.Sprintf("+CMGR: %q,%q,,\"26/10/09,02:51:43+08\",145,4,0,0,\"+48790998257\",145,5\r\n%s\r\nOK\r\n", status, from, body)
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
	f.feed("OK\r\n")                           // AT+CMGF=1
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
	for _, cmd := range []string{"ATE0\r", "AT+CMGF=1\r", "AT+CPMS=?\r"} {
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
	f.feed("OK\r\n") // AT+CMGF=1
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
	f.feed("OK\r\n") // AT+CMGF=1
	f.feed("OK\r\n") // AT+CSCS="IRA"
	f.feed("+CPMS: (\"ME\",\"MT\",\"SM\",\"SR\"),(\"ME\",\"MT\",\"SM\",\"SR\"),(\"ME\",\"MT\",\"SM\",\"SR\")\r\nOK\r\n")
	f.feed("+CPMS: \"SM\",1,1,\"SM\",1,1,\"SM\",1,1\r\nOK\r\n") // one SIM leftover
	f.feed(cmgrText("REC UNREAD", "+48123456789", "Zalegla wiadomosc"))
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
// TestDecodeUCS2Sender pins the address decoding and its conservatism: the
// modem's UCS2-hex originator becomes readable text, while a real
// alphanumeric sender or a plain number is left alone (both can look like
// hex, and mangling them would break the sender lookup).
func TestDecodeUCS2Sender(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		// The production case: a carrier short code the E173 reported as
		// UTF-16BE hex.
		{"00380030003700360036003500380039", "80766589"},
		// Trailing blanks inside the units are trimmed.
		{"0050006C006100790020", "Play"},
		// A plain number: units 0x8076/0x6589 are not ASCII.
		{"80766589", "80766589"},
		// One unit only: U+CAFE is a real character, not a sender.
		{"CAFE", "CAFE"},
		// Ordinary alphanumeric senders are not hex at all.
		{"PLAY24", "PLAY24"},
		{"ALERT RCB", "ALERT RCB"},
		// Odd length, empty and non-ASCII units pass through.
		{"0038003", "0038003"},
		{"", ""},
		{"+48123456789", "+48123456789"},
		// A long hex string whose units are not printable ASCII.
		{"00E400E500E600E7", "00E400E500E600E7"},
	} {
		if got := decodeUCS2Sender(tc.in); got != tc.want {
			t.Errorf("decodeUCS2Sender(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestPollUnreadDecodesTheUCS2Originator pins the wiring: an inbound message
// whose address the modem rendered as UCS2 hex reaches the history (and every
// log line) as the readable number, exactly like a plain one.
func TestPollUnreadDecodesTheUCS2Originator(t *testing.T) {
	h, f, rec := newTestHub(t)
	const hexSender = "00380030003700360036003500380039"
	// Storage with ONE used slot out of two: slot 1 holds the message (the
	// one with the hex originator), slot 2 is the empty remainder of the
	// scan.
	f.feed("+CPMS: \"SM\",1,2,\"SM\",1,2,\"SM\",1,2\r\nOK\r\n")
	f.feed(cmgrText("REC UNREAD", hexSender, "krotka wiadomosc"))
	f.feed("OK\r\n") // AT+CMGD=1
	f.feed("OK\r\n") // CMGR=2: empty slot (hole)

	h.mu.Lock()
	got, ok := h.pollUnreadLocked()
	h.mu.Unlock()
	if !ok || len(got) != 1 {
		t.Fatalf("poll = (%d messages, ok=%v)", len(got), ok)
	}
	if got[0].from != "80766589" {
		t.Fatalf("sender = %q, want the decoded number", got[0].from)
	}
	rows := rec.list()
	if len(rows) != 1 || rows[0].From != "80766589" {
		t.Fatalf("history = %+v, want the decoded sender", rows)
	}
}

func TestPollUnreadParses(t *testing.T) {
	h, f, rec := newTestHub(t)
	f.feed("+CPMS: \"SM\",4,5,\"SM\",4,5,\"SM\",4,5\r\nOK\r\n")
	f.feed("OK\r\n") // CMGR=1: empty slot (hole)
	f.feed(cmgrText("REC UNREAD", "+48123456789", "Wiadomosc testowa"))
	f.feed("OK\r\n") // AT+CMGD=2
	f.feed(cmgrText("STO SENT", "+48600999888", "kopia wysylki"))
	f.feed("OK\r\n") // AT+CMGD=3 (sent copy — deleted, not imported)
	f.feed(cmgrText("REC UNREAD", "+48600999888", "Druga wiadomosc"))
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

// TestPollUnreadTextPartsSeparate pins the text-mode multipart reality:
// the E173 stores each segment of a long message as its own slot with
// no user-data header (PDU-mode reads are unsupported on this
// firmware), so the hub imports the parts as separate messages and the
// web layer merges same-sender same-minute parts at display.
func TestPollUnreadTextPartsSeparate(t *testing.T) {
	h, f, rec := newTestHub(t)
	f.feed("+CPMS: \"ME\",3,3,\"ME\",3,3,\"ME\",3,3\r\nOK\r\n")
	// Slot 1 holds part 2, slot 2 holds part 1 — storage order reversed.
	f.feed(cmgrText("REC UNREAD", "+48123456789", "czesc druga"))
	f.feed("OK\r\n") // AT+CMGD=1
	f.feed(cmgrText("REC UNREAD", "+48123456789", "Czesc pierwsza"))
	f.feed("OK\r\n") // AT+CMGD=2
	f.feed(cmgrText("REC UNREAD", "+48600999888", "osobna wiadomosc"))
	f.feed("OK\r\n") // AT+CMGD=3

	h.mu.Lock()
	got, ok := h.pollUnreadLocked()
	h.mu.Unlock()
	if !ok {
		t.Fatalf("poll reported a transport failure")
	}
	if len(got) != 3 {
		t.Fatalf("received %d messages, want 3: %+v", len(got), got)
	}
	if got[0].text != "czesc druga" || got[1].text != "Czesc pierwsza" {
		t.Fatalf("parts must stay separate: %+v", got)
	}
	if rows := rec.list(); len(rows) != 3 {
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
	f.feed(cmgrText("REC UNREAD", "+48123456789", "zapomniana z SIM"))
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
	f.feed("> \r\n") // CMGS prompt
	f.feed("OK\r\n") // network accept
	if err := h.Send(context.Background(), "+48600111222", "Czesc"); err != nil {
		t.Fatalf("send: %v", err)
	}
	written := f.written()
	for _, cmd := range []string{"AT+CMGS=\"+48600111222\"\r", "Czesc\x1A"} {
		if !strings.Contains(written, cmd) {
			t.Fatalf("send sequence missing %q: %q", cmd, written)
		}
	}
	if strings.Contains(written, "AT+CMGF") {
		t.Fatalf("send must not toggle the mode (text mode is permanent): %q", written)
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
	f.feed("> \r\n")
	f.feed("OK\r\n")
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
	f.feed("> \r\n")
	f.feed("OK\r\n")
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

// TestSendCountsTheBodyItWrites pins the cap and the fold together: the limit
// is measured on the text the modem RECEIVES (after transliteration), a message
// that fits is submitted, and one that does not never touches the modem. The
// extension-table characters are folded into basic ones on the way — this
// modem answers +CMS ERROR 305 for a body carrying them (see TransliterateGSM7
// and gsm7ExtFold), so they must not survive into the body either.
func TestSendCountsTheBodyItWrites(t *testing.T) {
	// The transport idles instead of blocking, so a message the fix
	// refuses never waits for a response the test did not script.
	shortenTimeouts(t, 30*time.Millisecond, 30*time.Millisecond, 30*time.Millisecond)
	h, p := newQuietHub(t)
	p.send("> \r\nOK\r\n")
	if err := h.Send(context.Background(), "+48600111222", strings.Repeat("A", 160)); err != nil {
		t.Fatalf("send(160 plain) = %v, want nil", err)
	}
	if w := p.written(); !strings.Contains(w, strings.Repeat("A", 160)+"\x1A") {
		t.Fatalf("the accepted message was not submitted: %q", w)
	}

	// The fold is what the modem needs: the same send with the separator the
	// SMS formatter uses must arrive WITHOUT the pipe.
	p2 := p
	p2.send("> \r\nOK\r\n")
	if err := h.Send(context.Background(), "+48600111222", "SOSNA | EMCOM [WF-1]"); err != nil {
		t.Fatalf("send with separators = %v, want nil", err)
	}
	if w := p2.written(); strings.ContainsAny(w[strings.Index(w, "SOSNA"):], "|[]") {
		t.Fatalf("extension characters reached the modem: %q", w)
	}
	if !strings.Contains(p2.written(), "SOSNA / EMCOM (WF-1)") {
		t.Fatalf("the folded body is not what the modem was given: %q", p2.written())
	}

	before := p.written()
	err := h.Send(context.Background(), "+48600111222", strings.Repeat("A", 161))
	if !errors.Is(err, ErrTooLong) {
		t.Fatalf("send(161 plain) = %v, want ErrTooLong", err)
	}
	if p.written() != before {
		t.Fatalf("the oversized message reached the modem: %q", p.written()[len(before):])
	}
}

// TestFitGSM7KeepsTheSeptetBudget pins the shared fitter: the formatter
// and the automatic replies must fit in 160 septets, dropping a symbol
// that only half fits instead of sending it truncated.
func TestFitGSM7KeepsTheSeptetBudget(t *testing.T) {
	if got := FitGSM7(strings.Repeat("[", 200), MaxSeptets); Septets(got) != MaxSeptets || len([]rune(got)) != 80 {
		t.Fatalf("fit(200 '[') = %d chars / %d septets, want 80 / 160", len([]rune(got)), Septets(got))
	}
	// 159 plain characters leave room for half of one extension-table
	// symbol: it is dropped whole.
	got := FitGSM7(strings.Repeat("A", 159)+"^", MaxSeptets)
	if Septets(got) != 159 || strings.HasSuffix(got, "^") {
		t.Fatalf("fit(159 'A' + '^') = %q (%d septets), want the 159 characters", got, Septets(got))
	}
	// Text that already fits is untouched, and the reply formatter uses
	// the same budget.
	short := "OK: /status"
	if FitGSM7(short, MaxSeptets) != short {
		t.Fatalf("a short text was changed: %q", FitGSM7(short, MaxSeptets))
	}
	// A reply full of extension-table characters is FOLDED first, so it fits a
	// single segment as 160 basic septets and carries no escape character the
	// modem would refuse.
	reply := fitReply(strings.Repeat("[", 200))
	if Septets(reply) > MaxSeptets || len([]rune(reply)) != 160 {
		t.Fatalf("reply = %d chars / %d septets, want 160 / 160", len([]rune(reply)), Septets(reply))
	}
	if strings.ContainsAny(reply, "[]{}|\\~^€") {
		t.Fatalf("the reply still carries extension characters: %q", reply)
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
	f.feed("> ") // prompt only — no CRLF after it
	f.feed("\r\n+CMGS: 1\r\nOK\r\n")
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

// quietPort is a serial transport that never delivers a byte: every read
// is the VTIME tick a real port turns into an immediate empty read. Feed
// lets a test hand it the scripted part of a response first; everything
// after that stays "no data yet".
type quietPort struct {
	mu     sync.Mutex
	writes bytes.Buffer
	feed   bytes.Buffer
	closed bool
}

func (p *quietPort) Read(b []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return 0, io.EOF
	}
	if p.feed.Len() > 0 {
		return p.feed.Read(b[:1])
	}
	return 0, io.EOF // "no data yet", exactly like the port's idle read
}

func (p *quietPort) Write(b []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return 0, io.ErrClosedPipe
	}
	return p.writes.Write(b)
}

func (p *quietPort) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	return nil
}

func (p *quietPort) written() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.writes.String()
}

// send feeds a scripted response to the quiet transport.
func (p *quietPort) send(s string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.feed.WriteString(s)
}

// newQuietHub wires a hub onto a modem that never answers.
func newQuietHub(t *testing.T) (*Hub, *quietPort) {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := NewHub(Config{Enabled: true, Device: "/dev/null"}, &recStub{}, logger)
	p := &quietPort{}
	h.SetTransport(p)
	h.connected.Store(true)
	t.Cleanup(func() { h.Close() })
	return h, p
}

// shortenTimeouts shrinks the exchange budgets for one test (the fake
// transports cannot reproduce the modem's idle reads, so a real timeout
// would take the production budget).
func shortenTimeouts(t *testing.T, at, ack, resync time.Duration) {
	t.Helper()
	prevAt, prevAck, prevResync := atTimeout, sendAckTimeout, sendResyncTimeout
	atTimeout, sendAckTimeout, sendResyncTimeout = at, ack, resync
	t.Cleanup(func() { atTimeout, sendAckTimeout, sendResyncTimeout = prevAt, prevAck, prevResync })
}

// TestSendRejectsACancelledRequest pins the reported P1: a request whose
// context is already cancelled must not touch the modem at all — it used
// to send AT+CMGS, the body and Ctrl-Z and then answer success.
func TestSendRejectsACancelledRequest(t *testing.T) {
	h, f, rec := newTestHub(t)
	// The modem answers, so a request that ignored the cancellation would
	// complete and report success (the point of the report) instead of
	// blocking the test on an unanswered prompt.
	f.feed("> \r\nOK\r\n")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := h.Send(ctx, "+48600111222", "Czesc")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("send with a cancelled context = %v, want context.Canceled", err)
	}
	if w := f.written(); w != "" {
		t.Fatalf("a cancelled request wrote to the modem: %q", w)
	}
	if rows := rec.list(); len(rows) != 0 {
		t.Fatalf("a cancelled request wrote history rows: %+v", rows)
	}
}

// TestSendLeavesTheQueueWhenCancelledWhileWaiting pins the second half:
// a caller waiting for a busy modem must be able to give up — it must not
// transmit once the session finally frees up.
func TestSendLeavesTheQueueWhenCancelledWhileWaiting(t *testing.T) {
	h, f, rec := newTestHub(t)

	// The modem does answer, so a request that ignores the cancellation
	// would go out once the session frees up (the assertion below is about
	// what was written, not about the modem staying silent).
	f.feed("> \r\nOK\r\n")

	// The session is busy (another exchange holds it).
	h.mu.Lock()
	defer h.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- h.Send(ctx, "+48600111222", "Czesc") }()
	time.Sleep(30 * time.Millisecond) // let it reach the queue
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("send = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a cancelled request stayed in the modem queue")
	}
	if w := f.written(); w != "" {
		t.Fatalf("a cancelled request wrote to the modem: %q", w)
	}
	if rows := rec.list(); len(rows) != 0 {
		t.Fatalf("a cancelled request wrote history rows: %+v", rows)
	}
}

// TestSendFinishesAnExchangeItStarted pins the committed part: once the
// modem holds the body state, cancelling must not abandon the exchange —
// the message goes out and the caller learns the true outcome.
func TestSendFinishesAnExchangeItStarted(t *testing.T) {
	h, f, rec := newTestHub(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- h.Send(ctx, "+48600111222", "Czesc") }()

	// Up to the prompt the request may still be abandoned ...
	waitForWritten(t, f, "AT+CMGS=\"+48600111222\"\r")
	f.feed("> ")
	// ... but once the body is out the modem only waits for Ctrl-Z.
	waitForWritten(t, f, "Czesc\x1A")
	cancel()
	f.feed("\r\n+CMGS: 1\r\nOK\r\n")

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("send = %v, want nil (the message was accepted)", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("send did not finish after cancellation")
	}
	rows := rec.list()
	if len(rows) != 1 || rows[0].Text != "Czesc" {
		t.Fatalf("tx rows = %+v, want the accepted message", rows)
	}
}

// TestSendRepairsTheSessionBeforeTheNextCommand pins the safe ending: an
// exchange that ends without an answer abandons a pending body with ESC
// and probes the modem — so the next command never starts from a
// half-written state — but the repair runs BEFORE THAT COMMAND, not
// inside the call: a repair that outlasts the caller's budget is exactly
// what makes the worker disable a committed channel.
func TestSendRepairsTheSessionBeforeTheNextCommand(t *testing.T) {
	shortenTimeouts(t, 30*time.Millisecond, 30*time.Millisecond, 30*time.Millisecond)
	h, p := newQuietHub(t)

	err := h.Send(context.Background(), "+48600111222", "Czesc")
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("send to a silent modem = %v, want ErrTimeout", err)
	}
	w := p.written()
	if !strings.Contains(w, "AT+CMGS") {
		t.Fatalf("the send never started: %q", w)
	}
	if strings.Contains(w, "\x1b") {
		t.Fatalf("the repair ran inside the call's budget: %q", w)
	}
	if rows := p.written(); strings.Contains(rows, "Czesc") {
		t.Fatalf("the body was written although the prompt never came: %q", rows)
	}
	// The next command runs the repair first: ESC abandons the pending
	// body, the probe goes unanswered and the session is dropped, so the
	// command is refused instead of being written into a stale session.
	if err := h.Send(context.Background(), "+48600111222", "Czesc"); !errors.Is(err, ErrNoModem) {
		t.Fatalf("send after the failed repair = %v, want ErrNoModem", err)
	}
	w = p.written()
	if !strings.Contains(w, "\x1b") {
		t.Fatalf("the pending body was not abandoned with ESC: %q", w)
	}
	if !strings.Contains(w, "AT\r") {
		t.Fatalf("the session was not probed: %q", w)
	}
	if h.conn != nil {
		t.Fatal("a silent session was kept open after the repair failed")
	}
}

// TestSendStaysInsideTheCallersBudget pins the reported P1: from Ctrl-Z
// on the modem owns the message, but a slow confirmation must not outlive
// the caller's budget. The worker disables an action whose call overruns
// its deadline, which would take the whole SMS channel down (until a
// process restart) for a message that did go out. The wait is therefore
// bounded by the caller's deadline, a cut-short window is reported as an
// unconfirmed commit (never as "nothing was sent"), and the repair it
// needs is deferred to the next command.
func TestSendStaysInsideTheCallersBudget(t *testing.T) {
	// The modem budget is far wider than the caller's: only the caller
	// can end this wait.
	shortenTimeouts(t, 30*time.Millisecond, 10*time.Second, 30*time.Millisecond)
	h, p := newQuietHub(t)
	p.send("> \r\n") // the prompt; the accept reply never comes

	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := h.Send(ctx, "+48600111222", "Czesc")
	elapsed := time.Since(start)

	if !errors.Is(err, ErrUnconfirmed) {
		t.Fatalf("send with a slow modem = %v, want ErrUnconfirmed", err)
	}
	if elapsed > 400*time.Millisecond {
		t.Fatalf("send outlived the caller's budget (%s)", elapsed)
	}
	w := p.written()
	if !strings.Contains(w, "Czesc\x1A") {
		t.Fatalf("the body was not committed: %q", w)
	}
	if strings.Contains(w, "\x1b") {
		t.Fatalf("the repair ran inside the caller's budget: %q", w)
	}

	// The next command repairs the session first (ESC + probe), so the
	// channel keeps working after an unconfirmed send.
	p.send("OK\r\n> \r\n+CMGS: 7\r\nOK\r\n")
	if err := h.Send(context.Background(), "+48600111222", "Drugi"); err != nil {
		t.Fatalf("send after an unconfirmed one = %v, want nil", err)
	}
	w = p.written()
	if !strings.Contains(w, "\x1b") || !strings.Contains(w, "AT\r") {
		t.Fatalf("the deferred repair did not run: %q", w)
	}
}

// TestSendAbandonsAPendingBodyWhenTheCallerGivesUp pins the pre-commit
// half of the same report: while the modem takes its time to offer the
// text prompt, the caller's deadline still ends the request — the body is
// never written on its behalf, and the pending message is abandoned
// before the next command instead.
func TestSendAbandonsAPendingBodyWhenTheCallerGivesUp(t *testing.T) {
	shortenTimeouts(t, 5*time.Second, 5*time.Second, 30*time.Millisecond)
	h, p := newQuietHub(t)

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := h.Send(ctx, "+48600111222", "Czesc")
	elapsed := time.Since(start)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("send for a caller that gave up = %v, want the context error", err)
	}
	if elapsed > 1500*time.Millisecond {
		t.Fatalf("send waited for the modem after the caller gave up (%s)", elapsed)
	}
	w := p.written()
	if !strings.Contains(w, "AT+CMGS") {
		t.Fatalf("the send never started: %q", w)
	}
	if strings.Contains(w, "Czesc") {
		t.Fatalf("the body was written for a caller that is gone: %q", w)
	}
}

// TestStartKeepsRetryingAMissingModem pins the "start anyway" rule: a modem
// that is not plugged in must not end the GSM session for good. Start logs the
// failure (with the retry delay) and keeps trying in the background, so a
// device that appears later is picked up without an operator restart — the
// panel shows the missing device meanwhile (internal/preflight).
func TestStartKeepsRetryingAMissingModem(t *testing.T) {
	var buf logBuffer
	h := NewHub(Config{Enabled: true, Device: "/nonexistent/ttyUSB0"}, &recStub{},
		slog.New(slog.NewTextHandler(&buf, nil)))
	// Shorten only THIS hub's retry pace (a construction-time field, so the
	// background goroutine never races the test).
	h.openRetry = 20 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h.Start(ctx)

	if h.Ready() || h.Connected() {
		t.Fatal("hub reported a live session without a device")
	}
	// One attempt is announced with its retry delay, and the background loop
	// keeps attempting (several more warnings over time).
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && strings.Count(buf.String(), "cannot open modem") < 3 {
		time.Sleep(5 * time.Millisecond)
	}
	out := buf.String()
	if n := strings.Count(out, "cannot open modem"); n < 3 {
		t.Fatalf("open attempts = %d, want the background retry to keep trying:\n%s", n, out)
	}
	if !strings.Contains(out, "retry_in=") {
		t.Fatalf("the retry delay was not announced:\n%s", out)
	}

	// The loop stops with the station, leaving nothing behind.
	cancel()
	time.Sleep(60 * time.Millisecond)
	settled := strings.Count(buf.String(), "cannot open modem")
	time.Sleep(120 * time.Millisecond)
	if after := strings.Count(buf.String(), "cannot open modem"); after != settled {
		t.Fatalf("retries continued after cancellation (%d → %d)", settled, after)
	}
}

// TestOpenSessionWithInjectedTransportGoesStraightToReady pins that the retry
// path does not change the normal one: an available modem (injected transport)
// is initialised immediately, so Start never waits for a device.
func TestOpenSessionWithInjectedTransportGoesStraightToReady(t *testing.T) {
	h, f, _ := newTestHub(t)
	f.feed("ATE0\r\r\nOK\r\n")
	f.feed("AT+CMGF=1\r\r\nOK\r\n")
	go func() {
		for i := 0; i < 20; i++ {
			f.feed("OK\r\n")
			time.Sleep(2 * time.Millisecond)
		}
	}()
	if !h.openSession() {
		t.Fatal("openSession = false for an injected transport")
	}
	if !h.Ready() {
		t.Fatal("session not ready after a successful init")
	}
}
