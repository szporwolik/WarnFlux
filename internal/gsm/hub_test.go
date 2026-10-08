package gsm

import (
	"bufio"
	"bytes"
	"context"
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
	h.mu.Lock()
	h.reader = bufio.NewReader(f)
	h.connected.Store(true)
	h.mu.Unlock()
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
	f.feed("OK\r\n")
	f.feed("OK\r\n")
	h.mu.Lock()
	ok := h.initLocked()
	h.mu.Unlock()
	if !ok {
		t.Fatalf("init failed")
	}
	written := f.written()
	if !strings.Contains(written, "ATE0\r") || !strings.Contains(written, "AT+CMGF=1\r") {
		t.Fatalf("init commands not written: %q", written)
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

// TestPollUnreadParses pins the +CMGL parsing and the durable rx rows.
func TestPollUnreadParses(t *testing.T) {
	h, f, rec := newTestHub(t)
	f.feed("+CMGL: 1,\"REC UNREAD\",\"+48123456789\",,\"25/10/03,10:00:00+08\"\r\n")
	f.feed("Wiadomosc testowa\r\n")
	f.feed("+CMGL: 2,\"REC UNREAD\",\"+48600999888\",,\"25/10/03,10:05:00+08\"\r\n")
	f.feed("Druga wiadomosc\r\n")
	f.feed("OK\r\n")
	f.feed("OK\r\n") // AT+CMGD=1,4

	got := h.pollUnread()
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
	if !strings.Contains(written, "AT+CMGL=\"REC UNREAD\"\r") {
		t.Fatalf("list command missing: %q", written)
	}
	if !strings.Contains(written, "AT+CMGD=1,4\r") {
		t.Fatalf("delete command missing: %q", written)
	}
}

func TestSendFlow(t *testing.T) {
	h, f, rec := newTestHub(t)
	f.feed("> \r\n")
	f.feed("OK\r\n")
	if err := h.Send(context.Background(), "+48600111222", "Czesc"); err != nil {
		t.Fatalf("send: %v", err)
	}
	written := f.written()
	if !strings.Contains(written, "AT+CMGS=\"+48600111222\"\r") {
		t.Fatalf("send command missing: %q", written)
	}
	if !strings.Contains(written, "Czesc\x1A") {
		t.Fatalf("body+ctrl-z missing: %q", written)
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

func TestSetHandlerLastWins(t *testing.T) {
	rec := &recStub{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := NewHub(Config{}, rec, logger)
	var a, b int
	h.SetHandler(func(string, string) { a++ })
	h.SetHandler(func(string, string) { b++ })
	h.mu.Lock()
	fn := h.handler
	h.mu.Unlock()
	fn("+48", "x")
	if a != 0 || b != 1 {
		t.Fatalf("handler calls: a=%d b=%d, want 0/1", a, b)
	}
	h.SetHandler(nil)
	h.mu.Lock()
	fn = h.handler
	h.mu.Unlock()
	if fn != nil {
		t.Fatalf("handler not cleared")
	}
}
