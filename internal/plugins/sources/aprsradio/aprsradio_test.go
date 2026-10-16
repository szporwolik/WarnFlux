package aprsradio

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/szporwolik/WarnFlux/internal/aprs"
	"github.com/szporwolik/WarnFlux/internal/core"
	"github.com/szporwolik/WarnFlux/internal/plugin"
)

// sink records hub publications per suffix.
type sink struct {
	mu   sync.Mutex
	pubs map[string][][]byte
}

func newSink() *sink { return &sink{pubs: make(map[string][][]byte)} }
func (s *sink) PublishRaw(suffix string, _ bool, payload []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pubs[suffix] = append(s.pubs[suffix], payload)
	return nil
}
func (s *sink) payloads(suffix string) [][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([][]byte(nil), s.pubs[suffix]...)
}

// kissServer is a minimal TCP KISS endpoint: frames written by the plugin
// land in txFrames, frames pushed by the test go to the plugin.
type kissServer struct {
	ln       net.Listener
	conn     net.Conn
	connMu   sync.Mutex
	txFrames chan []byte
	// accepts counts the connections the plugin opened: the number of
	// accepted sessions is how a test proves the plugin did (or did not)
	// reconnect.
	accepts atomic.Int64
}

func newKissServer(t *testing.T) *kissServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &kissServer{ln: ln, txFrames: make(chan []byte, 16)}
	go s.accept(t)
	return s
}

func (s *kissServer) accept(t *testing.T) {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			return
		}
		s.accepts.Add(1)
		s.connMu.Lock()
		s.conn = conn
		s.connMu.Unlock()
		go s.serve(conn)
	}
}

// serve reads one client connection until it ends.
func (s *kissServer) serve(conn net.Conn) {
	dec := &aprs.KISSDecoder{}
	reader := bufio.NewReader(conn)
	for {
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		b, err := reader.ReadByte()
		if err != nil {
			return
		}
		for _, frame := range dec.Feed([]byte{b}) {
			select {
			case s.txFrames <- frame:
			default:
			}
		}
	}
}

// push sends one KISS frame to the plugin.
func (s *kissServer) push(t *testing.T, frame []byte) {
	t.Helper()
	s.connMu.Lock()
	defer s.connMu.Unlock()
	if s.conn == nil {
		t.Fatal("plugin never connected")
	}
	if _, err := s.conn.Write(aprs.EncodeKISS(frame)); err != nil {
		t.Fatalf("push: %v", err)
	}
}

// dropConn closes the CURRENT connection from the server side: what a dead
// TNC (or a vanished network path) looks like to the plugin.
func (s *kissServer) dropConn() {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	if s.conn != nil {
		s.conn.Close()
	}
}

func (s *kissServer) close() {
	s.ln.Close()
	s.connMu.Lock()
	if s.conn != nil {
		s.conn.Close()
	}
	s.connMu.Unlock()
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func newPlugin(t *testing.T, server string, hub *aprs.Hub) plugin.SourcePlugin {
	t.Helper()
	return newPluginReadTimeout(t, server, hub, "30s")
}

func newPluginReadTimeout(t *testing.T, server string, hub *aprs.Hub, readTimeout string) plugin.SourcePlugin {
	t.Helper()
	var node yaml.Node
	if err := yaml.Unmarshal([]byte("server: "+server+"\npath: [WIDE1-1]\nconnect_timeout: 5s\nread_timeout: "+readTimeout+"\n"), &node); err != nil {
		t.Fatal(err)
	}
	p, err := New(&node, hub)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return p
}

func newHub(t *testing.T) (*aprs.Hub, *sink) {
	t.Helper()
	s := newSink()
	hub, err := aprs.NewHub(aprs.HubConfig{
		Enabled:    true,
		Callsign:   "SP9MOA-10",
		Icon:       "/j",
		GridSquare: "JO90WW",
		RadiusKM:   aprs.DefaultRadiusKM,
		StationTTL: 30 * time.Minute,
	}, testLogger())
	if err != nil {
		t.Fatalf("NewHub: %v", err)
	}
	hub.SetSink(s)
	return hub, s
}

func TestRadioRXFeedsHub(t *testing.T) {
	hub, sink := newHub(t)
	srv := newKissServer(t)
	defer srv.close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hub.Start(ctx)
	p := newPlugin(t, srv.ln.Addr().String(), hub)
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx, nil) }()

	waitConnected(t, srv)
	frame, err := aprs.BuildUIFrame("SP9XYZ-7", "APRS", []string{"WIDE1-1"}, []byte("!5056.25N/01952.50E-"))
	if err != nil {
		t.Fatal(err)
	}
	srv.push(t, frame)

	waitFor(t, func() bool { return len(sink.payloads("aprs/stations/SP9XYZ-7")) >= 1 })

	var doc aprs.StationDocument
	if err := json.Unmarshal(sink.payloads("aprs/stations/SP9XYZ-7")[0], &doc); err != nil {
		t.Fatalf("station doc: %v", err)
	}
	if doc.Position == nil {
		t.Fatal("station has no position")
	}
	if doc.Origin != "rf" {
		t.Errorf("origin = %q, want rf (radio backend)", doc.Origin)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not stop after cancel")
	}
}

func TestRadioTXRoundtrip(t *testing.T) {
	hub, _ := newHub(t)
	srv := newKissServer(t)
	defer srv.close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hub.Start(ctx)
	p := newPlugin(t, srv.ln.Addr().String(), hub)
	go func() { _ = p.Run(ctx, nil) }()

	waitConnected(t, srv)

	done := make(chan error, 1)
	go func() { done <- hub.SendMessage(ctx, "SP9XYZ-7", "hello") }()

	var txFrame []byte
	select {
	case txFrame = <-srv.txFrames:
	case <-time.After(3 * time.Second):
		t.Fatal("no TX frame from the plugin")
	}
	src, dst, digis, info, ok := aprs.DecodeUIFrame(txFrame)
	if !ok {
		t.Fatalf("TX frame does not decode: % x", txFrame)
	}
	if src != "SP9MOA-10" || dst != "SP9XYZ-7" {
		t.Errorf("src/dst = %q/%q", src, dst)
	}
	if len(digis) != 1 || digis[0] != "WIDE1-1" {
		t.Errorf("digis = %v", digis)
	}
	if string(info) != fmt.Sprintf(":%-9s:%s", "SP9XYZ-7", "hello") {
		t.Errorf("info = %q", info)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("SendMessage: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("SendMessage did not return")
	}
}

func TestRadioAckRoundtrip(t *testing.T) {
	hub, sink := newHub(t)
	srv := newKissServer(t)
	defer srv.close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hub.Start(ctx)
	p := newPlugin(t, srv.ln.Addr().String(), hub)
	go func() { _ = p.Run(ctx, nil) }()

	waitConnected(t, srv)

	done := make(chan bool, 1)
	go func() {
		ack, err := hub.SendMessageWaitAck(ctx, "SP9XYZ-7", "hello", 5*time.Second)
		done <- err == nil && ack
	}()
	var txFrame []byte
	select {
	case txFrame = <-srv.txFrames:
	case <-time.After(3 * time.Second):
		t.Fatal("no TX frame")
	}

	// The on-wire frame must carry the {id} ack-request suffix — without
	// it the addressee never acks.
	if _, _, _, info, ok := aprs.DecodeUIFrame(txFrame); !ok {
		t.Fatalf("TX frame does not decode: % x", txFrame)
	} else if !strings.HasSuffix(string(info), "{00001}") {
		t.Fatalf("TX info = %q, want ack-request suffix {00001}", info)
	}

	ackFrame, err := aprs.BuildUIFrame("SP9XYZ-7", "SP9MOA-10", nil, []byte(":SP9MOA-10:ack00001"))
	if err != nil {
		t.Fatal(err)
	}
	srv.push(t, ackFrame)

	select {
	case ok := <-done:
		if !ok {
			t.Fatal("ack roundtrip failed")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("ack never arrived")
	}

	// The station origin is rf, and the ack appears on the message feed.
	if got := len(sink.payloads("aprs/messages")); got < 2 {
		t.Fatalf("message feed entries = %d, want tx + rx ack", got)
	}
	var txDoc, rxDoc aprs.MessageDocument
	for _, payload := range sink.payloads("aprs/messages") {
		var d aprs.MessageDocument
		if err := json.Unmarshal(payload, &d); err == nil {
			if d.Direction == "tx" {
				txDoc = d
			} else {
				rxDoc = d
			}
		}
	}
	if txDoc.ID != "00001" || txDoc.Text != "hello" {
		t.Errorf("tx doc = %+v", txDoc)
	}
	if rxDoc.Text != "ack00001" || !strings.HasPrefix(rxDoc.Text, "ack") {
		t.Errorf("rx doc = %+v", rxDoc)
	}
}

// healthRecorder is an Emitter that records degraded and healthy reports.
type healthRecorder struct {
	mu       sync.Mutex
	degraded []error
	healthy  int
}

func (r *healthRecorder) Emit(context.Context, core.HazardEvent) error { return nil }
func (r *healthRecorder) EmitInformation(context.Context, core.InformationMessage) error {
	return nil
}
func (r *healthRecorder) ReportSourceHealthy() {
	r.mu.Lock()
	r.healthy++
	r.mu.Unlock()
}
func (r *healthRecorder) ReportSourceDegraded(err error) {
	r.mu.Lock()
	r.degraded = append(r.degraded, err)
	r.mu.Unlock()
}
func (r *healthRecorder) ReportSourceStats(string) {}
func (r *healthRecorder) degradedCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.degraded)
}
func (r *healthRecorder) healthyCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.healthy
}

// TestIdleTNCStaysUp pins the quiet-channel behavior: a read timeout is
// normal (nothing heard), so the session must NOT degrade or reconnect.
// Regression for the "i/o timeout" degraded state on a silent TNC.
func TestIdleTNCStaysUp(t *testing.T) {
	hub, sink := newHub(t)
	srv := newKissServer(t)
	defer srv.close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hub.Start(ctx)
	p := newPluginReadTimeout(t, srv.ln.Addr().String(), hub, "5s")
	rec := &healthRecorder{}
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx, rec) }()

	waitConnected(t, srv)

	// Sleep past one read timeout (5s): the session must stay up.
	time.Sleep(6 * time.Second)
	if n := rec.degradedCount(); n != 0 {
		t.Fatalf("source degraded %d times on an idle TNC, want 0", n)
	}
	if n := rec.healthyCount(); n != 1 {
		t.Fatalf("source reported healthy %d times, want 1 (on connect)", n)
	}
	if !p.(*Source).Ready() {
		t.Fatal("plugin not ready after an idle read timeout")
	}

	// The connection is still alive: a frame still flows in.
	frame, err := aprs.BuildUIFrame("SP9XYZ-7", "APRS", []string{"WIDE1-1"}, []byte("!5056.25N/01952.50E-"))
	if err != nil {
		t.Fatal(err)
	}
	srv.push(t, frame)
	waitFor(t, func() bool { return len(sink.payloads("aprs/stations/SP9XYZ-7")) >= 1 })

	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not stop after cancel")
	}
}

// TestSilentTNCKeepsTheSessionAndReportsOnce pins the silence policy: a
// TNC that sends NOTHING is a quiet RF channel, not a failed session. The
// old contract ended the session after every idle window, which bought
// nothing (a live TCP peer says nothing about what the radio hears) and
// cost the production station a reconnect every 30 minutes — 78 of them in
// 40 hours, with the same churn in the TNC's own client log.
//
// The silence is REPORTED once per episode (a frame clears the flag) so the
// operator still learns that the receiver hears nothing; the panel shows
// the age of the last frame.
func TestSilentTNCKeepsTheSessionAndReportsOnce(t *testing.T) {
	hub, sink := newHub(t)
	srv := newKissServer(t)
	defer srv.close()

	// Shrink the reporting window: 1x read_timeout (the minimum the config
	// accepts is 5s).
	old := idleTimeoutFactor
	idleTimeoutFactor = 1
	defer func() { idleTimeoutFactor = old }()

	logs := &captureLogs{}
	oldDefault := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(logs, nil)))
	defer slog.SetDefault(oldDefault)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hub.Start(ctx)
	p := newPluginReadTimeout(t, srv.ln.Addr().String(), hub, "5s")
	rec := &healthRecorder{}
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx, rec) }()

	waitConnected(t, srv)

	// First episode: after the reporting window (5s) the silence is
	// announced exactly once — and the session stays up.
	waitUntil(t, 10*time.Second, func() bool { return logs.count("TNC silent") == 1 })
	if n := srv.accepts.Load(); n != 1 {
		t.Fatalf("connections opened = %d, want 1 (silence must not reconnect)", n)
	}
	time.Sleep(2 * time.Second) // a second window with no frames
	if n := logs.count("TNC silent"); n != 1 {
		t.Fatalf("silence reported %d times in one episode, want 1", n)
	}
	if n := rec.degradedCount(); n != 0 {
		t.Fatalf("source degraded %d times on a silent TNC, want 0", n)
	}
	if !p.(*Source).Ready() {
		t.Fatal("plugin not ready after the silence window")
	}

	// The very same session is still reading: a frame still reaches the hub.
	frame, err := aprs.BuildUIFrame("SP9XYZ-7", "APRS", []string{"WIDE1-1"}, []byte("!5056.25N/01952.50E-"))
	if err != nil {
		t.Fatal(err)
	}
	srv.push(t, frame)
	waitFor(t, func() bool { return len(sink.payloads("aprs/stations/SP9XYZ-7")) >= 1 })

	// A new episode (silence after a frame) is reported again.
	waitUntil(t, 10*time.Second, func() bool { return logs.count("TNC silent") == 2 })
	if n := srv.accepts.Load(); n != 1 {
		t.Fatalf("connections opened = %d, want 1 (still the same session)", n)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not stop after cancel")
	}
}

// TestDeadTNCSocketReconnects pins the other half of the policy: what DOES
// end the session is the socket dying — a connection the peer closed (the
// TNC restarted, the box vanished) reports EOF, the source is flagged
// degraded and the next session is dialled.
func TestDeadTNCSocketReconnects(t *testing.T) {
	hub, _ := newHub(t)
	srv := newKissServer(t)
	defer srv.close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hub.Start(ctx)
	p := newPluginReadTimeout(t, srv.ln.Addr().String(), hub, "5s")
	rec := &healthRecorder{}
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx, rec) }()

	waitConnected(t, srv)
	if n := srv.accepts.Load(); n != 1 {
		t.Fatalf("connections opened = %d, want 1", n)
	}

	// The TNC goes away: the session must end (degraded) and a new one must
	// be dialled — the reconnect loop is what keeps the RF feed alive.
	srv.dropConn()
	waitFor(t, func() bool { return rec.degradedCount() >= 1 })
	waitFor(t, func() bool { return srv.accepts.Load() >= 2 })
	waitFor(t, func() bool { return rec.healthyCount() >= 2 })

	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not stop after cancel")
	}
}

// captureLogs collects the plugin's log lines (the plugin logs to
// slog.Default()).
type captureLogs struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (c *captureLogs) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buf.Write(p)
}

func (c *captureLogs) count(substr string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return strings.Count(c.buf.String(), substr)
}

func waitConnected(t *testing.T, srv *kissServer) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		srv.connMu.Lock()
		connected := srv.conn != nil
		srv.connMu.Unlock()
		if connected {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("plugin never connected to the KISS server")
}

// waitUntil is waitFor with an explicit deadline: the silence windows these
// tests exercise are longer than waitFor's fixed 3s budget.
func waitUntil(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not met within the deadline")
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met within 3s")
}

func TestResetBackoff(t *testing.T) {
	old := healthySessionReset
	healthySessionReset = time.Minute
	defer func() { healthySessionReset = old }()

	// A healthy session resets the backoff to the minimum.
	if got := resetBackoff(maxReconnectDelay, 2*time.Minute); got != minReconnectDelay {
		t.Errorf("healthy session backoff = %v, want %v", got, minReconnectDelay)
	}
	// A short failed session keeps the current (growing) delay.
	if got := resetBackoff(maxReconnectDelay, time.Second); got != maxReconnectDelay {
		t.Errorf("short session backoff = %v, want unchanged %v", got, maxReconnectDelay)
	}
}
