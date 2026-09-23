package aprsradio

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/szporwolik/WarnFlux/internal/aprs"
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
	done     chan struct{}
}

func newKissServer(t *testing.T) *kissServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &kissServer{ln: ln, txFrames: make(chan []byte, 16), done: make(chan struct{})}
	go s.accept(t)
	return s
}

func (s *kissServer) accept(t *testing.T) {
	conn, err := s.ln.Accept()
	if err != nil {
		return
	}
	s.connMu.Lock()
	s.conn = conn
	s.connMu.Unlock()
	defer close(s.done)

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
	var node yaml.Node
	if err := yaml.Unmarshal([]byte("server: "+server+"\npath: [WIDE1-1]\nconnect_timeout: 5s\nread_timeout: 30s\n"), &node); err != nil {
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
	if string(info) != ":SP9XYZ-7 :hello" {
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
	select {
	case <-srv.txFrames:
	case <-time.After(3 * time.Second):
		t.Fatal("no TX frame")
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
