package aprsinet

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/szporwolik/WarnFlux/internal/aprs"
	"github.com/szporwolik/WarnFlux/internal/core"
	"github.com/szporwolik/WarnFlux/internal/plugin"
)

// recordingEmitter is a minimal plugin.Emitter with health/stats surfaces.
type recordingEmitter struct {
	mu      sync.Mutex
	healthy bool
	summary string
}

func (e *recordingEmitter) Emit(context.Context, core.HazardEvent) error { return nil }
func (e *recordingEmitter) EmitInformation(context.Context, core.InformationMessage) error {
	return nil
}
func (e *recordingEmitter) ReportSourceHealthy() {
	e.mu.Lock()
	e.healthy = true
	e.mu.Unlock()
}
func (e *recordingEmitter) ReportSourceDegraded(error) {
	e.mu.Lock()
	e.healthy = false
	e.mu.Unlock()
}
func (e *recordingEmitter) ReportSourceStats(summary string) {
	e.mu.Lock()
	e.summary = summary
	e.mu.Unlock()
}

// fakeSink records hub publications.
type fakeSink struct {
	mu   sync.Mutex
	pubs map[string][][]byte
}

func newFakeSink() *fakeSink { return &fakeSink{pubs: make(map[string][][]byte)} }

func (f *fakeSink) PublishRaw(suffix string, retained bool, payload []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pubs[suffix] = append(f.pubs[suffix], payload)
	return nil
}

func (f *fakeSink) payloads(suffix string) [][]byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([][]byte(nil), f.pubs[suffix]...)
}

func testHub(t *testing.T) (*aprs.Hub, *fakeSink) {
	t.Helper()
	hub, err := aprs.NewHub(aprs.HubConfig{
		Enabled:    true,
		Callsign:   "SP9MOA-10",
		Icon:       "/j",
		GridSquare: "JO90WW",
		RadiusKM:   60,
		StationTTL: 30 * time.Minute,
		Version:    "0.2.1",
	}, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	sink := newFakeSink()
	hub.SetSink(sink)
	return hub, sink
}

func configNode(t *testing.T, m map[string]any) *yaml.Node {
	t.Helper()
	var node yaml.Node
	if err := node.Encode(m); err != nil {
		t.Fatal(err)
	}
	return &node
}

func TestNewValidation(t *testing.T) {
	hub, _ := testHub(t)

	// Hub disabled → error.
	if _, err := New(nil, nil); err == nil {
		t.Error("nil hub accepted")
	}

	// Missing passcode.
	if _, err := New(configNode(t, map[string]any{"callsign": "SP9MOA-10"}), hub); err == nil {
		t.Error("missing passcode accepted")
	}
	// Mutual exclusion.
	if _, err := New(configNode(t, map[string]any{
		"passcode": "12345", "passcode_file": "/tmp/x",
	}), hub); err == nil {
		t.Error("passcode+passcode_file accepted")
	}
	// Invalid callsign.
	if _, err := New(configNode(t, map[string]any{
		"callsign": "BAD CALL", "passcode": "12345",
	}), hub); err == nil {
		t.Error("invalid callsign accepted")
	}
	// Invalid server.
	if _, err := New(configNode(t, map[string]any{
		"passcode": "12345", "server": "no-port",
	}), hub); err == nil {
		t.Error("server without port accepted")
	}
	// Valid minimal config.
	if _, err := New(configNode(t, map[string]any{"passcode": "12345"}), hub); err != nil {
		t.Errorf("valid config rejected: %v", err)
	}
}

// TestSessionFlow runs the plugin against a scripted fake APRS-IS server:
// the login line must carry callsign/passcode/vers/filter, incoming frames
// reach the hub, and hub.SendMessage transmits a well-formed frame back.
func TestSessionFlow(t *testing.T) {
	hub, sink := testHub(t)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	serverCh := make(chan string, 1) // login line received by the server
	txCh := make(chan string, 1)     // frame received after SendMessage
	serverErr := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			serverErr <- err
			return
		}
		defer conn.Close()
		sc := bufio.NewScanner(conn)
		if !sc.Scan() {
			serverErr <- fmt.Errorf("no login line")
			return
		}
		serverCh <- strings.TrimRight(sc.Text(), "\r\n")

		// Feed two frames: a position and a message addressed to us.
		fmt.Fprintf(conn, "# fake aprs-is server\r\n")
		fmt.Fprintf(conn, "SP9XYZ-7>APRS,TCPIP*:!5056.25N/01952.50E-\r\n")
		fmt.Fprintf(conn, "SP9XYZ>APRS,TCPIP*::SP9MOA-10:hello ops\r\n")

		// Wait for the plugin's outbound message frame.
		if !sc.Scan() {
			serverErr <- fmt.Errorf("no tx frame")
			return
		}
		txCh <- strings.TrimRight(sc.Text(), "\r\n")
	}()

	src, err := New(configNode(t, map[string]any{
		"passcode":        "12345",
		"server":          ln.Addr().String(),
		"connect_timeout": 5 * time.Second,
	}), hub)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hub.Start(ctx)

	emit := &recordingEmitter{}
	done := make(chan error, 1)
	go func() { done <- src.Run(ctx, emit) }()

	// The login line arrives.
	select {
	case login := <-serverCh:
		for _, want := range []string{
			"user SP9MOA-10 pass 12345",
			"vers WarnFlux 0.2.1",
			"filter r/50.9375/19.8750/60",
		} {
			if !strings.Contains(login, want) {
				t.Errorf("login line %q missing %q", login, want)
			}
		}
	case err := <-serverErr:
		t.Fatal(err)
	case <-time.After(5 * time.Second):
		t.Fatal("login line not received")
	}

	// The position lands in the hub station state.
	waitFor(t, func() bool {
		return len(sink.payloads(aprs.StationsTopicPrefix+"SP9XYZ-7")) >= 1
	})

	// The message addressed to us is published on the message feed.
	waitFor(t, func() bool {
		return len(sink.payloads(aprs.MessagesTopic)) >= 1
	})

	// TX through the hub goes out over the APRS-IS connection.
	if err := hub.SendMessage(context.Background(), "SP9XYZ", "test reply"); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	select {
	case frame := <-txCh:
		want := "SP9MOA-10>APRS,TCPIP*::SP9XYZ   :test reply"
		if frame != want {
			t.Errorf("tx frame = %q, want %q", frame, want)
		}
	case err := <-serverErr:
		t.Fatal(err)
	case <-time.After(5 * time.Second):
		t.Fatal("tx frame not received by the server")
	}

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop after cancel")
	}
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

var _ plugin.SourcePlugin = (*Source)(nil)

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
