package meshtastic

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/core"
	mesh "github.com/szporwolik/WarnFlux/internal/meshtastic"
	"github.com/szporwolik/WarnFlux/internal/plugin"
)

// recordingEmitter captures the informational documents the wrapper
// publishes and ignores hazard events (the mesh source never emits one).
type recordingEmitter struct {
	mu    sync.Mutex
	infos []core.InformationMessage
}

func (e *recordingEmitter) Emit(context.Context, core.HazardEvent) error { return nil }

func (e *recordingEmitter) EmitInformation(_ context.Context, message core.InformationMessage) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.infos = append(e.infos, message)
	return nil
}

func (e *recordingEmitter) messages() []core.InformationMessage {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]core.InformationMessage(nil), e.infos...)
}

// testHub builds an unconnected hub: the wrapper never dials anything
// itself, so the snapshot (node directory, channels, TTL) is readable
// without a radio.
func testHub(t *testing.T) *mesh.Hub {
	t.Helper()
	hub, err := mesh.NewHub(mesh.Config{Enabled: true, Device: "/dev/does-not-exist"}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("NewHub: %v", err)
	}
	return hub
}

// TestRegisterRefusesWithoutHub pins the guard: the hub IS the integration,
// so a station without one gets a clear startup error instead of an idle
// source that looks enabled.
func TestRegisterRefusesWithoutHub(t *testing.T) {
	reg := plugin.NewRegistry()
	if err := Register(reg, nil); err != nil {
		t.Fatalf("Register: %v", err)
	}
	factory, err := reg.Source(Type)
	if err != nil {
		t.Fatalf("Source(%q): %v", Type, err)
	}
	if _, err := factory(nil); !errors.Is(err, errHubDisabled) {
		t.Fatalf("factory without a hub = %v, want errHubDisabled", err)
	}

	// With a hub the factory succeeds, and the wrapper takes no plugin
	// configuration of its own (a node document is not even inspected).
	withHub := plugin.NewRegistry()
	if err := Register(withHub, testHub(t)); err != nil {
		t.Fatalf("Register: %v", err)
	}
	factory, err = withHub.Source(Type)
	if err != nil {
		t.Fatalf("Source(%q): %v", Type, err)
	}
	p, err := factory(nil)
	if err != nil {
		t.Fatalf("factory with a hub: %v", err)
	}
	if p.Name() != Type {
		t.Errorf("Name() = %q, want %q", p.Name(), Type)
	}
}

// TestPublishNodesDocumentsTheSnapshot pins the published document: the
// node directory travels as the retained mesh-nodes information message
// that the map layers consume.
func TestPublishNodesDocumentsTheSnapshot(t *testing.T) {
	src := &Source{hub: testHub(t)}
	emit := &recordingEmitter{}
	src.publishNodes(emit)

	got := emit.messages()
	if len(got) != 1 {
		t.Fatalf("published %d documents, want 1", len(got))
	}
	msg := got[0]
	if msg.Source != Type || msg.Kind != "mesh-nodes" || msg.Key != "nodes" {
		t.Errorf("document = source %q kind %q key %q", msg.Source, msg.Kind, msg.Key)
	}
	if msg.GeneratedAt.IsZero() {
		t.Error("the document carries no generation time")
	}
	var snap mesh.Snapshot
	if err := json.Unmarshal(msg.Payload, &snap); err != nil {
		t.Fatalf("payload is not a hub snapshot: %v", err)
	}
	if snap.Connected {
		t.Error("an unconnected hub must report Connected=false")
	}
	if snap.NodeTTL != mesh.DefaultNodeTTL {
		t.Errorf("snapshot TTL = %s, want the hub's default %s", snap.NodeTTL, mesh.DefaultNodeTTL)
	}
}

// TestRunStopsWithTheContext pins the lifecycle contract: Run owns the hub
// session and must return once the context is cancelled.
func TestRunStopsWithTheContext(t *testing.T) {
	src := &Source{hub: testHub(t)}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = src.Run(ctx, &recordingEmitter{})
		close(done)
	}()
	// Give the session a moment to start, then stop the station.
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after the context was cancelled")
	}
}
