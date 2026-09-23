package aprsout

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/szporwolik/WarnFlux/internal/action"
	"github.com/szporwolik/WarnFlux/internal/aprs"
	"github.com/szporwolik/WarnFlux/internal/dispatch"
)

// fakeTx is a minimal aprs.Transmitter recording sends.
type fakeTx struct {
	name  string
	ready bool
	mu    sync.Mutex
	sent  []string
}

func (f *fakeTx) Name() string { return f.name }
func (f *fakeTx) Ready() bool  { return f.ready }
func (f *fakeTx) Send(_ context.Context, to, text string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sent = append(f.sent, to+"|"+text)
	return nil
}
func (f *fakeTx) sends() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.sent...)
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func newTestHub(t *testing.T, tx *fakeTx) (*aprs.Hub, func()) {
	t.Helper()
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
	hub.AddTransmitter(tx.name, tx)
	ctx, cancel := context.WithCancel(context.Background())
	hub.Start(ctx)
	return hub, cancel
}

func newTestAction(t *testing.T, configYAML string, hub *aprs.Hub) *aprsOutAction {
	t.Helper()
	var node yaml.Node
	if err := yaml.Unmarshal([]byte(configYAML), &node); err != nil {
		t.Fatalf("unmarshal config: %v", err)
	}
	p, err := New(&node, hub)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return p.(*aprsOutAction)
}

func testReq() action.ActionRequest {
	return action.ActionRequest{
		Event: dispatch.Event{Hazard: &dispatch.HazardTransition{Hazard: dispatch.Hazard{
			Severity: "severe",
			Event:    "Storm",
			Headline: "Strong wind",
		}}},
	}
}

func TestAckSucceedsAndCooldownHolds(t *testing.T) {
	tx := &fakeTx{name: "aprs-radio", ready: true}
	hub, cancel := newTestHub(t, tx)
	defer cancel()
	a := newTestAction(t, "callsigns: [SP9XYZ-7]\nack_timeout: 5s\ncooldown: 300ms\ntx_interval: -1s", hub)

	done := make(chan error, 1)
	go func() { done <- a.Execute(context.Background(), testReq()) }()

	waitSends(t, tx, 1)
	if got := tx.sends()[0]; !strings.Contains(got, "{00001}") {
		t.Fatalf("first send = %q, want id 00001", got)
	}
	hub.Observe(aprs.ParseFeedLine("SP9XYZ-7>APRS::SP9MOA-10:ack00001", time.Now()), "aprs-radio")
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Execute with ack = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Execute did not finish after ack")
	}

	// The next transmission must wait out the 300ms cooldown.
	start := time.Now()
	done2 := make(chan error, 1)
	go func() { done2 <- a.Execute(context.Background(), testReq()) }()
	waitSends(t, tx, 2)
	elapsed := time.Since(start)
	if elapsed < 250*time.Millisecond {
		t.Fatalf("second send left after %s, cooldown not respected", elapsed)
	}
	hub.Observe(aprs.ParseFeedLine("SP9XYZ-7>APRS::SP9MOA-10:ack00002", time.Now()), "aprs-radio")
	select {
	case err := <-done2:
		if err != nil {
			t.Fatalf("second Execute = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("second Execute did not finish")
	}
}

func TestNoAckFails(t *testing.T) {
	tx := &fakeTx{name: "aprs-radio", ready: true}
	hub, cancel := newTestHub(t, tx)
	defer cancel()
	a := newTestAction(t, "callsigns: [SP9XYZ-7]\nack_timeout: 150ms\ncooldown: -1s\ntx_interval: -1s", hub)

	err := a.Execute(context.Background(), testReq())
	if err == nil || !strings.Contains(err.Error(), "no ack") {
		t.Fatalf("Execute without ack = %v, want no-ack error", err)
	}
	if got := len(tx.sends()); got != 1 {
		t.Fatalf("sends = %d, want 1", got)
	}
}

func TestConfigValidation(t *testing.T) {
	hub, cancel := newTestHub(t, &fakeTx{name: "aprs-radio", ready: true})
	defer cancel()

	// An empty static list is fine: routing may rely on group members.
	empty := newTestAction(t, "callsigns: []\n", hub)
	if err := empty.Execute(context.Background(), testReq()); err != nil {
		t.Fatalf("Execute without recipients = %v, want nil", err)
	}

	var node yaml.Node
	if err := yaml.Unmarshal([]byte("callsigns: [NOTACALL]\n"), &node); err != nil {
		t.Fatal(err)
	}
	if _, err := New(&node, hub); err == nil {
		t.Error("invalid callsign accepted")
	}
	var node2 yaml.Node
	if err := yaml.Unmarshal([]byte("callsigns: [SP9XYZ-7]\nack_timeout: 10m\n"), &node2); err != nil {
		t.Fatal(err)
	}
	if _, err := New(&node2, hub); err == nil {
		t.Error("ack_timeout above the bound accepted")
	}
}

func waitSends(t *testing.T, tx *fakeTx, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(tx.sends()) >= n {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("sends = %d, want %d", len(tx.sends()), n)
}
