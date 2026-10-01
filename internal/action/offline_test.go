package action_test

import (
	"context"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/szporwolik/WarnFlux/internal/action"
	"github.com/szporwolik/WarnFlux/internal/config"
)

// TestOfflineHoldsInternetActions proves the offline-mode switch: an
// internet-backed action holds queued work (nothing executes) while the
// switch is on and delivers it once the station goes online again. A
// local action is unaffected.
func TestOfflineHoldsInternetActions(t *testing.T) {
	reg := action.NewRegistry()
	net := &recordingPlugin{name: "net"}
	local := &recordingPlugin{name: "local"}
	if err := reg.Register("net", func(*yaml.Node) (action.Plugin, error) { return net, nil }); err != nil {
		t.Fatal(err)
	}
	if err := reg.Register("local", func(*yaml.Node) (action.Plugin, error) { return local, nil }); err != nil {
		t.Fatal(err)
	}
	reg.MarkInternet("net")

	m := newManager(t, reg, []config.Action{
		{ID: "smtp-alerts", Type: "net", Enabled: true},
		{ID: "log-action", Type: "local", Enabled: true},
	})

	// Offline: submit both — the internet action holds, the local one
	// executes.
	m.SetOffline(true)
	if err := m.Submit("smtp-alerts", action.ActionRequest{ID: "r1", Event: sampleEvent()}); err != nil {
		t.Fatalf("Submit smtp: %v", err)
	}
	if err := m.Submit("log-action", action.ActionRequest{ID: "r2", Event: sampleEvent()}); err != nil {
		t.Fatalf("Submit log: %v", err)
	}
	waitFor(t, func() bool { return local.handled.Load() >= 1 }, "local action never executed offline")
	if net.handled.Load() != 0 {
		t.Fatalf("internet action executed %d times while offline", net.handled.Load())
	}

	// Online: the held request delivers.
	m.SetOffline(false)
	waitFor(t, func() bool { return net.handled.Load() >= 1 }, "internet action never executed after going online")
}

// TestOfflineSkipsInternetDrain proves the shutdown drain never executes
// an internet-backed action while the switch is on.
func TestOfflineSkipsInternetDrain(t *testing.T) {
	reg := action.NewRegistry()
	net := &recordingPlugin{name: "net"}
	if err := reg.Register("net", func(*yaml.Node) (action.Plugin, error) { return net, nil }); err != nil {
		t.Fatal(err)
	}
	reg.MarkInternet("net")

	m := newManager(t, reg, []config.Action{
		{ID: "smtp-alerts", Type: "net", Enabled: true},
	})
	m.SetOffline(true)
	if err := m.Submit("smtp-alerts", action.ActionRequest{ID: "r1", Event: sampleEvent()}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = m.Shutdown(ctx)
	if net.handled.Load() != 0 {
		t.Fatalf("internet action executed %d times during an offline drain", net.handled.Load())
	}
}
