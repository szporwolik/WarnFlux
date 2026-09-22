package mqttreceiver

import (
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/config"
	"github.com/szporwolik/WarnFlux/internal/dispatch"
	"github.com/szporwolik/WarnFlux/internal/dispatch/state"
)

func mgrEnv(t *testing.T, receivers []config.Receiver) *Manager {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	m, err := NewManager(receivers, state.New(), dispatch.NewIngress(64), logger)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	return m
}

func recvTimeout(t *testing.T, g *dispatch.Ingress) dispatch.Event {
	t.Helper()
	select {
	case e := <-g.Events():
		return e
	case <-time.After(time.Second):
		t.Fatal("no event")
		return dispatch.Event{}
	}
}

// TestReceiverIndependence: receiver A cannot connect, receiver B keeps
// processing, statuses reflect both, nothing crashes.
func TestReceiverIndependence(t *testing.T) {
	m := mgrEnv(t, []config.Receiver{
		{
			ID: "bad", Enabled: true,
			Broker: "tcp://127.0.0.1:1", ClientID: "warnflux-test-bad",
			ConnectTimeout: 300 * time.Millisecond, KeepAlive: 30 * time.Second,
			WF: config.ReceiverWF{Enabled: true, TopicPrefix: "warnflux"},
		},
		{
			ID: "good", Enabled: true,
			Broker: "tcp://127.0.0.1:1", ClientID: "warnflux-test-good",
			ConnectTimeout: 300 * time.Millisecond, KeepAlive: 30 * time.Second,
			WF: config.ReceiverWF{Enabled: true, TopicPrefix: "warnflux"},
		},
	})
	m.StartAll()

	// Drive receiver "good" ingest directly (its client never connects, but
	// the ingestor is the unit under test here; receiver A failing must not
	// affect receiver B's pipeline). A valid /events frame produces one
	// canonical hazard_transition event.
	good := m.receivers[1]
	good.ingestor.HandleMessage(nil, &testMessage{
		topic:   "warnflux/events",
		payload: []byte(`{"schema_version":1,"change_id":7,"change_type":"new","event_key":"imgw-meteo:123","event":{"source":"imgw-meteo","source_id":"123","event":"Strong wind","severity":"severe","status":"active","received_at":"2026-09-22T10:00:00Z","updated_at":"2026-09-22T10:05:00Z"}}`),
	})

	ev := recvTimeout(t, good.ingestor.ingress)
	if ev.Origin.ReceiverID != "good" {
		t.Errorf("origin = %+v", ev.Origin)
	}

	// Wait for the bad receiver's initial connect attempt to fail.
	deadline := time.Now().Add(5 * time.Second)
	for {
		statuses := m.Statuses()
		var bad, goodS *Status
		for i := range statuses {
			if statuses[i].ID == "bad" {
				bad = &statuses[i]
			}
			if statuses[i].ID == "good" {
				goodS = &statuses[i]
			}
		}
		if bad != nil && !bad.Connected && bad.LastError != "" && goodS != nil && goodS.Messages == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected bad disconnected with error and good processing; got %+v", m.Statuses())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestManagerDisabledReceiver(t *testing.T) {
	m := mgrEnv(t, []config.Receiver{
		{ID: "off", Enabled: false, Broker: "tcp://unused:1883"},
	})
	m.StartAll()
	statuses := m.Statuses()
	if len(statuses) != 1 || statuses[0].Enabled || statuses[0].Connected {
		t.Errorf("statuses = %+v", statuses)
	}
}

func TestSanitizeBroker(t *testing.T) {
	cases := map[string]string{
		"tcp://mosquitto:1883":      "tcp://mosquitto:1883",
		"tcp://user:pass@host:1883": "tcp://host:1883",
		"ssl://a@b.c:8883":          "ssl://b.c:8883",
		"tcp://user:with@at@h:1883": "tcp://h:1883",
	}
	for in, want := range cases {
		if got := sanitizeBroker(in); got != want {
			t.Errorf("sanitizeBroker(%q) = %q, want %q", in, got, want)
		}
	}
}
