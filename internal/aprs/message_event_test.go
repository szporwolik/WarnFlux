package aprs

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// TestRoutedMessageEvent pins the APRS-message → /events bridge: a message
// addressed to us, sent by an operator on the sender allow-list (base
// callsign match, any SSID), is re-published on the events stream with the
// "aprs" source, the "Message from: <CALL>" prefix and severe severity.
func TestRoutedMessageEvent(t *testing.T) {
	hub, sink := testHub(t, HubConfig{
		Enabled:       true,
		Callsign:      "SP9MOA-10",
		Name:          "SOSNA Test",
		Icon:          "/j",
		GridSquare:    "JO90WW",
		RadiusKM:      DefaultRadiusKM,
		StationTTL:    30 * time.Minute,
		RouteMessages: true,
	})
	ctx, cancel := context.WithCancel(context.Background())
	hub.Start(ctx)
	defer cancel()

	// The operator is registered as SP9XYZ-4; the message arrives from
	// SP9XYZ-7 — the base callsign matches, so it routes.
	hub.SetSenderGate(func(base string) bool { return base == "SP9XYZ" })
	hub.Observe(ParseFeedLine("SP9XYZ-7>APRS,WIDE1-1*::SP9MOA-10:hello ops", time.Now()), BackendRadio)

	waitFor(t, func() bool { return len(sink.payloads("events")) >= 1 })
	var ev MessageEventWire
	if err := json.Unmarshal(sink.payloads("events")[0], &ev); err != nil {
		t.Fatalf("event payload: %v", err)
	}
	if ev.SchemaVersion != messageEventSchemaVersion || ev.ChangeType != "new" || ev.ChangeID == 0 {
		t.Fatalf("envelope = %+v", ev)
	}
	if !strings.HasPrefix(ev.EventKey, "aprs:SP9XYZ-7:") {
		t.Fatalf("event key = %q", ev.EventKey)
	}
	h := ev.Event
	if h.Source != "aprs" || h.SourceID != "SP9XYZ-7" || h.Event != "APRS message" {
		t.Fatalf("hazard identity = %+v", h)
	}
	if h.Severity != "severe" {
		t.Fatalf("severity = %q, want severe", h.Severity)
	}
	if h.Headline != "Message from: SP9XYZ-7: hello ops" {
		t.Fatalf("headline = %q, want the required prefix + text", h.Headline)
	}
	if !strings.Contains(h.Description, "SOSNA Test (SP9MOA-10)") {
		t.Fatalf("description = %q, want station name context", h.Description)
	}
	if h.ExpiresAt == nil || h.EffectiveAt == nil || h.Status != "active" {
		t.Fatalf("lifecycle fields = %+v", h)
	}
}

// TestRoutedMessageEventExclusions pins the anti-spoofing and noise rules:
// messages from unregistered callsigns, ack/rej frames and our own
// transmissions never become routed events.
func TestRoutedMessageEventExclusions(t *testing.T) {
	hub, sink := testHub(t, HubConfig{
		Enabled:       true,
		Callsign:      "SP9MOA-10",
		Icon:          "/j",
		GridSquare:    "JO90WW",
		RadiusKM:      DefaultRadiusKM,
		StationTTL:    30 * time.Minute,
		RouteMessages: true,
	})
	ctx, cancel := context.WithCancel(context.Background())
	hub.Start(ctx)
	defer cancel()

	hub.SetSenderGate(func(base string) bool { return base == "SP9XYZ" })

	hub.Observe(ParseFeedLine("SQ9UNK-1>APRS,TCPIP*::SP9MOA-10:internet hello", time.Now()), BackendInternet)
	hub.Observe(ParseFeedLine("SP9XYZ-7>APRS,WIDE1-1*::SP9MOA-10:ack00001", time.Now()), BackendRadio)
	hub.Observe(ParseFeedLine("SP9XYZ-7>APRS,WIDE1-1*::SP9MOA-10:rej00002", time.Now()), BackendRadio)
	hub.Observe(ParseFeedLine("SP9MOA-10>APRS,WIDE1-1*::SP9MOA-10:self test", time.Now()), BackendRadio)
	hub.Observe(ParseFeedLine("SQ9UNK-1>APRS,WIDE1-1*::SP9MOA-10:not on the list", time.Now()), BackendRadio)

	time.Sleep(150 * time.Millisecond)
	if got := len(sink.payloads("events")); got != 0 {
		t.Fatalf("excluded messages produced %d events: %s", got, sink.payloads("events"))
	}
}

// TestRoutedMessageEventInternetDelivery pins that APRS-IS delivered
// messages route too — the trust boundary moved from the transport to the
// sender allow-list (base callsigns registered on our users).
func TestRoutedMessageEventInternetDelivery(t *testing.T) {
	hub, sink := testHub(t, HubConfig{
		Enabled:       true,
		Callsign:      "SP9MOA-10",
		Icon:          "/j",
		GridSquare:    "JO90WW",
		RadiusKM:      DefaultRadiusKM,
		StationTTL:    30 * time.Minute,
		RouteMessages: true,
	})
	ctx, cancel := context.WithCancel(context.Background())
	hub.Start(ctx)
	defer cancel()

	hub.SetSenderGate(func(base string) bool { return base == "SP9XYZ" })
	hub.Observe(ParseFeedLine("SP9XYZ-2>APRS,TCPIP*,qAO::SP9MOA-10:internet alert", time.Now()), BackendInternet)

	waitFor(t, func() bool { return len(sink.payloads("events")) >= 1 })
	var ev MessageEventWire
	if err := json.Unmarshal(sink.payloads("events")[0], &ev); err != nil {
		t.Fatalf("event payload: %v", err)
	}
	if ev.Event.SourceID != "SP9XYZ-2" || ev.Event.Severity != "severe" {
		t.Fatalf("event = %+v", ev.Event)
	}
}

// TestRoutedMessageEventNoGate pins the fail-closed behavior: without a
// sender gate no message ever becomes a hazard event.
func TestRoutedMessageEventNoGate(t *testing.T) {
	hub, sink := testHub(t, HubConfig{
		Enabled:       true,
		Callsign:      "SP9MOA-10",
		Icon:          "/j",
		GridSquare:    "JO90WW",
		RadiusKM:      DefaultRadiusKM,
		StationTTL:    30 * time.Minute,
		RouteMessages: true,
	})
	ctx, cancel := context.WithCancel(context.Background())
	hub.Start(ctx)
	defer cancel()

	hub.Observe(ParseFeedLine("SP9XYZ-7>APRS,WIDE1-1*::SP9MOA-10:hello", time.Now()), BackendRadio)
	time.Sleep(150 * time.Millisecond)
	if got := len(sink.payloads("events")); got != 0 {
		t.Fatalf("no gate but %d events published", got)
	}
}

// TestRoutedMessageEventDisabled: route_messages off → no events.
func TestRoutedMessageEventDisabled(t *testing.T) {
	hub, sink := testHub(t, HubConfig{
		Enabled:    true,
		Callsign:   "SP9MOA-10",
		Icon:       "/j",
		GridSquare: "JO90WW",
		RadiusKM:   DefaultRadiusKM,
		StationTTL: 30 * time.Minute,
	})
	ctx, cancel := context.WithCancel(context.Background())
	hub.Start(ctx)
	defer cancel()

	hub.Observe(ParseFeedLine("SP9XYZ-7>APRS,WIDE1-1*::SP9MOA-10:hello", time.Now()), BackendRadio)
	time.Sleep(150 * time.Millisecond)
	if got := len(sink.payloads("events")); got != 0 {
		t.Fatalf("route_messages disabled but %d events published", got)
	}
}
