package aprs

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/radiocli"
)

// TestRoutedMessageEvent pins the APRS command → /events bridge: a /debug
// command addressed to us, sent by an operator on the sender allow-list
// (base callsign match, any SSID), is re-published on the events stream
// with the "aprs" source, the "Message from: <CALL>" prefix and severe
// severity. Plain messages never become events.
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
	hub.SetCLI(radiocli.New("WarnFlux v1.0 - SOSNA - sosna.sp9moa.pl"))
	hub.Observe(ParseFeedLine("SP9XYZ-7>APRS,WIDE1-1*::SP9MOA-10:/debug", time.Now()), BackendRadio)

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
	if h.Source != "aprs" || !strings.HasPrefix(h.SourceID, "SP9XYZ-7:msg:") || h.Event != "APRS message" {
		t.Fatalf("hazard identity = %+v", h)
	}
	if h.Severity != "severe" {
		t.Fatalf("severity = %q, want severe", h.Severity)
	}
	if h.Headline != "Message from: SP9XYZ-7: /debug" {
		t.Fatalf("headline = %q, want the required prefix + command", h.Headline)
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
	// Plain chat from an ALLOWED sender never becomes an event either:
	// alarms come from commands only.
	hub.Observe(ParseFeedLine("SP9XYZ-7>APRS,WIDE1-1*::SP9MOA-10:plain hello", time.Now()), BackendRadio)

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
	hub.SetCLI(radiocli.New("WarnFlux v1.0 - SOSNA - sosna.sp9moa.pl"))
	hub.Observe(ParseFeedLine("SP9XYZ-2>APRS,TCPIP*,qAO::SP9MOA-10:/debug", time.Now()), BackendInternet)

	waitFor(t, func() bool { return len(sink.payloads("events")) >= 1 })
	var ev MessageEventWire
	if err := json.Unmarshal(sink.payloads("events")[0], &ev); err != nil {
		t.Fatalf("event payload: %v", err)
	}
	if !strings.HasPrefix(ev.Event.SourceID, "SP9XYZ-2:msg:") || ev.Event.Severity != "severe" {
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

// TestCommandsWorkWithRouteMessagesOff pins the Meshtastic parity: the
// radio CLI does not depend on the legacy route_messages flag — plain
// messages stay off the alarm pipeline, while an allow-listed sender's
// /debug fires the alarm regardless.
func TestCommandsWorkWithRouteMessagesOff(t *testing.T) {
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

	hub.SetSenderGate(func(base string) bool { return base == "SP9XYZ" })
	hub.SetCLI(radiocli.New("WarnFlux v1.0 - SOSNA - sosna.sp9moa.pl"))

	// Plain message without a CLI-visible sender gate approval: no
	// events.
	hub.Observe(ParseFeedLine("SP9ZZZ-7>APRS,WIDE1-1*::SP9MOA-10:hello", time.Now()), BackendRadio)
	time.Sleep(150 * time.Millisecond)
	if got := len(sink.payloads("events")); got != 0 {
		t.Fatalf("plain message produced %d events", got)
	}

	// /debug from an allow-listed sender fires the alarm even though
	// route_messages is off.
	hub.Observe(ParseFeedLine("SP9XYZ-7>APRS,WIDE1-1*::SP9MOA-10:/debug", time.Now()), BackendRadio)
	waitFor(t, func() bool { return len(sink.payloads("events")) >= 1 })
	if got := len(sink.payloads("events")); got != 1 {
		t.Fatalf("events after /debug = %d, want 1", got)
	}
}

// TestBulletinRecordedNotRouted pins the bulletin policy: broadcast
// frames (addressed to BLNn) land in the received-message feed but never
// become routed /events, even when the sender sits on the allow-list.
func TestBulletinRecordedNotRouted(t *testing.T) {
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
	hub.Observe(ParseFeedLine("SP9XYZ-7>APRS,WIDE1-1*::BLN0     :ops bulletin", time.Now()), BackendRadio)

	waitFor(t, func() bool { return len(sink.payloads(MessagesTopic)) >= 1 })
	time.Sleep(150 * time.Millisecond)
	if got := len(sink.payloads("events")); got != 0 {
		t.Fatalf("bulletin produced %d routed events: %s", got, sink.payloads("events"))
	}
}
