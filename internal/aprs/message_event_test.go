package aprs

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/radiocli"
)

// TestRoutedMessageEventInternetDelivery pins the RF-only trust boundary:
// an APRS-IS delivered /alert — even from an allow-listed sender, with the
// command registered — never becomes a routed event or gets a reply. Only
// RF-heard frames react.
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
	cli := radiocli.New("WarnFlux v1.0 - SOSNA - sosna.sp9moa.pl")
	cli.RegisterRestricted("alert", "alert", func(args string, _ radiocli.Sender) radiocli.Result {
		return radiocli.Result{Handled: true, Alert: &radiocli.AlertSpec{Headline: strings.TrimSpace(args), TTL: 4 * time.Hour}, Reply: "OK: alert raised"}
	})
	hub.SetCLI(cli)
	tx := &fakeTransmitter{name: "aprs-inet", ready: true}
	hub.AddTransmitter("aprs-inet", tx)
	hub.Observe(ParseFeedLine("SP9XYZ-2>APRS,TCPIP*,qAO::SP9MOA-10:/alert alarm test", time.Now()), BackendInternet)

	time.Sleep(150 * time.Millisecond)
	if got := len(sink.payloads("events")); got != 0 {
		t.Fatalf("internet-delivered /alert produced %d events, want 0 (RF only)", got)
	}
	if got := len(tx.sends()); got != 0 {
		t.Fatalf("internet-delivered /alert produced %d replies, want 0 (RF only)", got)
	}
}

// TestRoutedMessageEventNoGate pins the fail-closed behavior: without a
// sender gate no frame ever becomes a hazard event.
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
	hub.Observe(ParseFeedLine("SP9XYZ-7>APRS,WIDE1-1*::SP9MOA-10:/alert alarm test", time.Now()), BackendRadio)
	time.Sleep(150 * time.Millisecond)
	if got := len(sink.payloads("events")); got != 0 {
		t.Fatalf("no gate but %d events published", got)
	}
}

// TestCommandsWorkWithRouteMessagesOff pins the Meshtastic parity: the
// radio CLI does not depend on the legacy route_messages flag — plain
// messages stay off the alarm pipeline, while an allow-listed sender's
// /alert raises the hazard regardless.
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
	cli := radiocli.New("WarnFlux v1.0 - SOSNA - sosna.sp9moa.pl")
	cli.RegisterRestricted("alert", "alert", func(args string, _ radiocli.Sender) radiocli.Result {
		return radiocli.Result{Handled: true, Alert: &radiocli.AlertSpec{Headline: strings.TrimSpace(args), TTL: 4 * time.Hour}, Reply: "OK: alert raised"}
	})
	hub.SetCLI(cli)

	// Plain message without a CLI-visible sender gate approval: no
	// events.
	hub.Observe(ParseFeedLine("SP9ZZZ-7>APRS,WIDE1-1*::SP9MOA-10:hello", time.Now()), BackendRadio)
	time.Sleep(150 * time.Millisecond)
	if got := len(sink.payloads("events")); got != 0 {
		t.Fatalf("plain message produced %d events", got)
	}

	// /alert from an allow-listed sender raises the hazard even though
	// route_messages is off.
	hub.Observe(ParseFeedLine("SP9XYZ-7>APRS,WIDE1-1*::SP9MOA-10:/alert alarm test", time.Now()), BackendRadio)
	waitFor(t, func() bool { return len(sink.payloads("events")) >= 1 })
	if got := len(sink.payloads("events")); got != 1 {
		t.Fatalf("events after /alert = %d, want 1", got)
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
