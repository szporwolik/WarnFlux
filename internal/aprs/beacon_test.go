package aprs

import (
	"context"
	"errors"
	"testing"
	"time"
)

// fakeBeaconTransmitter is a transmitter that also records beacons.
type fakeBeaconTransmitter struct {
	fakeTransmitter
	mu      chan struct{}
	beacons int
}

func newFakeBeaconTransmitter(name string) *fakeBeaconTransmitter {
	return &fakeBeaconTransmitter{
		fakeTransmitter: fakeTransmitter{name: name, ready: true},
		mu:              make(chan struct{}, 1),
	}
}

func (f *fakeBeaconTransmitter) Beacon(_ context.Context) error {
	select {
	case f.mu <- struct{}{}:
		f.beacons++
		<-f.mu
	default:
		f.beacons++
	}
	return nil
}

// TestSendBeacon pins the manual beacon trigger: no beacon-capable
// transmitter fails closed, a registered one transmits, and the radio
// backend is preferred over any other.
func TestSendBeacon(t *testing.T) {
	hub, _ := testHub(t, HubConfig{
		Enabled: true, Callsign: "SP9MOA-10", Name: "SOSNA Test",
		Icon: "/j", GridSquare: "JO90WW", RadiusKM: DefaultRadiusKM,
		StationTTL: 30 * time.Minute,
	})

	if err := hub.SendBeacon(context.Background()); !errors.Is(err, ErrNoBeacon) {
		t.Fatalf("no transmitter beacon = %v, want ErrNoBeacon", err)
	}

	inet := newFakeBeaconTransmitter(BackendInternet)
	radio := newFakeBeaconTransmitter(BackendRadio)
	hub.AddTransmitter(BackendInternet, inet)
	hub.AddTransmitter(BackendRadio, radio)
	if err := hub.SendBeacon(context.Background()); err != nil {
		t.Fatalf("SendBeacon = %v", err)
	}
	if radio.beacons != 1 || inet.beacons != 0 {
		t.Fatalf("beacons = radio:%d inet:%d, want radio 1 (preferred)", radio.beacons, inet.beacons)
	}

	// With only the internet backend left, it still beacons.
	hub.RemoveTransmitter(BackendRadio)
	if err := hub.SendBeacon(context.Background()); err != nil {
		t.Fatalf("SendBeacon via fallback = %v", err)
	}
	if inet.beacons != 1 {
		t.Fatalf("fallback beacons = %d, want 1", inet.beacons)
	}
}

// TestFormatPosition pins the uncompressed position rendering.
func TestFormatPosition(t *testing.T) {
	if got := FormatPosition(50.1234, 20.9876); got != "5007.40N/02059.26E" {
		t.Fatalf("position = %q, want 5007.40N/02059.26E", got)
	}
	if got := FormatPosition(-33.5, -70.25); got != "3330.00S/07015.00W" {
		t.Fatalf("southern position = %q", got)
	}
	if _, err := BuildPositionPacket(0, 0, "/j", "SOSNA"); err == nil {
		t.Fatal("zero position accepted")
	}
	info, err := BuildPositionPacket(50, 20, "/j", "SOSNA")
	if err != nil {
		t.Fatalf("BuildPositionPacket = %v", err)
	}
	if got, want := string(info), "!5000.00N/02000.00E/jSOSNA"; got != want {
		t.Fatalf("packet = %q, want %q", got, want)
	}
}
