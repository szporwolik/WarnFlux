package aprs

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestSendMessageWaitAckReceivesAck(t *testing.T) {
	hub, sink := testHub(t, HubConfig{
		Enabled:    true,
		Callsign:   "SP9MOA-10",
		Icon:       "/j",
		GridSquare: "JO90WW",
		RadiusKM:   DefaultRadiusKM,
		StationTTL: 30 * time.Minute,
	})
	tx := &fakeTransmitter{name: BackendRadio, ready: true}
	hub.AddTransmitter(BackendRadio, tx)
	ctx, cancel := context.WithCancel(context.Background())
	hub.Start(ctx)
	defer cancel()

	type result struct {
		ack bool
		err error
	}
	done := make(chan result, 1)
	go func() {
		ack, err := hub.SendMessageWaitAck(context.Background(), "SP9XYZ-7", "hello", 5*time.Second)
		done <- result{ack, err}
	}()

	// The outbound frame must carry the {id} suffix.
	waitFor(t, func() bool { return len(tx.sends()) == 1 })
	sent := tx.sends()[0]
	if sent[0] != "SP9XYZ-7" || !strings.HasSuffix(sent[1], "{00001}") {
		t.Fatalf("sent = %+v, want text with {00001}", sent)
	}

	hub.Observe(testPacket("SP9XYZ-7>APRS,WIDE1-1*::SP9MOA-10:ack00001"), BackendRadio)
	select {
	case r := <-done:
		if !r.ack || r.err != nil {
			t.Fatalf("ack wait = %v, %v", r.ack, r.err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("ack never delivered")
	}

	// The tx document on the message feed carries the id.
	var txDoc MessageDocument
	found := false
	for _, payload := range sink.payloads(MessagesTopic) {
		var d MessageDocument
		if err := json.Unmarshal(payload, &d); err == nil && d.Direction == "tx" && d.ID == "00001" {
			txDoc = d
			found = true
		}
	}
	if !found {
		t.Fatal("tx message feed document missing the id")
	}
	if txDoc.Text != "hello" {
		t.Errorf("tx doc text = %q, want %q (without the id suffix)", txDoc.Text, "hello")
	}
}

func TestSendMessageWaitAckTimeout(t *testing.T) {
	hub, _ := testHub(t, HubConfig{
		Enabled:    true,
		Callsign:   "SP9MOA-10",
		GridSquare: "JO90WW",
		RadiusKM:   DefaultRadiusKM,
		StationTTL: 30 * time.Minute,
	})
	hub.AddTransmitter("radio", &fakeTransmitter{name: "radio", ready: true})
	ctx, cancel := context.WithCancel(context.Background())
	hub.Start(ctx)
	defer cancel()

	ack, err := hub.SendMessageWaitAck(context.Background(), "SP9XYZ-7", "hello", 100*time.Millisecond)
	if ack || !errors.Is(err, ErrNoAck) {
		t.Fatalf("timeout wait = %v, %v; want ErrNoAck", ack, err)
	}
}

func TestSendMessageWaitAckReject(t *testing.T) {
	hub, _ := testHub(t, HubConfig{
		Enabled:    true,
		Callsign:   "SP9MOA-10",
		GridSquare: "JO90WW",
		RadiusKM:   DefaultRadiusKM,
		StationTTL: 30 * time.Minute,
	})
	tx := &fakeTransmitter{name: "radio", ready: true}
	hub.AddTransmitter("radio", tx)
	ctx, cancel := context.WithCancel(context.Background())
	hub.Start(ctx)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		_, err := hub.SendMessageWaitAck(context.Background(), "SP9XYZ-7", "hello", 5*time.Second)
		done <- err
	}()
	// Synchronize on the outbound frame: the waiter is registered before
	// the send, so once the frame is on the wire the rej must reach it.
	// Observing earlier races the goroutine (the rej is dropped and the
	// wait degrades into a timeout), which is what flaked on CI.
	waitFor(t, func() bool { return len(tx.sends()) == 1 })
	hub.Observe(testPacket("SP9XYZ-7>APRS::SP9MOA-10:rej00001"), "aprs-radio")
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "rejected") {
			t.Fatalf("reject wait = %v, want rejection error", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("reject never delivered")
	}
}

func TestSendMessageWaitAckNoTransmitter(t *testing.T) {
	hub, _ := testHub(t, HubConfig{
		Enabled:    true,
		Callsign:   "SP9MOA-10",
		GridSquare: "JO90WW",
		RadiusKM:   DefaultRadiusKM,
		StationTTL: 30 * time.Minute,
	})
	ctx, cancel := context.WithCancel(context.Background())
	hub.Start(ctx)
	defer cancel()

	_, err := hub.SendMessageWaitAck(context.Background(), "SP9XYZ-7", "hello", time.Second)
	if !errors.Is(err, ErrNoTransmitter) {
		t.Fatalf("no transmitter wait = %v, want ErrNoTransmitter", err)
	}
}

// TestTransmitterPreferenceByOrigin pins the parallel-backend policy: the
// radio carries messages to rf-heard stations, the internet backend to
// internet-injected ones, and unknown stations prefer the radio. The other
// backend is always the fallback.
func TestTransmitterPreferenceByOrigin(t *testing.T) {
	hub, _ := testHub(t, HubConfig{
		Enabled:    true,
		Callsign:   "SP9MOA-10",
		Icon:       "/j",
		GridSquare: "JO90WW",
		RadiusKM:   DefaultRadiusKM,
		StationTTL: 30 * time.Minute,
	})
	radio := &fakeTransmitter{name: BackendRadio, ready: true}
	inet := &fakeTransmitter{name: BackendInternet, ready: true}
	hub.AddTransmitter(BackendRadio, radio)
	hub.AddTransmitter(BackendInternet, inet)
	ctx, cancel := context.WithCancel(context.Background())
	hub.Start(ctx)
	defer cancel()

	hasStation := func(callsign string) bool {
		for _, s := range hub.Stations() {
			if s.Callsign == callsign {
				return true
			}
		}
		return false
	}

	// RF-heard station → radio.
	hub.Observe(testPacket("SP9AAA-1>APRS,SR9NR*,qAR,SR9NR:!5056.25N/01952.50E-"), BackendInternet)
	waitFor(t, func() bool { return hasStation("SP9AAA-1") })
	if err := hub.SendMessage(ctx, "SP9AAA-1", "hi"); err != nil {
		t.Fatalf("SendMessage rf: %v", err)
	}
	if got := len(radio.sends()); got != 1 {
		t.Errorf("radio sends = %d, want 1 (rf station)", got)
	}
	if got := len(inet.sends()); got != 0 {
		t.Errorf("inet sends = %d, want 0 (rf station)", got)
	}

	// Internet-injected station → APRS-IS.
	hub.Observe(testPacket("SP9BBB-2>APRS,TCPIP*:!5056.25N/01952.50E-"), BackendInternet)
	waitFor(t, func() bool { return hasStation("SP9BBB-2") })
	if err := hub.SendMessage(ctx, "SP9BBB-2", "hi"); err != nil {
		t.Fatalf("SendMessage inet: %v", err)
	}
	if got := len(inet.sends()); got != 1 {
		t.Errorf("inet sends = %d, want 1 (internet station)", got)
	}

	// Unknown station → radio preferred (works without internet).
	if err := hub.SendMessage(ctx, "SP9CCC-3", "hi"); err != nil {
		t.Fatalf("SendMessage unknown: %v", err)
	}
	if got := len(radio.sends()); got != 2 {
		t.Errorf("radio sends = %d, want 2 (unknown station)", got)
	}

	// Radio down → the internet backend takes over for any station.
	hub.AddTransmitter(BackendRadio, &fakeTransmitter{name: BackendRadio, ready: false})
	if err := hub.SendMessage(ctx, "SP9AAA-1", "hi"); err != nil {
		t.Fatalf("SendMessage fallback: %v", err)
	}
	if got := len(inet.sends()); got != 2 {
		t.Errorf("inet sends = %d, want 2 (radio down fallback)", got)
	}
}
