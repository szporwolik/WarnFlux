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
	hub.AddTransmitter("radio", &fakeTransmitter{name: "radio", ready: true})
	ctx, cancel := context.WithCancel(context.Background())
	hub.Start(ctx)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		_, err := hub.SendMessageWaitAck(context.Background(), "SP9XYZ-7", "hello", 5*time.Second)
		done <- err
	}()
	// The first send uses id 00001 for this hub.
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
