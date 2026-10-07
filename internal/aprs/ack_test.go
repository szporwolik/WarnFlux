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

// TestSendMessageWaitAckTimeoutMarksNoAck pins the durable trace of an
// unanswered send: when the ack wait elapses, the tx history row is
// marked no_ack so the admin menu shows the message went unconfirmed.
func TestSendMessageWaitAckTimeoutMarksNoAck(t *testing.T) {
	rec := &fakeAPRSRecorder{}
	hub, _ := testHub(t, HubConfig{
		Enabled:    true,
		Callsign:   "SP9MOA-10",
		GridSquare: "JO90WW",
		RadiusKM:   DefaultRadiusKM,
		StationTTL: 30 * time.Minute,

		MessageRecorder: rec,
	})
	hub.AddTransmitter("radio", &fakeTransmitter{name: "radio", ready: true})
	ctx, cancel := context.WithCancel(context.Background())
	hub.Start(ctx)
	defer cancel()

	ack, err := hub.SendMessageWaitAck(context.Background(), "SP9XYZ-7", "hello", 100*time.Millisecond)
	if ack || !errors.Is(err, ErrNoAck) {
		t.Fatalf("timeout wait = %v, %v; want ErrNoAck", ack, err)
	}
	if got := rec.statusFor(rec.txMsgID(0)); got != "no_ack" {
		t.Fatalf("history status after timeout = %q, want no_ack", got)
	}
}

// TestSendMessageWaitAckLateAckUpgradesHistory pins the durable binding:
// an ack heard AFTER the wait window still marks the tx row delivered.
func TestSendMessageWaitAckLateAckUpgradesHistory(t *testing.T) {
	rec := &fakeAPRSRecorder{}
	hub, _ := testHub(t, HubConfig{
		Enabled:    true,
		Callsign:   "SP9MOA-10",
		GridSquare: "JO90WW",
		RadiusKM:   DefaultRadiusKM,
		StationTTL: 30 * time.Minute,

		MessageRecorder: rec,
	})
	hub.AddTransmitter("radio", &fakeTransmitter{name: "radio", ready: true})
	ctx, cancel := context.WithCancel(context.Background())
	hub.Start(ctx)
	defer cancel()

	if _, err := hub.SendMessageWaitAck(context.Background(), "SP9XYZ-7", "hello", 100*time.Millisecond); !errors.Is(err, ErrNoAck) {
		t.Fatalf("wait = %v, want ErrNoAck", err)
	}
	// A late ack from the addressee upgrades the row.
	hub.Observe(testPacket("SP9XYZ-7>APRS,WIDE1-1*::SP9MOA-10:ack00001"), BackendRadio)
	waitFor(t, func() bool { return rec.statusFor("00001") == "delivered" })

	// A late REJ flips the row to failed.
	if _, err := hub.SendMessageWaitAck(context.Background(), "SP9WSS-2", "hello", 100*time.Millisecond); !errors.Is(err, ErrNoAck) {
		t.Fatalf("wait = %v, want ErrNoAck", err)
	}
	hub.Observe(testPacket("SP9WSS-2>APRS::SP9MOA-10:rej00002"), "aprs-radio")
	waitFor(t, func() bool { return rec.statusFor("00002") == "failed" })
}

// TestLateForeignAckNeverUpgradesHistory pins the durable binding of the
// late path: an ack from an unrelated station (even with the right id)
// must not touch the row.
func TestLateForeignAckNeverUpgradesHistory(t *testing.T) {
	rec := &fakeAPRSRecorder{}
	hub, _ := testHub(t, HubConfig{
		Enabled:    true,
		Callsign:   "SP9MOA-10",
		GridSquare: "JO90WW",
		RadiusKM:   DefaultRadiusKM,
		StationTTL: 30 * time.Minute,

		MessageRecorder: rec,
	})
	hub.AddTransmitter("radio", &fakeTransmitter{name: "radio", ready: true})
	ctx, cancel := context.WithCancel(context.Background())
	hub.Start(ctx)
	defer cancel()

	if _, err := hub.SendMessageWaitAck(context.Background(), "SP9XYZ-7", "hello", 100*time.Millisecond); !errors.Is(err, ErrNoAck) {
		t.Fatalf("wait = %v, want ErrNoAck", err)
	}
	hub.Observe(testPacket("SP9QQQ-9>APRS,WIDE1-1*::SP9MOA-10:ack00001"), BackendRadio)
	// The foreign ack still gets recorded as an rx row; only the status
	// must stay untouched.
	waitFor(t, func() bool {
		rec.mu.Lock()
		defer rec.mu.Unlock()
		return len(rec.rows) >= 2
	})
	if got := rec.statusFor("00001"); got != "no_ack" {
		t.Fatalf("foreign late ack changed history status to %q, want no_ack", got)
	}
}

// TestAckAfterRestartMarksHistory pins the restart case: no in-memory
// waiter exists at all (the service restarted after the send), yet the
// ack still lands on the durable row via the addressee binding.
func TestAckAfterRestartMarksHistory(t *testing.T) {
	rec := &fakeAPRSRecorder{}
	if err := rec.RecordAPRSMessage(context.Background(), "tx", "SP9MOA-10", "SP9XYZ-7", "hello", "00042", "aprs-radio", time.Now()); err != nil {
		t.Fatal(err)
	}
	hub, _ := testHub(t, HubConfig{
		Enabled:    true,
		Callsign:   "SP9MOA-10",
		GridSquare: "JO90WW",
		RadiusKM:   DefaultRadiusKM,
		StationTTL: 30 * time.Minute,

		MessageRecorder: rec,
	})
	ctx, cancel := context.WithCancel(context.Background())
	hub.Start(ctx)
	defer cancel()

	hub.Observe(testPacket("SP9XYZ-7>APRS,WIDE1-1*::SP9MOA-10:ack00042"), BackendRadio)
	waitFor(t, func() bool { return rec.statusFor("00042") == "delivered" })
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

// TestSendMessageWaitAckForeignAckIgnored pins the ack binding: an ack
// from an unrelated station (even with the right message id) never
// confirms the wait — only the addressee's ack does.
func TestSendMessageWaitAckForeignAckIgnored(t *testing.T) {
	hub, _ := testHub(t, HubConfig{
		Enabled:    true,
		Callsign:   "SP9MOA-10",
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
	waitFor(t, func() bool { return len(tx.sends()) == 1 })

	// A foreign station acks with the right id: the wait stays intact.
	hub.Observe(testPacket("SP9QQQ-9>APRS,WIDE1-1*::SP9MOA-10:ack00001"), BackendRadio)
	time.Sleep(150 * time.Millisecond)
	select {
	case r := <-done:
		t.Fatalf("foreign ack completed the wait: %v", r)
	default:
	}

	// The real addressee acks: the wait completes now.
	hub.Observe(testPacket("SP9XYZ-7>APRS,WIDE1-1*::SP9MOA-10:ack00001"), BackendRadio)
	select {
	case r := <-done:
		if !r.ack || r.err != nil {
			t.Fatalf("addressee ack wait = %v, %v", r.ack, r.err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("addressee ack never delivered")
	}
}

// TestSendMessageWaitAckIDReuseBumped pins the id-reuse guard: an id
// still in flight is never re-registered — the next outbound message
// takes a fresh id, so late acks of a previous cycle cannot collide.
func TestSendMessageWaitAckIDReuseBumped(t *testing.T) {
	hub, _ := testHub(t, HubConfig{
		Enabled:    true,
		Callsign:   "SP9MOA-10",
		GridSquare: "JO90WW",
		RadiusKM:   DefaultRadiusKM,
		StationTTL: 30 * time.Minute,
	})
	tx := &fakeTransmitter{name: BackendRadio, ready: true}
	hub.AddTransmitter(BackendRadio, tx)
	ctx, cancel := context.WithCancel(context.Background())
	hub.Start(ctx)
	defer cancel()

	// Simulate a previous cycle: id 00001 is still in flight.
	hub.mu.Lock()
	hub.pending["00001"] = &ackWait{from: "SP9MOA-10", to: "SP9OLD-1", at: time.Now(), ch: make(chan string, 1)}
	hub.mu.Unlock()

	done := make(chan result2, 1)
	go func() {
		ack, err := hub.SendMessageWaitAck(context.Background(), "SP9XYZ-7", "hello", 5*time.Second)
		done <- result2{ack, err}
	}()
	waitFor(t, func() bool { return len(tx.sends()) == 1 })
	if sent := tx.sends()[0][1]; !strings.HasSuffix(sent, "{00002}") {
		t.Fatalf("sent = %q, want a fresh id (00002), never the busy 00001", sent)
	}

	hub.Observe(testPacket("SP9XYZ-7>APRS,WIDE1-1*::SP9MOA-10:ack00002"), BackendRadio)
	select {
	case r := <-done:
		if !r.ack || r.err != nil {
			t.Fatalf("reuse-guarded wait = %v, %v", r.ack, r.err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("ack for the fresh id never delivered")
	}
}

type result2 struct {
	ack bool
	err error
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
