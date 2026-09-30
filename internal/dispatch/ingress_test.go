package dispatch

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestIngressEnqueueAndConsume(t *testing.T) {
	g := NewIngress(4)
	for i := 0; i < 4; i++ {
		if res := g.Enqueue(Event{Kind: EventMQTTMessage}); res != AcceptedEmergency {
			t.Fatalf("enqueue %d = %v, want emergency (no inbox attached)", i, res)
		}
	}
	// Queue full: non-blocking drop.
	if res := g.Enqueue(Event{Kind: EventMQTTMessage}); res != Rejected {
		t.Fatalf("enqueue into full queue = %v, want rejected", res)
	}
	recv, _, _, _, _ := g.Stats()
	if recv != 4 {
		t.Errorf("received = %d, want 4", recv)
	}
	_, dropped, _, _, _ := g.Stats()
	if dropped != 1 {
		t.Errorf("droppedFull = %d, want 1", dropped)
	}
	if g.EmergencyAccepted() != 4 || g.DurableAccepted() != 0 {
		t.Errorf("accepts = (%d durable, %d emergency), want (0, 4)",
			g.DurableAccepted(), g.EmergencyAccepted())
	}

	for i := 0; i < 4; i++ {
		select {
		case e := <-g.Events():
			if e.Kind != EventMQTTMessage {
				t.Errorf("kind = %q", e.Kind)
			}
		case <-time.After(time.Second):
			t.Fatal("event not delivered")
		}
	}
}

func TestIngressStopIntakeRejectsAndCloses(t *testing.T) {
	g := NewIngress(4)
	if res := g.Enqueue(Event{Kind: EventMQTTMessage}); res != AcceptedEmergency {
		t.Fatalf("enqueue = %v, want emergency", res)
	}
	g.StopIntake()

	if res := g.Enqueue(Event{Kind: EventMQTTMessage}); res != Rejected {
		t.Fatalf("enqueue after StopIntake = %v, want rejected", res)
	}
	_, _, late, _, _ := g.Stats()
	if late != 1 {
		t.Errorf("droppedLate = %d, want 1", late)
	}

	// The consumer sees the pre-stop event, then the channel closes.
	if _, ok := <-g.Events(); !ok {
		t.Fatal("channel closed before draining the queued event")
	}
	select {
	case _, ok := <-g.Events():
		if ok {
			t.Fatal("channel still open after drain")
		}
	case <-time.After(time.Second):
		t.Fatal("channel never closed")
	}

	// StopIntake is idempotent.
	g.StopIntake()
}

func TestIngressDrain(t *testing.T) {
	g := NewIngress(8)
	for i := 0; i < 5; i++ {
		g.Enqueue(Event{Kind: EventMQTTMessage})
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if n := g.Drain(ctx); n != 5 {
		t.Errorf("drained = %d, want 5", n)
	}
	g.StopIntake()
	if n := g.Drain(context.Background()); n != 0 {
		t.Errorf("drain after close = %d, want 0", n)
	}
}

func TestEventCloneDeepCopies(t *testing.T) {
	orig := Event{
		Kind:   EventHazardTransition,
		Origin: Origin{Type: "mqtt", ReceiverID: "local"},
		Hazard: &HazardTransition{
			Type:   TransitionNew,
			Key:    "imgw-meteo:123",
			Source: "imgw-meteo",
			Hazard: Hazard{
				Areas:       []string{"powiat slupski"},
				EffectiveAt: timePtr(time.Now()),
				ExpiresAt:   timePtr(time.Now().Add(time.Hour)),
			},
		},
	}
	c := orig.Clone()
	c.Hazard.Hazard.Areas[0] = "MUTATED"
	if orig.Hazard.Hazard.Areas[0] != "powiat slupski" {
		t.Error("areas slice aliased")
	}
	c.Hazard.Hazard.EffectiveAt = timePtr(time.Now().Add(time.Minute))
	if orig.Hazard.Hazard.EffectiveAt.Equal(*c.Hazard.Hazard.EffectiveAt) {
		t.Error("effective time pointer aliased")
	}

	payload := []byte("OPEN")
	gen := Event{Kind: EventMQTTMessage, MQTT: &MQTTMessage{Topic: "club/alarm/door", Payload: payload}}
	// Simulate the source buffer changing after the event was copied.
	payload[0] = 'X'
	gc := gen.Clone()
	gc.MQTT.Payload[0] = 'Y'
	if string(gen.MQTT.Payload) != "XPEN" {
		t.Errorf("clone shares payload with source: %q", gen.MQTT.Payload)
	}
}

func timePtr(t time.Time) *time.Time { return &t }

// fakeInbox records accepted events; fail simulates a broken backend and
// block simulates a database that stalls until the write deadline.
type fakeInbox struct {
	mu    sync.Mutex
	items []Event
	fail  bool
	block bool
}

func (f *fakeInbox) AppendEvent(ctx context.Context, e Event) (int64, error) {
	f.mu.Lock()
	if f.block {
		f.mu.Unlock()
		<-ctx.Done()
		return 0, ctx.Err()
	}
	fail := f.fail
	var id int64
	if !fail {
		f.items = append(f.items, e)
		id = int64(len(f.items))
	}
	f.mu.Unlock()
	if fail {
		return 0, errors.New("db down")
	}
	return id, nil
}

// TestIngressDurableAcceptance pins the inbox semantics: acceptance is
// persisted before the live queue is offered, so a full queue defers the
// event instead of dropping it; an inbox failure falls back to emergency
// acceptance (and a full queue then rejects).
func TestIngressDurableAcceptance(t *testing.T) {
	g := NewIngress(1)
	in := &fakeInbox{}
	g.SetInbox(in)

	if res := g.Enqueue(Event{Kind: EventMQTTMessage}); res != AcceptedDurable {
		t.Fatalf("first enqueue = %v, want durable", res)
	}
	// Live queue full: the inbox still accepts durably.
	if res := g.Enqueue(Event{Kind: EventMQTTMessage}); res != AcceptedDurable {
		t.Fatalf("durable acceptance while the live queue is full = %v, want durable", res)
	}
	in.mu.Lock()
	got := len(in.items)
	in.mu.Unlock()
	if got != 2 {
		t.Fatalf("inbox items = %d, want 2", got)
	}
	recv, dropped, _, _, _ := g.Stats()
	if recv != 1 || dropped != 1 {
		t.Fatalf("stats = (%d received, %d dropped), want (1, 1)", recv, dropped)
	}
	if g.DurableAccepted() != 2 || g.EmergencyAccepted() != 0 {
		t.Errorf("accepts = (%d durable, %d emergency), want (2, 0)",
			g.DurableAccepted(), g.EmergencyAccepted())
	}

	// Broken inbox + full queue: hard rejection, never silent.
	in.mu.Lock()
	in.fail = true
	in.mu.Unlock()
	if res := g.Enqueue(Event{Kind: EventMQTTMessage}); res != Rejected {
		t.Fatalf("enqueue with a broken inbox and a full queue = %v, want rejected", res)
	}
	if g.InboxFailures() != 1 {
		t.Fatalf("inbox failures = %d, want 1", g.InboxFailures())
	}

	// Broken inbox + free queue: the explicit emergency fallback —
	// delivered now, lost on restart, counted as emergency.
	<-g.Events()
	if res := g.Enqueue(Event{Kind: EventMQTTMessage}); res != AcceptedEmergency {
		t.Fatalf("enqueue with a broken inbox and a free queue = %v, want emergency", res)
	}
	if g.EmergencyAccepted() != 1 {
		t.Fatalf("emergency accepted = %d, want 1", g.EmergencyAccepted())
	}
}

// TestIngressWriteDeadline pins the bounded-wait guarantee: a stuck
// database degrades the event to emergency acceptance after the deadline
// instead of hanging the receiver callback.
func TestIngressWriteDeadline(t *testing.T) {
	g := NewIngress(4)
	in := &fakeInbox{block: true}
	g.SetInbox(in)
	g.SetInboxWriteTimeout(30 * time.Millisecond)

	start := time.Now()
	res := g.Enqueue(Event{Kind: EventMQTTMessage})
	elapsed := time.Since(start)
	if res != AcceptedEmergency {
		t.Fatalf("enqueue with a stuck inbox = %v, want emergency", res)
	}
	if elapsed > time.Second {
		t.Fatalf("Enqueue blocked for %v; the inbox write deadline must bound it", elapsed)
	}
	if g.InboxFailures() != 1 {
		t.Fatalf("inbox failures = %d, want 1", g.InboxFailures())
	}
}
