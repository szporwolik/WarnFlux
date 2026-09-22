package dispatch

import (
	"context"
	"testing"
	"time"
)

func TestIngressEnqueueAndConsume(t *testing.T) {
	g := NewIngress(4)
	for i := 0; i < 4; i++ {
		if !g.Enqueue(Event{Kind: EventMQTTMessage}) {
			t.Fatalf("enqueue %d failed", i)
		}
	}
	// Queue full: non-blocking drop.
	if g.Enqueue(Event{Kind: EventMQTTMessage}) {
		t.Fatal("enqueue into full queue succeeded, want drop")
	}
	recv, _, _, _, _ := g.Stats()
	if recv != 4 {
		t.Errorf("received = %d, want 4", recv)
	}
	_, dropped, _, _, _ := g.Stats()
	if dropped != 1 {
		t.Errorf("droppedFull = %d, want 1", dropped)
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
	g.Enqueue(Event{Kind: EventMQTTMessage})
	g.StopIntake()

	if g.Enqueue(Event{Kind: EventMQTTMessage}) {
		t.Fatal("enqueue after StopIntake succeeded")
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
