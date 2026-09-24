package mqttreceiver

import (
	"encoding/json"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/dispatch"
	"github.com/szporwolik/WarnFlux/internal/dispatch/state"
)

// testMessage is a minimal paho.Message fake.
type testMessage struct {
	topic     string
	payload   []byte
	retained  bool
	duplicate bool
}

func (m *testMessage) Duplicate() bool   { return m.duplicate }
func (m *testMessage) Qos() byte         { return 1 }
func (m *testMessage) Retained() bool    { return m.retained }
func (m *testMessage) Topic() string     { return m.topic }
func (m *testMessage) MessageID() uint16 { return 0 }
func (m *testMessage) Payload() []byte   { return m.payload }
func (m *testMessage) Ack()              {}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

type ingestorEnv struct {
	ingestor *Ingestor
	state    *state.State
	ingress  *dispatch.Ingress
	stats    *Stats
}

func newIngestorEnv(t *testing.T, receiverID string, wfEnabled bool, filters []string) *ingestorEnv {
	t.Helper()
	st := state.New()
	g := dispatch.NewIngress(64)
	stats := &Stats{}
	env := &ingestorEnv{
		ingestor: NewIngestor(receiverID, wfEnabled, "warnflux", filters, st, g, stats, testLogger(), nil),
		state:    st,
		ingress:  g,
		stats:    stats,
	}
	return env
}

// recv returns one canonical event from the ingress or fails.
func recv(t *testing.T, g *dispatch.Ingress) dispatch.Event {
	t.Helper()
	select {
	case e := <-g.Events():
		return e
	case <-time.After(time.Second):
		t.Fatal("no canonical event delivered")
		return dispatch.Event{}
	}
}

func expectNoEvent(t *testing.T, g *dispatch.Ingress) {
	t.Helper()
	select {
	case e := <-g.Events():
		t.Fatalf("unexpected canonical event: %+v", e)
	case <-time.After(100 * time.Millisecond):
	}
}

func mustActivePayload(t *testing.T, eventKey, source, sourceID, severity string) []byte {
	t.Helper()
	b, err := json.Marshal(ActivePayload{
		SchemaVersion: 1,
		Type:          "active_hazard",
		EventKey:      eventKey,
		Event: HazardPayload{
			Source:     source,
			SourceID:   sourceID,
			Event:      "Strong wind",
			Severity:   severity,
			Headline:   "Strong wind warning",
			Areas:      []string{"powiat slupski"},
			Status:     "active",
			ReceivedAt: "2026-09-22T10:00:00Z",
			UpdatedAt:  "2026-09-22T10:05:00Z",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestActiveHazardStoredAndDeleted(t *testing.T) {
	env := newIngestorEnv(t, "local", true, nil)
	key := "imgw-meteo:123"
	topic := "warnflux/active/imgw-meteo/" + TopicHash(key)
	env.ingestor.HandleMessage(nil, &testMessage{topic: topic, payload: mustActivePayload(t, key, "imgw-meteo", "123", "severe"), retained: true})

	snap := env.state.Snapshot()
	if len(snap.Hazards) != 1 {
		t.Fatalf("hazards = %d, want 1", len(snap.Hazards))
	}
	if snap.Hazards[0].ReceiverID != "local" {
		t.Errorf("receiver = %q, want local", snap.Hazards[0].ReceiverID)
	}
	// Zero-length retained payload removes the hazard (per receiver).
	env.ingestor.HandleMessage(nil, &testMessage{topic: topic, payload: []byte{}, retained: true})
	if len(env.state.Snapshot().Hazards) != 0 {
		t.Fatal("hazard not removed by retained delete")
	}
}

func TestActivePastExpiryDropped(t *testing.T) {
	env := newIngestorEnv(t, "local", true, nil)
	key := "compose:1"
	topic := "warnflux/active/compose/" + TopicHash(key)

	var ap ActivePayload
	if err := json.Unmarshal(mustActivePayload(t, key, "compose", "ops", "severe"), &ap); err != nil {
		t.Fatal(err)
	}
	past := time.Now().Add(-time.Hour).UTC().Format(time.RFC3339)
	ap.Event.ExpiresAt = &past
	payload, err := json.Marshal(ap)
	if err != nil {
		t.Fatal(err)
	}

	env.ingestor.HandleMessage(nil, &testMessage{topic: topic, payload: payload, retained: true})
	if got := len(env.state.Snapshot().Hazards); got != 0 {
		t.Fatalf("hazards = %d, want 0 (a document past its expires_at must never be mirrored)", got)
	}
}

// TestMultiBrokerStateCollision is the mandatory collision test: two
// receivers publishing the SAME topic with different payloads must both
// exist independently.
func TestMultiBrokerStateCollision(t *testing.T) {
	envA := newIngestorEnv(t, "local", true, nil)
	envB := newIngestorEnv(t, "remote", true, nil)
	envA.ingestor.state = envA.state
	envB.ingestor.state = envA.state // share ONE mirror store
	envA.ingestor.ingress = envA.ingress
	envB.ingestor.ingress = envA.ingress

	key := "imgw-meteo:abc"
	topic := "warnflux/active/imgw-meteo/" + TopicHash(key)

	envA.ingestor.HandleMessage(nil, &testMessage{topic: topic, payload: mustActivePayload(t, key, "imgw-meteo", "abc", "severe"), retained: true})
	envB.ingestor.HandleMessage(nil, &testMessage{topic: topic, payload: mustActivePayload(t, key, "imgw-meteo", "abc", "extreme"), retained: true})

	snap := envA.state.Snapshot()
	if len(snap.Hazards) != 2 {
		t.Fatalf("hazards = %d, want 2 (one per receiver)", len(snap.Hazards))
	}
	sev := map[string]bool{}
	for _, h := range snap.Hazards {
		sev[h.ReceiverID+":"+h.Severity] = true
	}
	if !sev["local:severe"] || !sev["remote:extreme"] {
		t.Errorf("no overwrite expected; got %v", sev)
	}

	// Deleting on receiver A must not touch receiver B.
	envA.ingestor.HandleMessage(nil, &testMessage{topic: topic, payload: []byte{}, retained: true})
	snap = envA.state.Snapshot()
	if len(snap.Hazards) != 1 || snap.Hazards[0].ReceiverID != "remote" {
		t.Fatalf("delete crossed receiver namespaces: %+v", snap.Hazards)
	}
}

func TestRouterStatusPerReceiver(t *testing.T) {
	envA := newIngestorEnv(t, "local", true, nil)
	envB := newIngestorEnv(t, "remote", true, nil)
	envB.ingestor.state = envA.state

	payloadA := []byte(`{"schema_version":1,"service":"warnflux","state":"running","generated_at":"2026-09-22T12:00:00Z","database_healthy":true,"pending_changes":1}`)
	payloadB := []byte(`{"schema_version":1,"service":"warnflux","state":"offline","generated_at":"2026-09-22T12:00:00Z","database_healthy":false,"pending_changes":9}`)
	envA.ingestor.HandleMessage(nil, &testMessage{topic: "warnflux/status", payload: payloadA, retained: true})
	envB.ingestor.HandleMessage(nil, &testMessage{topic: "warnflux/status", payload: payloadB, retained: true})

	snap := envA.state.Snapshot()
	if len(snap.Router) != 2 {
		t.Fatalf("router statuses = %d, want 2", len(snap.Router))
	}
	if snap.Router["local"].State != "running" || snap.Router["remote"].State != "offline" {
		t.Errorf("router statuses overwrote each other: %+v", snap.Router)
	}
}

func TestEventBecomesHazardTransition(t *testing.T) {
	env := newIngestorEnv(t, "local", true, nil)
	payload := []byte(`{
		"schema_version": 1,
		"change_id": 42,
		"change_type": "new",
		"event_key": "imgw-meteo:123",
		"event": {
			"source": "imgw-meteo",
			"source_id": "123",
			"event": "Strong wind",
			"severity": "extreme",
			"headline": "Strong wind warning",
			"status": "active",
			"received_at": "2026-09-22T10:00:00Z",
			"updated_at": "2026-09-22T10:05:00Z"
		}
	}`)
	env.ingestor.HandleMessage(nil, &testMessage{topic: "warnflux/events", payload: payload})

	ev := recv(t, env.ingress)
	if ev.Kind != dispatch.EventHazardTransition {
		t.Fatalf("kind = %q", ev.Kind)
	}
	if ev.Origin.ReceiverID != "local" {
		t.Errorf("origin receiver = %q", ev.Origin.ReceiverID)
	}
	if ev.Hazard == nil || ev.Hazard.Type != dispatch.TransitionNew || ev.Hazard.Key != "imgw-meteo:123" || ev.Hazard.ChangeID != 42 {
		t.Errorf("transition = %+v", ev.Hazard)
	}
	if ev.Hazard.Hazard.Severity != "extreme" {
		t.Errorf("severity = %q", ev.Hazard.Hazard.Severity)
	}
	// Exactly one event for one frame.
	expectNoEvent(t, env.ingress)
}

func TestStateTopicsDoNotBecomeEvents(t *testing.T) {
	env := newIngestorEnv(t, "local", true, nil)
	key := "imgw-meteo:123"
	env.ingestor.HandleMessage(nil, &testMessage{topic: "warnflux/active/imgw-meteo/" + TopicHash(key), payload: mustActivePayload(t, key, "imgw-meteo", "123", "severe"), retained: true})
	env.ingestor.HandleMessage(nil, &testMessage{topic: "warnflux/info/openmeteo/weather-home/home/weather", payload: []byte(`{"schema_version":1,"type":"weather","generated_at":"2026-09-22T12:00:00Z","provider":{"name":"Open-Meteo"},"location":{"id":"home","latitude":1,"longitude":2,"timezone":"Europe/Warsaw"}}`), retained: true})
	env.ingestor.HandleMessage(nil, &testMessage{topic: "warnflux/status", payload: []byte(`{"schema_version":1,"service":"warnflux","state":"running","generated_at":"2026-09-22T12:00:00Z","database_healthy":true}`), retained: true})
	expectNoEvent(t, env.ingress)

	snap := env.state.Snapshot()
	if len(snap.Hazards) != 1 || len(snap.Weather) != 1 || len(snap.Router) != 1 {
		t.Errorf("state not updated: %+v", snap)
	}
}

func TestGenericMQTTMessage(t *testing.T) {
	env := newIngestorEnv(t, "remote-club", false, []string{"club/alarm/#"})
	env.ingestor.HandleMessage(nil, &testMessage{
		topic: "club/alarm/door", payload: []byte("OPEN"), retained: true, duplicate: true,
	})

	ev := recv(t, env.ingress)
	if ev.Kind != dispatch.EventMQTTMessage {
		t.Fatalf("kind = %q", ev.Kind)
	}
	if ev.Origin.ReceiverID != "remote-club" {
		t.Errorf("receiver = %q", ev.Origin.ReceiverID)
	}
	m := ev.MQTT
	if m == nil || m.Topic != "club/alarm/door" || string(m.Payload) != "OPEN" || !m.Retained || !m.Duplicate {
		t.Errorf("message = %+v", m)
	}
	// No JSON assumptions: a non-JSON payload is accepted as-is.
	env.ingestor.HandleMessage(nil, &testMessage{topic: "club/alarm/window", payload: []byte{0x00, 0xff, 0x10}})
	ev2 := recv(t, env.ingress)
	if len(ev2.MQTT.Payload) != 3 || ev2.MQTT.Payload[0] != 0x00 {
		t.Errorf("binary payload not preserved: %v", ev2.MQTT.Payload)
	}
}

// TestPayloadOwnership verifies the canonical event payload does not alias
// the Paho-owned buffer.
func TestPayloadOwnership(t *testing.T) {
	env := newIngestorEnv(t, "remote-club", false, []string{"club/#"})
	buf := []byte("OPEN")
	env.ingestor.HandleMessage(nil, &testMessage{topic: "club/alarm/door", payload: buf})
	buf[0] = 'X' // source buffer mutates after the callback returned

	ev := recv(t, env.ingress)
	if string(ev.MQTT.Payload) != "OPEN" {
		t.Errorf("canonical event aliases source buffer: %q", ev.MQTT.Payload)
	}
}

// TestWFNamespacePrecedence: a malformed frame inside the WarnFlux
// prefix is rejected, never reclassified as a raw generic event even when
// generic subscriptions overlap.
func TestWFNamespacePrecedence(t *testing.T) {
	env := newIngestorEnv(t, "local", true, []string{"warnflux/#"})
	env.ingestor.HandleMessage(nil, &testMessage{topic: "warnflux/events", payload: []byte(`{"schema_version":1,"change_type":"bogus","event_key":"x:y"}`)})
	expectNoEvent(t, env.ingress)
	if env.stats.Malformed.Load() == 0 {
		t.Error("malformed counter not incremented")
	}

	// Valid generic topic outside the prefix still works.
	env.ingestor.HandleMessage(nil, &testMessage{topic: "warnflux-bis/whatever", payload: []byte("x")})
	// That topic is not matched by "warnflux/#", so nothing.
	expectNoEvent(t, env.ingress)
}

func TestMalformedProtocolIgnored(t *testing.T) {
	env := newIngestorEnv(t, "local", true, nil)
	topic := "warnflux/active/imgw-meteo/" + TopicHash("imgw-meteo:123")
	env.ingestor.HandleMessage(nil, &testMessage{topic: topic, payload: []byte("{not json"), retained: true})
	if len(env.state.Snapshot().Hazards) != 0 {
		t.Fatal("malformed active payload stored")
	}
	if env.stats.Malformed.Load() == 0 {
		t.Error("malformed counter not incremented")
	}
}

func TestActiveHashMismatchRejected(t *testing.T) {
	env := newIngestorEnv(t, "local", true, nil)
	topic := "warnflux/active/imgw-meteo/" + TopicHash("imgw-meteo:DIFFERENT")
	env.ingestor.HandleMessage(nil, &testMessage{topic: topic, payload: mustActivePayload(t, "imgw-meteo:123", "imgw-meteo", "123", "severe"), retained: true})
	if len(env.state.Snapshot().Hazards) != 0 {
		t.Fatal("hash mismatch accepted")
	}
}

func TestOversizedMessageIgnored(t *testing.T) {
	env := newIngestorEnv(t, "local", true, nil)
	big := strings.Repeat("a", MaxPayload+1)
	env.ingestor.HandleMessage(nil, &testMessage{topic: "warnflux/status", payload: []byte(big), retained: true})
	if env.stats.Oversized.Load() == 0 {
		t.Error("oversized counter not incremented")
	}
	if len(env.state.Snapshot().Router) != 0 {
		t.Error("oversized status stored")
	}
}

func TestWFDisabledTreatsPrefixAsGeneric(t *testing.T) {
	env := newIngestorEnv(t, "remote", false, []string{"warnflux/#"})
	env.ingestor.HandleMessage(nil, &testMessage{topic: "warnflux/events", payload: []byte("raw-bytes")})
	ev := recv(t, env.ingress)
	if ev.Kind != dispatch.EventMQTTMessage {
		t.Fatalf("kind = %q, want generic mqtt_message", ev.Kind)
	}
}
