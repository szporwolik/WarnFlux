package mqtt

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/szporwolik/WarnFlux/internal/core"
	"github.com/szporwolik/WarnFlux/internal/plugin"
)

func decodeConfig(t *testing.T, yamlText string) *yaml.Node {
	t.Helper()
	var node yaml.Node
	if err := yaml.Unmarshal([]byte(yamlText), &node); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return &node
}

func TestNewValidation(t *testing.T) {
	p, err := New(decodeConfig(t, "broker: tcp://localhost:1883\nclient_id: test-a\nqos: 1\n"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if p.Name() != Type {
		t.Errorf("Name = %q, want %q", p.Name(), Type)
	}

	if _, err := New(nil); err == nil {
		t.Fatal("missing broker must be rejected")
	}
	if _, err := New(decodeConfig(t, "broker: tcp://localhost:1883\nqos: 1\n")); err == nil {
		t.Fatal("missing client_id must be rejected (two instances would collide)")
	}
	if _, err := New(decodeConfig(t, "broker: tcp://localhost:1883\nclient_id: x\nqos: 0\n")); err == nil {
		t.Fatal("qos 0 must be rejected (best effort contradicts at-least-once hazard delivery)")
	}
	if _, err := New(decodeConfig(t, "broker: tcp://localhost:1883\nclient_id: x\nqos: 2\n")); err != nil {
		t.Fatalf("qos 2 must be accepted: %v", err)
	}
	if _, err := New(decodeConfig(t, "broker: tcp://localhost:1883\nclient_id: x\nqos: 7\n")); err == nil {
		t.Fatal("qos > 2 must be rejected")
	}
	if _, err := New(decodeConfig(t, "broker: tcp://localhost:1883\nclient_id: x\nbogus: 1\n")); err == nil {
		t.Fatal("unknown config key must be rejected")
	}
	if _, err := New(decodeConfig(t, "broker: tcp://localhost:1883\nclient_id: x\npassword: x\npassword_file: /tmp/x\n")); err == nil {
		t.Fatal("password and password_file together must be rejected")
	}
}

func TestTopicPrefixValidation(t *testing.T) {
	// Trailing slashes are normalized away (no warnflux//events).
	p, err := New(decodeConfig(t, "broker: tcp://localhost:1883\nclient_id: x\ntopic_prefix: warnflux/\n"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if got := p.(*Output).cfg.TopicPrefix; got != "warnflux" {
		t.Errorf("topic_prefix = %q, want trailing / removed", got)
	}

	// The helper rejects values that would corrupt published topics.
	for _, bad := range []string{"a+b", "a#b", "a\x00b"} {
		if _, err := normalizeTopicPrefix(bad); err == nil {
			t.Errorf("topic_prefix %q must be rejected", bad)
		}
	}
	// Empty (or only-slashes) normalizes to the default prefix.
	if got, err := normalizeTopicPrefix(""); err != nil || got != "warnflux" {
		t.Errorf("empty prefix = %q/%v, want default warnflux", got, err)
	}
	if got, err := normalizeTopicPrefix("/"); err != nil || got != "warnflux" {
		t.Errorf("\"/\" prefix = %q/%v, want default warnflux", got, err)
	}
}

func TestConfigDefaults(t *testing.T) {
	// client_id is required; everything else has defaults.
	if _, err := New(decodeConfig(t, "broker: tcp://localhost:1883\n")); err == nil {
		t.Fatal("missing client_id must be rejected")
	}
	p, err := New(decodeConfig(t, "broker: tcp://localhost:1883\nclient_id: x\n"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	out := p.(*Output)
	if out.cfg.TopicPrefix != "warnflux" {
		t.Errorf("topic_prefix = %q, want default", out.cfg.TopicPrefix)
	}
	if out.qos != 1 {
		t.Errorf("qos = %d, want default 1", out.qos)
	}
	if out.StatusInterval() != 0 {
		t.Errorf("heartbeat default = %v, want disabled", out.StatusInterval())
	}
}

func TestPasswordFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "secret")
	if err := os.WriteFile(path, []byte("s3cret\n"), 0o600); err != nil {
		t.Fatalf("write secret: %v", err)
	}
	p, err := New(decodeConfig(t, "broker: tcp://localhost:1883\nclient_id: x\npassword_file: "+path+"\n"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if got := p.(*Output).cfg.Password; got != "s3cret" {
		t.Errorf("password = %q, want trimmed file contents", got)
	}

	if _, err := New(decodeConfig(t, "broker: tcp://localhost:1883\npassword_file: /nonexistent\n")); err == nil {
		t.Fatal("missing password_file must fail at startup")
	}
}

func TestRegister(t *testing.T) {
	reg := plugin.NewRegistry()
	if err := Register(reg); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := Register(reg); err == nil {
		t.Fatal("double registration must fail")
	}
}

// TestLastWillOfflineStatus pins the retained last will: when the process
// disappears without disconnecting, the broker publishes a retained
// "offline" status on <prefix>/status.
func TestLastWillOfflineStatus(t *testing.T) {
	p, err := New(decodeConfig(t, "broker: tcp://localhost:1883\nclient_id: x\nqos: 2\n"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	opts := p.(*Output).client.OptionsReader()
	if got := opts.WillTopic(); got != "warnflux/status" {
		t.Errorf("will topic = %q, want warnflux/status", got)
	}
	if !opts.WillRetained() {
		t.Error("will must be retained")
	}
	if got := opts.WillQos(); got != 2 {
		t.Errorf("will qos = %d, want 2", got)
	}
	var will map[string]any
	if err := json.Unmarshal(opts.WillPayload(), &will); err != nil {
		t.Fatalf("will payload is not JSON: %v", err)
	}
	if will["schema_version"] != float64(1) || will["service"] != "warnflux" || will["state"] != "offline" {
		t.Errorf("unexpected will payload: %v", will)
	}
	if _, ok := will["generated_at"].(string); !ok {
		t.Errorf("will payload lacks generated_at: %v", will)
	}
}

// TestWireEventGoldenJSON pins the explicit MQTT wire schema (M14).
func TestWireEventGoldenJSON(t *testing.T) {
	eff := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	exp := eff.Add(24 * time.Hour)
	lat, lon := 50.06, 19.94
	change := core.EventChange{
		ID:   42,
		Type: core.ChangeUpdated,
		Event: core.HazardEvent{
			Source:      "meteoalarm",
			SourceID:    "2.49.0.1",
			Category:    "met",
			Event:       "Rain",
			Severity:    "orange",
			Headline:    "Heavy rain",
			EffectiveAt: &eff,
			ExpiresAt:   &exp,
			Latitude:    &lat,
			Longitude:   &lon,
			Areas:       []string{"DE-NW"},
			Status:      core.StatusActive,
			SourceURL:   "https://example.invalid/a",
			ReceivedAt:  eff.Add(-time.Minute),
			UpdatedAt:   eff,
		},
	}

	payload, err := json.Marshal(toWireEvent(change))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	want := `{"schema_version":1,"change_id":42,"change_type":"updated",` +
		`"event_key":"meteoalarm:2.49.0.1",` +
		`"event":{` +
		`"source":"meteoalarm","source_id":"2.49.0.1","category":"met","event":"Rain",` +
		`"severity":"orange","urgency":"","certainty":"","headline":"Heavy rain",` +
		`"description":"","instruction":"","effective_at":"2026-01-01T00:00:00Z",` +
		`"expires_at":"2026-01-02T00:00:00Z","latitude":50.06,"longitude":19.94,` +
		`"areas":["DE-NW"],"status":"active","source_url":"https://example.invalid/a",` +
		`"received_at":"2025-12-31T23:59:00Z","updated_at":"2026-01-01T00:00:00Z"}}`
	if string(payload) != want {
		t.Errorf("wire event mismatch:\n got %s\nwant %s", payload, want)
	}

	// The payload must be a pure wire DTO: no database metadata leaks.
	var decoded map[string]any
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for _, forbidden := range []string{"fingerprint", "first_seen_at", "last_seen_at", "received_at_ms", "expires_at_ms"} {
		if _, ok := decoded[forbidden]; ok {
			t.Errorf("wire payload leaks internal field %q", forbidden)
		}
		if _, ok := decoded["event"].(map[string]any)[forbidden]; ok {
			t.Errorf("wire event leaks internal field %q", forbidden)
		}
	}
}

// TestWireStatusGoldenJSON pins the status schema.
func TestWireStatusGoldenJSON(t *testing.T) {
	status := plugin.Status{
		Version:          "0.1.0",
		Uptime:           90 * time.Second,
		DatabaseHealthy:  true,
		PendingChanges:   3,
		OldestPendingAge: 25 * time.Second,
		Sources: []plugin.PluginStatus{
			{ID: "demo", Type: "demo", State: plugin.StateRunning, ConsecutiveFailures: 0, RestartCount: 1},
		},
		Outputs: []plugin.PluginStatus{
			{ID: "mqtt-main", Type: "mqtt", State: plugin.StateSuspended, ConsecutiveFailures: 5, RestartCount: 0, LastError: "connect: timeout"},
		},
	}

	payload, err := json.Marshal(toWireStatus(status, time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	want := `{"schema_version":1,"service":"warnflux","state":"running",` +
		`"generated_at":"2026-09-22T12:00:00Z","version":"0.1.0",` +
		`"uptime_seconds":90,"database_healthy":true,"pending_changes":3,` +
		`"oldest_pending_age_seconds":25,` +
		`"sources":[{"id":"demo","type":"demo","state":"running","consecutive_failures":0,"restart_count":1}],` +
		`"outputs":[{"id":"mqtt-main","type":"mqtt","state":"suspended","consecutive_failures":5,"restart_count":0,"last_error":"connect: timeout"}]}`
	if string(payload) != want {
		t.Errorf("wire status mismatch:\n got %s\nwant %s", payload, want)
	}
}

// TestInformationTopicMapping pins the retained information topic layout:
// <prefix>/info/<source>/<key>/<kind>.
func TestInformationTopicMapping(t *testing.T) {
	o := &Output{cfg: Config{TopicPrefix: "warnflux"}}
	msg := core.InformationMessage{Source: "openmeteo", Key: "home", Kind: "weather"}
	if got := o.informationTopic(msg); got != "warnflux/info/openmeteo/home/weather" {
		t.Errorf("topic = %q, want warnflux/info/openmeteo/home/weather", got)
	}
}
