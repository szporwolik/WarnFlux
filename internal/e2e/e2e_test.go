// Package e2e wires the REAL WarnFlux pipeline end-to-end, without MQTT
// brokers or network services: source adapter → ingestion → SQLite →
// the public wire contract → strict receiver parsing → dispatch ingress →
// routing engine → dedup ledger → action worker → audit trail → metrics.
//
// The tests here are the regression net for the closed severity model:
// provider scales are mapped by adapters, the canonical vocabulary is
// enforced at the wire boundary, and routing decisions happen exclusively
// on canonical values.
package e2e_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/szporwolik/WarnFlux/internal/action"
	"github.com/szporwolik/WarnFlux/internal/actions"
	"github.com/szporwolik/WarnFlux/internal/config"
	"github.com/szporwolik/WarnFlux/internal/dispatch"
	"github.com/szporwolik/WarnFlux/internal/ingest"
	"github.com/szporwolik/WarnFlux/internal/metrics"
	"github.com/szporwolik/WarnFlux/internal/mqttreceiver"
	"github.com/szporwolik/WarnFlux/internal/plugin"
	"github.com/szporwolik/WarnFlux/internal/plugins"
	"github.com/szporwolik/WarnFlux/internal/routing"
	"github.com/szporwolik/WarnFlux/internal/severity"
	"github.com/szporwolik/WarnFlux/internal/storage"
	"github.com/szporwolik/WarnFlux/internal/storage/sqlite"
	"github.com/szporwolik/WarnFlux/internal/trail"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func node(t *testing.T, v any) *yaml.Node {
	t.Helper()
	data, err := yaml.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	var n yaml.Node
	if err := yaml.Unmarshal(data, &n); err != nil {
		t.Fatal(err)
	}
	return &n
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// wirePayloadFor builds the public /events wire JSON for one persisted
// event — the same shape the MQTT output publishes.
func wirePayloadFor(t *testing.T, st *sqlite.Store, key string) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ev, err := st.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get(%q): %v", key, err)
	}
	payload := mqttreceiver.EventPayload{
		SchemaVersion: mqttreceiver.WireSchemaVersion,
		ChangeID:      1,
		ChangeType:    mqttreceiver.ChangeNew,
		EventKey:      ev.Event.Key(),
		Event: mqttreceiver.HazardPayload{
			Source:           ev.Event.Source,
			SourceID:         ev.Event.SourceID,
			Category:         ev.Event.Category,
			Event:            ev.Event.Event,
			Severity:         ev.Event.Severity,
			ProviderSeverity: ev.Event.ProviderSeverity,
			Urgency:          ev.Event.Urgency,
			Certainty:        ev.Event.Certainty,
			Headline:         ev.Event.Headline,
			Description:      ev.Event.Description,
			Instruction:      ev.Event.Instruction,
			Areas:            ev.Event.Areas,
			Status:           string(ev.Event.Status),
			SourceURL:        ev.Event.SourceURL,
			ReceivedAt:       ev.Event.ReceivedAt.Format(time.RFC3339Nano),
			UpdatedAt:        ev.Event.UpdatedAt.Format(time.RFC3339Nano),
		},
	}
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal wire payload: %v", err)
	}
	return data
}

// TestProviderToActionE2E runs the full chain for one IMGW meteo warning
// with provider degree 2: the adapter maps it to canonical "severe",
// keeps "2" as provider_severity, and the routing matrix fires the logger
// action at threshold ≥ moderate. A replayed wire message is deduplicated
// by the real SQLite ledger.
func TestProviderToActionE2E(t *testing.T) {
	// Provider side: a real IMGW warningsmeteo feed with degree 2.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`[{
			"id": "warn-1",
			"nazwa_zdarzenia": "Silny wiatr",
			"stopien": "2",
			"prawdopodobienstwo": "85",
			"obowiazuje_od": "2026-09-23 10:00:00",
			"obowiazuje_do": "2026-09-23 22:00:00",
			"opublikowano": "2026-09-23 08:00:00",
			"tresc": "Prognozuje się silny wiatr.",
			"komentarz": "Brak.",
			"biuro": "Biuro Prognoz",
			"teryt": ["1219"]
		}]`))
	}))
	defer srv.Close()

	store, _, err := sqlite.Open(filepath.Join(t.TempDir(), "e2e.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	met := metrics.New()
	rec := trail.NewRecorder(trail.DefaultMaxTrails)
	ing := ingest.NewIngester(store, testLogger(), met)

	// Real plugin manager with the real IMGW source.
	reg := plugin.NewRegistry()
	if err := plugins.RegisterBuiltins(reg); err != nil {
		t.Fatal(err)
	}
	mgr, err := plugin.NewManager(reg, []config.Source{{
		ID: "imgw-e2e", Type: "imgw", Enabled: true,
		Config: node(t, map[string]any{
			"base_url": srv.URL,
			"feeds":    []string{"meteo"},
		}),
	}}, nil, ing.Ingest, ing.Expire, store, plugin.ManagerOptions{
		ExpirationInterval: time.Minute,
		ChangeRetention:    time.Hour,
		Version:            "e2e",
	}, testLogger())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go mgr.Run(ctx)
	t.Cleanup(func() { cancel() })

	// Wait for the adapter → ingestion → SQLite chain.
	var key string
	waitFor(t, "IMGW event persisted", func() bool {
		// The stored identity is imgw-meteo:warn-1.
		stored, err := store.Get(ctx, "imgw-meteo:warn-1")
		if err != nil || stored == nil {
			return false
		}
		key = stored.Event.Key()
		return true
	})
	stored, err := store.Get(ctx, key)
	if err != nil || stored == nil {
		t.Fatalf("store.Get: %v", err)
	}
	if stored.Event.Severity != severity.Severe {
		t.Fatalf("stored severity = %q, want %q (adapter must map IMGW degree 2)", stored.Event.Severity, severity.Severe)
	}
	if stored.Event.ProviderSeverity != "2" {
		t.Fatalf("provider_severity = %q, want raw degree %q", stored.Event.ProviderSeverity, "2")
	}

	// Routing side: group ops with a matrix cell (any source, logger-a ≥
	// moderate) and a real action worker.
	group, err := store.CreateGroup("ops")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SetGroupRouting(group.ID, []storage.ChannelAssignment{
		{ID: "logger-a", MinSeverity: severity.Moderate},
	}); err != nil {
		t.Fatal(err)
	}

	areg := action.NewRegistry()
	if err := actions.RegisterAll(areg); err != nil {
		t.Fatal(err)
	}
	actionsMgr, err := action.NewManager([]config.Action{{
		ID: "logger-a", Type: "logger", Enabled: true,
		Config: node(t, map[string]any{"level": "info"}),
	}}, areg, testLogger(), rec, met)
	if err != nil {
		t.Fatal(err)
	}
	actionsMgr.Start(ctx)
	t.Cleanup(func() {
		shutCtx, shutCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer shutCancel()
		_ = actionsMgr.Shutdown(shutCtx)
	})

	engine := routing.New(store, actionsMgr, testLogger(), action.AppInfo{}, rec, met)
	events := make(chan dispatch.Event, 4)
	engCtx, engCancel := context.WithCancel(context.Background())
	defer engCancel()
	go engine.Run(engCtx, events)
	// Rules are loaded once at Run start; give the refresh a moment.
	time.Sleep(30 * time.Millisecond)

	// Feed the public wire payload through the same strict parsing the
	// receivers use.
	wire := wirePayloadFor(t, store, key)
	we, err := mqttreceiver.ParseEventPayload(wire)
	if err != nil {
		t.Fatalf("ParseEventPayload: %v", err)
	}
	events <- mqttreceiver.EventFromWire(we, "e2e-receiver", time.Now())

	waitFor(t, "notification delivered", func() bool {
		tr, _ := rec.Get(key)
		return tr.Outcome == trail.OutcomeDelivered
	})

	tr, _ := rec.Get(key)
	var steps []string
	for _, s := range tr.Steps {
		steps = append(steps, string(s.Kind)+": "+s.Text)
	}
	joined := strings.Join(steps, " | ")
	for _, want := range []string{
		"matched: matched group ops",
		"route: any → logger-a ≥ moderate",
		"submitted: logger-a action started",
		"delivered",
	} {
		if !strings.Contains(joined, want) {
			t.Errorf("trail missing %q:\n%s", want, joined)
		}
	}
	if out := met.Render(); !strings.Contains(out, `warnflux_notifications_total{action="logger-a",result="delivered"} 1`) {
		t.Errorf("delivery metric missing:\n%s", out)
	}

	// Replay the same wire message: the real SQLite fire ledger must
	// deduplicate it (no second delivery).
	we2, err := mqttreceiver.ParseEventPayload(wire)
	if err != nil {
		t.Fatal(err)
	}
	events <- mqttreceiver.EventFromWire(we2, "e2e-receiver", time.Now())
	waitFor(t, "duplicate deduplicated", func() bool {
		tr, _ := rec.Get(key)
		for _, s := range tr.Steps {
			if s.Kind == trail.StepSkipped && strings.Contains(s.Text, "already delivered") {
				return true
			}
		}
		return false
	})
	if got := actionsMgr.Statuses()[0].Handled; got != 1 {
		t.Errorf("logger handled %d deliveries, want 1 (replay must dedupe)", got)
	}
}

// TestWireSeverityClosure pins the model boundary: provider text never
// reaches routing — non-canonical severity is rejected at wire parse,
// case/space variants are normalized onto the canonical value.
func TestWireSeverityClosure(t *testing.T) {
	payload := func(sev string) []byte {
		return []byte(`{"schema_version":1,"change_id":7,"change_type":"new","event_key":"imgw-meteo:1",` +
			`"event":{"source":"imgw-meteo","source_id":"1","event":"Storm","severity":` +
			jsonQuote(sev) + `,"status":"active","received_at":"2026-09-23T10:00:00Z","updated_at":"2026-09-23T10:00:00Z"}}`)
	}

	if _, err := mqttreceiver.ParseEventPayload(payload("orange")); err == nil {
		t.Fatal("non-canonical severity accepted on the wire")
	}
	if _, err := mqttreceiver.ParseEventPayload(payload("")); err == nil {
		t.Fatal("empty severity accepted on the wire")
	}

	we, err := mqttreceiver.ParseEventPayload(payload("SEVERE"))
	if err != nil {
		t.Fatalf("case-variant severity rejected: %v", err)
	}
	if we.Event.Severity != severity.Severe {
		t.Fatalf("normalized severity = %q, want %q", we.Event.Severity, severity.Severe)
	}

	// The canonical event carries the diagnostic provider value through.
	we.Event.ProviderSeverity = "2"
	ev := mqttreceiver.EventFromWire(we, "r", time.Now())
	if ev.Hazard.Hazard.Severity != severity.Severe || ev.Hazard.Hazard.ProviderSeverity != "2" {
		t.Fatalf("dispatch hazard = %+v", ev.Hazard.Hazard)
	}
}

func jsonQuote(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}
