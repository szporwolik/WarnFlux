package adsb

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/szporwolik/WarnFlux/internal/aprs"
	"github.com/szporwolik/WarnFlux/internal/core"
)

type fakeEmitter struct {
	infos []core.InformationMessage
}

func (f *fakeEmitter) Emit(context.Context, core.HazardEvent) error { return nil }
func (f *fakeEmitter) EmitInformation(_ context.Context, m core.InformationMessage) error {
	f.infos = append(f.infos, m)
	return nil
}

func testHub(t *testing.T) *aprs.Hub {
	t.Helper()
	hub, err := aprs.NewHub(aprs.HubConfig{
		Enabled:    true,
		Callsign:   "SP9MOA-10",
		Icon:       "/j",
		GridSquare: "JO90WW",
		RadiusKM:   45,
		StationTTL: 30 * time.Minute,
	}, slog.New(slog.NewTextHandler(&discardWriter{}, nil)))
	if err != nil {
		t.Fatalf("NewHub: %v", err)
	}
	return hub
}

type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }

func decodeConfig(t *testing.T, text string) *yaml.Node {
	t.Helper()
	var doc yaml.Node
	if err := yaml.Unmarshal([]byte(text), &doc); err != nil {
		t.Fatalf("yaml: %v", err)
	}
	return doc.Content[0]
}

func apiServer(t *testing.T, handler http.HandlerFunc) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return srv
}

func TestNewValidation(t *testing.T) {
	hub := testHub(t)
	if _, err := New(decodeConfig(t, "provider: nope\n"), hub); err == nil {
		t.Error("unknown provider accepted")
	}
	if _, err := New(decodeConfig(t, "poll_interval: 1s\n"), hub); err == nil {
		t.Error("too-short poll_interval accepted")
	}
	if _, err := New(decodeConfig(t, "track_window: 10s\n"), hub); err == nil {
		t.Error("too-short track_window accepted")
	}
	if _, err := New(decodeConfig(t, "radius_km: 2000\n"), hub); err == nil {
		t.Error("out-of-range radius accepted")
	}
	if _, err := New(decodeConfig(t, "latitude: 50.0\n"), hub); err == nil {
		t.Error("half-override accepted")
	}

	src, err := New(decodeConfig(t, "{}"), hub)
	if err != nil {
		t.Fatalf("default config rejected: %v", err)
	}
	s := src.(*Source)
	if s.cfg.Provider != ProviderAdsbLol || s.cfg.RadiusKM != 45 {
		t.Errorf("defaults = provider %q radius %v", s.cfg.Provider, s.cfg.RadiusKM)
	}
	if s.cfg.PollInterval != defaultPollInterval || s.cfg.TrackWindow != defaultTrackWindow {
		t.Errorf("defaults = poll %v track %v", s.cfg.PollInterval, s.cfg.TrackWindow)
	}
}

func TestClientAdsbLol(t *testing.T) {
	srv := apiServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v2/point/50.0200/20.2100/24.3" {
			t.Errorf("path = %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ac":[
  {"hex":"49d418","flight":"TVP7111 ","t":"B38M","alt_baro":14550,"gs":344.9,
   "track":329.12,"baro_rate":-1984,"category":"A3","lat":49.968,"lon":19.859,
   "seen":0.5},
  {"hex":"","lat":1,"lon":1}
],"msg":"No error","now":1790295311003,"total":1}`))
	})

	c := NewClient(ProviderAdsbLol, srv.URL, time.Second)
	targets, now, err := c.Fetch(context.Background(), 50.02, 20.21, 45)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(targets) != 1 {
		t.Fatalf("targets = %d, want 1 (positionless entries skipped)", len(targets))
	}
	a := targets[0]
	if a.Hex != "49d418" || a.Flight != "TVP7111" || a.Category != "A3" {
		t.Errorf("target = %+v", a)
	}
	if a.AltBaroFt != 14550 || a.GSKt != 344.9 || a.TrackDeg != 329.12 {
		t.Errorf("units = %+v", a)
	}
	if now.IsZero() {
		t.Error("provider now timestamp missing")
	}
}

// TestClientAdsbLolStringNumbers pins the tolerance for the provider
// quirk of sending numeric fields as strings (api.adsb.lol does this for
// alt_baro and occasionally other fields), including non-numeric sentinel
// strings like "ground" (0.2.63: the whole snapshot used to be dropped).
func TestClientAdsbLolStringNumbers(t *testing.T) {
	srv := apiServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ac":[
  {"hex":"49d418","flight":"TVP7111 ","alt_baro":"14550","gs":"344.9",
   "track":"329.12","baro_rate":"-1984","lat":"49.968","lon":"19.859",
   "seen":"0.5"},
  {"hex":"abc123","alt_baro":null,"lat":50.1,"lon":20.2,"seen":1},
  {"hex":"def456","alt_baro":"ground","lat":50.15,"lon":20.25,"seen":2}
],"msg":"No error","now":1790295311003,"total":3}`))
	})

	c := NewClient(ProviderAdsbLol, srv.URL, time.Second)
	targets, _, err := c.Fetch(context.Background(), 50.02, 20.21, 45)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(targets) != 3 {
		t.Fatalf("targets = %d, want 3", len(targets))
	}
	a := targets[0]
	if a.AltBaroFt != 14550 || a.GSKt != 344.9 || a.TrackDeg != 329.12 || a.BaroRateFPM != -1984 {
		t.Errorf("units = %+v", a)
	}
	if targets[1].AltBaroFt != 0 || !targets[1].OnGround {
		t.Errorf("null alt_baro must decode as 0 and on-ground: %+v", targets[1])
	}
	if targets[2].AltBaroFt != 0 {
		t.Errorf("sentinel string alt_baro must decode as 0: %+v", targets[2])
	}
}

func TestClientTar1090(t *testing.T) {
	srv := apiServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/data/aircraft.json" {
			t.Errorf("path = %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"aircraft":[
  {"hex":"48ad01","flight":"LOT123","alt_baro":32000,"gs":410,"track":270,
   "lat":50.05,"lon":20.25,"seen":1790295000.0,"category":"A2"},
  {"hex":"","lat":1,"lon":1}
],"now":1790295311.0}`))
	})

	c := NewClient(ProviderTar1090, srv.URL, time.Second)
	targets, _, err := c.Fetch(context.Background(), 50.02, 20.21, 45)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(targets) != 1 || targets[0].Hex != "48ad01" || targets[0].Flight != "LOT123" {
		t.Fatalf("targets = %+v", targets)
	}
}

func TestPollOncePublishesSnapshot(t *testing.T) {
	srv := apiServer(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"ac":[
  {"hex":"49d418","flight":"TVP7111","alt_baro":10000,"gs":300,"track":90,
   "lat":50.03,"lon":20.22,"seen":0.1}
],"msg":"No error","now":1790295311003,"total":1}`))
	})

	hub := testHub(t)
	src, err := New(decodeConfig(t, "latitude: 50.02\nlongitude: 20.21\nradius_km: 45\nbase_url: "+strconvQuote(srv.URL)+"\n"), hub)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	em := &fakeEmitter{}
	n, err := src.(*Source).pollOnce(context.Background(), em)
	if err != nil {
		t.Fatalf("pollOnce: %v", err)
	}
	if n != 1 {
		t.Errorf("published count = %d", n)
	}
	if len(em.infos) != 1 {
		t.Fatalf("infos = %d", len(em.infos))
	}
	m := em.infos[0]
	if m.Source != "adsb" || m.Kind != "aircraft" || m.Key != "area" {
		t.Errorf("envelope = source %q kind %q key %q", m.Source, m.Kind, m.Key)
	}
	var wire wireSnapshot
	if err := json.Unmarshal(m.Payload, &wire); err != nil {
		t.Fatalf("payload invalid: %v", err)
	}
	if wire.Type != "aircraft" || wire.SchemaVersion != 1 {
		t.Errorf("wire header = %+v", wire)
	}
	if len(wire.Aircraft) != 1 {
		t.Fatalf("aircraft = %d", len(wire.Aircraft))
	}
	a := wire.Aircraft[0]
	if a.Icao24 != "49d418" || a.Callsign != "TVP7111" {
		t.Errorf("aircraft = %+v", a)
	}
	if a.AltitudeM == nil || *a.AltitudeM != 10000*0.3048 {
		t.Errorf("altitude = %v", a.AltitudeM)
	}
	if a.SpeedKmh == nil || *a.SpeedKmh != 300*1.852 {
		t.Errorf("speed = %v", a.SpeedKmh)
	}
	if a.TrackDeg == nil || *a.TrackDeg != 90 {
		t.Errorf("track = %v", a.TrackDeg)
	}
	// First poll: no trail yet (one point is not a trail).
	if len(a.Trail) != 0 {
		t.Errorf("first-poll trail = %d points, want 0", len(a.Trail))
	}
}

func TestPollOnceBuildsTrailAndVectorFallback(t *testing.T) {
	var calls int
	srv := apiServer(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		lon := 20.20 + float64(calls)*0.01 // moves east each poll
		_, _ = w.Write([]byte(`{"ac":[
  {"hex":"abc123","lat":50.00,"lon":` + json.Number(fmtF(lon)) + `,"seen":0.1}
],"msg":"No error","now":1790295311003,"total":1}`))
	})

	hub := testHub(t)
	src, err := New(decodeConfig(t, "latitude: 50.00\nlongitude: 20.21\nradius_km: 45\nbase_url: "+strconvQuote(srv.URL)+"\n"), hub)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	s := src.(*Source)
	em := &fakeEmitter{}

	// Drive the clock manually so the two observations are far enough
	// apart for the vector fallback (>= 1s) and the trail.
	clock := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	s.now = func() time.Time { return clock }

	if _, err := s.pollOnce(context.Background(), em); err != nil {
		t.Fatalf("poll 1: %v", err)
	}
	clock = clock.Add(2 * time.Second)
	if _, err := s.pollOnce(context.Background(), em); err != nil {
		t.Fatalf("poll 2: %v", err)
	}

	var wire wireSnapshot
	if err := json.Unmarshal(em.infos[1].Payload, &wire); err != nil {
		t.Fatalf("payload: %v", err)
	}
	if len(wire.Aircraft) != 1 {
		t.Fatalf("aircraft = %d", len(wire.Aircraft))
	}
	a := wire.Aircraft[0]
	// Two distinct positions → a one-segment trail.
	if len(a.Trail) < 2 {
		t.Fatalf("trail = %d points, want >= 2", len(a.Trail))
	}
	// No provider track/gs → fallback vector from the trail.
	if a.TrackDeg == nil || a.SpeedKmh == nil {
		t.Errorf("fallback vector missing: track %v speed %v", a.TrackDeg, a.SpeedKmh)
	}
	if *a.TrackDeg < 0 || *a.TrackDeg >= 360 {
		t.Errorf("fallback track out of range: %v", *a.TrackDeg)
	}
	if *a.SpeedKmh <= 0 {
		t.Errorf("fallback speed = %v", *a.SpeedKmh)
	}
}

func TestTrackerPrune(t *testing.T) {
	tr := newTracker(time.Minute)
	now := time.Now()
	tr.observe("abc123", 50, 20, now.Add(-2*time.Minute))
	tr.observe("abc123", 50.01, 20.01, now)
	tr.observe("old", 50, 20, now.Add(-10*time.Minute))
	tr.prune(now)
	if len(tr.byHex["abc123"]) < 1 {
		t.Error("fresh target was pruned")
	}
	if _, ok := tr.byHex["old"]; ok {
		t.Error("stale target was not pruned")
	}
}

func strconvQuote(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

func fmtF(v float64) string {
	b, _ := json.Marshal(v)
	return string(b)
}

func TestRateLimitHandling(t *testing.T) {
	srv := apiServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "45")
		w.WriteHeader(http.StatusTooManyRequests)
	})

	c := NewClient(ProviderAdsbLol, srv.URL, time.Second)
	_, _, err := c.Fetch(context.Background(), 50, 20, 45)
	rl, ok := err.(*rateLimitError)
	if !ok {
		t.Fatalf("error = %T %v, want rateLimitError", err, err)
	}
	if rl.retryAfter != 45*time.Second {
		t.Errorf("retryAfter = %v, want 45s", rl.retryAfter)
	}
}

func TestParseRetryAfter(t *testing.T) {
	if d := parseRetryAfter("12"); d != 12*time.Second {
		t.Errorf("seconds = %v", d)
	}
	if d := parseRetryAfter("bogus"); d != 30*time.Second {
		t.Errorf("fallback = %v", d)
	}
	if d := parseRetryAfter("10000"); d != 5*time.Minute {
		t.Errorf("cap = %v", d)
	}
	if d := parseRetryAfter("0"); d != 30*time.Second {
		t.Errorf("zero = %v", d)
	}
}
