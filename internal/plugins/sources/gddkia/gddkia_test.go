package gddkia

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/szporwolik/WarnFlux/internal/aprs"
	"github.com/szporwolik/WarnFlux/internal/core"
)

type fakeEmitter struct {
	events []core.HazardEvent
}

func (f *fakeEmitter) Emit(_ context.Context, ev core.HazardEvent) error {
	f.events = append(f.events, ev)
	return nil
}

func (f *fakeEmitter) EmitInformation(context.Context, core.InformationMessage) error {
	return nil
}

func testHub(t *testing.T) *aprs.Hub {
	t.Helper()
	lat, lon := 50.0212, 20.2075
	h, err := aprs.NewHub(aprs.HubConfig{
		Enabled:    true,
		Callsign:   "SP9TST-10",
		GridSquare: "KO00BA",
		Latitude:   &lat,
		Longitude:  &lon,
		RadiusKM:   30,
	}, nil)
	if err != nil {
		t.Fatalf("NewHub: %v", err)
	}
	return h
}

// decodeConfig decodes a YAML fragment into the mapping node the plugin
// framework hands to New.
func decodeConfig(t *testing.T, text string) *yaml.Node {
	t.Helper()
	var doc yaml.Node
	if err := yaml.Unmarshal([]byte(text), &doc); err != nil {
		t.Fatalf("yaml: %v", err)
	}
	return doc.Content[0]
}

func TestNewValidation(t *testing.T) {
	hub := testHub(t)

	if _, err := New(nil, nil); err == nil {
		t.Error("nil hub accepted")
	}
	if _, err := New(decodeConfig(t, "base_url: not a url\n"), hub); err == nil {
		t.Error("malformed base_url accepted")
	}
	if _, err := New(decodeConfig(t, "poll_interval: 10s\n"), hub); err == nil {
		t.Error("too-short poll_interval accepted")
	}
	if _, err := New(decodeConfig(t, "center_latitude: 50.0\n"), hub); err == nil {
		t.Error("center_latitude without center_longitude accepted")
	}

	src, err := New(decodeConfig(t, "poll_interval: 15m\nrequest_timeout: 10s\n"), hub)
	if err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	s := src.(*Source)
	if s.radiusKM != 30 || s.centerLat != 50.0212 || s.centerLon != 20.2075 {
		t.Errorf("hub defaults not applied: radius=%v center=(%v,%v)", s.radiusKM, s.centerLat, s.centerLon)
	}

	ovr, err := New(decodeConfig(t, "radius_km: 10\ncenter_latitude: 50.0\ncenter_longitude: 20.0\n"), hub)
	if err != nil {
		t.Fatalf("override config rejected: %v", err)
	}
	os := ovr.(*Source)
	if os.radiusKM != 10 || os.centerLat != 50.0 || os.centerLon != 20.0 {
		t.Errorf("overrides not applied: %+v", os)
	}
}

func TestPollOnceFiltersToArea(t *testing.T) {
	srv := testServer(t)
	hub := testHub(t)
	src, err := New(decodeConfig(t, "base_url: \""+srv.URL+"/dane/zima_html/utrdane.xml\"\n"), hub)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	em := &fakeEmitter{}
	if err := src.(*Source).pollOnce(context.Background(), em); err != nil {
		t.Fatalf("pollOnce: %v", err)
	}
	// Fixture: 94g and 75 and A4 inside 30 km; A1 (Pomerania) filtered.
	if len(em.events) != 3 {
		t.Fatalf("emitted %d events, want 3", len(em.events))
	}
	keys := make(map[string]bool, len(em.events))
	for _, e := range em.events {
		if e.Source != "gddkia" {
			t.Errorf("event source = %q", e.Source)
		}
		if e.Latitude == nil || e.Longitude == nil {
			t.Errorf("event %s has no coordinates", e.Key())
		}
		keys[e.Key()] = true
	}
	if len(keys) != 3 {
		t.Errorf("duplicate identities: %d events, %d keys", len(em.events), len(keys))
	}
}

type logBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *logBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *logBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func waitForCondition(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met within timeout")
}

// TestOutageIsLoggedAsOneEpisode proves a dead upstream cannot bury the log:
// the first failed poll warns, the following ones are debug lines, and the
// recovery reports the tally. The retired GDDKiA feed produced one warning
// every 15 minutes (96 a day) before this.
func TestOutageIsLoggedAsOneEpisode(t *testing.T) {
	var mu sync.Mutex
	polls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		polls++
		n := polls
		mu.Unlock()
		if n <= 2 {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(fixtureXML))
	}))
	t.Cleanup(srv.Close)

	hub := testHub(t)
	src, err := New(decodeConfig(t, "base_url: \""+srv.URL+"/dane/zima_html/utrdane.xml\"\n"), hub)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	s := src.(*Source)
	s.cfg.PollInterval = 20 * time.Millisecond

	logs := &logBuffer{}
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer slog.SetDefault(previous)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx, &fakeEmitter{}) }()

	waitForCondition(t, 5*time.Second, func() bool {
		return strings.Contains(logs.String(), "gddkia poll recovered")
	})
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop after its context was cancelled")
	}

	out := logs.String()
	if got := strings.Count(out, "gddkia poll failed"); got != 1 {
		t.Errorf("warning lines = %d, want 1 (one warning per outage episode):\n%s", got, out)
	}
	if !strings.Contains(out, "gddkia poll still failing") {
		t.Errorf("retries were not reported as debug lines:\n%s", out)
	}
	if !strings.Contains(out, "failed_attempts=2") {
		t.Errorf("recovery did not report the failure tally:\n%s", out)
	}
}
