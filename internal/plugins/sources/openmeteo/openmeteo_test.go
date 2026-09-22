package openmeteo

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
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

func locationYAML(id, lat, lon string) string {
	return fmt.Sprintf("poll_interval: 15m\nlocations:\n  - id: %s\n    latitude: %s\n    longitude: %s\n", id, lat, lon)
}

func TestNewDefaults(t *testing.T) {
	p, err := New(decodeConfig(t, locationYAML("home", "50.0", "20.0")))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	s := p.(*Source)
	if s.cfg.PollInterval != defaultPollInterval {
		t.Errorf("poll_interval = %v, want %v", s.cfg.PollInterval, defaultPollInterval)
	}
	if s.cfg.RequestTimeout != defaultRequestTimeout {
		t.Errorf("request_timeout = %v, want %v", s.cfg.RequestTimeout, defaultRequestTimeout)
	}
	if s.cfg.ForecastHours != 48 || s.cfg.ForecastDays != 7 {
		t.Errorf("forecast = (%d, %d), want (48, 7)", s.cfg.ForecastHours, s.cfg.ForecastDays)
	}
	if s.client.baseURL != defaultBaseURL {
		t.Errorf("base URL = %q, want public endpoint", s.client.baseURL)
	}
}

func TestNewValidation(t *testing.T) {
	cases := []struct {
		name string
		yaml string
	}{
		{"missing locations", "poll_interval: 15m"},
		{"invalid latitude", locationYAML("home", "91", "20")},
		{"invalid longitude", locationYAML("home", "50", "181")},
		{"duplicate location ids", "poll_interval: 15m\nlocations:\n  - {id: home, latitude: 50, longitude: 20}\n  - {id: home, latitude: 51, longitude: 21}"},
		{"invalid location slug", locationYAML("Home", "50", "20")},
		{"location slug with slash", locationYAML("ho/me", "50", "20")},
		{"poll too short", "poll_interval: 30s\nlocations:\n  - {id: home, latitude: 50, longitude: 20}"},
		{"poll too long", "poll_interval: 25h\nlocations:\n  - {id: home, latitude: 50, longitude: 20}"},
		{"forecast_hours too small", "forecast_hours: -1\nlocations:\n  - {id: home, latitude: 50, longitude: 20}"},
		{"forecast_hours too large", "forecast_hours: 169\nlocations:\n  - {id: home, latitude: 50, longitude: 20}"},
		{"forecast_days too small", "forecast_days: -1\nlocations:\n  - {id: home, latitude: 50, longitude: 20}"},
		{"forecast_days too large", "forecast_days: 17\nlocations:\n  - {id: home, latitude: 50, longitude: 20}"},
		{"api_key and api_key_file", "api_key: k\napi_key_file: f\nlocations:\n  - {id: home, latitude: 50, longitude: 20}"},
		{"unknown yaml field", "unknown_key: 1\nlocations:\n  - {id: home, latitude: 50, longitude: 20}"},
	}
	for _, c := range cases {
		if _, err := New(decodeConfig(t, c.yaml)); err == nil {
			t.Errorf("%s: expected rejection, got nil", c.name)
		}
	}
}

func TestAPIKeySelectsCustomerEndpoint(t *testing.T) {
	p, err := New(decodeConfig(t, "api_key: secret\nlocations:\n  - {id: home, latitude: 50, longitude: 20}"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	s := p.(*Source)
	if s.client.baseURL != customerBaseURL {
		t.Errorf("base URL = %q, want customer endpoint", s.client.baseURL)
	}
	if s.key != "secret" {
		t.Error("resolved key mismatch")
	}
}

func TestBaseURLOverrideWins(t *testing.T) {
	p, err := New(decodeConfig(t, "base_url: http://example.test/forecast\napi_key: k\nlocations:\n  - {id: home, latitude: 50, longitude: 20}"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if p.(*Source).client.baseURL != "http://example.test/forecast" {
		t.Errorf("custom base URL was not respected")
	}
}

func TestAPIKeyFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "key")
	if err := os.WriteFile(path, []byte("file-key\r\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	p, err := New(decodeConfig(t, fmt.Sprintf("api_key_file: %s\nlocations:\n  - {id: home, latitude: 50, longitude: 20}", path)))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if got := p.(*Source).key; got != "file-key" {
		t.Errorf("key = %q, want trimmed file content", got)
	}
}

// ---- Run behavior ----

type recordingEmitter struct {
	mu       sync.Mutex
	hazards  int
	messages []core.InformationMessage
}

func (e *recordingEmitter) Emit(context.Context, core.HazardEvent) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.hazards++
	return nil
}

func (e *recordingEmitter) EmitInformation(_ context.Context, m core.InformationMessage) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.messages = append(e.messages, m)
	return nil
}

func (e *recordingEmitter) count() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.messages)
}

func (e *recordingEmitter) snapshot() (int, []core.InformationMessage) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.hazards, append([]core.InformationMessage(nil), e.messages...)
}

func testSource(serverURL string, locations []Location, interval time.Duration) *Source {
	return &Source{
		cfg: Config{
			PollInterval:   interval,
			RequestTimeout: 5 * time.Second,
			ForecastHours:  48,
			ForecastDays:   7,
			Locations:      locations,
		},
		key:    "",
		client: NewClient(serverURL, "", 5*time.Second),
		now:    time.Now,
	}
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
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

func weatherHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, validResponseJSON())
	}
}

func TestRunFetchesImmediatelyAndPublishes(t *testing.T) {
	srv := httptest.NewServer(weatherHandler())
	defer srv.Close()

	s := testSource(srv.URL, []Location{{ID: "home", Latitude: 50, Longitude: 20}}, time.Hour)
	emit := &recordingEmitter{}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := s.Run(ctx, plugin.EmitterFunc{
			EmitFn:            emit.Emit,
			EmitInformationFn: emit.EmitInformation,
		}); err != nil {
			t.Errorf("Run: %v", err)
		}
	}()

	// Immediate fetch: one message within a short window, long before the
	// one-hour poll interval would fire.
	waitFor(t, 2*time.Second, func() bool { return emit.count() >= 1 })
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not terminate after cancellation")
	}

	hazards, messages := emit.snapshot()
	if hazards != 0 {
		t.Fatalf("Run emitted %d HazardEvents — weather must never become a hazard", hazards)
	}
	if len(messages) == 0 {
		t.Fatal("no weather message published")
	}
	m := messages[0]
	if m.Source != "openmeteo" || m.Key != "home" || m.Kind != "weather" {
		t.Errorf("message identity = (%s, %s, %s), want (openmeteo, home, weather)", m.Source, m.Key, m.Kind)
	}
}

func TestRunTransientFailureThenSuccess(t *testing.T) {
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			http.Error(w, "boom", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, validResponseJSON())
	}))
	defer srv.Close()

	s := testSource(srv.URL, []Location{{ID: "home", Latitude: 50, Longitude: 20}}, 20*time.Millisecond)
	emit := &recordingEmitter{}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = s.Run(ctx, plugin.EmitterFunc{
			EmitFn:            emit.Emit,
			EmitInformationFn: emit.EmitInformation,
		})
	}()

	// After a 500, the source must stay alive and publish on a later poll.
	waitFor(t, 3*time.Second, func() bool { return emit.count() >= 1 })
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not terminate")
	}
}

func TestRunPartialLocationFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.RawQuery, "latitude=51") { // cabin fails
			http.Error(w, "boom", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, validResponseJSON())
	}))
	defer srv.Close()

	locations := []Location{
		{ID: "home", Latitude: 50, Longitude: 20},
		{ID: "office", Latitude: 52, Longitude: 21},
		{ID: "cabin", Latitude: 51, Longitude: 22},
	}
	s := testSource(srv.URL, locations, time.Hour)
	emit := &recordingEmitter{}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = s.Run(ctx, plugin.EmitterFunc{
			EmitFn:            emit.Emit,
			EmitInformationFn: emit.EmitInformation,
		})
	}()

	waitFor(t, 2*time.Second, func() bool { return emit.count() >= 2 })
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not terminate")
	}

	_, messages := emit.snapshot()
	keys := map[string]bool{}
	for _, m := range messages {
		keys[m.Key] = true
	}
	if !keys["home"] || !keys["office"] {
		t.Errorf("published keys = %v, want home and office", keys)
	}
	if keys["cabin"] {
		t.Error("cabin must not be published after a provider failure")
	}
}

func TestRunCancellationDuringInFlightRequest(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-release // block until the test releases (simulates a slow provider)
		http.Error(w, "late", http.StatusInternalServerError)
	}))
	defer srv.Close()

	s := testSource(srv.URL, []Location{{ID: "home", Latitude: 50, Longitude: 20}}, time.Hour)
	emit := &recordingEmitter{}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = s.Run(ctx, plugin.EmitterFunc{
			EmitFn:            emit.Emit,
			EmitInformationFn: emit.EmitInformation,
		})
	}()

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("request never started")
	}
	// Cancel while the HTTP request is in flight; the client must respect
	// the request context and Run must return promptly.
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not terminate while the request was in flight")
	}
	close(release)
}
