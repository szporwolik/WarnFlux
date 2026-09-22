package imgw

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/szporwolik/WarnFlux/internal/core"
	"github.com/szporwolik/WarnFlux/internal/plugin"
)

// fakeEmitter implements every capability the imgw source may use and
// records what it received.
type fakeEmitter struct {
	mu         sync.Mutex
	emitted    []core.HazardEvent
	active     []core.HazardEvent
	healthy    int
	degraded   []error
	emitErr    error
	readerOnly bool // when true, ListSourceActiveEvents is absent
}

var _ plugin.Emitter = (*fakeEmitter)(nil)

func (f *fakeEmitter) Emit(_ context.Context, ev core.HazardEvent) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.emitted = append(f.emitted, ev)
	return f.emitErr
}

func (f *fakeEmitter) EmitInformation(context.Context, core.InformationMessage) error { return nil }

func (f *fakeEmitter) ListSourceActiveEvents(_ context.Context, source string) ([]core.HazardEvent, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []core.HazardEvent
	for _, ev := range f.active {
		if ev.Source == source {
			out = append(out, ev)
		}
	}
	return out, nil
}

func (f *fakeEmitter) ReportSourceHealthy() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.healthy++
}

func (f *fakeEmitter) ReportSourceDegraded(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.degraded = append(f.degraded, err)
}

func (f *fakeEmitter) emittedStatuses(status core.EventStatus) []core.HazardEvent {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []core.HazardEvent
	for _, ev := range f.emitted {
		if ev.Status == status {
			out = append(out, ev)
		}
	}
	return out
}

func (f *fakeEmitter) cancelledKeys() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for _, ev := range f.emitted {
		if ev.Status == core.StatusCancelled {
			out = append(out, ev.Key())
		}
	}
	return out
}

func (f *fakeEmitter) health() (int, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.healthy, len(f.degraded)
}

// noReaderEmitter implements plugin.Emitter but deliberately lacks
// SourceActiveEventReader (embedding would promote the reader method too).
type noReaderEmitter struct{}

func (f *noReaderEmitter) Emit(context.Context, core.HazardEvent) error { return nil }
func (f *noReaderEmitter) EmitInformation(context.Context, core.InformationMessage) error {
	return nil
}

// testSource builds a Source against an httptest server.
func testSource(t *testing.T, baseURL string, feeds ...string) *Source {
	t.Helper()
	return &Source{
		cfg: Config{
			PollInterval:   50 * time.Millisecond,
			RequestTimeout: 2 * time.Second,
			BaseURL:        baseURL,
			Feeds:          feeds,
		},
		client: NewClient(baseURL, 2*time.Second),
	}
}

func meteoEvent(id string, expires *time.Time) core.HazardEvent {
	ev := core.HazardEvent{
		Source:    sourceMeteo,
		SourceID:  id,
		Category:  "met",
		Event:     "Burze",
		Severity:  "severe",
		Headline:  "Burze",
		ExpiresAt: expires,
		Status:    core.StatusActive,
	}
	ev.Normalize()
	return ev
}

// TestSnapshotReconciliation: active A/B/C vs snapshot A/C/D → D new, B
// cancelled (future expiry); an already-expired disappeared event is left
// to the expiration worker.
func TestSnapshotReconciliation(t *testing.T) {
	future := time.Now().Add(time.Hour)
	past := time.Now().Add(-time.Hour)
	em := &fakeEmitter{active: []core.HazardEvent{
		meteoEvent("A", &future),
		meteoEvent("B", &future),
		meteoEvent("E", &past),
	}}

	// Exact key control: snapshot contains A, C and D.
	keys := map[string]bool{
		(sourceMeteo + ":A"): true,
		(sourceMeteo + ":C"): true,
		(sourceMeteo + ":D"): true,
	}
	cancelled, err := reconcileSnapshots(context.Background(), em, sourceMeteo, keys, time.Now())
	if err != nil {
		t.Fatalf("reconcileSnapshots: %v", err)
	}
	if cancelled != 1 {
		t.Errorf("cancelled = %d, want 1 (B only; E is expired)", cancelled)
	}
	got := em.cancelledKeys()
	if len(got) != 1 || got[0] != sourceMeteo+":B" {
		t.Errorf("cancelled keys = %v, want only imgw-meteo:B", got)
	}
	// The cancellation preserves the original event content.
	c := em.emittedStatuses(core.StatusCancelled)[0]
	if c.Event != "Burze" || c.SourceID != "B" {
		t.Errorf("cancellation lost the original event: %+v", c)
	}
}

// TestReconciliationAcrossProcessRestart: a non-expiring drought warning
// that vanished upstream is cancelled even though this "process" has no
// plugin-local memory of it — only SQLite state matters.
func TestReconciliationAcrossProcessRestart(t *testing.T) {
	drought := core.HazardEvent{
		Source:   sourceHydro,
		SourceID: "hydro:abc",
		Category: "met",
		Event:    "Susza hydrologiczna",
		Severity: "unknown",
		Headline: "Susza hydrologiczna",
		Status:   core.StatusActive,
	}
	drought.Normalize()
	em := &fakeEmitter{active: []core.HazardEvent{drought}}

	cancelled, err := reconcileSnapshots(context.Background(), em, sourceHydro, map[string]bool{}, time.Now())
	if err != nil {
		t.Fatalf("reconcileSnapshots: %v", err)
	}
	if cancelled != 1 {
		t.Errorf("cancelled = %d, want 1 (withdrawn non-expiring drought)", cancelled)
	}
	if got := em.cancelledKeys(); len(got) != 1 || got[0] != drought.Key() {
		t.Errorf("cancelled keys = %v, want [%s]", got, drought.Key())
	}
}

// TestFailedFetchDoesNotCancel: a provider outage must never look like a
// mass cancellation.
func TestFailedFetchDoesNotCancel(t *testing.T) {
	future := time.Now().Add(time.Hour)
	em := &fakeEmitter{active: []core.HazardEvent{meteoEvent("A", &future), meteoEvent("B", &future), meteoEvent("C", &future)}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()

	s := testSource(t, srv.URL, feedMeteo)
	s.pollOnce(context.Background(), em, em)
	if got := em.cancelledKeys(); len(got) != 0 {
		t.Fatalf("provider outage cancelled warnings: %v", got)
	}
	if h, d := em.health(); h != 0 || d != 1 {
		t.Errorf("health = (healthy %d, degraded %d), want (0, 1)", h, d)
	}
}

// TestEmptySnapshotCancels: a successful empty snapshot withdraws every
// non-expired current warning; expired ones stay with the expiration
// worker. The documented 404 no-products payload behaves identically.
func TestEmptySnapshotCancels(t *testing.T) {
	for name, handler := range map[string]http.HandlerFunc{
		"empty array": func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, `[]`) },
		"404 no products": func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, `{"status":false,"message":"No products were found"}`)
		},
	} {
		t.Run(name, func(t *testing.T) {
			future := time.Now().Add(time.Hour)
			past := time.Now().Add(-time.Hour)
			em := &fakeEmitter{active: []core.HazardEvent{
				meteoEvent("A", &future),
				meteoEvent("B", &future),
				meteoEvent("E", &past),
			}}
			srv := httptest.NewServer(handler)
			defer srv.Close()

			s := testSource(t, srv.URL, feedMeteo)
			s.pollOnce(context.Background(), em, em)
			got := em.cancelledKeys()
			if len(got) != 2 {
				t.Fatalf("cancelled keys = %v, want exactly A and B (E stays for expiration)", got)
			}
			seen := map[string]bool{got[0]: true, got[1]: true}
			if !seen[sourceMeteo+":A"] || !seen[sourceMeteo+":B"] {
				t.Errorf("cancelled keys = %v, want A and B", got)
			}
			if seen[sourceMeteo+":E"] {
				t.Error("expired event was cancelled by reconciliation")
			}
			if h, d := em.health(); h != 1 || d != 0 {
				t.Errorf("health = (%d, %d), want (1, 0) for a successful empty snapshot", h, d)
			}
		})
	}
}

// TestMalformedItemSafety: one bad item makes the snapshot incomplete —
// valid items may still be emitted, but NO disappearance reconciliation
// runs (a bad item must not look like a withdrawal).
func TestMalformedItemSafety(t *testing.T) {
	future := time.Now().Add(time.Hour)
	em := &fakeEmitter{active: []core.HazardEvent{meteoEvent("Ghost", &future)}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `[
			{"id":"Good1","nazwa_zdarzenia":"Burze","stopien":"2","obowiazuje_od":"2026-09-21 18:00:00","obowiazuje_do":"2026-09-22 06:00:00","prawdopodobienstwo":"70","teryt":["1217"],"tresc":"x","komentarz":""},
			{"id":"Bad1","nazwa_zdarzenia":"Burze","stopien":"2","obowiazuje_od":"not-a-time","obowiazuje_do":"2026-09-22 06:00:00","teryt":["1217"],"tresc":"x"}
		]`)
	}))
	defer srv.Close()

	s := testSource(t, srv.URL, feedMeteo)
	s.pollOnce(context.Background(), em, em)
	if got := em.cancelledKeys(); len(got) != 0 {
		t.Fatalf("malformed snapshot cancelled warnings: %v", got)
	}
	emittedActive := 0
	em.mu.Lock()
	for _, ev := range em.emitted {
		if ev.Status == core.StatusActive {
			emittedActive++
		}
	}
	em.mu.Unlock()
	if emittedActive != 1 {
		t.Errorf("valid items emitted = %d, want 1", emittedActive)
	}
	if h, d := em.health(); h != 0 || d != 1 {
		t.Errorf("health = (%d, %d), want degraded (incomplete snapshot)", h, d)
	}
}

// TestDuplicateIdentitySafety: duplicate logical IDs make the snapshot
// incomplete (no reconciliation).
func TestDuplicateIdentitySafety(t *testing.T) {
	future := time.Now().Add(time.Hour)
	em := &fakeEmitter{active: []core.HazardEvent{meteoEvent("Dup", &future)}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `[
			{"id":"Dup","nazwa_zdarzenia":"Burze","stopien":"2","obowiazuje_od":"2026-09-21 18:00:00","obowiazuje_do":"2026-09-22 06:00:00","teryt":["1217"],"tresc":"x"},
			{"id":"Dup","nazwa_zdarzenia":"Burze","stopien":"2","obowiazuje_od":"2026-09-21 18:00:00","obowiazuje_do":"2026-09-22 06:00:00","teryt":["1217"],"tresc":"x"}
		]`)
	}))
	defer srv.Close()

	s := testSource(t, srv.URL, feedMeteo)
	s.pollOnce(context.Background(), em, em)
	if got := em.cancelledKeys(); len(got) != 0 {
		t.Fatalf("duplicate-identity snapshot cancelled warnings: %v", got)
	}
	if h, d := em.health(); h != 0 || d != 1 {
		t.Errorf("health = (%d, %d), want degraded", h, d)
	}
}

// TestFeedIndependence: a failing meteo feed does not suppress hydro
// processing; health is degraded; meteo events are never reconciled.
func TestFeedIndependence(t *testing.T) {
	future := time.Now().Add(time.Hour)
	meteoActive := meteoEvent("M-A", &future)
	em := &fakeEmitter{active: []core.HazardEvent{meteoActive}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/warningsmeteo" {
			http.Error(w, "boom", http.StatusInternalServerError)
			return
		}
		fmt.Fprint(w, hydroDroughtFixture)
	}))
	defer srv.Close()

	s := testSource(t, srv.URL, feedMeteo, feedHydro)
	s.pollOnce(context.Background(), em, em)
	// Hydro drought was ingested.
	em.mu.Lock()
	found := false
	for _, ev := range em.emitted {
		if ev.Source == sourceHydro && ev.Status == core.StatusActive {
			found = true
		}
	}
	em.mu.Unlock()
	if !found {
		t.Error("hydro feed was not processed while meteo failed")
	}
	// No imgw-meteo cancellation happened (failed feed ⇒ no reconciliation).
	for _, k := range em.cancelledKeys() {
		if k == meteoActive.Key() {
			t.Fatal("failed meteo feed cancelled a meteo warning")
		}
	}
	if h, d := em.health(); h != 0 || d != 1 {
		t.Errorf("health = (%d, %d), want degraded", h, d)
	}
}

// TestRunImmediatePollAndCancellation: Run polls immediately and returns
// promptly on cancellation.
func TestRunImmediatePollAndCancellation(t *testing.T) {
	em := &fakeEmitter{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, meteoFixture)
	}))
	defer srv.Close()

	s := testSource(t, srv.URL, feedMeteo)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := s.Run(ctx, em); err != nil {
			t.Errorf("Run: %v", err)
		}
	}()
	waitFor(t, 3*time.Second, func() bool {
		em.mu.Lock()
		defer em.mu.Unlock()
		return len(em.emitted) > 0
	})
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop after cancellation")
	}
}

// TestRunRequiresReaderCapability: without SourceActiveEventReader the
// source refuses to run (silent stale-drought risk is not acceptable).
func TestRunRequiresReaderCapability(t *testing.T) {
	s := testSource(t, "http://127.0.0.1:1", feedMeteo)
	if err := s.Run(context.Background(), &noReaderEmitter{}); err == nil {
		t.Fatal("Run must fail when the emitter lacks the active-event reader")
	}
}

// TestConfigValidation covers defaults, bounds, feeds and base URL rules.
func TestConfigValidation(t *testing.T) {
	if _, err := New(nil); err != nil {
		t.Fatalf("default config must be accepted: %v", err)
	}
	p, err := New(nil)
	if err != nil {
		t.Fatal(err)
	}
	s := p.(*Source)
	if s.cfg.PollInterval != defaultPollInterval || s.cfg.RequestTimeout != defaultRequestTimeout {
		t.Errorf("defaults = %s/%s", s.cfg.PollInterval, s.cfg.RequestTimeout)
	}
	if len(s.cfg.Feeds) != 2 || s.cfg.Feeds[0] != feedMeteo || s.cfg.Feeds[1] != feedHydro {
		t.Errorf("default feeds = %v, want [meteo hydro]", s.cfg.Feeds)
	}
	if s.cfg.BaseURL != defaultBaseURL {
		t.Errorf("default base url = %q", s.cfg.BaseURL)
	}

	cases := []string{
		"feeds: [bad]\n",
		"feeds: [meteo, meteo]\n",
		"poll_interval: 30s\n",
		"poll_interval: 2h\n",
		"request_timeout: -5s\n",
		"request_timeout: 2m\n",
		"base_url: not-a-url\n",
	}
	for _, c := range cases {
		var node yaml.Node
		if err := yaml.Unmarshal([]byte(c), &node); err != nil {
			t.Fatalf("yaml %q: %v", c, err)
		}
		if _, err := New(&node); err == nil {
			t.Errorf("config %q must be rejected", strings.TrimSpace(c))
		}
	}
}

func waitFor(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met in time")
}

// TestHTTP200TopLevelShapeSafety: a successful HTTP response whose
// top-level JSON value is not an array must NOT trigger cancellation.
func TestHTTP200TopLevelShapeSafety(t *testing.T) {
	for _, body := range []string{"null", "{}", "false", "123", `"foo"`} {
		t.Run(body, func(t *testing.T) {
			future := time.Now().Add(time.Hour)
			em := &fakeEmitter{active: []core.HazardEvent{
				meteoEvent("A", &future), meteoEvent("B", &future), meteoEvent("C", &future),
			}}
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				fmt.Fprint(w, body)
			}))
			defer srv.Close()

			s := testSource(t, srv.URL, feedMeteo)
			s.pollOnce(context.Background(), em, em)
			if got := em.cancelledKeys(); len(got) != 0 {
				t.Fatalf("top-level %q caused cancellations: %v", body, got)
			}
			if h, d := em.health(); h != 0 || d != 1 {
				t.Errorf("health = (%d, %d), want degraded", h, d)
			}
		})
	}
}

// TestDuplicateIdentityNotEmitted: an ambiguous duplicate identity is never
// arbitrarily emitted (neither occurrence), unique records beside it are
// emitted, and no disappearance reconciliation runs.
func TestDuplicateIdentityNotEmitted(t *testing.T) {
	future := time.Now().Add(time.Hour)
	em := &fakeEmitter{active: []core.HazardEvent{meteoEvent("Ghost", &future)}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `[
			{"id":"Dup","nazwa_zdarzenia":"Burze","stopien":"2","obowiazuje_od":"2026-09-21 18:00:00","obowiazuje_do":"2026-09-22 06:00:00","teryt":["1217"],"tresc":"x","prawdopodobienstwo":"70"},
			{"id":"Dup","nazwa_zdarzenia":"Burze","stopien":"3","obowiazuje_od":"2026-09-21 18:00:00","obowiazuje_do":"2026-09-22 06:00:00","teryt":["1217"],"tresc":"x","prawdopodobienstwo":"90"},
			{"id":"Good","nazwa_zdarzenia":"Upał","stopien":"1","obowiazuje_od":"2026-09-21 18:00:00","obowiazuje_do":"2026-09-22 06:00:00","teryt":["1261"],"tresc":"x"}
		]`)
	}))
	defer srv.Close()

	s := testSource(t, srv.URL, feedMeteo)
	s.pollOnce(context.Background(), em, em)

	em.mu.Lock()
	var emittedIDs []string
	for _, ev := range em.emitted {
		if ev.Status == core.StatusActive {
			emittedIDs = append(emittedIDs, ev.SourceID)
		}
	}
	em.mu.Unlock()
	if len(emittedIDs) != 1 || emittedIDs[0] != "Good" {
		t.Fatalf("emitted IDs = %v, want only Good (neither duplicate emitted)", emittedIDs)
	}
	if got := em.cancelledKeys(); len(got) != 0 {
		t.Fatalf("duplicate snapshot cancelled warnings: %v", got)
	}
	if h, d := em.health(); h != 0 || d != 1 {
		t.Errorf("health = (%d, %d), want degraded", h, d)
	}
}
