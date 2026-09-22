package openmeteo

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/plugin"
)

// fakeReporter records source-health reports.
type fakeReporter struct {
	mu       sync.Mutex
	healthy  int
	degraded []error
}

func (r *fakeReporter) ReportSourceHealthy() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.healthy++
}

func (r *fakeReporter) ReportSourceDegraded(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.degraded = append(r.degraded, err)
}

func (r *fakeReporter) stats() (int, int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.healthy, len(r.degraded)
}

// atomicBool is a tiny test helper.
type atomicBool struct{ v atomic.Bool }

func (b *atomicBool) Load() bool   { return b.v.Load() }
func (b *atomicBool) Store(x bool) { b.v.Store(x) }

// TestPollOnceSourceHealth: all-failed polls report degraded; any success
// reports healthy; a later successful poll recovers the state; partial
// failure still reports healthy (individual failures are logged).
func TestPollOnceSourceHealth(t *testing.T) {
	var healthyMode atomicBool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if healthyMode.Load() {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, validResponseJSON())
			return
		}
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()

	s := testSource(srv.URL, []Location{{ID: "home", Latitude: 50, Longitude: 20}}, time.Minute)
	emit := &recordingEmitter{}
	rep := &fakeReporter{}
	emitFn := plugin.EmitterFunc{EmitFn: emit.Emit, EmitInformationFn: emit.EmitInformation}

	// All locations fail → degraded, schedule unchanged.
	if next := s.pollOnce(context.Background(), emitFn, rep); next != time.Minute {
		t.Errorf("next = %v, want the configured interval (no Retry-After)", next)
	}
	if h, d := rep.stats(); h != 0 || d != 1 {
		t.Errorf("health = (healthy %d, degraded %d), want (0, 1)", h, d)
	}

	// Provider recovers → healthy.
	healthyMode.Store(true)
	s.pollOnce(context.Background(), emitFn, rep)
	if h, d := rep.stats(); h != 1 || d != 1 {
		t.Errorf("health = (healthy %d, degraded %d), want (1, 1)", h, d)
	}

	// Partial failure (cabin down): healthy, only home publishes.
	partial := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.RawQuery, "latitude=51") {
			http.Error(w, "boom", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, validResponseJSON())
	}))
	defer partial.Close()
	s2 := testSource(partial.URL, []Location{
		{ID: "home", Latitude: 50, Longitude: 20},
		{ID: "cabin", Latitude: 51, Longitude: 22},
	}, time.Minute)
	emit2 := &recordingEmitter{}
	s2.pollOnce(context.Background(), plugin.EmitterFunc{EmitFn: emit2.Emit, EmitInformationFn: emit2.EmitInformation}, rep)
	if h, d := rep.stats(); h != 2 || d != 1 {
		t.Errorf("health = (healthy %d, degraded %d), want (2, 1)", h, d)
	}
	if emit2.count() != 1 {
		t.Errorf("published = %d, want 1 (home only)", emit2.count())
	}
}

// TestPollOnceRetryAfterExtendsNextPoll: a rate-limited location lengthens
// the NEXT whole poll without blocking the other locations in this poll;
// a plain 500 does not change the schedule (no hot loop).
func TestPollOnceRetryAfterExtendsNextPoll(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if strings.Contains(r.URL.RawQuery, "latitude=51") {
			w.Header().Set("Retry-After", "120")
			http.Error(w, "slow down", http.StatusTooManyRequests)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, validResponseJSON())
	}))
	defer srv.Close()

	s := testSource(srv.URL, []Location{
		{ID: "home", Latitude: 50, Longitude: 20},
		{ID: "cabin", Latitude: 51, Longitude: 22},
	}, time.Minute)
	emit := &recordingEmitter{}
	emitFn := plugin.EmitterFunc{EmitFn: emit.Emit, EmitInformationFn: emit.EmitInformation}

	next := s.pollOnce(context.Background(), emitFn, nil)
	if next != 2*time.Minute {
		t.Errorf("next = %v, want 2m (Retry-After 120s extends the 1m interval)", next)
	}
	if got := calls.Load(); got != 2 {
		t.Errorf("requests = %d, want 2 (both locations fetched in the same poll)", got)
	}

	srv500 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv500.Close()
	s2 := testSource(srv500.URL, []Location{{ID: "home", Latitude: 50, Longitude: 20}}, 30*time.Second)
	if next2 := s2.pollOnce(context.Background(), emitFn, nil); next2 != 30*time.Second {
		t.Errorf("next after 500 = %v, want the configured interval", next2)
	}
}
