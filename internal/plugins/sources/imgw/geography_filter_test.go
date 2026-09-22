package imgw

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/core"
)

// geoSource builds a Source with the target geography enabled against an
// httptest server.
func geoSource(t *testing.T, baseURL string) *Source {
	t.Helper()
	g, err := buildGeography(&GeographyConfig{Enabled: true, Include: []string{
		"gmina:niepolomice", "miasto:krakow", "powiat:wielicki", "powiat:bochenski",
	}})
	if err != nil {
		t.Fatalf("buildGeography: %v", err)
	}
	return &Source{
		cfg:    Config{PollInterval: 50 * time.Millisecond, RequestTimeout: 2 * time.Second, BaseURL: baseURL, Feeds: []string{feedMeteo}},
		client: NewClient(baseURL, 2*time.Second),
		geo:    g,
	}
}

func meteoWarningJSON(id, event string, teryt []string) map[string]any {
	return map[string]any{
		"id":                 id,
		"nazwa_zdarzenia":    event,
		"stopien":            "1",
		"prawdopodobienstwo": "80",
		"obowiazuje_do":      "2026-09-23 00:00:00",
		"obowiazuje_od":      "2026-09-21 18:00:00",
		"opublikowano":       "2026-09-21 11:48:00",
		"tresc":              "Prognozowane opady.",
		"komentarz":          "Brak.",
		"biuro":              "Biuro Prognoz Meteorologicznych w Krakowie",
		"teryt":              teryt,
	}
}

func meteoBody(items ...map[string]any) []byte {
	body, err := json.Marshal(items)
	if err != nil {
		panic(err)
	}
	return body
}

// TestGeographicFilteringDoesNotBreakSnapshot: policy-filtered warnings
// stay out of the accepted key set without marking the provider snapshot
// incomplete or degrading source health.
func TestGeographicFilteringDoesNotBreakSnapshot(t *testing.T) {
	var body []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, string(body))
	}))
	defer srv.Close()

	em := &fakeEmitter{}
	src := geoSource(t, srv.URL)

	// Snapshot: one irrelevant (tatrzański), one relevant (wielicki), one
	// mixed with an unknown TERYT next to a relevant one.
	body = meteoBody(
		meteoWarningJSON("irrelevant", "Intensywne opady deszczu", []string{"1217"}),
		meteoWarningJSON("relevant", "Burze", []string{"1219"}),
		meteoWarningJSON("mixed", "Silny wiatr", []string{"1219", "9999999"}),
	)
	// One full poll: policy-filtered warnings must neither enter the key
	// set nor degrade provider health.
	src.pollOnce(context.Background(), em, em)
	h, d := em.health()
	em.mu.Lock()
	defer em.mu.Unlock()
	if len(em.emitted) != 2 {
		t.Fatalf("emitted = %d events, want 2 (relevant + mixed)", len(em.emitted))
	}
	keys := map[string]bool{}
	for _, ev := range em.emitted {
		keys[ev.Key()] = true
	}
	if keys[sourceMeteo+":irrelevant"] {
		t.Error("geographically irrelevant warning entered the accepted set")
	}
	// The unknown TERYT is preserved alongside the resolved token.
	for _, ev := range em.emitted {
		if ev.SourceID == "mixed" {
			foundUnknown, foundSlug := false, false
			for _, a := range ev.Areas {
				if a == "teryt:9999999" {
					foundUnknown = true
				}
				if a == "powiat:wielicki" {
					foundSlug = true
				}
			}
			if !foundUnknown || !foundSlug {
				t.Errorf("mixed areas = %v, want teryt:9999999 and powiat:wielicki", ev.Areas)
			}
		}
	}

	// Health was reported and not degraded.
	if h == 0 || d != 0 {
		t.Errorf("health = (%d healthy, %d degraded), want healthy only", h, d)
	}
}

// TestPreviouslyRelevantWarningReconciled: a warning that was accepted
// before but whose areas no longer intersect the target geography is
// cancelled by the next complete snapshot.
func TestPreviouslyRelevantWarningReconciled(t *testing.T) {
	var body []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, string(body))
	}))
	defer srv.Close()

	future := time.Now().Add(time.Hour)
	em := &fakeEmitter{active: []core.HazardEvent{meteoEvent("w1", &future)}}
	src := geoSource(t, srv.URL)

	// The provider now covers only powiat tatrzański for w1: the event is
	// no longer part of THIS source's logical snapshot.
	body = meteoBody(meteoWarningJSON("w1", "Burze", []string{"1217"}))
	if !src.pollFeed(context.Background(), em, feedMeteo, time.Now()) {
		t.Fatal("snapshot must stay complete")
	}
	got := em.cancelledKeys()
	if len(got) != 1 || got[0] != sourceMeteo+":w1" {
		t.Errorf("cancelled keys = %v, want [imgw-meteo:w1]", got)
	}
}

// TestUnknownTerytNeverSoleMatch: an unknown code alone never matches and
// never marks the snapshot incomplete.
func TestUnknownTerytNeverSoleMatch(t *testing.T) {
	var body []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, string(body))
	}))
	defer srv.Close()

	em := &fakeEmitter{}
	src := geoSource(t, srv.URL)
	body = meteoBody(meteoWarningJSON("u1", "Mgła", []string{"9999999"}))
	if !src.pollFeed(context.Background(), em, feedMeteo, time.Now()) {
		t.Fatal("unknown TERYT must not make the snapshot incomplete")
	}
	em.mu.Lock()
	defer em.mu.Unlock()
	if len(em.emitted) != 0 {
		t.Fatalf("emitted %d events, want 0 (unknown TERYT alone must not match)", len(em.emitted))
	}
}

// TestGeographyDisabledIsPassThrough keeps historic behaviour: without the
// config block everything is accepted.
func TestGeographyDisabledIsPassThrough(t *testing.T) {
	var body []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, string(body))
	}))
	defer srv.Close()

	em := &fakeEmitter{}
	src := &Source{
		cfg:    Config{BaseURL: srv.URL, Feeds: []string{feedMeteo}},
		client: NewClient(srv.URL, 2*time.Second),
		geo:    &geography{},
	}
	body = meteoBody(meteoWarningJSON("any", "Burze", []string{"1217", "1219"}))
	if !src.pollFeed(context.Background(), em, feedMeteo, time.Now()) {
		t.Fatal("snapshot must stay complete")
	}
	em.mu.Lock()
	defer em.mu.Unlock()
	if len(em.emitted) != 1 {
		t.Fatalf("emitted = %d, want 1 (pass-through)", len(em.emitted))
	}
	if strings.Join(em.emitted[0].Areas, "|") != "powiat:tatrzanski|powiat:wielicki|teryt:1217|teryt:1219" {
		t.Errorf("areas = %v, want enriched pass-through list", em.emitted[0].Areas)
	}
}
