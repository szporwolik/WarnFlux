package web_test

import (
	"context"
	"net/http"
	"net/url"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/core"
	"github.com/szporwolik/WarnFlux/internal/dispatch"
	"github.com/szporwolik/WarnFlux/internal/storage/sqlite"
)

// TestExternalExpire pins the panel-side retirement of active
// communications that came from other sources: the Messages page lists
// them below the issued messages, the Expire action cancels the local
// record quietly (no notifications), routes the canonical transition and
// removes the retained broker document.
func TestExternalExpire(t *testing.T) {
	store, _, err := sqlite.Open(filepath.Join(t.TempDir(), "external.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	ev := core.HazardEvent{
		Source: "imgw-meteo", SourceID: "warn-42", Event: "Test noise",
		Severity: "minor", Urgency: "expected", Certainty: "observed",
		Headline: "Noise alert", Status: core.StatusActive,
	}
	if _, _, err := store.Ingest(ctx, ev, core.Fingerprint(ev)); err != nil {
		t.Fatalf("seed event: %v", err)
	}

	env := newTestEnvWithStore(t, store)
	env.login()

	// The Messages page lists the external communication below the
	// issued list, with the Expire action.
	resp, html := env.get("/compose")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /compose = %d", resp.StatusCode)
	}
	if !strings.Contains(html, "Active communications from other sources") ||
		!strings.Contains(html, "Noise alert") ||
		!strings.Contains(html, "/compose/expire-external") {
		t.Fatalf("compose page missing the external list: %.500s", html)
	}
	csrf := extractCSRF(t, html)

	// Unknown key: 404.
	resp, _ = env.postForm("/compose/expire-external", url.Values{"csrf": {csrf}, "event_key": {"imgw-meteo:nope"}})
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expire unknown key = %d, want 404", resp.StatusCode)
	}

	// Expire the nuisance: local cancellation + broker tombstone.
	resp, _ = env.postForm("/compose/expire-external", url.Values{"csrf": {csrf}, "event_key": {ev.Key()}})
	if resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/compose?msg=external-expired" {
		t.Fatalf("expire external = %d %q, want the flash redirect", resp.StatusCode, resp.Header.Get("Location"))
	}
	st, err := store.Get(ctx, ev.Key())
	if err != nil || st.Event.Status != core.StatusCancelled {
		t.Fatalf("stored status = %q, %v; want cancelled", st.Event.Status, err)
	}
	if !slices.Contains(env.pub.expiredSnapshot(), ev.Key()) {
		t.Fatalf("broker tombstone missing for %s: %v", ev.Key(), env.pub.expiredSnapshot())
	}
	select {
	case got := <-env.ingress.Events():
		if got.Hazard == nil || got.Hazard.Type != dispatch.TransitionCancelled {
			t.Fatalf("transition = %+v, want cancelled", got.Hazard)
		}
	case <-time.After(time.Second):
		t.Fatal("no cancellation transition enqueued")
	}

	// The page no longer lists the retired communication.
	_, html = env.get("/compose")
	if strings.Contains(html, "Noise alert") {
		t.Fatalf("retired communication still listed: %.500s", html)
	}
}

// TestComposeRowDetailPopup pins the compose detail popup wiring: every
// message row is a click target carrying its event key, the shared
// detail dialog (the one the public page uses) is on the page, and the
// API the popup falls back to serves the message with its FULL payload —
// that is what the map inside the popup draws.
func TestComposeRowDetailPopup(t *testing.T) {
	env := newTestEnv(t)
	env.login()
	_, page := env.get("/compose")
	csrf := extractCSRF(t, page)

	expStr := time.Now().Add(6 * time.Hour).Format("2006-01-02T15:04")
	resp, _ := env.postForm("/compose", url.Values{
		"csrf":        {csrf},
		"event_key":   {"compose:popup-check"},
		"event":       {"Flood"},
		"headline":    {"Raba flooding"},
		"severity":    {"severe"},
		"status":      {"active"},
		"areas":       {"wieliczka"},
		"latitude":    {"49.985"},
		"longitude":   {"20.065"},
		"expires_at":  {expStr},
		"description": {"Heavy rain"},
		"instruction": {"Avoid the river bank."},
	})
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("POST /compose = %d, want redirect", resp.StatusCode)
	}
	// The fake publisher does not echo: mirror the broker loopback into
	// the state the panel and the popup API both read.
	pub := env.pub.publishedSnapshot()
	if len(pub) != 1 {
		t.Fatalf("publisher saw %d publishes, want 1", len(pub))
	}
	if err := env.state.AddOrUpdateActive("local", "warnflux/active/compose/popup", pub[0]); err != nil {
		t.Fatal(err)
	}

	_, page = env.get("/compose")
	for _, want := range []string{
		`class="hazard-click"`,
		`data-key="compose:popup-check"`,
		`id="hz-dialog"`,
		`id="hz-content"`,
		"Click a message for every detail",
	} {
		if !strings.Contains(page, want) {
			t.Errorf("compose page missing %q", want)
		}
	}

	// The popup fetches unknown keys from the API: the payload must carry
	// the coordinates (the mini map) and the free-text details.
	resp, body := env.get("/api/hazard/compose:popup-check")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/hazard/compose:popup-check = %d: %s", resp.StatusCode, body)
	}
	for _, want := range []string{
		`"event_key":"compose:popup-check"`,
		`"latitude":49.985`,
		`"longitude":20.065`,
		`"description":"Heavy rain"`,
		`"instruction":"Avoid the river bank."`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("hazard payload missing %q: %s", want, body)
		}
	}
}
