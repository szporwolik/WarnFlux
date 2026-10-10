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
