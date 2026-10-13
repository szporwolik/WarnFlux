package web

import (
	"strings"
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/dispatch/state"
)

// TestClientJSONStampsUTC pins the machine-readable rule: the timestamps
// the panel hands to the browser (detail endpoints, compose payloads) are
// UTC RFC3339 whatever zone the station host runs in. A host in
// Europe/Warsaw must not turn the same event into "+02:00" — the API
// surface has to look identical on every instance.
func TestClientJSONStampsUTC(t *testing.T) {
	warsaw, err := time.LoadLocation("Europe/Warsaw")
	if err != nil {
		t.Fatal(err)
	}
	// Times built with time.Now() on a Warsaw host carry that zone.
	updated := time.Date(2026, 10, 13, 15, 1, 0, 0, warsaw)
	expires := time.Date(2026, 10, 13, 17, 0, 0, 0, warsaw)

	payload := hazardJSONFromState(state.Hazard{
		EventKey:  "imgw-meteo:1",
		Severity:  "severe",
		Headline:  "Strong wind",
		UpdatedAt: updated,
		ExpiresAt: &expires,
	}, "powiat wielicki", "en")

	if payload.UpdatedAt != "2026-10-13T13:01:00Z" {
		t.Errorf("updated_at = %q, want a UTC stamp", payload.UpdatedAt)
	}
	if payload.ExpiresAt != "2026-10-13T15:00:00Z" {
		t.Errorf("expires_at = %q, want a UTC stamp", payload.ExpiresAt)
	}

	// A local-zone clock must never leak a numeric offset into the payload.
	local := time.Now()
	if got := hazardTime(&local); !strings.HasSuffix(got, "Z") {
		t.Errorf("hazardTime = %q, want a Z suffix", got)
	}
}
