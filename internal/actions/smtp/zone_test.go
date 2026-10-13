package smtp

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/action"
	"github.com/szporwolik/WarnFlux/internal/dispatch"
	"github.com/szporwolik/WarnFlux/internal/i18n"
)

// TestMailRendersStationZone pins the time policy of the notification
// bodies: every timestamp is rendered in the STATION's display zone and the
// mail's own clock names that zone, so a reader can never mistake a UTC
// time for a local one. 17:00Z is 19:00 in Europe/Warsaw (CEST).
func TestMailRendersStationZone(t *testing.T) {
	now := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	expires := time.Date(2026, 9, 25, 17, 0, 0, 0, time.UTC)
	event := dispatch.Event{
		Kind: dispatch.EventHazardTransition,
		Hazard: &dispatch.HazardTransition{
			Type: dispatch.TransitionNew,
			Hazard: dispatch.Hazard{
				Source:    "imgw-meteo",
				EventKey:  "imgw-meteo:1",
				Event:     "Storm — strong wind",
				Severity:  "severe",
				Headline:  "Strong wind warning",
				Areas:     []string{"powiat wielicki"},
				ExpiresAt: &expires,
			},
		},
	}

	cases := []struct {
		name     string
		timezone string
		want     []string
	}{
		{
			name:     "named station zone",
			timezone: "Europe/Warsaw",
			want:     []string{"valid until: 2026-09-25 19:00", "14:00 (Europe/Warsaw)"},
		},
		{
			name:     "no zone configured falls back to a named UTC",
			timezone: "",
			want:     []string{"valid until: 2026-09-25 17:00", "12:00 (UTC)"},
		},
		{
			name:     "unusable zone name degrades to a named UTC",
			timezone: "Mars/Olympus",
			want:     []string{"valid until: 2026-09-25 17:00", "12:00 (UTC)"},
		},
	}

	for _, c := range cases {
		req := action.ActionRequest{
			Event: event,
			App:   action.AppInfo{Header1: "SOSNA", Timezone: c.timezone},
		}
		body := string(buildMessage(context.Background(), Config{From: "a@b.c", To: []string{"c@d.e"}}, req, i18n.LangEN, now))
		for _, want := range c.want {
			if !strings.Contains(body, want) {
				t.Errorf("%s: body lacks %q: %s", c.name, want, body)
			}
		}
		if strings.Contains(body, "2026-09-25T17:00:00Z") {
			t.Errorf("%s: body leaked a raw RFC3339 stamp: %s", c.name, body)
		}
	}
}
