package web

import (
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/meshtastic"
)

// meshStatusServer builds a Server carrying one radio hub.
func meshStatusServer(t *testing.T, cfg meshtastic.Config) *Server {
	t.Helper()
	hub, err := meshtastic.NewHub(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("NewHub: %v", err)
	}
	return &Server{meshtastic: hub}
}

// TestHelpMeshStatusCard pins the Meshtastic card: it describes the running
// station (the two alert paths and the cadence of both periodic lines), the
// same way the Discord card does.
func TestHelpMeshStatusCard(t *testing.T) {
	// No radio at all: no card.
	if view := (&Server{}).helpMeshStatus("en"); view != nil {
		t.Fatalf("card without a radio = %+v", view)
	}
	if view := meshStatusServer(t, meshtastic.Config{}).helpMeshStatus("en"); view != nil {
		t.Fatalf("card for a disabled radio = %+v", view)
	}

	srv := meshStatusServer(t, meshtastic.Config{
		Enabled: true, Device: "/dev/fake", EmcomChannel: 1,
		EmcomIdentity: "WarnFlux v0.7.7 - SOSDEV", EmcomAt: "06:00",
		EmcomHazardsInterval: time.Hour,
	})
	view := srv.helpMeshStatus("en")
	if view == nil {
		t.Fatal("no card for an enabled radio")
	}
	facts := map[string]string{}
	for _, f := range view.Facts {
		facts[f.Label] = f.Value
	}
	if facts["Group channel alerts"] != "enabled" || facts["Direct messages"] != "enabled" {
		t.Errorf("alert paths = %q / %q", facts["Group channel alerts"], facts["Direct messages"])
	}
	if got := facts["Hazard status"]; got != "every 1h (only for severe or higher alerts)" {
		t.Errorf("hazard status = %q", got)
	}
	if got := facts["Presence beacon"]; got != "daily at 06:00" {
		t.Errorf("presence beacon = %q", got)
	}
	if len(view.Items) != 3 {
		t.Errorf("items = %v", view.Items)
	}

	// The cadence follows the configuration: a spacing instead of the
	// anchor, and both zero means the lines are off.
	spaced := meshStatusServer(t, meshtastic.Config{
		Enabled: true, Device: "/dev/fake", EmcomChannel: 1,
		EmcomIdentity: "WarnFlux v0.7.7 - SOSDEV", EmcomInterval: 4 * time.Hour,
	})
	for _, f := range spaced.helpMeshStatus("pl").Facts {
		if f.Label == "Beacon obecności" && f.Value != "co 4h" {
			t.Errorf("spaced beacon = %q", f.Value)
		}
	}
	quiet := meshStatusServer(t, meshtastic.Config{
		Enabled: true, Device: "/dev/fake", EmcomChannel: 1, EmcomIdentity: "WarnFlux",
	})
	view = quiet.helpMeshStatus("en")
	for _, f := range view.Facts {
		if f.Label == "Hazard status" || f.Label == "Presence beacon" {
			if f.Value != "disabled" || f.On {
				t.Errorf("%s = %q (on=%v), want disabled", f.Label, f.Value, f.On)
			}
		}
	}
}
