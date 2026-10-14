package web

import (
	"strings"
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/i18n"
	"github.com/szporwolik/WarnFlux/internal/plugin"
)

// TestHealthCardNamesTheZone pins the dashboard's time-zone row: the panel
// quotes the configured IANA zone verbatim, and an unnamed process zone
// reports its offset instead of pretending to be something it may not be.
func TestHealthCardNamesTheZone(t *testing.T) {
	statuses := []plugin.PluginStatus{{ID: "imgw-warnings", Type: "imgw", Kind: plugin.KindSource, State: plugin.StateRunning}}

	warsaw, err := time.LoadLocation("Europe/Warsaw")
	if err != nil {
		t.Fatal(err)
	}
	srv := verdictFixture(t, statuses)
	srv.SetTimezone(warsaw)
	row := srv.buildHealthView(i18n.LangEN).Zone
	if row.BadgeText != "Europe/Warsaw" {
		t.Errorf("zone badge = %q, want Europe/Warsaw", row.BadgeText)
	}
	if !strings.Contains(row.Detail, "Europe/Warsaw") {
		t.Errorf("zone detail does not name the zone: %q", row.Detail)
	}
	if row.Name != "Time zone" {
		t.Errorf("zone row name = %q", row.Name)
	}

	// No zone pinned: an unnamed process zone carries no IANA name, so the
	// row must show the offset rather than a bare, meaningless "Local".
	local := verdictFixture(t, statuses)
	local.SetTimezone(nil)
	row = local.buildHealthView(i18n.LangEN).Zone
	if name := time.Local.String(); name == "Local" {
		if !strings.HasPrefix(row.BadgeText, "Local (UTC") {
			t.Errorf("local zone badge = %q, want an offset-qualified Local", row.BadgeText)
		}
	} else if row.BadgeText != name {
		t.Errorf("local zone badge = %q, want %q", row.BadgeText, name)
	}
}
