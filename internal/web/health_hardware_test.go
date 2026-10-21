package web

import (
	"strings"
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/i18n"
	"github.com/szporwolik/WarnFlux/internal/plugin"
	"github.com/szporwolik/WarnFlux/internal/preflight"
)

// fakeHardware is a preflight stand-in: the test decides which devices are
// missing, so the card renders without touching the host.
type fakeHardware struct {
	problems []preflight.Condition
	checks   int
}

func (f *fakeHardware) Active() []preflight.Condition { return f.problems }
func (f *fakeHardware) Len() int                      { return f.checks }

// TestHealthCardShowsMissingHardware pins the point of the preflight probe: a
// configured-but-unplugged device must be visible on the panel's health card
// (and must not leave the verdict green), instead of the station dying at
// startup and leaving the operator with a dead process to explain why.
func TestHealthCardShowsMissingHardware(t *testing.T) {
	statuses := []plugin.PluginStatus{{ID: "imgw-warnings", Type: "imgw", Kind: plugin.KindSource, State: plugin.StateRunning}}
	srv := verdictFixture(t, statuses)
	srv.SetHardwareProbe(&fakeHardware{
		checks: 2,
		problems: []preflight.Condition{{
			Key:      "gsm.modem",
			Severity: preflight.SeverityBad,
			Detail:   "/dev/ttyUSB0: no such file or directory",
			Since:    time.Date(2026, 10, 22, 8, 15, 0, 0, time.UTC),
		}},
	})

	v := srv.buildHealthView(i18n.LangEN)
	if len(v.Hardware) != 1 {
		t.Fatalf("hardware rows = %d, want 1", len(v.Hardware))
	}
	row := v.Hardware[0]
	if row.Name != "GSM modem" {
		t.Errorf("row name = %q, want the translated device name", row.Name)
	}
	if row.BadgeClass != badgeBad {
		t.Errorf("row badge = %q, want %q", row.BadgeClass, badgeBad)
	}
	if row.BadgeText != "unavailable" {
		t.Errorf("row badge text = %q", row.BadgeText)
	}
	if !strings.Contains(row.Detail, "/dev/ttyUSB0") || !strings.Contains(row.Detail, "since") {
		t.Errorf("row detail = %q, want the path and when it vanished", row.Detail)
	}
	if v.Overall == badgeOK {
		t.Error("verdict stayed green with an unusable modem")
	}
	if v.HardwareOK {
		t.Error("hardware reported as fine while a device is missing")
	}

	// The Polish card names the same finding in Polish.
	pl := srv.buildHealthView(i18n.LangPL)
	if len(pl.Hardware) != 1 || pl.Hardware[0].Name != "Modem GSM" {
		t.Errorf("PL hardware row = %+v", pl.Hardware)
	}
	if !strings.Contains(pl.Hardware[0].BadgeText, "niedostępne") {
		t.Errorf("PL badge text = %q", pl.Hardware[0].BadgeText)
	}
}

// TestHealthCardSaysHardwareIsFine pins the other side: with every probed
// device present the card says so, so "is the hardware OK?" has a visible
// answer instead of an absent section.
func TestHealthCardSaysHardwareIsFine(t *testing.T) {
	statuses := []plugin.PluginStatus{{ID: "imgw-warnings", Type: "imgw", Kind: plugin.KindSource, State: plugin.StateRunning}}
	srv := verdictFixture(t, statuses)
	srv.SetHardwareProbe(&fakeHardware{checks: 2})

	v := srv.buildHealthView(i18n.LangEN)
	if !v.HardwareOK {
		t.Error("hardware not reported as fine although every probe passed")
	}
	if len(v.Hardware) != 0 {
		t.Errorf("hardware rows = %+v, want none", v.Hardware)
	}
}

// TestHealthCardWithoutProbeHidesHardware pins the unwired case (minimal
// constructions): no probe means nothing to claim, so the card shows no
// hardware claim at all rather than a misleading green one.
func TestHealthCardWithoutProbeHidesHardware(t *testing.T) {
	statuses := []plugin.PluginStatus{{ID: "imgw-warnings", Type: "imgw", Kind: plugin.KindSource, State: plugin.StateRunning}}
	srv := verdictFixture(t, statuses)

	v := srv.buildHealthView(i18n.LangEN)
	if v.HardwareOK || len(v.Hardware) != 0 {
		t.Errorf("hardware section claimed without a probe: ok=%v rows=%+v", v.HardwareOK, v.Hardware)
	}
}
