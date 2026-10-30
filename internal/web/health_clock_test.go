package web

import (
	"strings"
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/health"
	"github.com/szporwolik/WarnFlux/internal/i18n"
)

// TestHealthCardShowsFreeRunningClock pins the answer to "is the time right?":
// the time-zone row carries where the clock came from, and a station whose
// clock has no reference never shows a green verdict.
func TestHealthCardShowsFreeRunningClock(t *testing.T) {
	srv := verdictFixture(t, nil)
	srv.SetClockHealth(func() *health.Clock {
		return &health.Clock{Enabled: true, LastError: "open gps /dev/ttyACM0: no such file"}
	})

	v := srv.buildHealthView(i18n.LangEN)
	if !strings.Contains(v.Zone.Detail, "without a reference") || !strings.Contains(v.Zone.Detail, "/dev/ttyACM0") {
		t.Fatalf("zone detail = %q, want the reason the clock has no reference", v.Zone.Detail)
	}
	if v.Zone.BadgeClass != badgeWarn {
		t.Fatalf("zone badge = %q, want %q", v.Zone.BadgeClass, badgeWarn)
	}
	if v.Zone.BadgeText != "FREE-RUNNING" {
		t.Fatalf("zone badge text = %q", v.Zone.BadgeText)
	}
	if v.Overall == badgeOK {
		t.Fatal("verdict stayed green while the clock had no reference")
	}
}

// TestHealthCardShowsClockSource is the healthy case: the operator sees which
// source disciplined the clock and how long ago, without opening the log.
func TestHealthCardShowsClockSource(t *testing.T) {
	srv := verdictFixture(t, nil)
	srv.SetClockHealth(func() *health.Clock {
		return &health.Clock{
			Enabled: true, Source: "gps", Stepped: true,
			At: time.Now().Add(-2 * time.Hour), MaxAge: 12 * time.Hour,
		}
	})

	v := srv.buildHealthView(i18n.LangEN)
	if !strings.Contains(v.Zone.Detail, "clock from gps") || !strings.Contains(v.Zone.Detail, "2h0m0s") {
		t.Fatalf("zone detail = %q, want the source and the age", v.Zone.Detail)
	}
	if v.Zone.BadgeClass != badgeOK {
		t.Fatalf("zone badge = %q, want %q", v.Zone.BadgeClass, badgeOK)
	}
}

// TestHealthCardSkipsDisabledClock: a clock discipline the operator switched
// off must not colour the verdict — that is a decision, not a fault.
func TestHealthCardSkipsDisabledClock(t *testing.T) {
	srv := verdictFixture(t, nil)
	srv.SetClockHealth(func() *health.Clock {
		return &health.Clock{Enabled: false, LastError: "no ntp server answered"}
	})

	v := srv.buildHealthView(i18n.LangEN)
	if strings.Contains(v.Zone.Detail, "without a reference") {
		t.Fatalf("zone detail = %q, want the switched-off clock left out", v.Zone.Detail)
	}
	if v.Overall != badgeOK {
		t.Fatalf("verdict = %q, want unaffected by a switched-off clock", v.Overall)
	}
}
