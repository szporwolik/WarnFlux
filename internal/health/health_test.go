package health

import (
	"strings"
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/action"
	"github.com/szporwolik/WarnFlux/internal/mqttreceiver"
	"github.com/szporwolik/WarnFlux/internal/plugin"
	"github.com/szporwolik/WarnFlux/internal/preflight"
)

// TestAssessCoversEverySubsystem pins the shared verdict: each subsystem the
// station owns can colour it, so no consumer can claim health while one of
// them is broken (reported P2: the digest looked at source plugins only).
func TestAssessCoversEverySubsystem(t *testing.T) {
	cases := []struct {
		name string
		in   Inputs
		want Level
	}{
		{"nothing wrong", Inputs{
			Plugins:   []plugin.PluginStatus{{ID: "imgw", State: plugin.StateRunning}},
			Receivers: []mqttreceiver.Status{{ID: "local", Enabled: true, Connected: true}},
			Database:  &Database{},
			Queue:     &QueueState{Depth: 0, Capacity: 64},
		}, LevelOK},
		// Switch-off components never colour the verdict.
		{"switched off", Inputs{
			Plugins:   []plugin.PluginStatus{{ID: "gddkia", State: plugin.StateDisabled}},
			Receivers: []mqttreceiver.Status{{ID: "backup", Enabled: false, Connected: false}},
			Actions:   []action.Status{{ID: "sms", Enabled: false, State: action.StateDegraded}},
			Database:  &Database{},
		}, LevelOK},
		{"degraded plugin", Inputs{Plugins: []plugin.PluginStatus{{ID: "adsb", State: plugin.StateDegraded}}}, LevelWarn},
		{"stopped plugin", Inputs{Plugins: []plugin.PluginStatus{{ID: "mqtt", State: plugin.StateStopped}}}, LevelBad},
		{"disconnected receiver", Inputs{
			Receivers: []mqttreceiver.Status{{ID: "local", Enabled: true, Connected: false}},
		}, LevelBad},
		{"degraded action", Inputs{
			Actions: []action.Status{{ID: "smtp", Enabled: true, State: action.StateDegraded}},
		}, LevelWarn},
		{"missing modem", Inputs{
			Hardware: []preflight.Condition{{Key: "gsm.modem", Severity: preflight.SeverityBad}},
		}, LevelBad},
		{"warning hardware", Inputs{
			Hardware: []preflight.Condition{{Key: "gsm.modem", Severity: preflight.SeverityWarn}},
		}, LevelWarn},
		{"broken database", Inputs{Database: &Database{Err: "database is locked"}}, LevelBad},
		{"full intake", Inputs{Queue: &QueueState{Depth: 64, Capacity: 64}}, LevelBad},
		{"emergency acceptance", Inputs{Queue: &QueueState{Depth: 1, Capacity: 64, Emergency: 3}}, LevelWarn},
		{"dispatch drops", Inputs{Queue: &QueueState{Depth: 1, Capacity: 64, Dropped: 2}}, LevelWarn},
		// A free-running clock stamps every published warning with the wrong
		// time, so "the container is up" is not the same as "the time is right".
		{"clock without a reference", Inputs{Clock: &Clock{Enabled: true, LastError: "no ntp server answered"}}, LevelWarn},
		{"clock never disciplined", Inputs{Clock: &Clock{Enabled: true}}, LevelWarn},
		{"clock pass too old", Inputs{Clock: &Clock{Enabled: true, Source: "ntp",
			At: time.Now().Add(-24 * time.Hour), MaxAge: 12 * time.Hour}}, LevelWarn},
		{"clock switched off", Inputs{Clock: &Clock{Enabled: false, LastError: "no ntp server answered"}}, LevelOK},
		{"clock just disciplined", Inputs{Clock: &Clock{Enabled: true, Source: "gps",
			At: time.Now().Add(-time.Hour), Stepped: true, MaxAge: 12 * time.Hour}}, LevelOK},
		// "I could not look" is never "all good".
		{"missing input", Inputs{Missing: []string{"counters"}}, LevelWarn},
	}
	for _, c := range cases {
		if got := Assess(c.in).Level(); got != c.want {
			t.Errorf("%s: level = %s, want %s", c.name, got, c.want)
		}
	}
}

// TestReportIncompleteNeverOK pins the difference between "nothing wrong" and
// "nothing could be read": an incomplete report is never a plain OK, while its
// findings still drive the wording of the verdict.
func TestReportIncompleteNeverOK(t *testing.T) {
	r := Assess(Inputs{Missing: []string{"counters"}})
	if got := r.FindingsLevel(); got != LevelOK {
		t.Fatalf("FindingsLevel = %s, want ok (no finding to report)", got)
	}
	if !r.Incomplete() {
		t.Fatal("report with a missing input is not marked incomplete")
	}
	if got := r.Level(); got != LevelWarn {
		t.Fatalf("Level = %s, want warn for an incomplete report", got)
	}
}

// TestReportFindingsKeepSubjectAndDetail documents what a consumer renders:
// the subsystem, the component and the cleaned-up detail.
func TestReportFindingsKeepSubjectAndDetail(t *testing.T) {
	r := Assess(Inputs{
		Receivers: []mqttreceiver.Status{{ID: "local", Enabled: true, LastError: "connection\nrefused"}},
		Queue:     &QueueState{Depth: 1, Capacity: 64, Dropped: 0},
	})
	got := r.Of(SubsystemReceivers)
	if len(got) != 1 || got[0].Subject != "local" || got[0].Detail != "connection refused" {
		t.Fatalf("receiver findings = %+v", got)
	}
	if len(r.Of(SubsystemQueue)) != 0 {
		t.Fatalf("healthy queue produced a finding: %+v", r.Of(SubsystemQueue))
	}
}

// TestClockFindingNamesTheReason pins what the digest and the card show: the
// operator needs to know whether the station has no reference at all or simply
// has not run a pass yet.
func TestClockFindingNamesTheReason(t *testing.T) {
	r := Assess(Inputs{Clock: &Clock{Enabled: true, LastError: "open gps /dev/ttyACM0: no such file"}})
	got := r.Of(SubsystemClock)
	if len(got) != 1 {
		t.Fatalf("clock findings = %+v, want exactly one", got)
	}
	if !strings.Contains(got[0].Detail, "no reference source") || !strings.Contains(got[0].Detail, "/dev/ttyACM0") {
		t.Fatalf("detail = %q, want the reason and the device", got[0].Detail)
	}

	r = Assess(Inputs{Clock: &Clock{Enabled: true}})
	if got := r.Of(SubsystemClock); len(got) != 1 || !strings.Contains(got[0].Detail, "no discipline pass") {
		t.Fatalf("findings = %+v, want the pending-pass wording", got)
	}
}
