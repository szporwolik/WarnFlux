package health

import (
	"testing"

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
