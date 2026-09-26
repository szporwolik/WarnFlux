package web

import (
	"testing"

	"github.com/szporwolik/WarnFlux/internal/action"
)

// TestPublicChannels pins the friendly public channel list: enabled
// instances only, internal types hidden, one row per medium in the fixed
// friendly order.
func TestPublicChannels(t *testing.T) {
	statuses := []action.Status{
		{ID: "wh-discord", Type: "http_webhook", Enabled: false, State: action.StateDisabled},
		{ID: "logger-a", Type: "logger", Enabled: true, State: action.StateHealthy},
		{ID: "aprs-hams-rf", Type: "aprs-out", Enabled: true, State: action.StateHealthy},
		{ID: "discord-alerts", Type: "discord", Enabled: true, State: action.StateHealthy},
		{ID: "smtp-alerts", Type: "smtp", Enabled: true, State: action.StateHealthy},
		{ID: "smtp-backup", Type: "smtp", Enabled: true, State: action.StateHealthy},
		{ID: "custom-thing", Type: "mystery", Enabled: true, State: action.StateHealthy},
	}
	got := publicChannels(statuses)
	want := []string{"Email", "APRS radio", "Discord"}
	if len(got) != len(want) {
		t.Fatalf("channels = %+v, want %v", got, want)
	}
	for i, name := range want {
		if got[i].Name != name {
			t.Errorf("channel %d = %s, want %s", i, got[i].Name, name)
		}
		if got[i].Icon == "" || got[i].Description == "" {
			t.Errorf("channel %s lacks icon/description: %+v", name, got[i])
		}
	}
}
