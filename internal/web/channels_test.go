package web

import (
	"testing"

	"github.com/szporwolik/WarnFlux/internal/action"
	"github.com/szporwolik/WarnFlux/internal/plugin"
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

// TestPublicSources pins the friendly public source list: enabled source
// instances only, outputs and unknown types hidden, one row per feed in
// the fixed friendly order.
func TestPublicSources(t *testing.T) {
	statuses := []plugin.PluginStatus{
		{ID: "mqtt-main", Type: "mqtt", Kind: plugin.KindOutput, State: plugin.StateRunning},
		{ID: "imgw-old", Type: "imgw", Kind: plugin.KindSource, State: plugin.StateDisabled},
		{ID: "adsb-main", Type: "adsb", Kind: plugin.KindSource, State: plugin.StateRunning},
		{ID: "giosaq-main", Type: "giosaq", Kind: plugin.KindSource, State: plugin.StateRunning},
		{ID: "imgw-warnings", Type: "imgw", Kind: plugin.KindSource, State: plugin.StateRunning},
		{ID: "rso-main", Type: "rso", Kind: plugin.KindSource, State: plugin.StateRunning},
		{ID: "future-x", Type: "something-new", Kind: plugin.KindSource, State: plugin.StateRunning},
	}
	got := publicSources(statuses)
	want := []string{"RSO / Alert RCB", "IMGW-PIB", "GIOŚ (air quality)", "ADS-B aircraft"}
	if len(got) != len(want) {
		t.Fatalf("sources = %+v, want %v", got, want)
	}
	for i, name := range want {
		if got[i].Name != name {
			t.Errorf("source %d = %s, want %s", i, got[i].Name, name)
		}
		if got[i].Icon == "" || got[i].Description == "" {
			t.Errorf("source %s lacks icon/description: %+v", name, got[i])
		}
	}
}
