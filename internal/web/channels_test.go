package web

import (
	"testing"

	"github.com/szporwolik/WarnFlux/internal/action"
	"github.com/szporwolik/WarnFlux/internal/i18n"
	"github.com/szporwolik/WarnFlux/internal/plugin"
)

// TestPublicChannels pins the friendly public channel list: enabled
// instances only, internal types hidden, one row per medium in the fixed
// friendly order.
func TestPublicChannels(t *testing.T) {
	statuses := []action.Status{
		{ID: "wh-discord", Type: "http_webhook", Enabled: false, State: action.StateDisabled},
		{ID: "logger-a", Type: "logger", Enabled: true, State: action.StateHealthy},
		{ID: "mesh-main", Type: "meshtastic", Enabled: true, State: action.StateHealthy},
		{ID: "aprs-hams-rf", Type: "aprs-out", Enabled: true, State: action.StateHealthy},
		{ID: "discord-alerts", Type: "discord", Enabled: true, State: action.StateHealthy},
		{ID: "smtp-alerts", Type: "smtp", Enabled: true, State: action.StateHealthy},
		{ID: "smtp-backup", Type: "smtp", Enabled: true, State: action.StateHealthy},
		{ID: "custom-thing", Type: "mystery", Enabled: true, State: action.StateHealthy},
	}
	got := publicChannels(statuses, i18n.LangEN)
	want := []string{"Email", "APRS radio", "Meshtastic radio", "Discord"}
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

	// Polish: names and descriptions come from the page catalog.
	gotPL := publicChannels(statuses, i18n.LangPL)
	wantPL := []string{"E-mail", "Radio APRS", "Radio Meshtastic", "Discord"}
	if len(gotPL) != len(wantPL) {
		t.Fatalf("pl channels = %+v, want %v", gotPL, wantPL)
	}
	for i, name := range wantPL {
		if gotPL[i].Name != name {
			t.Errorf("pl channel %d = %s, want %s", i, gotPL[i].Name, name)
		}
		if gotPL[i].Description == "" || gotPL[i].Description == got[i].Description {
			t.Errorf("pl channel %s leaks the English description: %+v", name, gotPL[i])
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
	got := publicSources(statuses, i18n.LangEN)
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

	// Polish: names and descriptions come from the page catalog.
	gotPL := publicSources(statuses, i18n.LangPL)
	wantPL := []string{"RSO / Alert RCB", "IMGW-PIB", "GIOŚ (jakość powietrza)", "Samoloty ADS-B"}
	if len(gotPL) != len(wantPL) {
		t.Fatalf("pl sources = %+v, want %v", gotPL, wantPL)
	}
	for i, name := range wantPL {
		if gotPL[i].Name != name {
			t.Errorf("pl source %d = %s, want %s", i, gotPL[i].Name, name)
		}
		if gotPL[i].Description == "" || gotPL[i].Description == got[i].Description {
			t.Errorf("pl source %s leaks the English description: %+v", name, gotPL[i])
		}
	}
}
