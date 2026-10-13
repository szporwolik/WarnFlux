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
		{ID: "aprs-hams-inet", Type: "aprs", Enabled: true, State: action.StateHealthy},
		{ID: "sms-alerts", Type: "sms", Enabled: true, State: action.StateHealthy},
		{ID: "discord-alerts", Type: "discord", Enabled: true, State: action.StateHealthy},
		{ID: "smtp-alerts", Type: "smtp", Enabled: true, State: action.StateHealthy},
		{ID: "smtp-backup", Type: "smtp", Enabled: true, State: action.StateHealthy},
		{ID: "custom-thing", Type: "mystery", Enabled: true, State: action.StateHealthy},
	}
	got := publicChannels(statuses, i18n.LangEN)
	// One row per MEDIUM: aprs and aprs-out are both "APRS radio", so the
	// station running both must not list it twice.
	want := []string{"Email", "APRS radio", "Meshtastic radio", "SMS", "Discord"}
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
	wantPL := []string{"E-mail", "Radio APRS", "Radio Meshtastic", "SMS", "Discord"}
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
		{ID: "burze-main", Type: "burzedzis", Kind: plugin.KindSource, State: plugin.StateRunning},
		{ID: "aprs-inet-main", Type: "aprs-inet", Kind: plugin.KindSource, State: plugin.StateRunning},
		{ID: "aprs-radio-main", Type: "aprs-radio", Kind: plugin.KindSource, State: plugin.StateRunning},
		{ID: "future-x", Type: "something-new", Kind: plugin.KindSource, State: plugin.StateRunning},
	}
	// The station's own feeds: no ingest endpoint and no EMCOM network here,
	// so only the messages row is added.
	got := publicSources(statuses, operatorSources{}, i18n.LangEN)
	// One row per FEED: the two APRS flavours collapse into one row.
	want := []string{"RSO / Alert RCB", "IMGW-PIB", "burze.dzis.net (storms)", "GIOŚ (air quality)", "APRS network", "ADS-B aircraft", "WarnFlux messages"}
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
	byName := map[string]publicChannelView{}
	for _, v := range got {
		byName[v.Name] = v
	}
	// Every PROVIDER row carries the provider's own page: the data belongs to
	// the provider and the reader must be able to reach it (the
	// burze.dzis.net API terms require exactly that attribution).
	for name, wantURL := range map[string]string{
		"RSO / Alert RCB":         "https://komunikaty.tvp.pl/",
		"IMGW-PIB":                "https://danepubliczne.imgw.pl/",
		"burze.dzis.net (storms)": "https://burze.dzis.net/",
		"GIOŚ (air quality)":      "https://powietrze.gios.gov.pl/",
		"APRS network":            "https://www.aprs-is.net/",
		"ADS-B aircraft":          "https://adsb.lol/",
	} {
		row, ok := byName[name]
		if !ok {
			t.Errorf("source row %q is missing", name)
			continue
		}
		if row.URL != wantURL {
			t.Errorf("source %s reference = %q, want %q", name, row.URL, wantURL)
		}
	}
	// The station's own feeds have no provider behind them.
	if row := byName["WarnFlux messages"]; row.URL != "" {
		t.Errorf("the station's own messages carry a provider reference: %q", row.URL)
	}
	// Delivery media are not a web service: no reference under them.
	for _, v := range publicChannels([]action.Status{{ID: "smtp-a", Type: "smtp", Enabled: true, State: action.StateHealthy}}, i18n.LangEN) {
		if v.URL != "" {
			t.Errorf("channel %s carries a provider reference: %q", v.Name, v.URL)
		}
	}

	// Polish: names and descriptions come from the page catalog.
	gotPL := publicSources(statuses, operatorSources{}, i18n.LangPL)
	wantPL := []string{"RSO / Alert RCB", "IMGW-PIB", "burze.dzis.net (burze)", "GIOŚ (jakość powietrza)", "Sieć APRS", "Samoloty ADS-B", "Komunikaty WarnFlux"}
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

// TestPublicOperatorSources pins the feeds that are not plugins: the station
// has no foreign provider at all in this fixture, yet the page must still say
// where data comes from — its own messages always, its REST API only when an
// endpoint exists, EMCOM only when a network is managed.
func TestPublicOperatorSources(t *testing.T) {
	names := func(op operatorSources) []string {
		var out []string
		for _, v := range publicSources(nil, op, i18n.LangEN) {
			out = append(out, v.Name)
		}
		return out
	}

	bare := names(operatorSources{})
	if len(bare) != 1 || bare[0] != "WarnFlux messages" {
		t.Fatalf("bare station sources = %v, want only the messages row", bare)
	}
	full := names(operatorSources{Ingest: 1, Emcom: 2})
	want := []string{"WarnFlux messages", "REST API", "EMCOM network"}
	if len(full) != len(want) {
		t.Fatalf("operator sources = %v, want %v", full, want)
	}
	for i := range want {
		if full[i] != want[i] {
			t.Errorf("operator source %d = %q, want %q", i, full[i], want[i])
		}
	}
	// The REST API row is the ingest feed the routing matrix names REST API,
	// and it carries the station's own endpoint claim, not a provider link.
	for _, v := range publicSources(nil, operatorSources{Ingest: 1}, i18n.LangPL) {
		if v.Name == "REST API" && v.URL != "" {
			t.Errorf("the operator's own endpoint carries a provider reference: %q", v.URL)
		}
		if v.Name == "REST API" && v.Description == "" {
			t.Error("the REST API row has no description")
		}
	}
}
