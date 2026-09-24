package gios

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/szporwolik/WarnFlux/internal/aprs"
	"github.com/szporwolik/WarnFlux/internal/core"
)

type fakeEmitter struct {
	events []core.HazardEvent
}

func (f *fakeEmitter) Emit(_ context.Context, ev core.HazardEvent) error {
	f.events = append(f.events, ev)
	return nil
}

func (f *fakeEmitter) EmitInformation(context.Context, core.InformationMessage) error {
	return nil
}

func testHub(t *testing.T) *aprs.Hub {
	t.Helper()
	lat, lon := 50.0212, 20.2075
	h, err := aprs.NewHub(aprs.HubConfig{
		Enabled:    true,
		Callsign:   "SP9TST-10",
		GridSquare: "KO00BA",
		Latitude:   &lat,
		Longitude:  &lon,
		RadiusKM:   30,
	}, nil)
	if err != nil {
		t.Fatalf("NewHub: %v", err)
	}
	return h
}

// decodeConfig decodes a YAML fragment into the mapping node the plugin
// framework hands to New.
func decodeConfig(t *testing.T, text string) *yaml.Node {
	t.Helper()
	var doc yaml.Node
	if err := yaml.Unmarshal([]byte(text), &doc); err != nil {
		t.Fatalf("yaml: %v", err)
	}
	return doc.Content[0]
}

func TestNewValidation(t *testing.T) {
	hub := testHub(t)

	if _, err := New(nil, nil); err == nil {
		t.Error("nil hub accepted")
	}
	if _, err := New(decodeConfig(t, "base_url: not a url\n"), hub); err == nil {
		t.Error("malformed base_url accepted")
	}
	if _, err := New(decodeConfig(t, "geocoder_url: not a url\n"), hub); err == nil {
		t.Error("malformed geocoder_url accepted")
	}
	if _, err := New(decodeConfig(t, "poll_interval: 5m\n"), hub); err == nil {
		t.Error("too-short poll_interval accepted")
	}
	if _, err := New(decodeConfig(t, "wojewodztwo: ślonskie\n"), hub); err == nil {
		t.Error("unknown voivodeship accepted")
	}
	if _, err := New(decodeConfig(t, "center_latitude: 50.0\n"), hub); err == nil {
		t.Error("center_latitude without center_longitude accepted")
	}
	if _, err := New(decodeConfig(t, "rok: 999\n"), hub); err == nil {
		t.Error("3-digit rok accepted")
	}

	src, err := New(decodeConfig(t, "poll_interval: 6h\nrequest_timeout: 10s\nwojewodztwo: małopolskie\n"), hub)
	if err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	s := src.(*Source)
	if s.radiusKM != 30 || s.centerLat != 50.0212 || s.centerLon != 20.2075 {
		t.Errorf("hub defaults not applied: radius=%v center=(%v,%v)", s.radiusKM, s.centerLat, s.centerLon)
	}
	if s.cfg.Wojewodztwo != "małopolskie" {
		t.Errorf("wojewodztwo = %q", s.cfg.Wojewodztwo)
	}
	if s.cfg.BaseURL != "https://dane.gios.gov.pl/api/powazne-awarie" {
		t.Errorf("base_url = %q", s.cfg.BaseURL)
	}

	ovr, err := New(decodeConfig(t, "radius_km: 10\ncenter_latitude: 50.0\ncenter_longitude: 20.0\n"), hub)
	if err != nil {
		t.Fatalf("override config rejected: %v", err)
	}
	os := ovr.(*Source)
	if os.radiusKM != 10 || os.centerLat != 50.0 || os.centerLon != 20.0 {
		t.Errorf("overrides not applied: %+v", os)
	}
}

// TestPollOnceGeoPipeline runs the full flow against fake services:
// admin-filtered API pages, a fake ULDK geocoder and the hub radius.
func TestPollOnceGeoPipeline(t *testing.T) {
	hub := testHub(t)
	recs := sampleRecords()
	// Oświęcim is ~44 km from Niepołomice — outside the 30 km radius.
	// Skawina is ~24 km away — inside.
	towns := map[string][2]float64{
		"oświęcim": {50.0358, 19.2100},
		"skawina":  {49.9750, 19.8280},
	}
	api := apiServer(t, [][]awariaRekord{recs})
	defer api.Close()
	geo := geocoderServer(t, towns)
	defer geo.Close()

	src, err := New(decodeConfig(t,
		"base_url: \""+api.URL+"\"\n"+
			"geocoder_url: \""+geo.URL+"\"\n"+
			"wojewodztwo: małopolskie\n"+
			"rok: 2026\n"), hub)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	em := &fakeEmitter{}
	if err := src.(*Source).pollOnce(context.Background(), em); err != nil {
		t.Fatalf("pollOnce: %v", err)
	}
	if len(em.events) != 1 {
		t.Fatalf("emitted %d events, want 1 (Skawina only)", len(em.events))
	}
	ev := em.events[0]
	if ev.Event != "Chemical release" {
		t.Errorf("event = %q", ev.Event)
	}
	if ev.Latitude == nil || *ev.Latitude != 49.975 || *ev.Longitude != 19.828 {
		t.Errorf("coordinates = (%v, %v)", ev.Latitude, ev.Longitude)
	}
	if !strings.Contains(ev.Areas[0], "małopolskie") {
		t.Errorf("areas = %v", ev.Areas)
	}
}

// TestPollOnceAutoVoivodeship verifies the reverse-geocoded administrative
// scope when the configuration does not name a voivodeship.
func TestPollOnceAutoVoivodeship(t *testing.T) {
	hub := testHub(t)
	recs := sampleRecords()
	towns := map[string][2]float64{
		"skawina": {49.9750, 19.8280},
	}
	api := apiServer(t, [][]awariaRekord{recs[:1]})
	defer api.Close()
	geo := geocoderServer(t, towns)
	defer geo.Close()

	// No wojewodztwo in the configuration: the home position must be
	// reverse-geocoded. The fake reverse URL answers only GetAddress
	// requests, so the reverse lookup fails and the poll must fail with
	// zero emissions.
	cfgYAML := "base_url: " + strconv.Quote(api.URL) + "\n" +
		"geocoder_url: " + strconv.Quote(geo.URL) + "\n" +
		"reverse_url: " + strconv.Quote(geo.URL) + "\n"
	src, err := New(decodeConfig(t, cfgYAML), hub)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	em := &fakeEmitter{}
	if err := src.(*Source).pollOnce(context.Background(), em); err == nil {
		t.Fatal("pollOnce without a working reverse geocoder must fail")
	}
	if len(em.events) != 0 {
		t.Fatalf("events emitted despite unresolved admin scope: %d", len(em.events))
	}
}
