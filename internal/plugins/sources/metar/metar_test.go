package metar

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/szporwolik/WarnFlux/internal/core"
)

type fakeEmitter struct {
	infos []core.InformationMessage
}

func (f *fakeEmitter) Emit(context.Context, core.HazardEvent) error { return nil }
func (f *fakeEmitter) EmitInformation(_ context.Context, m core.InformationMessage) error {
	f.infos = append(f.infos, m)
	return nil
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

// apiServer serves one fake NOAA-style METAR response.
func apiServer(t *testing.T, records []metarRecord) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/metar" || r.URL.Query().Get("format") != "json" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(records)
	}))
}

func epkkRecord() metarRecord {
	return metarRecord{
		IcaoID:     "EPKK",
		Name:       "Kraków/Paul II Arpt, ML, PL",
		Lat:        50.078,
		Lon:        19.797,
		Elev:       237,
		ObsTime:    1790290800,
		ReportTime: "2026-09-24T23:00:00.000Z",
		Temp:       11,
		Dewp:       10,
		Wdir:       230,
		Wspd:       5,
		Altim:      30.09,
		RawOb:      "METAR EPKK 242300Z 23005KT 6000 -RA OVC005 11/10 Q1019",
		Clouds: []struct {
			Cover string  `json:"cover"`
			Base  float64 `json:"base"`
		}{{Cover: "OVC", Base: 500}},
	}
}

func TestNewValidation(t *testing.T) {
	if _, err := New(decodeConfig(t, "stations: []\n")); err == nil {
		t.Error("no stations accepted")
	}
	if _, err := New(decodeConfig(t, "stations:\n  - id: EPKK\n  - id: EPKK\n")); err == nil {
		t.Error("duplicate stations accepted")
	}
	if _, err := New(decodeConfig(t, "stations:\n  - id: XXX\n")); err == nil {
		t.Error("non-ICAO id accepted")
	}
	if _, err := New(decodeConfig(t, "base_url: not a url\nstations:\n  - id: EPKK\n")); err == nil {
		t.Error("malformed base_url accepted")
	}
	if _, err := New(decodeConfig(t, "poll_interval: 1m\nstations:\n  - id: EPKK\n")); err == nil {
		t.Error("too-short poll_interval accepted")
	}

	src, err := New(decodeConfig(t, "stations:\n  - id: epkk\n    name: Balice (EPKK)\n"))
	if err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	s := src.(*Source)
	if s.cfg.Stations[0].ID != "EPKK" {
		t.Errorf("station id not normalized: %q", s.cfg.Stations[0].ID)
	}
	if s.cfg.BaseURL != defaultBaseURL {
		t.Errorf("base_url = %q", s.cfg.BaseURL)
	}
}

func TestPollOncePublishesWeather(t *testing.T) {
	srv := apiServer(t, []metarRecord{epkkRecord()})
	defer srv.Close()

	src, err := New(decodeConfig(t,
		"base_url: "+strconvQuote(srv.URL)+"\n"+
			"stations:\n  - id: EPKK\n    name: Balice (EPKK)\n"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	em := &fakeEmitter{}
	if err := src.(*Source).pollOnce(context.Background(), em); err != nil {
		t.Fatalf("pollOnce: %v", err)
	}
	if len(em.infos) != 1 {
		t.Fatalf("published %d snapshots, want 1", len(em.infos))
	}
	m := em.infos[0]
	if m.Source != "metar" || m.Kind != "weather" || m.Key != "epkk" {
		t.Errorf("envelope = source %q kind %q key %q", m.Source, m.Kind, m.Key)
	}

	var wire struct {
		Type     string `json:"type"`
		Location struct {
			Name      string  `json:"name"`
			Latitude  float64 `json:"latitude"`
			Longitude float64 `json:"longitude"`
		} `json:"location"`
		Current struct {
			TemperatureC   *float64 `json:"temperature_c"`
			WindSpeedKmh   *float64 `json:"wind_speed_kmh"`
			PressureMSLHpa *float64 `json:"pressure_msl_hpa"`
			Condition      string   `json:"condition"`
		} `json:"current"`
	}
	if err := json.Unmarshal(m.Payload, &wire); err != nil {
		t.Fatalf("payload invalid: %v", err)
	}
	if wire.Type != "weather" || wire.Location.Name != "Balice (EPKK)" {
		t.Errorf("wire = %+v", wire)
	}
	if wire.Location.Latitude != 50.078 || wire.Location.Longitude != 19.797 {
		t.Errorf("position = %v, %v", wire.Location.Latitude, wire.Location.Longitude)
	}
	if wire.Current.Condition != "rain" {
		t.Errorf("condition = %q, want rain (-RA in rawOb)", wire.Current.Condition)
	}
	if wire.Current.TemperatureC == nil || *wire.Current.TemperatureC != 11 {
		t.Errorf("temperature = %v", wire.Current.TemperatureC)
	}
	if wire.Current.WindSpeedKmh == nil || *wire.Current.WindSpeedKmh < 9.2 || *wire.Current.WindSpeedKmh > 9.3 {
		t.Errorf("wind = %v, want ~9.26 km/h (5 kt)", wire.Current.WindSpeedKmh)
	}
	// altim 30.09 is inHg-style (<100) → converted to hPa.
	if wire.Current.PressureMSLHpa == nil || *wire.Current.PressureMSLHpa < 1018 || *wire.Current.PressureMSLHpa > 1020 {
		t.Errorf("pressure = %v, want ~1019 hPa", wire.Current.PressureMSLHpa)
	}
}

func TestPollOnceMissingStationSkipped(t *testing.T) {
	srv := apiServer(t, []metarRecord{epkkRecord()})
	defer srv.Close()

	src, err := New(decodeConfig(t,
		"base_url: "+strconvQuote(srv.URL)+"\n"+
			"stations:\n  - id: EPKK\n  - id: EPWA\n"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	em := &fakeEmitter{}
	if err := src.(*Source).pollOnce(context.Background(), em); err != nil {
		t.Fatalf("pollOnce: %v", err)
	}
	if len(em.infos) != 1 || em.infos[0].Key != "epkk" {
		t.Errorf("published %d snapshots, want only EPKK", len(em.infos))
	}
}

// strconvQuote quotes a string with strconv.Quote (avoids embedding raw
// quotes in YAML fragments).
func strconvQuote(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

func TestConditionMapping(t *testing.T) {
	cases := map[string]string{
		"METAR EPKK 242300Z 23005KT 9999 FEW040 11/10 Q1019":        "mainly_clear",
		"METAR EPKK 242300Z 23005KT 9999 SCT040 11/10 Q1019":        "partly_cloudy",
		"METAR EPKK 242300Z 23005KT 9999 BKN040 11/10 Q1019":        "overcast",
		"METAR EPKK 242300Z 23005KT 9999 OVC005 11/10 Q1019":        "overcast",
		"METAR EPKK 242300Z 23005KT 9999 SKC 11/10 Q1019":           "clear",
		"METAR EPKK 242300Z 23005KT 2000 BR OVC005 11/10 Q1019":     "fog",
		"METAR EPKK 242300Z 23005KT 4000 -DZ OVC005 11/10 Q1019":    "drizzle",
		"METAR EPKK 242300Z 23005KT 3000 SN OVC005 M01/M02 Q1019":   "snow",
		"METAR EPKK 242300Z 23005KT 6000 TSRA SCT020CB 11/10 Q1019": "thunderstorm",
		"METAR EPKK 242300Z 23005KT 6000 FZRA OVC005 00/M01 Q1019":  "freezing_rain",
		"METAR EPKK 242300Z 23005KT 8000 SHSN BKN015 M01/M02 Q1019": "snow_showers",
	}
	for raw, want := range cases {
		rec := epkkRecord()
		rec.RawOb = raw
		rec.Clouds = nil
		if got := conditionOf(rec); got != want {
			t.Errorf("raw %q: condition = %q, want %q", raw, got, want)
		}
	}
}

func TestRelativeHumidity(t *testing.T) {
	rh := relativeHumidity(11, 10)
	if rh == nil || *rh < 90 || *rh > 96 {
		t.Errorf("RH(11°C, 10°C) = %v, want ~93%%", rh)
	}
	if rh := relativeHumidity(0, 0); rh == nil || *rh != 100 {
		t.Errorf("RH(0°C, 0°C) = %v, want 100%%", rh)
	}
	if rh := relativeHumidity(0, math.NaN()); rh != nil {
		t.Errorf("NaN dew point must yield nil, got %v", rh)
	}
}

func TestPressureUnits(t *testing.T) {
	rec := epkkRecord()
	rec.Altim = 1019 // hPa already (non-US Q-group)
	snap, err := normalize(rec, "", "UTC", time.Now(), time.Now())
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}
	if p := snap.Current.PressureMSLHpa; p == nil || *p != 1019 {
		t.Errorf("pressure = %v, want 1019 hPa unchanged", p)
	}

	rec.Altim = 29.92 // inHg (US A-group)
	snap, err = normalize(rec, "", "UTC", time.Now(), time.Now())
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}
	if p := snap.Current.PressureMSLHpa; p == nil || *p < 1013 || *p > 1014 {
		t.Errorf("pressure = %v, want ~1013 hPa from 29.92 inHg", p)
	}

	rec.Altim = 30.09
	rec.Wgst = 0 // no gust reported → omitted
	snap, err = normalize(rec, "", "UTC", time.Now(), time.Now())
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}
	if snap.Current.WindGustsKmh != nil {
		t.Errorf("wgst 0 must yield nil gust, got %v", *snap.Current.WindGustsKmh)
	}
}

func TestNormalizeRejects(t *testing.T) {
	rec := epkkRecord()
	rec.IcaoID = ""
	if _, err := normalize(rec, "", "UTC", time.Now(), time.Now()); err == nil {
		t.Error("missing ICAO id accepted")
	}
	rec = epkkRecord()
	rec.Lat, rec.Lon = 0, 0
	if _, err := normalize(rec, "", "UTC", time.Now(), time.Now()); err == nil {
		t.Error("missing position accepted")
	}
}
