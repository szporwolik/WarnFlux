package openmeteo

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func fp(v float64) *float64 { return &v }

func TestEuropeanAQILevelMapping(t *testing.T) {
	cases := []struct {
		aqi  float64
		want int
	}{
		{0, 0}, {19.9, 0}, {20, 1}, {39.9, 1}, {40, 2}, {59.9, 2},
		{60, 3}, {79.9, 3}, {80, 4}, {99.9, 4}, {100, 5}, {150, 5},
	}
	for _, c := range cases {
		if got := europeanAQILevel(fp(c.aqi)); got == nil || *got != c.want {
			t.Errorf("europeanAQILevel(%v) = %v, want %d", c.aqi, got, c.want)
		}
	}
	if got := europeanAQILevel(nil); got != nil {
		t.Errorf("europeanAQILevel(nil) = %v, want nil", got)
	}
}

func TestBuildAirQualityPayload(t *testing.T) {
	resp := &AirQualityResponse{
		Current: &AirQualityCurrent{
			Time:        "2026-09-26T12:00",
			EuropeanAQI: fp(35),
			AQIPM10:     fp(12),
			AQIPM25:     fp(50),
			AQIOzone:    fp(75),
		},
	}
	loc := Location{ID: "skala", Name: "Skała", Latitude: 50.2313, Longitude: 19.8536}
	generated := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	p := buildAirQualityPayload(loc, resp, generated)

	if p.StationCode != "om-aq-skala" {
		t.Errorf("station_code = %q, want om-aq-skala", p.StationCode)
	}
	if p.StationName != "Skała" {
		t.Errorf("station_name = %q, want Skała", p.StationName)
	}
	if p.IndexLevelID == nil || *p.IndexLevelID != 1 {
		t.Errorf("index_level_id = %v, want 1", p.IndexLevelID)
	}
	if p.IndexLevelName != "Dobry" {
		t.Errorf("index_level_name = %q, want Dobry", p.IndexLevelName)
	}
	if len(p.Pollutants) != 3 {
		t.Fatalf("pollutants = %d, want 3", len(p.Pollutants))
	}
	want := map[string]string{"PM10": "Bardzo dobry", "PM2.5": "Umiarkowany", "O3": "Dostateczny"}
	for _, pol := range p.Pollutants {
		if pol.LevelName != want[pol.Code] {
			t.Errorf("%s level = %q, want %q", pol.Code, pol.LevelName, want[pol.Code])
		}
	}

	data, err := json.Marshal(p)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var roundtrip map[string]any
	if err := json.Unmarshal(data, &roundtrip); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if roundtrip["station_code"] != "om-aq-skala" || roundtrip["latitude"] != 50.2313 {
		t.Errorf("wire shape mismatch: %v", roundtrip)
	}
}

func TestFetchAirQualityRequestParameters(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/air-quality" {
			t.Errorf("path = %q, want /air-quality", r.URL.Path)
		}
		q := r.URL.Query()
		if q.Get("latitude") != "50.2313" || q.Get("longitude") != "19.8536" {
			t.Errorf("coordinates = (%s, %s), want (50.2313, 19.8536)", q.Get("latitude"), q.Get("longitude"))
		}
		if q.Get("timezone") != "auto" {
			t.Errorf("timezone = %q, want auto", q.Get("timezone"))
		}
		if q.Get("apikey") != "" {
			t.Errorf("apikey must be absent without a configured key")
		}
		if !strings.Contains(q.Get("current"), "european_aqi") {
			t.Errorf("current = %q, want european_aqi included", q.Get("current"))
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"latitude":50.2,"longitude":19.9,"elevation":300,"timezone":"Europe/Warsaw","current":{"time":"2026-09-26T12:00","european_aqi":35,"pm10":12.5}}`)
	}))
	defer srv.Close()

	c := NewClient(srv.URL+"/forecast", "", time.Second)
	resp, err := c.FetchAirQuality(context.Background(), Location{ID: "skala", Latitude: 50.2313, Longitude: 19.8536})
	if err != nil {
		t.Fatalf("FetchAirQuality: %v", err)
	}
	if resp.Current == nil || resp.Current.EuropeanAQI == nil || *resp.Current.EuropeanAQI != 35 {
		t.Errorf("response = %+v, want european_aqi 35", resp.Current)
	}
}

func TestAirQualityURL(t *testing.T) {
	cases := []struct{ in, want string }{
		{defaultBaseURL, defaultAirQualityBaseURL},
		{customerBaseURL, customerAirQualityBaseURL},
		{"http://127.0.0.1:9999/forecast", "http://127.0.0.1:9999/air-quality"},
	}
	for _, c := range cases {
		if got := airQualityURL(c.in); got != c.want {
			t.Errorf("airQualityURL(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestLocAirQualityOverride(t *testing.T) {
	if !locAirQuality(Location{ID: "home"}) {
		t.Error("unset override must default to enabled")
	}
	off := false
	on := true
	if locAirQuality(Location{ID: "home", AirQuality: &off}) {
		t.Error("explicit false must disable the AQ companion")
	}
	if !locAirQuality(Location{ID: "home", AirQuality: &on}) {
		t.Error("explicit true must enable the AQ companion")
	}
}
