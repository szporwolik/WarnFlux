package openmeteo

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/core"
)

// validResponseJSON is a realistic provider fixture (Europe/Warsaw, two
// hourly entries, two daily entries).
func validResponseJSON() string {
	return `{
  "latitude": 50.003273, "longitude": 20.00447, "elevation": 250.0,
  "timezone": "Europe/Warsaw", "utc_offset_seconds": 7200,
  "current": {
    "time": "2026-09-22T14:00", "interval": 900,
    "temperature_2m": 18.2, "relative_humidity_2m": 71,
    "apparent_temperature": 17.5, "is_day": 1,
    "precipitation": 0.0, "rain": 0.0, "showers": 0.0, "snowfall": 0.0,
    "weather_code": 2, "cloud_cover": 42,
    "pressure_msl": 1017.4, "surface_pressure": 986.2,
    "wind_speed_10m": 12.1, "wind_direction_10m": 245, "wind_gusts_10m": 21.0
  },
  "hourly": {
    "time": ["2026-09-22T15:00", "2026-09-22T16:00"],
    "temperature_2m": [18.5, 18.1], "relative_humidity_2m": [68, 69],
    "apparent_temperature": [17.9, 17.4], "precipitation_probability": [10, 20],
    "precipitation": [0.0, 0.1], "weather_code": [2, 3], "cloud_cover": [38, 45],
    "pressure_msl": [1017.1, 1017.0], "wind_speed_10m": [11.6, 11.0],
    "wind_direction_10m": [248, 250], "wind_gusts_10m": [20.4, 19.8]
  },
  "daily": {
    "time": ["2026-09-22", "2026-09-23"],
    "weather_code": [2, 3], "temperature_2m_max": [20.4, 21.0],
    "temperature_2m_min": [10.2, 10.8], "apparent_temperature_max": [19.8, 20.1],
    "apparent_temperature_min": [9.1, 9.6], "precipitation_probability_max": [20, 30],
    "precipitation_sum": [0.3, 0.5], "wind_speed_10m_max": [21.0, 18.4],
    "wind_gusts_10m_max": [35.0, 30.2], "wind_direction_10m_dominant": [240, 210],
    "sunrise": ["2026-09-22T06:24", "2026-09-23T06:26"],
    "sunset": ["2026-09-22T18:37", "2026-09-23T18:35"]
  }
}`
}

func parseFixture(t *testing.T) *ProviderResponse {
	t.Helper()
	resp, err := parseResponse([]byte(validResponseJSON()))
	if err != nil {
		t.Fatalf("parseResponse: %v", err)
	}
	return resp
}

func TestParseResponseFixture(t *testing.T) {
	resp := parseFixture(t)
	if resp.Timezone != "Europe/Warsaw" || resp.UTCOffsetSeconds != 7200 {
		t.Errorf("timezone info = (%q, %d)", resp.Timezone, resp.UTCOffsetSeconds)
	}
	if *resp.Current.Temperature2m != 18.2 || *resp.Current.WeatherCode != 2 || *resp.Current.IsDay != 1 {
		t.Errorf("current = temp %v code %v is_day %v", *resp.Current.Temperature2m, *resp.Current.WeatherCode, *resp.Current.IsDay)
	}
	if len(resp.Hourly.Time) != 2 || len(resp.Daily.Time) != 2 {
		t.Errorf("forecast lengths = (%d, %d)", len(resp.Hourly.Time), len(resp.Daily.Time))
	}
}

func TestParseResponseRejects(t *testing.T) {
	cases := []struct {
		name string
		json string
	}{
		{"invalid JSON", `{nope`},
		{"missing timezone", `{"current": {"time": "2026-09-22T14:00", "temperature_2m": 1, "weather_code": 2}}`},
		{"missing current", `{"timezone": "UTC"}`},
		{"missing current time", `{"timezone": "UTC", "current": {"temperature_2m": 1, "weather_code": 2}}`},
		{"missing temperature", `{"timezone": "UTC", "current": {"time": "2026-09-22T14:00", "weather_code": 2}}`},
		{"missing weather code", `{"timezone": "UTC", "current": {"time": "2026-09-22T14:00", "temperature_2m": 1}}`},
		{"hourly mismatch", strings.Replace(validResponseJSON(), `"temperature_2m": [18.5, 18.1]`, `"temperature_2m": [18.5]`, 1)},
		{"daily mismatch", strings.Replace(validResponseJSON(), `"sunrise": ["2026-09-22T06:24", "2026-09-23T06:26"]`, `"sunrise": ["2026-09-22T06:24"]`, 1)},
	}
	for _, c := range cases {
		if _, err := parseResponse([]byte(c.json)); err == nil {
			t.Errorf("%s: expected rejection, got nil", c.name)
		}
	}
}

// TestWMOExhaustiveMapping covers every documented Open-Meteo WMO code and
// unknown codes: unknown codes map to "unknown" and never crash.
func TestWMOExhaustiveMapping(t *testing.T) {
	expected := map[int]string{
		0: core.ConditionClear, 1: core.ConditionMainlyClear,
		2: core.ConditionPartlyCloudy, 3: core.ConditionOvercast,
		45: core.ConditionFog, 48: core.ConditionFog,
		51: core.ConditionDrizzle, 53: core.ConditionDrizzle, 55: core.ConditionDrizzle,
		56: core.ConditionFreezingDrizzle, 57: core.ConditionFreezingDrizzle,
		61: core.ConditionRain, 63: core.ConditionRain, 65: core.ConditionRain,
		66: core.ConditionFreezingRain, 67: core.ConditionFreezingRain,
		71: core.ConditionSnow, 73: core.ConditionSnow, 75: core.ConditionSnow,
		77: core.ConditionSnowGrains,
		80: core.ConditionShowers, 81: core.ConditionShowers, 82: core.ConditionShowers,
		85: core.ConditionSnowShowers, 86: core.ConditionSnowShowers,
		95: core.ConditionThunderstorm,
		96: core.ConditionThunderstormHail, 99: core.ConditionThunderstormHail,
	}
	for code, want := range expected {
		got, ok := conditionForWMO(code)
		if !ok || got != want {
			t.Errorf("code %d → (%q, %v), want (%q, true)", code, got, ok, want)
		}
	}
	for _, code := range []int{4, 42, 100, 999} {
		got, ok := conditionForWMO(code)
		if got != core.ConditionUnknown || ok {
			t.Errorf("unknown code %d → (%q, %v), want (unknown, false)", code, got, ok)
		}
	}
}

func TestNormalizeProducesCanonicalSnapshot(t *testing.T) {
	resp := parseFixture(t)
	generated := time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)
	snap, err := Normalize(Location{ID: "home", Name: "Home", Latitude: 50, Longitude: 20}, resp, generated)
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	if snap.Provider.ID != "openmeteo" || snap.Provider.Name != "Open-Meteo" ||
		snap.Provider.Attribution != "Weather data by Open-Meteo" {
		t.Errorf("provider = %+v", snap.Provider)
	}
	if snap.Location.ID != "home" || snap.Location.Timezone != "Europe/Warsaw" ||
		snap.Location.ElevationM == nil || *snap.Location.ElevationM != 250 {
		t.Errorf("location = %+v", snap.Location)
	}
	if snap.Current == nil {
		t.Fatal("current missing")
	}
	if snap.Current.Condition != core.ConditionPartlyCloudy {
		t.Errorf("condition = %q, want partly_cloudy", snap.Current.Condition)
	}
	if snap.Current.ProviderConditionCode == nil || *snap.Current.ProviderConditionCode != "2" {
		t.Errorf("provider_condition_code = %v, want 2", snap.Current.ProviderConditionCode)
	}
	want, _ := time.Parse(time.RFC3339, "2026-09-22T14:00:00+02:00")
	if !snap.Current.Time.Equal(want) {
		t.Errorf("current time = %v, want %v", snap.Current.Time, want)
	}
	if snap.Current.IsDay == nil || !*snap.Current.IsDay {
		t.Error("is_day must be true")
	}
	if len(snap.Hourly) != 2 || len(snap.Daily) != 2 {
		t.Errorf("forecast lengths = (%d, %d)", len(snap.Hourly), len(snap.Daily))
	}
	if snap.Daily[0].Sunrise == nil || snap.Daily[0].Sunrise.Format(time.RFC3339) != "2026-09-22T06:24:00+02:00" {
		t.Errorf("sunrise = %v", snap.Daily[0].Sunrise)
	}
}

func TestNormalizeInvalidTimezone(t *testing.T) {
	resp := parseFixture(t)
	resp.Timezone = "Mars/Olympus"
	if _, err := Normalize(Location{ID: "home", Latitude: 50, Longitude: 20}, resp, time.Now()); err == nil {
		t.Fatal("invalid provider timezone must fail")
	}
}

// TestCanonicalWeatherPayload proves the wire payload is the canonical
// provider-neutral schema: no Open-Meteo field names leak through.
func TestCanonicalWeatherPayload(t *testing.T) {
	resp := parseFixture(t)
	snap, err := Normalize(Location{ID: "home", Latitude: 50, Longitude: 20}, resp, time.Now())
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	msg, err := core.NewWeatherInformation("", snap)
	if err != nil {
		t.Fatalf("NewWeatherInformation: %v", err)
	}
	if msg.Source != "openmeteo" || msg.Key != "home" || msg.Kind != "weather" {
		t.Errorf("envelope = (%s, %s, %s)", msg.Source, msg.Key, msg.Kind)
	}
	var wire map[string]any
	if err := json.Unmarshal(msg.Payload, &wire); err != nil {
		t.Fatalf("payload: %v", err)
	}
	if wire["schema_version"] != float64(1) || wire["type"] != "weather" {
		t.Errorf("schema = (%v, %v)", wire["schema_version"], wire["type"])
	}
	for _, forbidden := range []string{"temperature_2m", "weather_code", "current_units", "hourly_units"} {
		if strings.Contains(string(msg.Payload), forbidden) {
			t.Errorf("payload leaks provider field name %q", forbidden)
		}
	}
	prov := wire["provider"].(map[string]any)
	if prov["id"] != "openmeteo" || prov["attribution"] != "Weather data by Open-Meteo" {
		t.Errorf("provider = %v", prov)
	}
	current := wire["current"].(map[string]any)
	if current["condition"] != "partly_cloudy" || current["provider_condition_code"] != "2" {
		t.Errorf("current = %v", current)
	}
}

// TestWeatherPayloadBounded covers the full 48h/7d forecast staying below
// the information payload cap.
func TestWeatherPayloadBounded(t *testing.T) {
	hours, days := 48, 7
	resp := parseFixture(t)
	times := make([]string, hours)
	temps := make([]*float64, hours)
	codes := make([]*int, hours)
	base := time.Date(2026, 9, 22, 0, 0, 0, 0, time.UTC)
	for i := 0; i < hours; i++ {
		times[i] = base.Add(time.Duration(i) * time.Hour).Format("2006-01-02T15:04")
		v := 10.0
		temps[i] = &v
		c := 2
		codes[i] = &c
	}
	resp.Hourly.Time = times
	resp.Hourly.Temperature2m = temps
	resp.Hourly.RelativeHumidity = temps
	resp.Hourly.ApparentTemp = temps
	resp.Hourly.PrecipitationProbability = temps
	resp.Hourly.Precipitation = temps
	resp.Hourly.CloudCover = temps
	resp.Hourly.PressureMSL = temps
	resp.Hourly.WindSpeed = temps
	resp.Hourly.WindDirection = temps
	resp.Hourly.WindGusts = temps
	resp.Hourly.WeatherCode = codes

	dates := make([]string, days)
	sun := make([]string, days)
	for i := 0; i < days; i++ {
		dates[i] = base.AddDate(0, 0, i).Format("2006-01-02")
		sun[i] = base.AddDate(0, 0, i).Format("2006-01-02") + "T06:24"
	}
	resp.Daily.Time = dates
	resp.Daily.WeatherCode = codes[:days]
	resp.Daily.TemperatureMax = temps[:days]
	resp.Daily.TemperatureMin = temps[:days]
	resp.Daily.ApparentTempMax = temps[:days]
	resp.Daily.ApparentTempMin = temps[:days]
	resp.Daily.PrecipProbabilityMax = temps[:days]
	resp.Daily.PrecipitationSum = temps[:days]
	resp.Daily.WindSpeedMax = temps[:days]
	resp.Daily.WindGustsMax = temps[:days]
	resp.Daily.WindDirectionDominant = temps[:days]
	resp.Daily.Sunrise = sun
	resp.Daily.Sunset = sun

	snap, err := Normalize(Location{ID: "home", Latitude: 50, Longitude: 20}, resp, time.Now())
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	msg, err := core.NewWeatherInformation("", snap)
	if err != nil {
		t.Fatalf("NewWeatherInformation: %v", err)
	}
	if len(msg.Payload) > core.MaxInformationPayloadBytes {
		t.Errorf("payload = %d bytes, exceeds %d", len(msg.Payload), core.MaxInformationPayloadBytes)
	}
}
