package openmeteo

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/core"
)

// validResponseJSON is a realistic provider fixture (Europe/Warsaw, two
// hourly entries, two daily entries).
func validResponseJSON() string {
	return `{
  "latitude": 50.0, "longitude": 20.0, "elevation": 250.0,
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
	if resp.Latitude != 50.0 || resp.Longitude != 20.0 || resp.Elevation != 250.0 {
		t.Errorf("coordinates/elevation = (%v, %v, %v)", resp.Latitude, resp.Longitude, resp.Elevation)
	}
	if resp.Timezone != "Europe/Warsaw" || resp.UTCOffsetSeconds != 7200 {
		t.Errorf("timezone info = (%q, %d)", resp.Timezone, resp.UTCOffsetSeconds)
	}
	if *resp.Current.Temperature2m != 18.2 || *resp.Current.WeatherCode != 2 || *resp.Current.IsDay != 1 {
		t.Errorf("current = temp %v code %v is_day %v", *resp.Current.Temperature2m, *resp.Current.WeatherCode, *resp.Current.IsDay)
	}
	if len(resp.Hourly.Time) != 2 || len(resp.Daily.Time) != 2 {
		t.Errorf("forecast lengths = (%d, %d)", len(resp.Hourly.Time), len(resp.Daily.Time))
	}
	if *resp.Hourly.PrecipitationProbability[1] != 20 {
		t.Errorf("hourly precipitation_probability[1] = %v", *resp.Hourly.PrecipitationProbability[1])
	}
	if resp.Daily.Sunrise[0] != "2026-09-22T06:24" {
		t.Errorf("sunrise[0] = %q", resp.Daily.Sunrise[0])
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
		{"hourly mismatch", hourlyMismatchJSON()},
		{"daily mismatch", dailyMismatchJSON()},
	}
	for _, c := range cases {
		if _, err := parseResponse([]byte(c.json)); err == nil {
			t.Errorf("%s: expected rejection, got nil", c.name)
		}
	}
}

func hourlyMismatchJSON() string {
	base := validResponseJSON()
	// Drop one temperature entry while time keeps two.
	return strings.Replace(base, `"temperature_2m": [18.5, 18.1]`, `"temperature_2m": [18.5]`, 1)
}

func dailyMismatchJSON() string {
	base := validResponseJSON()
	// Drop one sunrise entry while time keeps two.
	return strings.Replace(base, `"sunrise": ["2026-09-22T06:24", "2026-09-23T06:26"]`, `"sunrise": ["2026-09-22T06:24"]`, 1)
}

func TestCheckFiniteRejectsNonFinite(t *testing.T) {
	resp := parseFixture(t)
	nan := math.NaN()
	resp.Current.WindSpeed = &nan
	if err := checkFiniteResponse(resp); err == nil {
		t.Error("NaN current value must be rejected")
	}
	resp = parseFixture(t)
	inf := math.Inf(1)
	resp.Hourly.WindGusts[0] = &inf
	if err := checkFiniteResponse(resp); err == nil {
		t.Error("Inf hourly value must be rejected")
	}
}

func TestBuildWeatherMessage(t *testing.T) {
	resp := parseFixture(t)
	generated := time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)
	msg, err := buildWeatherMessage(Location{ID: "home", Name: "Home", Latitude: 50, Longitude: 20}, resp, generated)
	if err != nil {
		t.Fatalf("buildWeatherMessage: %v", err)
	}
	if msg.Source != "openmeteo" || msg.Key != "home" || msg.Kind != "weather" {
		t.Errorf("identity = (%s, %s, %s)", msg.Source, msg.Key, msg.Kind)
	}
	if msg.GeneratedAt != generated {
		t.Errorf("GeneratedAt = %v, want %v", msg.GeneratedAt, generated)
	}

	var wire map[string]any
	if err := json.Unmarshal(msg.Payload, &wire); err != nil {
		t.Fatalf("payload is not valid JSON: %v", err)
	}
	if wire["schema_version"] != float64(1) {
		t.Errorf("schema_version = %v", wire["schema_version"])
	}
	if wire["type"] != "weather" || wire["source"] != "openmeteo" {
		t.Errorf("type/source = (%v, %v)", wire["type"], wire["source"])
	}
	if wire["attribution"] != "Weather data by Open-Meteo" {
		t.Errorf("attribution = %v", wire["attribution"])
	}
	if wire["generated_at"] != "2026-09-22T12:00:00Z" {
		t.Errorf("generated_at = %v", wire["generated_at"])
	}

	loc := wire["location"].(map[string]any)
	if loc["id"] != "home" || loc["name"] != "Home" || loc["timezone"] != "Europe/Warsaw" || loc["elevation_m"] != float64(250) {
		t.Errorf("location = %v", loc)
	}

	// Offset-aware RFC3339 conversion: 14:00 local in Europe/Warsaw (UTC+2)
	// must be published with its offset, never as a naive time.
	current := wire["current"].(map[string]any)
	if current["time"] != "2026-09-22T14:00:00+02:00" {
		t.Errorf("current.time = %v, want offset-aware RFC3339", current["time"])
	}
	// The provider's 0/1 is_day becomes a JSON boolean on the wire.
	if current["is_day"] != true {
		t.Errorf("current.is_day = %v, want true", current["is_day"])
	}

	hourly := wire["hourly"].([]any)
	if len(hourly) != 2 {
		t.Fatalf("hourly entries = %d, want 2", len(hourly))
	}
	if hourly[0].(map[string]any)["time"] != "2026-09-22T15:00:00+02:00" {
		t.Errorf("hourly[0].time = %v", hourly[0].(map[string]any)["time"])
	}

	daily := wire["daily"].([]any)
	if len(daily) != 2 {
		t.Fatalf("daily entries = %d, want 2", len(daily))
	}
	d0 := daily[0].(map[string]any)
	if d0["date"] != "2026-09-22" {
		t.Errorf("daily[0].date = %v", d0["date"])
	}
	if d0["sunrise"] != "2026-09-22T06:24:00+02:00" || d0["sunset"] != "2026-09-22T18:37:00+02:00" {
		t.Errorf("sunrise/sunset = (%v, %v), want offset-aware RFC3339", d0["sunrise"], d0["sunset"])
	}
}

func TestBuildWeatherMessageInvalidTimezone(t *testing.T) {
	resp := parseFixture(t)
	resp.Timezone = "Mars/Olympus"
	_, err := buildWeatherMessage(Location{ID: "home", Latitude: 50, Longitude: 20}, resp, time.Now())
	if err == nil {
		t.Fatal("invalid provider timezone must fail the location")
	}
}

func TestWeatherPayloadStaysBounded(t *testing.T) {
	// A full 48-hour / 7-day synthetic response must stay far below the
	// 256 KiB information payload cap.
	hours := 48
	days := 7
	resp := parseFixture(t)
	times := make([]string, hours)
	temps := make([]*float64, hours)
	for i := 0; i < hours; i++ {
		times[i] = "2026-09-22T00:00"
		t := 10.0
		temps[i] = &t
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
	codes := make([]*int, hours)
	for i := range codes {
		c := 2
		codes[i] = &c
	}
	resp.Hourly.WeatherCode = codes

	dates := make([]string, days)
	sun := make([]string, days)
	for i := 0; i < days; i++ {
		dates[i] = "2026-09-22"
		sun[i] = "2026-09-22T06:24"
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

	msg, err := buildWeatherMessage(Location{ID: "home", Latitude: 50, Longitude: 20}, resp, time.Now())
	if err != nil {
		t.Fatalf("buildWeatherMessage: %v", err)
	}
	if len(msg.Payload) > core.MaxInformationPayloadBytes {
		t.Errorf("payload = %d bytes, exceeds %d", len(msg.Payload), core.MaxInformationPayloadBytes)
	}
}
