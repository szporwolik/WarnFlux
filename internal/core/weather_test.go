package core

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"
)

func baseWeatherSnapshot() WeatherSnapshot {
	temp := 18.2
	code := "2"
	day := true
	return WeatherSnapshot{
		SchemaVersion: 1,
		GeneratedAt:   time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC),
		Provider: WeatherProvider{
			ID:          "openmeteo",
			Name:        "Open-Meteo",
			Attribution: "Weather data by Open-Meteo",
		},
		Location: WeatherLocation{
			ID:        "home",
			Name:      "Home",
			Latitude:  50,
			Longitude: 20,
			Timezone:  "Europe/Warsaw",
		},
		Current: &WeatherCurrent{
			Time:                  time.Date(2026, 9, 22, 14, 0, 0, 0, time.FixedZone("CEST", 2*3600)),
			TemperatureC:          &temp,
			Condition:             ConditionPartlyCloudy,
			ProviderConditionCode: &code,
			IsDay:                 &day,
		},
	}
}

func TestWeatherSnapshotValidateAccepts(t *testing.T) {
	if err := baseWeatherSnapshot().Validate(); err != nil {
		t.Fatalf("valid snapshot rejected: %v", err)
	}
	// Current-only (observation-only providers) is valid.
	obs := baseWeatherSnapshot()
	if err := obs.Validate(); err != nil {
		t.Errorf("current-only snapshot rejected: %v", err)
	}
	// Forecast-only (no current) is valid.
	fc := baseWeatherSnapshot()
	fc.Current = nil
	fc.Hourly = []WeatherHourly{{Time: time.Now(), Condition: ConditionClear}}
	if err := fc.Validate(); err != nil {
		t.Errorf("forecast-only snapshot rejected: %v", err)
	}
}

func TestWeatherSnapshotValidateRejects(t *testing.T) {
	cases := []struct {
		name string
		mut  func(*WeatherSnapshot)
	}{
		{"unsupported schema", func(s *WeatherSnapshot) { s.SchemaVersion = 2 }},
		{"zero generated", func(s *WeatherSnapshot) { s.GeneratedAt = time.Time{} }},
		{"valid_until before generated", func(s *WeatherSnapshot) {
			t := s.GeneratedAt.Add(-time.Minute)
			s.ValidUntil = &t
		}},
		{"empty provider id", func(s *WeatherSnapshot) { s.Provider.ID = "" }},
		{"uppercase provider id", func(s *WeatherSnapshot) { s.Provider.ID = "OpenMeteo" }},
		{"empty provider name", func(s *WeatherSnapshot) { s.Provider.Name = "" }},
		{"empty attribution", func(s *WeatherSnapshot) { s.Provider.Attribution = "" }},
		{"empty location id", func(s *WeatherSnapshot) { s.Location.ID = "" }},
		{"invalid latitude", func(s *WeatherSnapshot) { s.Location.Latitude = 91 }},
		{"NaN longitude", func(s *WeatherSnapshot) { s.Location.Longitude = math.NaN() }},
		{"empty timezone", func(s *WeatherSnapshot) { s.Location.Timezone = "" }},
		{"empty snapshot", func(s *WeatherSnapshot) {
			s.Current = nil
			s.Hourly = nil
			s.Daily = nil
		}},
		{"invalid condition", func(s *WeatherSnapshot) { s.Current.Condition = "hurricane" }},
		{"non-finite current", func(s *WeatherSnapshot) {
			v := math.Inf(1)
			s.Current.TemperatureC = &v
		}},
		{"humidity over 100", func(s *WeatherSnapshot) {
			v := 101.0
			s.Current.RelativeHumidityPct = &v
		}},
		{"wind direction negative", func(s *WeatherSnapshot) {
			v := -1.0
			s.Current.WindDirectionDeg = &v
		}},
		{"hourly unsorted", func(s *WeatherSnapshot) {
			now := time.Now()
			s.Current = nil
			s.Hourly = []WeatherHourly{
				{Time: now, Condition: ConditionClear},
				{Time: now.Add(-time.Hour), Condition: ConditionClear},
			}
		}},
		{"hourly duplicate time", func(s *WeatherSnapshot) {
			now := time.Now()
			s.Current = nil
			s.Hourly = []WeatherHourly{
				{Time: now, Condition: ConditionClear},
				{Time: now, Condition: ConditionClear},
			}
		}},
		{"daily duplicate date", func(s *WeatherSnapshot) {
			s.Current = nil
			s.Daily = []WeatherDaily{
				{Date: "2026-09-22", Condition: ConditionClear},
				{Date: "2026-09-22", Condition: ConditionClear},
			}
		}},
		{"daily unsorted", func(s *WeatherSnapshot) {
			s.Current = nil
			s.Daily = []WeatherDaily{
				{Date: "2026-09-23", Condition: ConditionClear},
				{Date: "2026-09-22", Condition: ConditionClear},
			}
		}},
	}
	for _, c := range cases {
		s := baseWeatherSnapshot()
		c.mut(&s)
		if err := s.Validate(); err == nil {
			t.Errorf("%s: expected rejection, got nil", c.name)
		}
	}
}

// TestMarshalWeatherSnapshotGolden pins the canonical weather JSON as the
// public contract every provider must match.
func TestMarshalWeatherSnapshotGolden(t *testing.T) {
	s := baseWeatherSnapshot()
	until := s.GeneratedAt.Add(30 * time.Minute)
	s.ValidUntil = &until
	s.Hourly = []WeatherHourly{{
		Time:         time.Date(2026, 9, 22, 15, 0, 0, 0, time.FixedZone("CEST", 2*3600)),
		Condition:    ConditionPartlyCloudy,
		TemperatureC: fptr(18.5),
		WindSpeedKmh: fptr(11.6),
	}}
	s.Daily = []WeatherDaily{{
		Date:            "2026-09-22",
		Condition:       ConditionPartlyCloudy,
		TemperatureMaxC: fptr(20.4),
		TemperatureMinC: fptr(10.2),
	}}

	data, err := MarshalWeatherSnapshot(s)
	if err != nil {
		t.Fatalf("MarshalWeatherSnapshot: %v", err)
	}

	want := `{"schema_version":1,"type":"weather","generated_at":"2026-09-22T12:00:00Z",` +
		`"valid_until":"2026-09-22T12:30:00Z",` +
		`"provider":{"id":"openmeteo","name":"Open-Meteo","attribution":"Weather data by Open-Meteo"},` +
		`"location":{"id":"home","name":"Home","latitude":50,"longitude":20,"timezone":"Europe/Warsaw"},` +
		`"current":{"time":"2026-09-22T14:00:00+02:00","temperature_c":18.2,"condition":"partly_cloudy","provider_condition_code":"2","is_day":true},` +
		`"hourly":[{"time":"2026-09-22T15:00:00+02:00","temperature_c":18.5,"wind_speed_kmh":11.6,"condition":"partly_cloudy"}],` +
		`"daily":[{"date":"2026-09-22","condition":"partly_cloudy","temperature_max_c":20.4,"temperature_min_c":10.2}]}`
	if string(data) != want {
		t.Errorf("golden JSON mismatch:\n got %s\nwant %s", data, want)
	}
}

func fptr(v float64) *float64 { return &v }

// TestNewWeatherInformation builds the standard envelope: source, key and
// kind are derived from the snapshot; the producer is stamped upstream.
func TestNewWeatherInformation(t *testing.T) {
	s := baseWeatherSnapshot()
	msg, err := NewWeatherInformation("weather-home", s)
	if err != nil {
		t.Fatalf("NewWeatherInformation: %v", err)
	}
	if msg.Source != "openmeteo" || msg.ProducerID != "weather-home" ||
		msg.Key != "home" || msg.Kind != "weather" {
		t.Errorf("envelope = %+v", msg)
	}
	if msg.GeneratedAt != s.GeneratedAt {
		t.Errorf("GeneratedAt = %v, want %v", msg.GeneratedAt, s.GeneratedAt)
	}
	var wire map[string]any
	if err := json.Unmarshal(msg.Payload, &wire); err != nil {
		t.Fatalf("payload is not valid JSON: %v", err)
	}
	if wire["type"] != "weather" {
		t.Errorf("type = %v", wire["type"])
	}
	if strings.Contains(string(msg.Payload), "temperature_2m") {
		t.Error("canonical payload must not contain provider field names")
	}
}

// TestMarshalWeatherSnapshotValidatesBeforeEncoding: serialization must
// never emit invalid snapshots.
func TestMarshalWeatherSnapshotValidatesBeforeEncoding(t *testing.T) {
	s := baseWeatherSnapshot()
	s.Location.Timezone = ""
	if _, err := MarshalWeatherSnapshot(s); err == nil {
		t.Fatal("invalid snapshot must not be serialized")
	}
}
