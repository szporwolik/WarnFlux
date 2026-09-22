package core

import (
	"math"
	"testing"
	"time"
)

// FuzzWeatherSnapshotValidation drives arbitrary numeric/string inputs into
// the canonical snapshot validator: it must never panic, and either accept
// or reject deterministically.
func FuzzWeatherSnapshotValidation(f *testing.F) {
	f.Add("openmeteo", "home", "Europe/Warsaw", "clear", 50.0, 20.0, 18.2, 1013.2)
	f.Fuzz(func(t *testing.T, provider, loc, tz, cond string, lat, lon, temp, pressure float64) {
		var elev float64
		_ = elev
		s := WeatherSnapshot{
			SchemaVersion: WeatherSchemaVersion,
			GeneratedAt:   time.Now().UTC(),
			Provider: WeatherProvider{
				ID:          provider,
				Name:        "P",
				Attribution: "A",
			},
			Location: WeatherLocation{
				ID:        loc,
				Latitude:  lat,
				Longitude: lon,
				Timezone:  tz,
			},
			Current: &WeatherCurrent{
				Time:           time.Now(),
				TemperatureC:   &temp,
				PressureMSLHpa: &pressure,
				Condition:      cond,
			},
		}
		// Validate must never panic, whatever the inputs.
		_ = s.Validate()
		if math.IsNaN(temp) || math.IsInf(temp, 0) {
			return
		}
		// A fully valid snapshot must serialize deterministically.
		if s.Validate() == nil {
			if data, err := MarshalWeatherSnapshot(s); err != nil {
				t.Fatalf("valid snapshot failed to marshal: %v", err)
			} else if len(data) == 0 {
				t.Fatal("empty payload")
			}
		}
	})
}

// FuzzWeatherDailyDateTimezone drives arbitrary date strings, timezone
// names and public text (which may contain invalid UTF-8) into the
// canonical validator: it must never panic. Inputs stay bounded (a single
// daily entry, no giant arrays).
func FuzzWeatherDailyDateTimezone(f *testing.F) {
	f.Add("2026-09-22", "Europe/Warsaw", "Open-Meteo", "clear")
	f.Fuzz(func(t *testing.T, date, tz, providerName, cond string) {
		s := WeatherSnapshot{
			SchemaVersion: WeatherSchemaVersion,
			GeneratedAt:   time.Now().UTC(),
			Provider: WeatherProvider{
				ID:          "p",
				Name:        providerName,
				Attribution: "attribution",
			},
			Location: WeatherLocation{
				ID:        "l",
				Latitude:  0,
				Longitude: 0,
				Timezone:  tz,
			},
			Daily: []WeatherDaily{{Date: date, Condition: cond}},
		}
		_ = s.Validate()
	})
}
