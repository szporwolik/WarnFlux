// Package weatherreport aggregates current weather readings and
// forecasts from every available source into one short region summary
// for the public radio /weather command. It is built for EMCOM
// conditions: the source set ranges from many (internet providers plus
// lots of stations) down to zero, and in the worst case a single
// reading is better than none — the aggregation therefore degrades
// gracefully instead of refusing to answer.
package weatherreport

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

// Freshness windows: current readings stay usable for a few hours (APRS
// weather beacons are sparse and providers refresh hourly); forecasts
// age much more slowly.
const (
	MaxReadingAge  = 3 * time.Hour
	MaxForecastAge = 48 * time.Hour
)

// Reading is one current observation from one station/provider. The
// presence flags matter: a valid measurement can be zero (0°C).
type Reading struct {
	TempC   float64
	HumPct  float64
	HasTemp bool
	HasHum  bool
	At      time.Time
	Source  string
}

// Forecast is one provider/location's next-day prediction.
type Forecast struct {
	TempMaxC float64
	TempMinC float64
	HasMax   bool
	HasMin   bool
	At       time.Time
	Source   string
}

// Report is the aggregated region summary.
type Report struct {
	TempC        float64
	HumPct       float64
	HasTemp      bool
	HasHum       bool
	Stations     int
	ForecastMaxC float64
	ForecastMinC float64
	HasForecast  bool
}

// Summarize aggregates readings and forecasts as of now. Gross errors
// are rejected with a median/MAD filter once at least three values are
// available; with one or two values everything passes (EMCOM: one
// reading is better than none).
func Summarize(now time.Time, readings []Reading, forecasts []Forecast) Report {
	var temps, hums []float64
	stations := 0
	for _, r := range readings {
		if now.Sub(r.At) > MaxReadingAge {
			continue
		}
		used := false
		if r.HasTemp && r.TempC > -60 && r.TempC < 60 {
			temps = append(temps, r.TempC)
			used = true
		}
		if r.HasHum && r.HumPct > 0 && r.HumPct <= 100 {
			hums = append(hums, r.HumPct)
			used = true
		}
		if used {
			stations++
		}
	}
	var rep Report
	if m, n := robustMean(temps); n > 0 {
		rep.TempC = m
		rep.HasTemp = true
	}
	if m, n := robustMean(hums); n > 0 {
		rep.HumPct = m
		rep.HasHum = true
	}
	rep.Stations = stations

	var maxs, mins []float64
	for _, f := range forecasts {
		if now.Sub(f.At) > MaxForecastAge {
			continue
		}
		if f.HasMax && f.HasMin && f.TempMaxC > -60 && f.TempMaxC < 60 &&
			f.TempMinC > -60 && f.TempMinC < 60 {
			maxs = append(maxs, f.TempMaxC)
			mins = append(mins, f.TempMinC)
		}
	}
	if m, n := robustMean(maxs); n > 0 {
		if m2, n2 := robustMean(mins); n2 > 0 {
			rep.ForecastMaxC = m
			rep.ForecastMinC = m2
			rep.HasForecast = true
		}
	}
	return rep
}

// robustMean averages values after dropping gross outliers (values more
// than 3 MADs off the median). With fewer than three values every value
// counts; if the filter would drop everything, the median survives —
// EMCOM: one value is better than none.
func robustMean(values []float64) (float64, int) {
	if len(values) == 0 {
		return 0, 0
	}
	if len(values) < 3 {
		sum := 0.0
		for _, v := range values {
			sum += v
		}
		return sum / float64(len(values)), len(values)
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	median := sorted[len(sorted)/2]
	devs := make([]float64, len(sorted))
	for i, v := range sorted {
		devs[i] = math.Abs(v - median)
	}
	sort.Float64s(devs)
	mad := devs[len(devs)/2]
	if mad == 0 {
		sum := 0.0
		for _, v := range values {
			sum += v
		}
		return sum / float64(len(values)), len(values)
	}
	sum, kept := 0.0, 0
	for _, v := range values {
		if math.Abs(v-median) <= 3*mad {
			sum += v
			kept++
		}
	}
	if kept == 0 {
		return median, 1
	}
	return sum / float64(kept), kept
}

// Text renders the summary for the radio reply (kept short — APRS
// messages carry at most 67 characters).
func (r Report) Text() string {
	var parts []string
	switch {
	case r.HasTemp && r.HasHum:
		parts = append(parts, fmt.Sprintf("T %.1fC / RH %.0f%%", r.TempC, r.HumPct))
	case r.HasTemp:
		parts = append(parts, fmt.Sprintf("T %.1fC", r.TempC))
	case r.HasHum:
		parts = append(parts, fmt.Sprintf("RH %.0f%%", r.HumPct))
	}
	if len(parts) == 0 {
		return "no weather data"
	}
	if r.Stations > 1 {
		parts[0] += fmt.Sprintf(" (%d st)", r.Stations)
	}
	if r.HasForecast {
		parts = append(parts, fmt.Sprintf("fcst %.0f/%.0fC", r.ForecastMaxC, r.ForecastMinC))
	}
	return strings.Join(parts, " | ")
}
