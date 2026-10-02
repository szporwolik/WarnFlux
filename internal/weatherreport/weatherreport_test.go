package weatherreport

import (
	"strings"
	"testing"
	"time"
)

func reading(temp, hum float64, hasTemp, hasHum bool) Reading {
	return Reading{TempC: temp, HumPct: hum, HasTemp: hasTemp, HasHum: hasHum, At: time.Now()}
}

func forecast(max, min float64) Forecast {
	return Forecast{TempMaxC: max, TempMinC: min, HasMax: true, HasMin: true, At: time.Now()}
}

// TestSummarizeSingleReading pins the EMCOM worst case: one station is
// enough — the report carries its values verbatim.
func TestSummarizeSingleReading(t *testing.T) {
	rep := Summarize(time.Now(), []Reading{reading(7.5, 62, true, true)}, nil)
	if !rep.HasTemp || rep.TempC != 7.5 {
		t.Fatalf("single reading temp = %+v, want 7.5", rep)
	}
	if !rep.HasHum || rep.HumPct != 62 {
		t.Fatalf("single reading hum = %+v, want 62", rep)
	}
	if rep.Stations != 1 {
		t.Fatalf("stations = %d, want 1", rep.Stations)
	}
	if rep.HasForecast {
		t.Fatalf("forecast = %+v, want none", rep)
	}
}

// TestSummarizeAverages pins the plain mean over several readings.
func TestSummarizeAverages(t *testing.T) {
	now := time.Now()
	rep := Summarize(now, []Reading{
		reading(10, 50, true, true),
		reading(12, 70, true, true),
		reading(11, 60, true, true),
	}, nil)
	if rep.TempC != 11 || rep.HumPct != 60 || rep.Stations != 3 {
		t.Fatalf("report = %+v, want temp 11 / hum 60 / 3 stations", rep)
	}
}

// TestSummarizeRejectsGrossError pins the MAD filter: one wildly wrong
// sensor is dropped from the mean.
func TestSummarizeRejectsGrossError(t *testing.T) {
	now := time.Now()
	rep := Summarize(now, []Reading{
		reading(10.0, 50, true, true),
		reading(10.5, 50, true, true),
		reading(11.0, 50, true, true),
		reading(42.0, 50, true, true), // broken sensor
	}, nil)
	if rep.TempC < 10 || rep.TempC > 11 {
		t.Fatalf("temp = %v, want ~10.5 (the 42C outlier dropped)", rep.TempC)
	}
}

// TestSummarizeIgnoresStaleAndInvalid drops readings outside the
// freshness window and physically impossible values.
func TestSummarizeIgnoresStaleAndInvalid(t *testing.T) {
	now := time.Now()
	stale := now.Add(-MaxReadingAge - time.Minute)
	rep := Summarize(now, []Reading{
		{TempC: 9, HasTemp: true, At: stale}, // too old
		{TempC: 80, HasTemp: true, At: now},  // impossible
		{TempC: 9, HasTemp: true, At: now},   // good
		{TempC: -70, HasTemp: true, At: now}, // impossible
		{HumPct: 130, HasHum: true, At: now}, // impossible
	}, nil)
	if !rep.HasTemp || rep.TempC != 9 {
		t.Fatalf("report = %+v, want temp 9 from the single valid reading", rep)
	}
	if rep.HasHum {
		t.Fatalf("hum = %+v, want none", rep)
	}
	if rep.Stations != 1 {
		t.Fatalf("stations = %d, want 1", rep.Stations)
	}
}

// TestSummarizeForecastAverage averages the next-day predictions across
// providers; a single forecast passes through.
func TestSummarizeForecastAverage(t *testing.T) {
	now := time.Now()
	rep := Summarize(now, nil, []Forecast{
		forecast(22, 11),
		forecast(24, 13),
	})
	if !rep.HasForecast || rep.ForecastMaxC != 23 || rep.ForecastMinC != 12 {
		t.Fatalf("forecast = %+v, want 23/12", rep)
	}

	rep = Summarize(now, nil, []Forecast{forecast(20, 9)})
	if !rep.HasForecast || rep.ForecastMaxC != 20 || rep.ForecastMinC != 9 {
		t.Fatalf("single forecast = %+v, want 20/9", rep)
	}

	rep = Summarize(now, nil, []Forecast{{TempMaxC: 20, TempMinC: 9, HasMax: true, HasMin: true, At: now.Add(-MaxForecastAge - time.Minute)}})
	if rep.HasForecast {
		t.Fatalf("stale forecast = %+v, want none", rep)
	}
}

// TestText pins the short radio rendering (APRS: max 67 characters).
func TestText(t *testing.T) {
	rep := Report{HasTemp: true, TempC: 7.75, HasHum: true, HumPct: 62.4, Stations: 5,
		HasForecast: true, ForecastMaxC: 22.4, ForecastMinC: 10.6}
	got := rep.Text()
	if !strings.Contains(got, "T 7.8C / RH 62% (5 st)") || !strings.Contains(got, "fcst 22/11C") {
		t.Fatalf("text = %q", got)
	}
	if len(got) > 67 {
		t.Fatalf("text = %q, %d chars — over the APRS limit", got, len(got))
	}

	if got := (Report{}).Text(); got != "no weather data" {
		t.Fatalf("empty text = %q", got)
	}
	if got := (Report{HasTemp: true, TempC: -1.2}).Text(); got != "T -1.2C" {
		t.Fatalf("temp-only text = %q", got)
	}
}
