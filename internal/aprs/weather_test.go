package aprs

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestParseWeatherPositioned(t *testing.T) {
	p := ParseFeedLine("SP9WX>APRS,TCPIP*:!5056.25N/01952.50E_220/004g005t077r000p000P000h50b09900L123", time.Now())
	if p.Kind != KindPosition || p.Symbol != '_' {
		t.Fatalf("packet kind/symbol = %s/%c, want position/_", p.Kind, p.Symbol)
	}
	w := ParseWeather(&p)
	if w == nil {
		t.Fatal("ParseWeather = nil")
	}
	checkFloat(t, "wind_dir", w.WindDirectionDeg, 220)
	checkFloat(t, "wind_speed", w.WindSpeedKmh, 6.4)
	checkFloat(t, "wind_gusts", w.WindGustsKmh, 8.0)
	checkFloat(t, "temp", w.TemperatureC, 25.0)
	checkFloat(t, "humidity", w.HumidityPct, 50)
	checkFloat(t, "pressure", w.PressureHpa, 990.0)
	checkFloat(t, "luminosity", w.LuminosityWm2, 123)
	if w.Latitude == 0 || w.Longitude == 0 {
		t.Fatalf("position not carried: %v %v", w.Latitude, w.Longitude)
	}
}

func TestParseWeatherPositionless(t *testing.T) {
	p := ParseFeedLine("SP9WX-13>APRS,TCPIP*:_.../...g...t-12r001p002P003h00b10200s005 with 0.12uSv/h", time.Now())
	if p.Kind != KindWeather {
		t.Fatalf("kind = %s, want weather", p.Kind)
	}
	w := ParseWeather(&p)
	if w == nil {
		t.Fatal("ParseWeather = nil")
	}
	checkFloat(t, "temp", w.TemperatureC, -24.4)
	checkFloat(t, "rain 1h", w.Rain1hMm, 0.25)
	checkFloat(t, "rain 24h", w.Rain24hMm, 0.51)
	checkFloat(t, "rain midnight", w.RainSinceMidnightMm, 0.76)
	checkFloat(t, "humidity", w.HumidityPct, 100)
	checkFloat(t, "pressure", w.PressureHpa, 1020.0)
	checkFloat(t, "snow", w.Snow24hCm, 12.7)
	checkFloat(t, "radiation", w.RadiationUSvh, 0.12)
}

func TestParseWeatherRadiationComment(t *testing.T) {
	p := ParseFeedLine("SP9WX>APRS,TCPIP*:!5056.25N/01952.50E_220/004g005t077r000p000P000h50b09900 X-Ray 0.13 uSv/h CPM 15", time.Now())
	w := ParseWeather(&p)
	if w == nil {
		t.Fatal("ParseWeather = nil")
	}
	checkFloat(t, "radiation", w.RadiationUSvh, 0.13)
	checkFloat(t, "cpm", w.RadiationCPM, 15)
}

func TestParseWeatherNotWeather(t *testing.T) {
	p := ParseFeedLine("SP9XYZ-7>APRS,TCPIP*:!5056.25N/01952.50E-", time.Now())
	if w := ParseWeather(&p); w != nil {
		t.Fatalf("ParseWeather = %+v, want nil for a plain position", w)
	}
}

func TestWeatherReportToInformation(t *testing.T) {
	r := WeatherReport{
		Time:             time.Now(),
		Callsign:         "SP9WX-13",
		Latitude:         50.9375,
		Longitude:        19.875,
		TemperatureC:     fptr(21.5),
		HumidityPct:      fptr(55),
		PressureHpa:      fptr(1005.4),
		WindSpeedKmh:     fptr(6.4),
		WindDirectionDeg: fptr(220),
		Rain1hMm:         fptr(1.2),
		RadiationUSvh:    fptr(0.12),
		RadiationCPM:     fptr(15),
	}
	msg, err := r.ToInformation()
	if err != nil {
		t.Fatalf("ToInformation: %v", err)
	}
	if msg.Kind != "weather" || msg.Key != "sp9wx-13" || msg.Source != "aprs" {
		t.Fatalf("envelope = %q/%q/%q", msg.Source, msg.Key, msg.Kind)
	}
	if !strings.Contains(string(msg.Payload), `"radiation_usv_h":0.12`) {
		t.Fatalf("payload missing radiation: %s", msg.Payload)
	}
	if !strings.Contains(string(msg.Payload), `"radiation_cpm":15`) {
		t.Fatalf("payload missing cpm: %s", msg.Payload)
	}
}

func TestHubWeatherSinkAndStationDoc(t *testing.T) {
	hub, sink := testHub(t, HubConfig{
		Enabled:    true,
		Callsign:   "SP9MOA-10",
		Icon:       "/j",
		GridSquare: "JO90WW",
		RadiusKM:   DefaultRadiusKM,
		StationTTL: 30 * time.Minute,
	})
	// The sink callback runs on the hub worker goroutine: collect the
	// reports over a channel so the test never races the worker.
	gotCh := make(chan WeatherReport, 4)
	hub.SetWeatherSink(func(_ context.Context, w WeatherReport) error {
		gotCh <- w
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	hub.Start(ctx)
	defer cancel()

	hub.Observe(ParseFeedLine("SP9WX>APRS,TCPIP*:!5056.25N/01952.50E_220/004g005t077r000p000P000h50b09900", time.Now()), "aprs-inet")

	var got []WeatherReport
	waitFor(t, func() bool { return len(gotCh) >= 1 })
	for len(gotCh) > 0 {
		got = append(got, <-gotCh)
	}
	if len(got) != 1 || got[0].Callsign != "SP9WX" || got[0].TemperatureC == nil || *got[0].TemperatureC != 25.0 {
		t.Fatalf("weather sink report = %+v", got)
	}

	waitFor(t, func() bool { return len(sink.payloads(StationsTopicPrefix+"SP9WX")) >= 1 })
	var doc StationDocument
	if err := json.Unmarshal(sink.payloads(StationsTopicPrefix + "SP9WX")[0], &doc); err != nil {
		t.Fatalf("station doc: %v", err)
	}
	if doc.Weather == nil || doc.Weather.TemperatureC == nil {
		t.Fatalf("station doc weather = %+v", doc.Weather)
	}
	if !strings.Contains(string(mustJSON(doc.Weather)), `"temperature_c":25`) {
		t.Fatalf("station doc weather wire = %s", mustJSON(doc.Weather))
	}
}

func checkFloat(t *testing.T, field string, got *float64, want float64) {
	t.Helper()
	if got == nil {
		t.Fatalf("%s = nil, want %v", field, want)
	}
	if *got < want-0.05 || *got > want+0.05 {
		t.Fatalf("%s = %v, want %v", field, *got, want)
	}
}

func mustJSON(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}
