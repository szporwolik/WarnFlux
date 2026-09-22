package openmeteo

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/szporwolik/WarnFlux/internal/core"
)

const (
	// infoSchemaVersion is the wire schema of the weather snapshot.
	infoSchemaVersion = 1
	// attribution is required Open-Meteo data attribution, present in
	// every payload.
	attribution = "Weather data by Open-Meteo"
)

// providerLocalTime is the format Open-Meteo uses for naive local times in
// current/hourly/sunrise/sunset fields.
const providerLocalTime = "2006-01-02T15:04"

// wireWeather is the stable MQTT weather schema. Fields are deliberately
// explicit DTOs — the raw provider JSON is never exposed.
type wireWeather struct {
	SchemaVersion int          `json:"schema_version"`
	Type          string       `json:"type"`
	Source        string       `json:"source"`
	GeneratedAt   string       `json:"generated_at"`
	Location      wireLocation `json:"location"`
	Current       wireCurrent  `json:"current"`
	Hourly        []wireHourly `json:"hourly"`
	Daily         []wireDaily  `json:"daily"`
	Attribution   string       `json:"attribution"`
}

type wireLocation struct {
	ID         string  `json:"id"`
	Name       string  `json:"name,omitempty"`
	Latitude   float64 `json:"latitude"`
	Longitude  float64 `json:"longitude"`
	Timezone   string  `json:"timezone"`
	ElevationM float64 `json:"elevation_m"`
}

type wireCurrent struct {
	Time                 string   `json:"time"`
	TemperatureC         float64  `json:"temperature_c"`
	ApparentTemperatureC *float64 `json:"apparent_temperature_c,omitempty"`
	RelativeHumidityPct  *float64 `json:"relative_humidity_pct,omitempty"`
	PrecipitationMm      *float64 `json:"precipitation_mm,omitempty"`
	RainMm               *float64 `json:"rain_mm,omitempty"`
	ShowersMm            *float64 `json:"showers_mm,omitempty"`
	SnowfallCm           *float64 `json:"snowfall_cm,omitempty"`
	WeatherCode          *int     `json:"weather_code,omitempty"`
	CloudCoverPct        *float64 `json:"cloud_cover_pct,omitempty"`
	PressureMslHpa       *float64 `json:"pressure_msl_hpa,omitempty"`
	SurfacePressureHpa   *float64 `json:"surface_pressure_hpa,omitempty"`
	WindSpeedKmh         *float64 `json:"wind_speed_kmh,omitempty"`
	WindDirectionDeg     *float64 `json:"wind_direction_deg,omitempty"`
	WindGustsKmh         *float64 `json:"wind_gusts_kmh,omitempty"`
	IsDay                *bool    `json:"is_day,omitempty"`
}

type wireHourly struct {
	Time                     string   `json:"time"`
	TemperatureC             *float64 `json:"temperature_c,omitempty"`
	ApparentTemperatureC     *float64 `json:"apparent_temperature_c,omitempty"`
	RelativeHumidityPct      *float64 `json:"relative_humidity_pct,omitempty"`
	PrecipitationProbability *float64 `json:"precipitation_probability_pct,omitempty"`
	PrecipitationMm          *float64 `json:"precipitation_mm,omitempty"`
	WeatherCode              *int     `json:"weather_code,omitempty"`
	CloudCoverPct            *float64 `json:"cloud_cover_pct,omitempty"`
	PressureMslHpa           *float64 `json:"pressure_msl_hpa,omitempty"`
	WindSpeedKmh             *float64 `json:"wind_speed_kmh,omitempty"`
	WindDirectionDeg         *float64 `json:"wind_direction_deg,omitempty"`
	WindGustsKmh             *float64 `json:"wind_gusts_kmh,omitempty"`
}

type wireDaily struct {
	Date                    string   `json:"date"`
	WeatherCode             *int     `json:"weather_code,omitempty"`
	TemperatureMaxC         *float64 `json:"temperature_max_c,omitempty"`
	TemperatureMinC         *float64 `json:"temperature_min_c,omitempty"`
	ApparentTemperatureMaxC *float64 `json:"apparent_temperature_max_c,omitempty"`
	ApparentTemperatureMinC *float64 `json:"apparent_temperature_min_c,omitempty"`
	PrecipProbabilityMaxPct *float64 `json:"precipitation_probability_max_pct,omitempty"`
	PrecipitationSumMm      *float64 `json:"precipitation_sum_mm,omitempty"`
	WindSpeedMaxKmh         *float64 `json:"wind_speed_max_kmh,omitempty"`
	WindGustsMaxKmh         *float64 `json:"wind_gusts_max_kmh,omitempty"`
	WindDirectionDominant   *float64 `json:"wind_direction_dominant_deg,omitempty"`
	Sunrise                 string   `json:"sunrise,omitempty"`
	Sunset                  string   `json:"sunset,omitempty"`
}

// buildWeatherMessage converts a validated provider response into the
// normalized information message for one location. Provider local
// timestamps are parsed in the returned provider timezone and emitted as
// offset-aware RFC3339. An invalid/missing timezone or an unparsable
// timestamp fails the whole location — malformed timestamps are never
// published.
func buildWeatherMessage(loc Location, resp *ProviderResponse, generatedAt time.Time) (core.InformationMessage, error) {
	tz, err := time.LoadLocation(resp.Timezone)
	if err != nil {
		return core.InformationMessage{}, fmt.Errorf("provider timezone %q is invalid: %w", resp.Timezone, err)
	}

	current, err := buildCurrent(resp.Current, tz)
	if err != nil {
		return core.InformationMessage{}, err
	}

	hourly := make([]wireHourly, len(resp.Hourly.Time))
	for i := range resp.Hourly.Time {
		h, err := buildHourly(&resp.Hourly, i, tz)
		if err != nil {
			return core.InformationMessage{}, fmt.Errorf("hourly entry %d: %w", i, err)
		}
		hourly[i] = h
	}

	daily := make([]wireDaily, len(resp.Daily.Time))
	for i := range resp.Daily.Time {
		d, err := buildDaily(&resp.Daily, i, tz)
		if err != nil {
			return core.InformationMessage{}, fmt.Errorf("daily entry %d: %w", i, err)
		}
		daily[i] = d
	}

	payload, err := json.Marshal(wireWeather{
		SchemaVersion: infoSchemaVersion,
		Type:          "weather",
		Source:        Type,
		GeneratedAt:   generatedAt.UTC().Format(time.RFC3339),
		Location: wireLocation{
			ID:         loc.ID,
			Name:       loc.Name,
			Latitude:   loc.Latitude,
			Longitude:  loc.Longitude,
			Timezone:   resp.Timezone,
			ElevationM: resp.Elevation,
		},
		Current:     current,
		Hourly:      hourly,
		Daily:       daily,
		Attribution: attribution,
	})
	if err != nil {
		return core.InformationMessage{}, fmt.Errorf("marshal weather snapshot: %w", err)
	}

	message := core.InformationMessage{
		Source:      Type,
		Key:         loc.ID,
		Kind:        "weather",
		GeneratedAt: generatedAt,
		Payload:     payload,
	}
	if err := message.Validate(); err != nil {
		return core.InformationMessage{}, fmt.Errorf("weather snapshot exceeds bounds: %w", err)
	}
	return message, nil
}

func buildCurrent(c *CurrentData, tz *time.Location) (wireCurrent, error) {
	t, err := parseLocal(c.Time, tz)
	if err != nil {
		return wireCurrent{}, fmt.Errorf("parse current time: %w", err)
	}
	var isDay *bool
	if c.IsDay != nil {
		b := *c.IsDay == 1
		isDay = &b
	}
	return wireCurrent{
		Time:                 t.Format(time.RFC3339),
		TemperatureC:         *c.Temperature2m,
		ApparentTemperatureC: c.ApparentTemp,
		RelativeHumidityPct:  c.RelativeHumidity,
		PrecipitationMm:      c.Precipitation,
		RainMm:               c.Rain,
		ShowersMm:            c.Showers,
		SnowfallCm:           c.Snowfall,
		WeatherCode:          c.WeatherCode,
		CloudCoverPct:        c.CloudCover,
		PressureMslHpa:       c.PressureMSL,
		SurfacePressureHpa:   c.SurfacePressure,
		WindSpeedKmh:         c.WindSpeed,
		WindDirectionDeg:     c.WindDirection,
		WindGustsKmh:         c.WindGusts,
		IsDay:                isDay,
	}, nil
}

func buildHourly(h *HourlyData, i int, tz *time.Location) (wireHourly, error) {
	t, err := parseLocal(h.Time[i], tz)
	if err != nil {
		return wireHourly{}, fmt.Errorf("parse time: %w", err)
	}
	return wireHourly{
		Time:                     t.Format(time.RFC3339),
		TemperatureC:             h.Temperature2m[i],
		ApparentTemperatureC:     h.ApparentTemp[i],
		RelativeHumidityPct:      h.RelativeHumidity[i],
		PrecipitationProbability: h.PrecipitationProbability[i],
		PrecipitationMm:          h.Precipitation[i],
		WeatherCode:              h.WeatherCode[i],
		CloudCoverPct:            h.CloudCover[i],
		PressureMslHpa:           h.PressureMSL[i],
		WindSpeedKmh:             h.WindSpeed[i],
		WindDirectionDeg:         h.WindDirection[i],
		WindGustsKmh:             h.WindGusts[i],
	}, nil
}

func buildDaily(d *DailyData, i int, tz *time.Location) (wireDaily, error) {
	out := wireDaily{
		Date:                    d.Time[i],
		WeatherCode:             d.WeatherCode[i],
		TemperatureMaxC:         d.TemperatureMax[i],
		TemperatureMinC:         d.TemperatureMin[i],
		ApparentTemperatureMaxC: d.ApparentTempMax[i],
		ApparentTemperatureMinC: d.ApparentTempMin[i],
		PrecipProbabilityMaxPct: d.PrecipProbabilityMax[i],
		PrecipitationSumMm:      d.PrecipitationSum[i],
		WindSpeedMaxKmh:         d.WindSpeedMax[i],
		WindGustsMaxKmh:         d.WindGustsMax[i],
		WindDirectionDominant:   d.WindDirectionDominant[i],
	}
	for name, in := range map[string]struct {
		src  string
		dest *string
	}{
		"sunrise": {d.Sunrise[i], &out.Sunrise},
		"sunset":  {d.Sunset[i], &out.Sunset},
	} {
		if in.src == "" {
			continue
		}
		t, err := parseLocal(in.src, tz)
		if err != nil {
			return wireDaily{}, fmt.Errorf("parse %s: %w", name, err)
		}
		*in.dest = t.Format(time.RFC3339)
	}
	return out, nil
}

// parseLocal parses a naive provider local time in the given timezone and
// returns the corresponding absolute time.
func parseLocal(s string, tz *time.Location) (time.Time, error) {
	return time.ParseInLocation(providerLocalTime, s, tz)
}
