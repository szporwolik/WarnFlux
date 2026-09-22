package core

import (
	"encoding/json"
	"fmt"
	"time"
)

// Canonical weather wire DTOs: the single provider-neutral public JSON
// schema for weather information. Provider adapters produce
// WeatherSnapshot structs; this package serializes them. Providers must
// NOT define their own weather wire structs.

// wireWeather is the canonical weather schema v1 document.
type wireWeather struct {
	SchemaVersion int          `json:"schema_version"`
	Type          string       `json:"type"`
	GeneratedAt   string       `json:"generated_at"`
	ValidUntil    string       `json:"valid_until,omitempty"`
	Provider      wireProvider `json:"provider"`
	Location      wireLocation `json:"location"`
	Current       *wireCurrent `json:"current,omitempty"`
	Hourly        []wireHourly `json:"hourly,omitempty"`
	Daily         []wireDaily  `json:"daily,omitempty"`
}

type wireProvider struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Attribution string `json:"attribution"`
}

type wireLocation struct {
	ID         string   `json:"id"`
	Name       string   `json:"name,omitempty"`
	Latitude   float64  `json:"latitude"`
	Longitude  float64  `json:"longitude"`
	ElevationM *float64 `json:"elevation_m,omitempty"`
	Timezone   string   `json:"timezone"`
}

type wireCurrent struct {
	Time string `json:"time"`

	TemperatureC         *float64 `json:"temperature_c,omitempty"`
	ApparentTemperatureC *float64 `json:"apparent_temperature_c,omitempty"`
	RelativeHumidityPct  *float64 `json:"relative_humidity_pct,omitempty"`

	PressureMSLHpa     *float64 `json:"pressure_msl_hpa,omitempty"`
	SurfacePressureHpa *float64 `json:"surface_pressure_hpa,omitempty"`

	PrecipitationMm *float64 `json:"precipitation_mm,omitempty"`
	RainMm          *float64 `json:"rain_mm,omitempty"`
	ShowersMm       *float64 `json:"showers_mm,omitempty"`
	SnowfallCm      *float64 `json:"snowfall_cm,omitempty"`

	CloudCoverPct *float64 `json:"cloud_cover_pct,omitempty"`

	WindSpeedKmh     *float64 `json:"wind_speed_kmh,omitempty"`
	WindDirectionDeg *float64 `json:"wind_direction_deg,omitempty"`
	WindGustsKmh     *float64 `json:"wind_gusts_kmh,omitempty"`

	Condition             string  `json:"condition"`
	ProviderConditionCode *string `json:"provider_condition_code,omitempty"`
	IsDay                 *bool   `json:"is_day,omitempty"`
}

type wireHourly struct {
	Time string `json:"time"`

	TemperatureC         *float64 `json:"temperature_c,omitempty"`
	ApparentTemperatureC *float64 `json:"apparent_temperature_c,omitempty"`
	RelativeHumidityPct  *float64 `json:"relative_humidity_pct,omitempty"`

	PrecipitationProbabilityPct *float64 `json:"precipitation_probability_pct,omitempty"`
	PrecipitationMm             *float64 `json:"precipitation_mm,omitempty"`

	CloudCoverPct  *float64 `json:"cloud_cover_pct,omitempty"`
	PressureMSLHpa *float64 `json:"pressure_msl_hpa,omitempty"`

	WindSpeedKmh     *float64 `json:"wind_speed_kmh,omitempty"`
	WindDirectionDeg *float64 `json:"wind_direction_deg,omitempty"`
	WindGustsKmh     *float64 `json:"wind_gusts_kmh,omitempty"`

	Condition             string  `json:"condition"`
	ProviderConditionCode *string `json:"provider_condition_code,omitempty"`
}

type wireDaily struct {
	Date      string `json:"date"`
	Condition string `json:"condition"`

	ProviderConditionCode *string `json:"provider_condition_code,omitempty"`

	TemperatureMaxC *float64 `json:"temperature_max_c,omitempty"`
	TemperatureMinC *float64 `json:"temperature_min_c,omitempty"`

	ApparentTemperatureMaxC *float64 `json:"apparent_temperature_max_c,omitempty"`
	ApparentTemperatureMinC *float64 `json:"apparent_temperature_min_c,omitempty"`

	PrecipitationProbabilityMaxPct *float64 `json:"precipitation_probability_max_pct,omitempty"`
	PrecipitationSumMm             *float64 `json:"precipitation_sum_mm,omitempty"`

	WindSpeedMaxKmh       *float64 `json:"wind_speed_max_kmh,omitempty"`
	WindGustsMaxKmh       *float64 `json:"wind_gusts_max_kmh,omitempty"`
	WindDirectionDominant *float64 `json:"wind_direction_dominant_deg,omitempty"`

	Sunrise *string `json:"sunrise,omitempty"`
	Sunset  *string `json:"sunset,omitempty"`
}

// MarshalWeatherSnapshot validates and serializes a canonical weather
// snapshot to the public schema v1 JSON document. This is the single
// serializer every weather provider must use.
func MarshalWeatherSnapshot(s WeatherSnapshot) ([]byte, error) {
	if err := s.Validate(); err != nil {
		return nil, fmt.Errorf("invalid weather snapshot: %w", err)
	}

	out := wireWeather{
		SchemaVersion: WeatherSchemaVersion,
		Type:          "weather",
		GeneratedAt:   s.GeneratedAt.UTC().Format(time.RFC3339),
		Provider: wireProvider{
			ID:          s.Provider.ID,
			Name:        s.Provider.Name,
			Attribution: s.Provider.Attribution,
		},
		Location: wireLocation{
			ID:         s.Location.ID,
			Name:       s.Location.Name,
			Latitude:   s.Location.Latitude,
			Longitude:  s.Location.Longitude,
			ElevationM: s.Location.ElevationM,
			Timezone:   s.Location.Timezone,
		},
	}
	if s.ValidUntil != nil {
		out.ValidUntil = s.ValidUntil.UTC().Format(time.RFC3339)
	}
	if s.Current != nil {
		c := s.Current
		out.Current = &wireCurrent{
			Time:                  c.Time.Format(time.RFC3339),
			TemperatureC:          c.TemperatureC,
			ApparentTemperatureC:  c.ApparentTemperatureC,
			RelativeHumidityPct:   c.RelativeHumidityPct,
			PressureMSLHpa:        c.PressureMSLHpa,
			SurfacePressureHpa:    c.SurfacePressureHpa,
			PrecipitationMm:       c.PrecipitationMm,
			RainMm:                c.RainMm,
			ShowersMm:             c.ShowersMm,
			SnowfallCm:            c.SnowfallCm,
			CloudCoverPct:         c.CloudCoverPct,
			WindSpeedKmh:          c.WindSpeedKmh,
			WindDirectionDeg:      c.WindDirectionDeg,
			WindGustsKmh:          c.WindGustsKmh,
			Condition:             c.Condition,
			ProviderConditionCode: c.ProviderConditionCode,
			IsDay:                 c.IsDay,
		}
	}
	for _, h := range s.Hourly {
		out.Hourly = append(out.Hourly, wireHourly{
			Time:                        h.Time.Format(time.RFC3339),
			TemperatureC:                h.TemperatureC,
			ApparentTemperatureC:        h.ApparentTemperatureC,
			RelativeHumidityPct:         h.RelativeHumidityPct,
			PrecipitationProbabilityPct: h.PrecipitationProbabilityPct,
			PrecipitationMm:             h.PrecipitationMm,
			CloudCoverPct:               h.CloudCoverPct,
			PressureMSLHpa:              h.PressureMSLHpa,
			WindSpeedKmh:                h.WindSpeedKmh,
			WindDirectionDeg:            h.WindDirectionDeg,
			WindGustsKmh:                h.WindGustsKmh,
			Condition:                   h.Condition,
			ProviderConditionCode:       h.ProviderConditionCode,
		})
	}
	for _, d := range s.Daily {
		wd := wireDaily{
			Date:                           d.Date,
			Condition:                      d.Condition,
			ProviderConditionCode:          d.ProviderConditionCode,
			TemperatureMaxC:                d.TemperatureMaxC,
			TemperatureMinC:                d.TemperatureMinC,
			ApparentTemperatureMaxC:        d.ApparentTemperatureMaxC,
			ApparentTemperatureMinC:        d.ApparentTemperatureMinC,
			PrecipitationProbabilityMaxPct: d.PrecipitationProbabilityMaxPct,
			PrecipitationSumMm:             d.PrecipitationSumMm,
			WindSpeedMaxKmh:                d.WindSpeedMaxKmh,
			WindGustsMaxKmh:                d.WindGustsMaxKmh,
			WindDirectionDominant:          d.WindDirectionDominant,
		}
		if d.Sunrise != nil {
			v := d.Sunrise.Format(time.RFC3339)
			wd.Sunrise = &v
		}
		if d.Sunset != nil {
			v := d.Sunset.Format(time.RFC3339)
			wd.Sunset = &v
		}
		out.Daily = append(out.Daily, wd)
	}

	data, err := json.Marshal(out)
	if err != nil {
		return nil, fmt.Errorf("marshal weather snapshot: %w", err)
	}
	return data, nil
}

// WeatherInformationKind is the information kind every weather provider
// uses.
const WeatherInformationKind = "weather"

// NewWeatherInformation builds the standard InformationMessage envelope for
// a canonical weather snapshot: it validates the snapshot, serializes the
// canonical schema and fills every routing/metadata field centrally so
// future providers cannot create subtly different weather envelopes.
// ProducerID is the configured source instance ID: it is stamped by the
// manager when the message crosses the source boundary, so plugins pass it
// empty here (the manager re-validates the complete envelope).
func NewWeatherInformation(producerID string, snapshot WeatherSnapshot) (InformationMessage, error) {
	payload, err := MarshalWeatherSnapshot(snapshot)
	if err != nil {
		return InformationMessage{}, err
	}
	return InformationMessage{
		Source:      snapshot.Provider.ID,
		ProducerID:  producerID,
		Key:         snapshot.Location.ID,
		Kind:        WeatherInformationKind,
		GeneratedAt: snapshot.GeneratedAt,
		ValidUntil:  snapshot.ValidUntil,
		Payload:     payload,
	}, nil
}
