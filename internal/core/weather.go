package core

import (
	"fmt"
	"math"
	"time"
	"unicode/utf8"
)

// Package-level canonical weather model. This is the PROVIDER-NEUTRAL
// domain representation every weather source adapter must produce, and the
// single source for the public MQTT weather schema. Provider-specific
// field names, array layouts and quirks never appear here.

// WeatherSchemaVersion is the current canonical weather schema version.
// Existing field meanings never change; new optional fields may be added;
// breaking changes require a bump. Adapters must set
// WeatherSnapshot.SchemaVersion to this constant rather than a literal.
const WeatherSchemaVersion = 1

// Canonical model bounds. These are generous, provider-neutral limits —
// far above normal operational use (Open-Meteo currently normalizes 48
// hourly and 7 daily entries) — that stop a buggy adapter from building
// an unbounded snapshot before the payload-size check runs.
const (
	// MaxWeatherHourlyEntries bounds the canonical hourly forecast array.
	MaxWeatherHourlyEntries = 1000
	// MaxWeatherDailyEntries bounds the canonical daily forecast array.
	MaxWeatherDailyEntries = 366

	// Public-model string byte caps. Oversized snapshots are rejected,
	// never truncated: canonical provider data must not be silently
	// modified.
	MaxProviderNameBytes          = 256
	MaxProviderAttributionBytes   = 2048
	MaxLocationNameBytes          = 512
	MaxTimezoneBytes              = 128
	MaxProviderConditionCodeBytes = 128
)

// WeatherProvider identifies the data provenance.
type WeatherProvider struct {
	ID          string // stable provider slug, e.g. "openmeteo"
	Name        string // display name, e.g. "Open-Meteo"
	Attribution string // required attribution text
}

// WeatherLocation describes the configured location a snapshot belongs to.
// Latitude/Longitude are the CONFIGURED requested coordinates, not the
// provider-adjusted coordinates.
type WeatherLocation struct {
	ID         string
	Name       string
	Latitude   float64
	Longitude  float64
	ElevationM *float64 // provider elevation, optional
	Timezone   string   // IANA timezone, required when provided by the provider
}

// WeatherCurrent is the instantaneous observation block.
type WeatherCurrent struct {
	Time time.Time // offset-aware local provider time

	TemperatureC         *float64
	ApparentTemperatureC *float64
	RelativeHumidityPct  *float64

	PressureMSLHpa     *float64
	SurfacePressureHpa *float64

	PrecipitationMm *float64
	RainMm          *float64
	ShowersMm       *float64
	SnowfallCm      *float64

	CloudCoverPct *float64

	WindSpeedKmh     *float64
	WindDirectionDeg *float64
	WindGustsKmh     *float64

	Condition string // canonical condition enum value

	ProviderConditionCode *string // raw provider code, informational only
	IsDay                 *bool
}

// WeatherHourly is one hourly forecast entry.
type WeatherHourly struct {
	Time time.Time

	TemperatureC         *float64
	ApparentTemperatureC *float64
	RelativeHumidityPct  *float64

	PrecipitationProbabilityPct *float64
	PrecipitationMm             *float64

	CloudCoverPct  *float64
	PressureMSLHpa *float64

	WindSpeedKmh     *float64
	WindDirectionDeg *float64
	WindGustsKmh     *float64

	Condition             string
	ProviderConditionCode *string
}

// WeatherDaily is one daily forecast entry. Date is YYYY-MM-DD.
type WeatherDaily struct {
	Date      string
	Condition string

	ProviderConditionCode *string

	TemperatureMaxC *float64
	TemperatureMinC *float64

	ApparentTemperatureMaxC *float64
	ApparentTemperatureMinC *float64

	PrecipitationProbabilityMaxPct *float64
	PrecipitationSumMm             *float64

	WindSpeedMaxKmh       *float64
	WindGustsMaxKmh       *float64
	WindDirectionDominant *float64

	Sunrise *time.Time
	Sunset  *time.Time
}

// WeatherSnapshot is the canonical weather state for one location.
type WeatherSnapshot struct {
	SchemaVersion int

	// GeneratedAt is the application processing time. It may carry any
	// time zone; the canonical serializer normalizes generated_at and
	// valid_until to UTC on the wire.
	GeneratedAt time.Time
	ValidUntil  *time.Time // freshness bound; application metadata, NOT a provider guarantee

	Provider WeatherProvider
	Location WeatherLocation

	Current *WeatherCurrent
	Hourly  []WeatherHourly
	Daily   []WeatherDaily
}

// Weather conditions: the stable WarnFlux enum shared by all weather
// providers. New values may only be added compatibly. It is deliberately a
// small common denominator (no meteorological taxonomy): a provider that
// cannot classify a condition uses ConditionUnknown.
const (
	ConditionClear            = "clear"
	ConditionMainlyClear      = "mainly_clear"
	ConditionPartlyCloudy     = "partly_cloudy"
	ConditionOvercast         = "overcast"
	ConditionFog              = "fog"
	ConditionDrizzle          = "drizzle"
	ConditionFreezingDrizzle  = "freezing_drizzle"
	ConditionRain             = "rain"
	ConditionFreezingRain     = "freezing_rain"
	ConditionSnow             = "snow"
	ConditionSnowGrains       = "snow_grains"
	ConditionShowers          = "showers"
	ConditionSnowShowers      = "snow_showers"
	ConditionThunderstorm     = "thunderstorm"
	ConditionThunderstormHail = "thunderstorm_hail"
	ConditionUnknown          = "unknown"
)

// validConditions is the accepted condition enum.
var validConditions = map[string]bool{
	ConditionClear:            true,
	ConditionMainlyClear:      true,
	ConditionPartlyCloudy:     true,
	ConditionOvercast:         true,
	ConditionFog:              true,
	ConditionDrizzle:          true,
	ConditionFreezingDrizzle:  true,
	ConditionRain:             true,
	ConditionFreezingRain:     true,
	ConditionSnow:             true,
	ConditionSnowGrains:       true,
	ConditionShowers:          true,
	ConditionSnowShowers:      true,
	ConditionThunderstorm:     true,
	ConditionThunderstormHail: true,
	ConditionUnknown:          true,
}

// IsValidCondition reports whether the condition string is a known enum
// member.
func IsValidCondition(condition string) bool { return validConditions[condition] }

// Validate checks the whole snapshot for structural sanity. It is the
// provider-independent gate every adapter must pass before the snapshot is
// serialized. It deliberately avoids meteorological policy (no arbitrary
// temperature bounds) but enforces the public schema contract: bounded
// arrays, byte-capped UTF-8 strings, real YYYY-MM-DD daily dates and a
// loadable IANA timezone.
func (s WeatherSnapshot) Validate() error {
	if s.SchemaVersion != WeatherSchemaVersion {
		return fmt.Errorf("unsupported weather schema version %d", s.SchemaVersion)
	}
	if s.GeneratedAt.IsZero() {
		return fmt.Errorf("generated_at must be non-zero")
	}
	if s.ValidUntil != nil && (!s.ValidUntil.After(s.GeneratedAt) || s.ValidUntil.IsZero()) {
		return fmt.Errorf("valid_until must be after generated_at")
	}
	if !informationSlugRE.MatchString(s.Provider.ID) {
		return fmt.Errorf("provider id must be a lowercase slug, got %q", s.Provider.ID)
	}
	if err := validateBoundedText("provider name", s.Provider.Name, MaxProviderNameBytes, false); err != nil {
		return err
	}
	if err := validateBoundedText("provider attribution", s.Provider.Attribution, MaxProviderAttributionBytes, false); err != nil {
		return err
	}
	if !informationSlugRE.MatchString(s.Location.ID) {
		return fmt.Errorf("location id must be a lowercase slug, got %q", s.Location.ID)
	}
	if err := validateBoundedText("location name", s.Location.Name, MaxLocationNameBytes, true); err != nil {
		return err
	}
	if math.IsNaN(s.Location.Latitude) || math.IsInf(s.Location.Latitude, 0) ||
		s.Location.Latitude < -90 || s.Location.Latitude > 90 {
		return fmt.Errorf("location latitude out of range: %v", s.Location.Latitude)
	}
	if math.IsNaN(s.Location.Longitude) || math.IsInf(s.Location.Longitude, 0) ||
		s.Location.Longitude < -180 || s.Location.Longitude > 180 {
		return fmt.Errorf("location longitude out of range: %v", s.Location.Longitude)
	}
	if s.Location.ElevationM != nil && (math.IsNaN(*s.Location.ElevationM) || math.IsInf(*s.Location.ElevationM, 0)) {
		return fmt.Errorf("location elevation must be finite")
	}
	if err := validateBoundedText("location timezone", s.Location.Timezone, MaxTimezoneBytes, false); err != nil {
		return err
	}
	// The public contract promises an IANA timezone. time.LoadLocation
	// consults the system tz database and Go's embedded zoneinfo, so this
	// check protects the contract even for adapters that never validated
	// the provider-reported value.
	if _, err := time.LoadLocation(s.Location.Timezone); err != nil {
		return fmt.Errorf("location timezone %q is not a valid IANA timezone", s.Location.Timezone)
	}

	if s.Current == nil && len(s.Hourly) == 0 && len(s.Daily) == 0 {
		return fmt.Errorf("snapshot must contain at least one of current, hourly or daily data")
	}
	if len(s.Hourly) > MaxWeatherHourlyEntries {
		return fmt.Errorf("hourly entries: %d, maximum %d", len(s.Hourly), MaxWeatherHourlyEntries)
	}
	if len(s.Daily) > MaxWeatherDailyEntries {
		return fmt.Errorf("daily entries: %d, maximum %d", len(s.Daily), MaxWeatherDailyEntries)
	}
	if s.Current != nil {
		if s.Current.Time.IsZero() {
			return fmt.Errorf("current time must be non-zero")
		}
		if err := validateWeatherNumbers("current",
			s.Current.TemperatureC, s.Current.ApparentTemperatureC,
			s.Current.PressureMSLHpa, s.Current.SurfacePressureHpa,
			s.Current.PrecipitationMm, s.Current.RainMm, s.Current.ShowersMm, s.Current.SnowfallCm,
			s.Current.WindSpeedKmh, s.Current.WindDirectionDeg, s.Current.WindGustsKmh); err != nil {
			return err
		}
		if err := validatePercent("current relative_humidity", s.Current.RelativeHumidityPct); err != nil {
			return err
		}
		if err := validatePercent("current cloud_cover", s.Current.CloudCoverPct); err != nil {
			return err
		}
		if err := validateDirection("current wind_direction", s.Current.WindDirectionDeg); err != nil {
			return err
		}
		if !IsValidCondition(s.Current.Condition) {
			return fmt.Errorf("current condition %q is not a known condition", s.Current.Condition)
		}
		if err := validateConditionCode("current", s.Current.ProviderConditionCode); err != nil {
			return err
		}
	}

	var prev time.Time
	for i, h := range s.Hourly {
		if h.Time.IsZero() {
			return fmt.Errorf("hourly entry %d has zero time", i)
		}
		if i > 0 && !h.Time.After(prev) {
			return fmt.Errorf("hourly entries must be strictly ascending by time (entry %d)", i)
		}
		prev = h.Time
		if !IsValidCondition(h.Condition) {
			return fmt.Errorf("hourly entry %d condition %q is not a known condition", i, h.Condition)
		}
		if err := validateWeatherNumbers(fmt.Sprintf("hourly entry %d", i),
			h.TemperatureC, h.ApparentTemperatureC, h.PressureMSLHpa,
			h.PrecipitationMm, h.WindSpeedKmh, h.WindDirectionDeg, h.WindGustsKmh); err != nil {
			return err
		}
		if err := validatePercent(fmt.Sprintf("hourly entry %d relative_humidity", i), h.RelativeHumidityPct); err != nil {
			return err
		}
		if err := validatePercent(fmt.Sprintf("hourly entry %d cloud_cover", i), h.CloudCoverPct); err != nil {
			return err
		}
		if err := validatePercent(fmt.Sprintf("hourly entry %d precipitation_probability", i), h.PrecipitationProbabilityPct); err != nil {
			return err
		}
		if err := validateDirection(fmt.Sprintf("hourly entry %d wind_direction", i), h.WindDirectionDeg); err != nil {
			return err
		}
		if err := validateConditionCode(fmt.Sprintf("hourly entry %d", i), h.ProviderConditionCode); err != nil {
			return err
		}
	}

	// Daily dates are real calendar dates: YYYY-MM-DD, strictly ascending
	// by parsed date (duplicates and non-dates are rejected here, not
	// silently accepted as strings).
	var prevDate time.Time
	for i, d := range s.Daily {
		parsed, err := time.Parse("2006-01-02", d.Date)
		if err != nil {
			return fmt.Errorf("daily entry %d date %q is not a valid YYYY-MM-DD date", i, d.Date)
		}
		if i > 0 && !parsed.After(prevDate) {
			return fmt.Errorf("daily entries must be strictly ascending by date (entry %d)", i)
		}
		prevDate = parsed
		if !IsValidCondition(d.Condition) {
			return fmt.Errorf("daily entry %d condition %q is not a known condition", i, d.Condition)
		}
		if err := validateConditionCode(fmt.Sprintf("daily entry %d", i), d.ProviderConditionCode); err != nil {
			return err
		}
		if err := validateWeatherNumbers(fmt.Sprintf("daily entry %d", i),
			d.TemperatureMaxC, d.TemperatureMinC,
			d.ApparentTemperatureMaxC, d.ApparentTemperatureMinC,
			d.PrecipitationSumMm, d.WindSpeedMaxKmh, d.WindGustsMaxKmh,
			d.WindDirectionDominant); err != nil {
			return err
		}
		if err := validatePercent(fmt.Sprintf("daily entry %d precipitation_probability_max", i), d.PrecipitationProbabilityMaxPct); err != nil {
			return err
		}
		if err := validateDirection(fmt.Sprintf("daily entry %d wind_direction_dominant", i), d.WindDirectionDominant); err != nil {
			return err
		}
	}
	return nil
}

// validateBoundedText enforces the public-model string rules: optional
// non-emptiness, valid UTF-8 and a byte cap. Canonical text is never
// truncated silently — oversized values are rejected.
func validateBoundedText(field, value string, maxBytes int, allowEmpty bool) error {
	if !allowEmpty && value == "" {
		return fmt.Errorf("%s must be non-empty", field)
	}
	if !utf8.ValidString(value) {
		return fmt.Errorf("%s is not valid UTF-8", field)
	}
	if n := len([]byte(value)); n > maxBytes {
		return fmt.Errorf("%s is %d bytes, maximum %d", field, n, maxBytes)
	}
	return nil
}

// validateConditionCode applies the provider condition code contract:
// nil means the provider supplied no raw code (unavailable); a non-nil
// value must be non-empty, valid UTF-8 and bounded — empty metadata is
// not useful and is rejected instead of being published.
func validateConditionCode(scope string, code *string) error {
	if code == nil {
		return nil
	}
	if *code == "" {
		return fmt.Errorf("%s provider_condition_code must not be empty (use nil when unavailable)", scope)
	}
	if !utf8.ValidString(*code) {
		return fmt.Errorf("%s provider_condition_code is not valid UTF-8", scope)
	}
	if n := len([]byte(*code)); n > MaxProviderConditionCodeBytes {
		return fmt.Errorf("%s provider_condition_code is %d bytes, maximum %d", scope, n, MaxProviderConditionCodeBytes)
	}
	return nil
}

func validateWeatherNumbers(scope string, values ...*float64) error {
	for _, v := range values {
		if v != nil && (math.IsNaN(*v) || math.IsInf(*v, 0)) {
			return fmt.Errorf("%s contains a non-finite value", scope)
		}
	}
	return nil
}

func validatePercent(scope string, v *float64) error {
	if v == nil {
		return nil
	}
	if math.IsNaN(*v) || math.IsInf(*v, 0) {
		return fmt.Errorf("%s is not finite", scope)
	}
	if *v < 0 || *v > 100 {
		return fmt.Errorf("%s must be 0..100, got %v", scope, *v)
	}
	return nil
}

func validateDirection(scope string, v *float64) error {
	if v == nil {
		return nil
	}
	if math.IsNaN(*v) || math.IsInf(*v, 0) {
		return fmt.Errorf("%s is not finite", scope)
	}
	if *v < 0 || *v > 360 {
		return fmt.Errorf("%s must be 0..360, got %v", scope, *v)
	}
	return nil
}
