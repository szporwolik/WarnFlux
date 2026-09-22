package openmeteo

import (
	"fmt"
	"strconv"
	"time"

	"github.com/szporwolik/WarnFlux/internal/core"
)

// providerName and providerAttribution identify Open-Meteo in the canonical
// weather model; the attribution is a provider requirement.
const (
	providerName        = "Open-Meteo"
	providerAttribution = "Weather data by Open-Meteo"
)

// wmoCondition maps an Open-Meteo WMO weather code to the canonical
// WarnFlux condition enum. Unknown future codes map to "unknown" and
// keep the raw provider code — consumers never need provider knowledge.
var wmoCondition = map[int]string{
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

// conditionForWMO maps a WMO code to the canonical condition and reports
// whether the code was known.
func conditionForWMO(code int) (string, bool) {
	c, ok := wmoCondition[code]
	if !ok {
		return core.ConditionUnknown, false
	}
	return c, true
}

// codeString renders an optional provider condition code for the wire
// payload, preserving unknown codes as informational metadata.
func codeString(code *int) *string {
	if code == nil {
		return nil
	}
	s := strconv.Itoa(*code)
	return &s
}

// conditionForCode maps an optional provider code to the canonical
// condition; an absent code is "unknown", never silently "clear".
func conditionForCode(code *int) string {
	if code == nil {
		return core.ConditionUnknown
	}
	c, _ := conditionForWMO(*code)
	return c
}

// parseLocal parses a naive provider local time in the given timezone and
// returns the corresponding absolute time.
func parseLocal(s string, tz *time.Location) (time.Time, error) {
	return time.ParseInLocation(providerLocalTime, s, tz)
}

// providerLocalTime is the format Open-Meteo uses for naive local times in
// current/hourly/sunrise/sunset fields.
const providerLocalTime = "2006-01-02T15:04"

// Normalize converts a validated Open-Meteo response into the canonical,
// provider-neutral core.WeatherSnapshot. Provider field names, WMO codes
// and parallel-array layout never leave this function.
func Normalize(loc Location, resp *ProviderResponse, generatedAt time.Time) (core.WeatherSnapshot, error) {
	tz, err := time.LoadLocation(resp.Timezone)
	if err != nil {
		return core.WeatherSnapshot{}, fmt.Errorf("provider timezone %q is invalid: %w", resp.Timezone, err)
	}

	snapshot := core.WeatherSnapshot{
		SchemaVersion: core.WeatherSchemaVersion,
		GeneratedAt:   generatedAt,
		Provider: core.WeatherProvider{
			ID:          Type,
			Name:        providerName,
			Attribution: providerAttribution,
		},
		Location: core.WeatherLocation{
			ID:         loc.ID,
			Name:       loc.Name,
			Latitude:   loc.Latitude,
			Longitude:  loc.Longitude,
			ElevationM: &resp.Elevation,
			Timezone:   resp.Timezone,
		},
	}

	if resp.Current != nil {
		c, err := normalizeCurrent(resp.Current, tz)
		if err != nil {
			return core.WeatherSnapshot{}, err
		}
		snapshot.Current = &c
	}
	for i := range resp.Hourly.Time {
		h, err := normalizeHourly(&resp.Hourly, i, tz)
		if err != nil {
			return core.WeatherSnapshot{}, fmt.Errorf("hourly entry %d: %w", i, err)
		}
		snapshot.Hourly = append(snapshot.Hourly, h)
	}
	for i := range resp.Daily.Time {
		d, err := normalizeDaily(&resp.Daily, i, tz)
		if err != nil {
			return core.WeatherSnapshot{}, fmt.Errorf("daily entry %d: %w", i, err)
		}
		snapshot.Daily = append(snapshot.Daily, d)
	}

	if err := snapshot.Validate(); err != nil {
		return core.WeatherSnapshot{}, fmt.Errorf("normalized snapshot is invalid: %w", err)
	}
	return snapshot, nil
}

func normalizeCurrent(c *CurrentData, tz *time.Location) (core.WeatherCurrent, error) {
	t, err := parseLocal(c.Time, tz)
	if err != nil {
		return core.WeatherCurrent{}, fmt.Errorf("parse current time: %w", err)
	}
	var isDay *bool
	if c.IsDay != nil {
		b := *c.IsDay == 1
		isDay = &b
	}
	return core.WeatherCurrent{
		Time:                  t,
		TemperatureC:          c.Temperature2m,
		ApparentTemperatureC:  c.ApparentTemp,
		RelativeHumidityPct:   c.RelativeHumidity,
		PressureMSLHpa:        c.PressureMSL,
		SurfacePressureHpa:    c.SurfacePressure,
		PrecipitationMm:       c.Precipitation,
		RainMm:                c.Rain,
		ShowersMm:             c.Showers,
		SnowfallCm:            c.Snowfall,
		CloudCoverPct:         c.CloudCover,
		WindSpeedKmh:          c.WindSpeed,
		WindDirectionDeg:      c.WindDirection,
		WindGustsKmh:          c.WindGusts,
		Condition:             conditionForCode(c.WeatherCode),
		ProviderConditionCode: codeString(c.WeatherCode),
		IsDay:                 isDay,
	}, nil
}

func normalizeHourly(h *HourlyData, i int, tz *time.Location) (core.WeatherHourly, error) {
	t, err := parseLocal(h.Time[i], tz)
	if err != nil {
		return core.WeatherHourly{}, fmt.Errorf("parse time: %w", err)
	}
	return core.WeatherHourly{
		Time:                        t,
		TemperatureC:                h.Temperature2m[i],
		ApparentTemperatureC:        h.ApparentTemp[i],
		RelativeHumidityPct:         h.RelativeHumidity[i],
		PrecipitationProbabilityPct: h.PrecipitationProbability[i],
		PrecipitationMm:             h.Precipitation[i],
		CloudCoverPct:               h.CloudCover[i],
		PressureMSLHpa:              h.PressureMSL[i],
		WindSpeedKmh:                h.WindSpeed[i],
		WindDirectionDeg:            h.WindDirection[i],
		WindGustsKmh:                h.WindGusts[i],
		Condition:                   conditionForCode(h.WeatherCode[i]),
		ProviderConditionCode:       codeString(h.WeatherCode[i]),
	}, nil
}

func normalizeDaily(d *DailyData, i int, tz *time.Location) (core.WeatherDaily, error) {
	out := core.WeatherDaily{
		Date:                           d.Time[i],
		Condition:                      conditionForCode(d.WeatherCode[i]),
		ProviderConditionCode:          codeString(d.WeatherCode[i]),
		TemperatureMaxC:                d.TemperatureMax[i],
		TemperatureMinC:                d.TemperatureMin[i],
		ApparentTemperatureMaxC:        d.ApparentTempMax[i],
		ApparentTemperatureMinC:        d.ApparentTempMin[i],
		PrecipitationProbabilityMaxPct: d.PrecipProbabilityMax[i],
		PrecipitationSumMm:             d.PrecipitationSum[i],
		WindSpeedMaxKmh:                d.WindSpeedMax[i],
		WindGustsMaxKmh:                d.WindGustsMax[i],
		WindDirectionDominant:          d.WindDirectionDominant[i],
	}
	for _, in := range []struct {
		name string
		src  string
		dest **time.Time
	}{
		{"sunrise", d.Sunrise[i], &out.Sunrise},
		{"sunset", d.Sunset[i], &out.Sunset},
	} {
		if in.src == "" {
			continue
		}
		t, err := parseLocal(in.src, tz)
		if err != nil {
			return core.WeatherDaily{}, fmt.Errorf("parse %s: %w", in.name, err)
		}
		*in.dest = &t
	}
	return out, nil
}
