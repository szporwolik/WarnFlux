package openmeteo

import (
	"encoding/json"
	"fmt"
	"math"
)

// ProviderResponse is the typed Open-Meteo forecast response. Only the
// fields WarnFlux requests are decoded; parallel forecast arrays are
// validated for equal length after decoding.
type ProviderResponse struct {
	Latitude         float64      `json:"latitude"`
	Longitude        float64      `json:"longitude"`
	Elevation        float64      `json:"elevation"`
	Timezone         string       `json:"timezone"`
	UTCOffsetSeconds int          `json:"utc_offset_seconds"`
	Current          *CurrentData `json:"current"`
	Hourly           HourlyData   `json:"hourly"`
	Daily            DailyData    `json:"daily"`
}

// CurrentData is the instantaneous weather block.
type CurrentData struct {
	Time             string   `json:"time"`
	Interval         int      `json:"interval"`
	Temperature2m    *float64 `json:"temperature_2m"`
	RelativeHumidity *float64 `json:"relative_humidity_2m"`
	ApparentTemp     *float64 `json:"apparent_temperature"`
	// IsDay is provider 0/1 (not a JSON boolean).
	IsDay           *int     `json:"is_day"`
	Precipitation   *float64 `json:"precipitation"`
	Rain            *float64 `json:"rain"`
	Showers         *float64 `json:"showers"`
	Snowfall        *float64 `json:"snowfall"`
	WeatherCode     *int     `json:"weather_code"`
	CloudCover      *float64 `json:"cloud_cover"`
	PressureMSL     *float64 `json:"pressure_msl"`
	SurfacePressure *float64 `json:"surface_pressure"`
	WindSpeed       *float64 `json:"wind_speed_10m"`
	WindDirection   *float64 `json:"wind_direction_10m"`
	WindGusts       *float64 `json:"wind_gusts_10m"`
}

// HourlyData holds parallel hourly forecast arrays.
type HourlyData struct {
	Time                     []string   `json:"time"`
	Temperature2m            []*float64 `json:"temperature_2m"`
	RelativeHumidity         []*float64 `json:"relative_humidity_2m"`
	ApparentTemp             []*float64 `json:"apparent_temperature"`
	PrecipitationProbability []*float64 `json:"precipitation_probability"`
	Precipitation            []*float64 `json:"precipitation"`
	WeatherCode              []*int     `json:"weather_code"`
	CloudCover               []*float64 `json:"cloud_cover"`
	PressureMSL              []*float64 `json:"pressure_msl"`
	WindSpeed                []*float64 `json:"wind_speed_10m"`
	WindDirection            []*float64 `json:"wind_direction_10m"`
	WindGusts                []*float64 `json:"wind_gusts_10m"`
}

// DailyData holds parallel daily forecast arrays.
type DailyData struct {
	Time                  []string   `json:"time"`
	WeatherCode           []*int     `json:"weather_code"`
	TemperatureMax        []*float64 `json:"temperature_2m_max"`
	TemperatureMin        []*float64 `json:"temperature_2m_min"`
	ApparentTempMax       []*float64 `json:"apparent_temperature_max"`
	ApparentTempMin       []*float64 `json:"apparent_temperature_min"`
	PrecipProbabilityMax  []*float64 `json:"precipitation_probability_max"`
	PrecipitationSum      []*float64 `json:"precipitation_sum"`
	WindSpeedMax          []*float64 `json:"wind_speed_10m_max"`
	WindGustsMax          []*float64 `json:"wind_gusts_10m_max"`
	WindDirectionDominant []*float64 `json:"wind_direction_10m_dominant"`
	Sunrise               []string   `json:"sunrise"`
	Sunset                []string   `json:"sunset"`
}

// parseResponse decodes and validates a provider response body.
func parseResponse(data []byte) (*ProviderResponse, error) {
	var resp ProviderResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, fmt.Errorf("parse provider JSON: %w", err)
	}
	if resp.Timezone == "" {
		return nil, fmt.Errorf("provider response is missing timezone")
	}
	if resp.Current == nil {
		return nil, fmt.Errorf("provider response is missing current data")
	}
	if resp.Current.Time == "" {
		return nil, fmt.Errorf("current data is missing time")
	}
	if resp.Current.Temperature2m == nil {
		return nil, fmt.Errorf("current data is missing temperature_2m")
	}
	if resp.Current.WeatherCode == nil {
		return nil, fmt.Errorf("current data is missing weather_code")
	}

	// Parallel forecast arrays must all agree with the time axis; indexing
	// them blindly would silently misalign values.
	hourlyLen := len(resp.Hourly.Time)
	hourlyArrays := []struct {
		name string
		n    int
	}{
		{"temperature_2m", len(resp.Hourly.Temperature2m)},
		{"relative_humidity_2m", len(resp.Hourly.RelativeHumidity)},
		{"apparent_temperature", len(resp.Hourly.ApparentTemp)},
		{"precipitation_probability", len(resp.Hourly.PrecipitationProbability)},
		{"precipitation", len(resp.Hourly.Precipitation)},
		{"weather_code", len(resp.Hourly.WeatherCode)},
		{"cloud_cover", len(resp.Hourly.CloudCover)},
		{"pressure_msl", len(resp.Hourly.PressureMSL)},
		{"wind_speed_10m", len(resp.Hourly.WindSpeed)},
		{"wind_direction_10m", len(resp.Hourly.WindDirection)},
		{"wind_gusts_10m", len(resp.Hourly.WindGusts)},
	}
	for _, a := range hourlyArrays {
		if a.n != hourlyLen {
			return nil, fmt.Errorf("hourly array length mismatch: time has %d entries, %s has %d", hourlyLen, a.name, a.n)
		}
	}

	dailyLen := len(resp.Daily.Time)
	dailyArrays := []struct {
		name string
		n    int
	}{
		{"weather_code", len(resp.Daily.WeatherCode)},
		{"temperature_2m_max", len(resp.Daily.TemperatureMax)},
		{"temperature_2m_min", len(resp.Daily.TemperatureMin)},
		{"apparent_temperature_max", len(resp.Daily.ApparentTempMax)},
		{"apparent_temperature_min", len(resp.Daily.ApparentTempMin)},
		{"precipitation_probability_max", len(resp.Daily.PrecipProbabilityMax)},
		{"precipitation_sum", len(resp.Daily.PrecipitationSum)},
		{"wind_speed_10m_max", len(resp.Daily.WindSpeedMax)},
		{"wind_gusts_10m_max", len(resp.Daily.WindGustsMax)},
		{"wind_direction_10m_dominant", len(resp.Daily.WindDirectionDominant)},
		{"sunrise", len(resp.Daily.Sunrise)},
		{"sunset", len(resp.Daily.Sunset)},
	}
	for _, a := range dailyArrays {
		if a.n != dailyLen {
			return nil, fmt.Errorf("daily array length mismatch: time has %d entries, %s has %d", dailyLen, a.name, a.n)
		}
	}

	if err := checkFiniteResponse(&resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// checkFiniteResponse rejects NaN/Inf values so normalized internal data
// stays valid even if a broken endpoint produces them.
func checkFiniteResponse(resp *ProviderResponse) error {
	c := resp.Current
	for name, v := range map[string]*float64{
		"current temperature_2m":       c.Temperature2m,
		"current relative_humidity_2m": c.RelativeHumidity,
		"current apparent_temperature": c.ApparentTemp,
		"current precipitation":        c.Precipitation,
		"current rain":                 c.Rain,
		"current showers":              c.Showers,
		"current snowfall":             c.Snowfall,
		"current cloud_cover":          c.CloudCover,
		"current pressure_msl":         c.PressureMSL,
		"current surface_pressure":     c.SurfacePressure,
		"current wind_speed_10m":       c.WindSpeed,
		"current wind_direction_10m":   c.WindDirection,
		"current wind_gusts_10m":       c.WindGusts,
	} {
		if v != nil && (math.IsNaN(*v) || math.IsInf(*v, 0)) {
			return fmt.Errorf("%s is not finite", name)
		}
	}
	for _, arr := range [][]*float64{
		resp.Hourly.Temperature2m, resp.Hourly.RelativeHumidity, resp.Hourly.ApparentTemp,
		resp.Hourly.PrecipitationProbability, resp.Hourly.Precipitation, resp.Hourly.CloudCover,
		resp.Hourly.PressureMSL, resp.Hourly.WindSpeed, resp.Hourly.WindDirection, resp.Hourly.WindGusts,
		resp.Daily.TemperatureMax, resp.Daily.TemperatureMin, resp.Daily.ApparentTempMax,
		resp.Daily.ApparentTempMin, resp.Daily.PrecipProbabilityMax, resp.Daily.PrecipitationSum,
		resp.Daily.WindSpeedMax, resp.Daily.WindGustsMax, resp.Daily.WindDirectionDominant,
	} {
		for _, v := range arr {
			if v != nil && (math.IsNaN(*v) || math.IsInf(*v, 0)) {
				return fmt.Errorf("forecast contains a non-finite value")
			}
		}
	}
	return nil
}
