package metar

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/szporwolik/WarnFlux/internal/core"
)

// Provider identity for the canonical weather model.
const (
	providerID   = "metar"
	providerName = "METAR"
)

// normalize converts one METAR record into a canonical weather snapshot.
// displayName optionally overrides the provider's long airport name
// (e.g. "Balice (EPKK)" instead of "Kraków/Paul II Arpt, ML, PL").
func normalize(rec metarRecord, displayName, timezone string, generated time.Time, validUntil time.Time) (core.WeatherSnapshot, error) {
	id := strings.TrimSpace(rec.IcaoID)
	if id == "" {
		return core.WeatherSnapshot{}, fmt.Errorf("METAR record has no ICAO station id")
	}
	if rec.Lat == 0 && rec.Lon == 0 {
		return core.WeatherSnapshot{}, fmt.Errorf("METAR %s has no station position", id)
	}
	if rec.Lat < -90 || rec.Lat > 90 || rec.Lon < -180 || rec.Lon > 180 {
		return core.WeatherSnapshot{}, fmt.Errorf("METAR %s position out of range (%v, %v)", id, rec.Lat, rec.Lon)
	}

	name := strings.TrimSpace(rec.Name)
	if dn := strings.TrimSpace(displayName); dn != "" {
		name = dn
	}
	if name == "" {
		name = id
	}
	locID := strings.ToLower(id)

	current := core.WeatherCurrent{
		Time:                  observationTime(rec),
		Condition:             conditionOf(rec),
		ProviderConditionCode: codePtr(strings.TrimSpace(rec.RawOb)),
	}
	if !math.IsNaN(rec.Temp) {
		current.TemperatureC = ptr(rec.Temp)
	}
	if !math.IsNaN(rec.Dewp) {
		current.RelativeHumidityPct = relativeHumidity(rec.Temp, rec.Dewp)
	}
	if !math.IsNaN(rec.Altim) && rec.Altim > 0 {
		// The API reports altimeter setting in the station's local unit:
		// hPa for non-US stations (rawOb "Q1019" → altim 1019) and inHg
		// for US ones ("A2992" → altim 29.92).
		hpa := rec.Altim
		if hpa < 100 {
			hpa *= 33.8638816
		}
		current.PressureMSLHpa = &hpa
	}
	if !math.IsNaN(rec.Wspd) {
		kmh := rec.Wspd * 1.852
		current.WindSpeedKmh = &kmh
	}
	if !math.IsNaN(rec.Wdir) {
		current.WindDirectionDeg = ptr(rec.Wdir)
	}
	// wgst == 0 means "no gust reported", not a real zero reading.
	if !math.IsNaN(rec.Wgst) && rec.Wgst > 0 {
		kmh := rec.Wgst * 1.852
		current.WindGustsKmh = &kmh
	}
	if c := cloudCoverPct(rec); c != nil {
		current.CloudCoverPct = c
	}

	var elev *float64
	if rec.Elev != 0 {
		elev = ptr(rec.Elev)
	}

	return core.WeatherSnapshot{
		SchemaVersion: core.WeatherSchemaVersion,
		GeneratedAt:   generated,
		ValidUntil:    &validUntil,
		Provider: core.WeatherProvider{
			ID:          providerID,
			Name:        providerName,
			Attribution: "NOAA Aviation Weather Center",
		},
		Location: core.WeatherLocation{
			ID:         locID,
			Name:       name,
			Latitude:   rec.Lat,
			Longitude:  rec.Lon,
			ElevationM: elev,
			Timezone:   timezone,
		},
		Current: &current,
	}, nil
}

// observationTime prefers the provider's report timestamp and falls back
// to the unix obsTime, then to the snapshot generation time.
func observationTime(rec metarRecord) time.Time {
	if t, err := time.Parse(time.RFC3339, strings.TrimSpace(rec.ReportTime)); err == nil {
		return t
	}
	if rec.ObsTime > 0 {
		return time.Unix(rec.ObsTime, 0)
	}
	return time.Time{}
}

// conditionOf derives the canonical condition: first from the weather
// phenomena in the raw report, then from the cloud cover.
func conditionOf(rec metarRecord) string {
	raw := strings.ToUpper(strings.TrimSpace(rec.RawOb))
	if raw != "" {
		if c := phenomenaCondition(raw); c != core.ConditionUnknown {
			return c
		}
	}
	return coverCondition(rec)
}

// phenomenaCondition maps METAR weather groups to the canonical enum.
// Priority order matters: TS > freezing > showers > snow > rain >
// drizzle > fog; unknown phenomena fall through to the cover mapping.
func phenomenaCondition(raw string) string {
	switch {
	case strings.Contains(raw, "TS"):
		if strings.Contains(raw, "GR") || strings.Contains(raw, "GS") {
			return core.ConditionThunderstormHail
		}
		return core.ConditionThunderstorm
	case strings.Contains(raw, "FZRA"):
		return core.ConditionFreezingRain
	case strings.Contains(raw, "FZDZ"):
		return core.ConditionFreezingDrizzle
	case strings.Contains(raw, "SHSN"):
		return core.ConditionSnowShowers
	case strings.Contains(raw, "SH"):
		return core.ConditionShowers
	case strings.Contains(raw, "SN"):
		return core.ConditionSnow
	case strings.Contains(raw, "SG"):
		return core.ConditionSnowGrains
	case strings.Contains(raw, "RA"):
		return core.ConditionRain
	case strings.Contains(raw, "DZ"):
		return core.ConditionDrizzle
	case strings.Contains(raw, "FG") || strings.Contains(raw, "BR"):
		return core.ConditionFog
	default:
		return core.ConditionUnknown
	}
}

// coverCondition maps the dominant cloud layer to the canonical enum.
// When the structured cloud array is absent (some stations), the cover
// groups in the raw report are used instead.
func coverCondition(rec metarRecord) string {
	clouds := rec.Clouds
	if len(clouds) == 0 {
		clouds = cloudsFromRaw(rec.RawOb)
	}
	cover := ""
	for _, c := range clouds {
		cov := strings.ToUpper(strings.TrimSpace(c.Cover))
		switch {
		case cov == "OVC" || cov == "BKN":
			cover = "OVC" // broken is practically overcast for the public view
		case cov == "SCT" && cover != "OVC":
			cover = "SCT"
		case cov == "FEW" && cover == "":
			cover = "FEW"
		case (cov == "CLR" || cov == "SKC") && cover == "":
			cover = "CLR"
		}
	}
	switch cover {
	case "OVC":
		return core.ConditionOvercast
	case "SCT":
		return core.ConditionPartlyCloudy
	case "FEW":
		return core.ConditionMainlyClear
	case "CLR":
		return core.ConditionClear
	default:
		return core.ConditionUnknown
	}
}

// cloudsFromRaw extracts cover groups (OVC005, BKN040, SKC, ...) from the
// raw METAR text.
func cloudsFromRaw(raw string) []struct {
	Cover string  `json:"cover"`
	Base  float64 `json:"base"`
} {
	clouds := []struct {
		Cover string  `json:"cover"`
		Base  float64 `json:"base"`
	}{}
	for _, tok := range strings.Fields(strings.ToUpper(strings.TrimSpace(raw))) {
		if len(tok) < 3 || len(tok) > 7 {
			continue
		}
		code := tok[:3]
		if code != "OVC" && code != "BKN" && code != "SCT" && code != "FEW" &&
			code != "SKC" && code != "CLR" {
			continue
		}
		clouds = append(clouds, struct {
			Cover string  `json:"cover"`
			Base  float64 `json:"base"`
		}{Cover: code})
	}
	return clouds
}

// cloudCoverPct maps the dominant layer to a rough percentage (0-100),
// or nil when the record carries no cloud information.
func cloudCoverPct(rec metarRecord) *float64 {
	var pct float64
	found := false
	for _, c := range rec.Clouds {
		switch strings.ToUpper(strings.TrimSpace(c.Cover)) {
		case "OVC":
			pct, found = 100, true
		case "BKN":
			if !found || pct < 75 {
				pct, found = 75, true
			}
		case "SCT":
			if !found {
				pct, found = 50, true
			}
		case "FEW":
			if !found {
				pct, found = 25, true
			}
		case "CLR", "SKC":
			if !found {
				pct, found = 0, true
			}
		}
	}
	if !found {
		return nil
	}
	return &pct
}

// relativeHumidity computes RH% from temperature and dew point (Magnus
// formula); nil when either value is missing.
func relativeHumidity(tempC, dewpC float64) *float64 {
	if math.IsNaN(tempC) || math.IsNaN(dewpC) || math.IsInf(tempC, 0) || math.IsInf(dewpC, 0) {
		return nil
	}
	e := func(t float64) float64 { return math.Exp(17.625 * t / (243.04 + t)) }
	rh := math.Round(10*100*e(dewpC)/e(tempC)) / 10
	if rh < 0 || rh > 100 {
		return nil
	}
	return &rh
}

func ptr(v float64) *float64 { return &v }

func codePtr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
