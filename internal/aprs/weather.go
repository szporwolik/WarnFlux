package aprs

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/szporwolik/WarnFlux/internal/core"
)

// WeatherReport is one decoded APRS weather observation. It travels two
// ways: as the "weather" block of the retained station document
// (aprs/stations/<CALLSIGN>), and — via the hub's weather sink — into the
// canonical weather information pipeline, where it becomes a retained
// <prefix>/info/aprs/<callsign>/weather document and a dashboard card.
type WeatherReport struct {
	// GeneratedAt is the RFC 3339 time when WarnFlux received the report.
	GeneratedAt string `json:"generated_at"`
	// Time is the processing timestamp (not serialized; GeneratedAt is
	// its wire form).
	Time time.Time `json:"-"`
	// Callsign is the reporting station (filled by the hub).
	Callsign string `json:"-"`
	// Latitude/Longitude carry the station position (from the packet or,
	// for positionless reports, the merged station state; the hub fills
	// the fallback). Not serialized — the station document has Position.
	Latitude  float64 `json:"-"`
	Longitude float64 `json:"-"`

	WindDirectionDeg *float64 `json:"wind_direction_deg,omitempty"`
	WindSpeedKmh     *float64 `json:"wind_speed_kmh,omitempty"`
	WindGustsKmh     *float64 `json:"wind_gusts_kmh,omitempty"`

	TemperatureC *float64 `json:"temperature_c,omitempty"`

	Rain1hMm            *float64 `json:"rain_1h_mm,omitempty"`
	Rain24hMm           *float64 `json:"rain_24h_mm,omitempty"`
	RainSinceMidnightMm *float64 `json:"rain_since_midnight_mm,omitempty"`
	Snow24hCm           *float64 `json:"snow_24h_cm,omitempty"`

	HumidityPct   *float64 `json:"humidity_pct,omitempty"`
	PressureHpa   *float64 `json:"pressure_hpa,omitempty"`
	LuminosityWm2 *float64 `json:"luminosity_w_m2,omitempty"`

	// Radiation is not a standard APRS weather field: some stations (e.g.
	// uRadMon-style sensors) append it to the weather comment.
	RadiationUSvh *float64 `json:"radiation_usv_h,omitempty"`
	RadiationCPM  *float64 `json:"radiation_cpm,omitempty"`

	// Raw is the undecoded weather block (not serialized).
	Raw string `json:"-"`
}

// radiation comment patterns: 0.12uSv/h, 0.12 uSv/h, 15 cpm, X-Ray 0.13
var (
	reRadiationUSvh = regexp.MustCompile(`(?i)([0-9]+(?:\.[0-9]+)?)\s*(?:[µmμ]sv/h|usv/h|usv|msv/h)`)
	// Both "15 cpm" and "CPM: 15" appear in the wild.
	reRadiationCPM = regexp.MustCompile(`(?i)([0-9]+)\s*cpm|cpm\s*[:=]?\s*([0-9]+)`)
)

// ParseWeather decodes the APRS weather block of a packet. It accepts both
// forms: positioned weather (`!lat/lon_220/004g005t077...`, parsed as a
// position packet with the '_' symbol whose wind fields were consumed as
// course/speed) and positionless weather (`_.../...g...t...`). Packets that
// carry no weather block yield nil.
func ParseWeather(p *Packet) *WeatherReport {
	if p == nil {
		return nil
	}
	var block string
	var windDir, windMph *float64
	switch {
	case p.Kind == KindWeather:
		// Positionless report: everything after '_' is the weather block.
		block = p.Comment
	case p.Kind == KindPosition && p.Symbol == '_':
		// Positioned report: parseExtensions consumed the c/s wind block
		// as course/speed (knots-based km/h). Recover the APRS wind.
		block = p.Comment
		if p.SpeedKMH != 0 || p.CourseDeg != 0 {
			dir := float64(p.CourseDeg)
			if dir == 0 {
				dir = 360 // c000 = north
			}
			mph := p.SpeedKMH / 1.852 // reverse the knots conversion
			windDir, windMph = &dir, &mph
		}
	default:
		return nil
	}
	if strings.TrimSpace(block) == "" && windDir == nil {
		return nil
	}

	w := &WeatherReport{
		GeneratedAt: formatTime(p.ReceivedAt),
		Time:        time.Unix(p.ReceivedAt, 0),
		Raw:         block,
	}
	if p.Position != nil {
		w.Latitude = p.Position.Latitude
		w.Longitude = p.Position.Longitude
	}
	if windDir != nil {
		w.WindDirectionDeg = windDir
	}
	if windMph != nil {
		v := round2(*windMph * 1.609344)
		w.WindSpeedKmh = &v
	}
	rest := scanWeatherFields(block, w)
	parseRadiationComment(rest, w)

	// Nothing decoded: not actually a weather report.
	if w.TemperatureC == nil && w.HumidityPct == nil && w.PressureHpa == nil &&
		w.WindSpeedKmh == nil && w.WindGustsKmh == nil &&
		w.Rain1hMm == nil && w.Rain24hMm == nil && w.RainSinceMidnightMm == nil &&
		w.Snow24hCm == nil && w.LuminosityWm2 == nil &&
		w.RadiationUSvh == nil && w.RadiationCPM == nil {
		return nil
	}
	return w
}

// scanWeatherFields consumes the standard APRS weather fields in order
// (APRS101 §12) and returns the remainder (free comment / extra fields).
// Unknown or all-dots values are skipped, never fatal.
func scanWeatherFields(s string, w *WeatherReport) string {
	// Optional positionless wind placeholder ".../...".
	if len(s) >= 7 && s[3] == '/' && isWindField(s[0:3]) && isWindField(s[4:7]) {
		s = s[7:]
	}
	for len(s) > 0 {
		c := s[0]
		switch c {
		case ' ', '\t':
			s = s[1:]
			continue
		case 'g': // gust, mph, 3 digits
			v, rest, st := takeFixed(s, 1, 3)
			s = rest
			if st == fieldDots {
				continue
			}
			if st == fieldValue {
				w.WindGustsKmh = fptr(round2(v * 1.609344))
				continue
			}
		case 't': // temperature, Fahrenheit, 3 digits (2 after '-')
			v, rest, st := takeTemp(s)
			s = rest
			if st == fieldDots {
				continue
			}
			if st == fieldValue {
				w.TemperatureC = fptr(round1((v - 32) * 5.0 / 9.0))
				continue
			}
		case 'r': // rain, last hour, hundredths of an inch
			v, rest, st := takeFixed(s, 1, 3)
			s = rest
			if st == fieldDots {
				continue
			}
			if st == fieldValue {
				w.Rain1hMm = fptr(round2(v * 0.254))
				continue
			}
		case 'p': // rain, last 24 hours
			v, rest, st := takeFixed(s, 1, 3)
			s = rest
			if st == fieldDots {
				continue
			}
			if st == fieldValue {
				w.Rain24hMm = fptr(round2(v * 0.254))
				continue
			}
		case 'P': // rain, since midnight
			v, rest, st := takeFixed(s, 1, 3)
			s = rest
			if st == fieldDots {
				continue
			}
			if st == fieldValue {
				w.RainSinceMidnightMm = fptr(round2(v * 0.254))
				continue
			}
		case 'h': // relative humidity, 2 digits; h00 = 100%
			v, rest, st := takeFixed(s, 1, 2)
			s = rest
			if st == fieldDots {
				continue
			}
			if st == fieldValue {
				if v == 0 {
					v = 100
				}
				w.HumidityPct = fptr(v)
				continue
			}
		case 'b': // barometric pressure, tenths of millibar
			v, rest, st := takeFixed(s, 1, 5)
			s = rest
			if st == fieldDots {
				continue
			}
			if st == fieldValue {
				w.PressureHpa = fptr(round1(v / 10))
				continue
			}
		case 'L': // luminosity, W/m², below 1000
			v, rest, st := takeFixed(s, 1, 3)
			s = rest
			if st == fieldDots {
				continue
			}
			if st == fieldValue {
				w.LuminosityWm2 = fptr(v)
				continue
			}
		case 'l': // luminosity, 1000+ W/m²
			v, rest, st := takeFixed(s, 1, 3)
			s = rest
			if st == fieldDots {
				continue
			}
			if st == fieldValue {
				w.LuminosityWm2 = fptr(1000 + v)
				continue
			}
		case 's': // snowfall, last 24 hours, inches (positionless only)
			v, rest, st := takeFixed(s, 1, 3)
			s = rest
			if st == fieldDots {
				continue
			}
			if st == fieldValue {
				w.Snow24hCm = fptr(round1(v * 2.54))
				continue
			}
		case '#': // raw rain counter, hundredths of an inch
			_, rest, st := takeFixed(s, 1, 3)
			s = rest
			if st != fieldBad {
				continue
			}
		}
		return s
	}
	return s
}

// parseRadiationComment scans the free remainder of a weather report for
// radiation readings appended by radiation sensors.
func parseRadiationComment(rest string, w *WeatherReport) {
	if m := reRadiationUSvh.FindStringSubmatch(rest); m != nil {
		if v, err := strconv.ParseFloat(m[1], 64); err == nil {
			if strings.Contains(strings.ToLower(m[0]), "msv") {
				v *= 1000 // mSv/h → µSv/h
			}
			w.RadiationUSvh = fptr(v)
		}
	}
	if m := reRadiationCPM.FindStringSubmatch(rest); m != nil {
		raw := m[1]
		if raw == "" {
			raw = m[2]
		}
		if v, err := strconv.ParseFloat(raw, 64); err == nil {
			w.RadiationCPM = fptr(v)
		}
	}
}

// isWindField reports whether a 3-character wind subfield is all digits or
// the positionless "..." placeholder.
func isWindField(s string) bool {
	if s == "..." {
		return true
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return len(s) == 3
}

type fieldState int

const (
	fieldBad   fieldState = iota // not a number: stop scanning
	fieldDots                    // all-dots unknown: consumed, keep scanning
	fieldValue                   // numeric value parsed
)

// takeFixed consumes n chars after position i: numeric value, all-dots
// placeholder (unknown, consumed) or anything else (stop).
func takeFixed(s string, i, n int) (float64, string, fieldState) {
	if len(s) < i+n {
		return 0, s, fieldBad
	}
	field := s[i : i+n]
	if field == strings.Repeat(".", n) {
		return 0, s[i+n:], fieldDots
	}
	v, err := strconv.ParseFloat(field, 64)
	if err != nil {
		return 0, s, fieldBad
	}
	return v, s[i+n:], fieldValue
}

// takeTemp consumes the temperature field: "t" + optional '-' + up to 3
// digits (APRS101 allows t-12 for negative values).
func takeTemp(s string) (float64, string, fieldState) {
	if len(s) < 2 || s[0] != 't' {
		return 0, s, fieldBad
	}
	if strings.HasPrefix(s[1:], "...") {
		return 0, s[4:], fieldDots
	}
	i := 1
	neg := false
	if s[i] == '-' {
		neg = true
		i++
	}
	start := i
	for i < len(s) && i < start+3 && s[i] >= '0' && s[i] <= '9' {
		i++
	}
	if i == start {
		return 0, s, fieldBad
	}
	v, err := strconv.ParseFloat(s[start:i], 64)
	if err != nil {
		return 0, s, fieldBad
	}
	if neg {
		v = -v
	}
	return v, s[i:], fieldValue
}

func fptr(v float64) *float64 { return &v }

func round1(v float64) float64 { return math.Round(v*10) / 10 }
func round2(v float64) float64 { return math.Round(v*100) / 100 }

// ToInformation converts the APRS observation into the canonical weather
// information message. ProducerID is empty here: the manager stamps the
// configured ID ("aprs") when the message crosses the source boundary.
func (r *WeatherReport) ToInformation() (core.InformationMessage, error) {
	cond := core.ConditionUnknown
	if r.Rain1hMm != nil && *r.Rain1hMm > 0 || r.Rain24hMm != nil && *r.Rain24hMm > 0 {
		cond = core.ConditionRain
	}
	if r.Snow24hCm != nil && *r.Snow24hCm > 0 {
		cond = core.ConditionSnow
	}
	snap := core.WeatherSnapshot{
		SchemaVersion: core.WeatherSchemaVersion,
		GeneratedAt:   r.Time.UTC(),
		Provider: core.WeatherProvider{
			ID:          "aprs",
			Name:        "APRS",
			Attribution: "APRS weather stations heard over APRS-IS and radio",
		},
		Location: core.WeatherLocation{
			ID:        strings.ToLower(r.Callsign),
			Name:      r.Callsign,
			Latitude:  r.Latitude,
			Longitude: r.Longitude,
			Timezone:  "UTC",
		},
		Current: &core.WeatherCurrent{
			Time:                r.Time.UTC(),
			TemperatureC:        r.TemperatureC,
			RelativeHumidityPct: r.HumidityPct,
			PressureMSLHpa:      r.PressureHpa,
			PrecipitationMm:     r.Rain1hMm,
			SnowfallCm:          r.Snow24hCm,
			WindSpeedKmh:        r.WindSpeedKmh,
			WindDirectionDeg:    r.WindDirectionDeg,
			WindGustsKmh:        r.WindGustsKmh,
			RadiationUSvh:       r.RadiationUSvh,
			RadiationCPM:        r.RadiationCPM,
			Condition:           cond,
		},
	}
	msg, err := core.NewWeatherInformation("", snap)
	if err != nil {
		return core.InformationMessage{}, fmt.Errorf("aprs weather from %s: %w", r.Callsign, err)
	}
	return msg, nil
}
