package giosaq

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/szporwolik/WarnFlux/internal/core"
)

// sourceName is the normalized source namespace for routing and display.
const sourceName = "giosaq"

// warsawLoc is the time zone of the provider's "Data i godzina" values.
var warsawLoc = func() *time.Location {
	loc, err := time.LoadLocation("Europe/Warsaw")
	if err != nil {
		return time.UTC
	}
	return loc
}()

// pollutantPattern matches the trailing sensor suffix of "Stanowisko
// pomiarowe" (e.g. "MpSzarowSpok-O3-1g").
var pollutantPattern = regexp.MustCompile(`-(PM10|PM25|PM2\.5|SO2|NO2|O3|C6H6|CO)-`)

// stationRefOf converts one directory row into a geocoding reference.
// Stations without parseable coordinates are skipped.
func stationRefOf(st station) (stationRef, bool) {
	lat, err := strconv.ParseFloat(strings.TrimSpace(st.Lat), 64)
	if err != nil {
		return stationRef{}, false
	}
	lon, err := strconv.ParseFloat(strings.TrimSpace(st.Lon), 64)
	if err != nil {
		return stationRef{}, false
	}
	return stationRef{Name: strings.TrimSpace(st.Name), Lat: lat, Lon: lon}, true
}

// parseProviderTime parses the provider's local wall-clock timestamp
// ("2026-08-06 16:00", Europe/Warsaw).
func parseProviderTime(s string) (time.Time, error) {
	t, err := time.ParseInLocation("2006-01-02 15:04", strings.TrimSpace(s), warsawLoc)
	if err != nil {
		return time.Time{}, fmt.Errorf("unparseable timestamp %q", s)
	}
	return t, nil
}

// pollutantOf extracts the pollutant code from the station/sensor suffix.
func pollutantOf(station string) string {
	m := pollutantPattern.FindStringSubmatch(station)
	if m == nil {
		return ""
	}
	return strings.ReplaceAll(m[1], "PM2.5", "PM25")
}

// normSeverity maps the official norm type onto a canonical severity using
// the configured keyword thresholds (alarm first, then information, then
// the admissible/target limits).
func normSeverity(normType, alarmSev, infoSev, limitSev string) string {
	n := strings.ToLower(strings.TrimSpace(normType))
	switch {
	case strings.Contains(n, "alarmowy"):
		return alarmSev
	case strings.Contains(n, "informowania"):
		return infoSev
	case strings.Contains(n, "dopuszczalnego"), strings.Contains(n, "docelowego"):
		return limitSev
	default:
		return limitSev
	}
}

// normSummary shortens the official norm type into a compact headline
// fragment (e.g. "Próg Informowania (PI) - Ochrona Zdrowia (OZ)" ->
// "próg informowania (ochrona zdrowia)").
func normSummary(normType string) string {
	n := strings.TrimSpace(normType)
	if n == "" {
		return "przekroczenie norm"
	}
	n = strings.ToLower(n)
	if idx := strings.Index(n, "/"); idx > 0 {
		n = strings.TrimSpace(n[:idx])
	}
	return n
}

// normalize converts one exceedance record into a canonical HazardEvent.
// The identity is a stable hash of the identifying content (norm type,
// station and the recorded hour), so the same record keeps one event key
// across polls while the provider keeps it on the feed.
func normalize(rec exceedance, sevCfg SeverityConfig, sourceURL string) (core.HazardEvent, error) {
	normType := strings.TrimSpace(rec.NormType)
	zone := strings.TrimSpace(rec.Zone)
	station := strings.TrimSpace(rec.Station)
	at, err := parseProviderTime(rec.DateTime)
	if err != nil {
		return core.HazardEvent{}, err
	}
	if normType == "" {
		return core.HazardEvent{}, fmt.Errorf("record has no norm type")
	}

	sum := sha256.Sum256([]byte(strings.Join([]string{
		normType, zone, station, rec.DateTime,
	}, "\x00")))
	sourceID := hex.EncodeToString(sum[:12])

	sev := normSeverity(normType, sevCfg.Alarm, sevCfg.Info, sevCfg.Limit)
	pollutant := pollutantOf(station)
	event := "Air quality exceedance"
	headline := normSummary(normType)
	if pollutant != "" {
		event = "Air quality exceedance (" + pollutant + ")"
		headline = pollutant + ": " + headline
	}
	if zone != "" {
		headline += " — " + zone
	}

	descParts := []string{}
	if v := strings.TrimSpace(rec.Duration); v != "" {
		descParts = append(descParts, "Czas trwania: "+v)
	}
	if rec.MaxValue > 0 {
		descParts = append(descParts, fmt.Sprintf("Maksymalne stężenie: %.1f µg/m³", rec.MaxValue))
	}
	if v := strings.TrimSpace(rec.Causes); v != "" {
		descParts = append(descParts, "Przyczyny: "+v)
	}
	if v := strings.TrimSpace(rec.Forecast); v != "" {
		descParts = append(descParts, "Prognoza: "+v)
	}
	if v := strings.TrimSpace(rec.RiskGroups); v != "" {
		descParts = append(descParts, "Grupy ryzyka: "+v)
	}
	if v := strings.TrimSpace(rec.Precautions); v != "" {
		descParts = append(descParts, "Zalecenia: "+v)
	}

	expires := at.Add(24 * time.Hour)
	ev := core.HazardEvent{
		Source:           sourceName,
		SourceID:         sourceID,
		Category:         "environment",
		Event:            event,
		Severity:         sev,
		ProviderSeverity: normType,
		Headline:         headline,
		Description:      strings.Join(descParts, "\n"),
		Areas:            []string{zoneArea(zone)},
		Status:           core.StatusActive,
		SourceURL:        sourceURL,
		EffectiveAt:      &at,
		ExpiresAt:        &expires,
	}
	if ev.Description == "" {
		ev.Description = "Oficjalna informacja GIOŚ o przekroczeniu poziomów jakości powietrza."
	}
	if link := strings.TrimSpace(rec.InfoLink); link != "" {
		ev.SourceURL = link
	}
	if err := ev.Validate(); err != nil {
		return core.HazardEvent{}, fmt.Errorf("exceedance record is oversized/invalid: %w", err)
	}
	return ev, nil
}

// zoneArea normalizes a zone string into a deterministic area token (the
// textual part after the numeric zone code, e.g. "PL1203 strefa
// małopolska" -> "strefa małopolska").
func zoneArea(zone string) string {
	fields := strings.Fields(strings.TrimSpace(zone))
	if len(fields) > 1 {
		return strings.ToLower(strings.Join(fields[1:], " "))
	}
	return strings.ToLower(zone)
}
