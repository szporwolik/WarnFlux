package burzedzis

import (
	"fmt"
	"strings"
	"time"

	"github.com/szporwolik/WarnFlux/internal/burzedzis"
	"github.com/szporwolik/WarnFlux/internal/core"
	"github.com/szporwolik/WarnFlux/internal/severity"
)

// sourceURLs point at the provider's own pages, so a reader can open the
// map behind an alert.
const (
	warningsURL = "https://burze.dzis.net/?page=severe_weather_alert_map_poland"
	stormURL    = "https://burze.dzis.net/?page=storm_report_map_of_poland"
)

// category is the event category of every burze.dzis.net event: these are
// meteorological warnings, the same category IMGW uses.
const category = "met"

// stormEventTitle is the canonical event type of the lightning nowcast.
const stormEventTitle = "Lightning nearby"

// warningSeverity applies the degree mapping and then the phenomenon floor.
// The floor only ever raises: a third-degree warning stays extreme even if
// its floor is severe.
func (s *Source) warningSeverity(ph burzedzis.Phenomenon, degree int) string {
	if degree < 1 {
		degree = 1
	}
	if degree > 3 {
		degree = 3
	}
	sev := s.plan.byDegree[degree]
	if floor := s.plan.floors[ph.Key]; floor != "" {
		rank, _ := severity.Rank(floor)
		base, _ := severity.Rank(sev)
		if rank > base {
			return floor
		}
	}
	return sev
}

// stormSeverity maps the lightning search onto the canonical scale: the
// closer the nearest discharge, the higher the severity, exactly as the
// configured thresholds say.
func (s *Source) stormSeverity(st burzedzis.Storm) string {
	switch {
	case st.DistanceKM > 0 && st.DistanceKM <= s.cfg.StormDirectKM:
		return s.cfg.StormDirectSeverity
	case st.DistanceKM > 0 && st.DistanceKM <= s.cfg.StormNearKM:
		return s.cfg.StormNearSeverity
	default:
		return s.cfg.StormSeverity
	}
}

// warningEvent converts one warning in force into a canonical event. The
// identity is stable per locality, phenomenon and warning window: a
// re-issued window (the forecast moved) becomes a new event, and the old one
// is cancelled by the next reconciliation, while a re-issued degree updates
// the event in place.
func (s *Source) warningEvent(in burzedzis.Warning) core.HazardEvent {
	id := fmt.Sprintf("%s:%s:%s", slug(in.Point.Name), in.Phenomenon.Key, in.From.UTC().Format("200601021504"))
	headline := fmt.Sprintf("%s — %s, %s stopień", in.Phenomenon.Label, in.Point.Name, roman(in.Degree))
	effective, expires := in.From.UTC(), in.To.UTC()

	desc := []string{
		"Miejscowość: " + localityLine(in.Point),
		fmt.Sprintf("Stopień ostrzeżenia: %s (%d z 3)", roman(in.Degree), in.Degree),
		fmt.Sprintf("Okres: %s – %s UTC", effective.Format("2006-01-02 15:04"), expires.Format("2006-01-02 15:04")),
		"Źródło: burze.dzis.net (ostrzeżenia IMGW dla wskazanego punktu)",
	}
	return core.HazardEvent{
		Source:           Type,
		SourceID:         id,
		Category:         category,
		Event:            in.Phenomenon.Title,
		Severity:         s.warningSeverity(in.Phenomenon, in.Degree),
		ProviderSeverity: fmt.Sprintf("%d", in.Degree),
		Headline:         headline,
		Description:      strings.Join(desc, "\n"),
		Areas:            []string{in.Point.Name},
		Status:           core.StatusActive,
		SourceURL:        warningsURL,
		EffectiveAt:      &effective,
		ExpiresAt:        &expires,
		Latitude:         &in.Point.Lat,
		Longitude:        &in.Point.Lon,
	}
}

// stormEvent converts one lightning detection into a canonical event. The
// identity is the locality alone: one storm per locality at a time, updated
// as the discharges move closer or fade, cancelled by the first quiet poll
// and expired by itself if the station stops.
func (s *Source) stormEvent(st burzedzis.Storm, now time.Time) core.HazardEvent {
	window := st.Window()
	if window <= 0 {
		window = fallbackStormWindow
	}
	// The validity is pinned to a TTL-sized bucket: a storm that is simply
	// still there keeps the same validity window (and therefore the same
	// event fingerprint) while its facts are unchanged, so an unchanged
	// detection is never re-notified merely because time passed.
	bucket := now.UTC().Truncate(s.cfg.StormTTL)
	effective := bucket.Add(-window)
	expires := bucket.Add(s.cfg.StormTTL)

	nearest := ""
	if st.DistanceKM > 0 {
		nearest = fmt.Sprintf(", najbliższe %.1f km", st.DistanceKM)
		if dir := burzedzis.DirectionName(st.Direction); dir != "" {
			nearest += " (" + dir + ")"
		}
	}
	headline := fmt.Sprintf("%s: %d wyładowań%s", st.Locality, st.Count, nearest)

	desc := []string{
		"Miejscowość: " + localityLine(burzedzis.Locality{Name: st.Locality, Lat: st.Lat, Lon: st.Lon}),
		fmt.Sprintf("Wyładowania w promieniu %d km: %d", st.RadiusKM, st.Count),
	}
	if st.DistanceKM > 0 {
		line := fmt.Sprintf("Najbliższe wyładowanie: %.1f km", st.DistanceKM)
		if dir := burzedzis.DirectionName(st.Direction); dir != "" {
			line += ", kierunek " + dir
		}
		desc = append(desc, line)
	}
	desc = append(desc,
		fmt.Sprintf("Okno obserwacji: %d minut", int(window.Minutes())),
		"Źródło: burze.dzis.net (wykrywanie wyładowań atmosferycznych)")

	return core.HazardEvent{
		Source:           TypeStorm,
		SourceID:         slug(st.Locality),
		Category:         category,
		Event:            stormEventTitle,
		Severity:         s.stormSeverity(st),
		ProviderSeverity: fmt.Sprintf("%d @ %.1f km", st.Count, st.DistanceKM),
		Headline:         headline,
		Description:      strings.Join(desc, "\n"),
		Areas:            []string{st.Locality},
		Status:           core.StatusActive,
		SourceURL:        stormURL,
		EffectiveAt:      &effective,
		ExpiresAt:        &expires,
		Latitude:         &st.Lat,
		Longitude:        &st.Lon,
	}
}

// localityLine renders the locality with the coordinates the provider's
// directory returned, so the coordinates behind an alert are visible.
func localityLine(at burzedzis.Locality) string {
	return fmt.Sprintf("%s (%.2f, %.2f)", at.Name, at.Lat, at.Lon)
}

// roman renders a warning degree the way Polish warnings spell it.
func roman(degree int) string {
	switch degree {
	case 1:
		return "I"
	case 2:
		return "II"
	case 3:
		return "III"
	default:
		return fmt.Sprintf("%d", degree)
	}
}

// polishDiacritics folds the letters the provider's directory needs but
// event ids cannot carry (they travel in URLs and MQTT topics).
var polishDiacritics = strings.NewReplacer(
	"ą", "a", "ć", "c", "ę", "e", "ł", "l", "ń", "n", "ó", "o",
	"ś", "s", "ź", "z", "ż", "z",
)

// slug builds the event-id fragment of one locality: lowercase ASCII,
// words joined by dashes ("Niepołomice" becomes "niepolomice"). A name
// without any ASCII letter falls back to "locality", so the id stays
// usable.
func slug(name string) string {
	folded := polishDiacritics.Replace(strings.ToLower(strings.TrimSpace(name)))
	var b strings.Builder
	lastDash := true
	for _, r := range folded {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			b.WriteRune(r)
			lastDash = false
		case !lastDash:
			b.WriteByte('-')
			lastDash = true
		}
	}
	out := strings.Trim(b.String(), "-")
	if out == "" {
		return "locality"
	}
	return out
}
