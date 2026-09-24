package gios

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/szporwolik/WarnFlux/internal/core"
	"github.com/szporwolik/WarnFlux/internal/plugins/sources/snapshotutil"
	"github.com/szporwolik/WarnFlux/internal/severity"
)

// sourceName is the normalized source namespace for routing and display.
const sourceName = "gios"

// normalize converts one register record (already geocoded by the caller)
// into a canonical HazardEvent. The provider carries no unique IDs, so the
// identity is a stable hash of the identifying content: the same accident
// keeps one event key across polls.
func normalize(r awariaRekord, lat, lon float64, sourceURL string) (core.HazardEvent, error) {
	miejscowosc := strings.TrimSpace(r.Miejscowosc)
	if miejscowosc == "" {
		return core.HazardEvent{}, fmt.Errorf("record has no locality")
	}
	if lat < -90 || lat > 90 || lon < -180 || lon > 180 {
		return core.HazardEvent{}, fmt.Errorf("record has out-of-range position (%v, %v)", lat, lon)
	}

	sum := sha256.Sum256([]byte(strings.Join([]string{
		strings.TrimSpace(r.Data),
		strings.TrimSpace(r.Wojewodztwo),
		strings.TrimSpace(r.Powiat),
		strings.TrimSpace(r.Gmina),
		miejscowosc,
		strings.TrimSpace(r.MiejsceZdarzenia),
		strings.TrimSpace(r.RodzajZdarzenia),
		strings.TrimSpace(r.ZrodloZdarzenia),
		strings.TrimSpace(r.OpisZdarzenia),
	}, "\x00")))
	sourceID := hex.EncodeToString(sum[:12])

	// Event typing: chemical releases are the only explicit mapping
	// ("emisja (wyciek)" and close variants); everything else is an
	// industrial accident.
	rodzaj := strings.ToLower(strings.TrimSpace(r.RodzajZdarzenia))
	sev, event, category := severity.Moderate, "Industrial accident", "industrial"
	if strings.Contains(rodzaj, "emisja") || strings.Contains(rodzaj, "wyciek") ||
		strings.Contains(rodzaj, "uwolnienie") {
		sev, event, category = severity.Severe, "Chemical release", "chemical"
	}

	headline := miejscowosc + " — " + strings.TrimSpace(r.RodzajZdarzenia)
	desc := strings.TrimSpace(r.OpisZdarzenia)
	if s := strings.TrimSpace(r.ZrodloZdarzenia); s != "" && s != "-" {
		if desc != "" {
			desc += "\n\nŹródło zdarzenia: " + s
		} else {
			desc = "Źródło zdarzenia: " + s
		}
	}

	ev := core.HazardEvent{
		Source:           sourceName,
		SourceID:         sourceID,
		Category:         category,
		Event:            event,
		Severity:         sev,
		ProviderSeverity: strings.TrimSpace(r.RodzajZdarzenia),
		Headline:         headline,
		Description:      desc,
		Latitude:         &lat,
		Longitude:        &lon,
		Areas:            adminAreas(r),
		Status:           core.StatusActive,
		SourceURL:        sourceURL,
	}
	if s := strings.TrimSpace(r.Data); s != "" {
		if t, err := snapshotutil.ParseWarsawLocal(s + " 00:00:00"); err == nil {
			ev.EffectiveAt = &t
		}
	}
	if err := ev.Validate(); err != nil {
		return core.HazardEvent{}, fmt.Errorf("accident entry is oversized/invalid: %w", err)
	}
	return ev, nil
}

// adminAreas builds the deterministic administrative area tokens for the
// record: the voivodeship and, when present, the powiat.
func adminAreas(r awariaRekord) []string {
	areas := make([]string, 0, 2)
	if w := strings.ToLower(strings.TrimSpace(r.Wojewodztwo)); w != "" {
		areas = append(areas, "wojewodztwo:"+w)
	}
	if p := strings.ToLower(strings.TrimSpace(r.Powiat)); p != "" {
		areas = append(areas, "powiat:"+p)
	}
	return areas
}
