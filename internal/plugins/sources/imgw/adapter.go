package imgw

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/szporwolik/WarnFlux/internal/core"
	"github.com/szporwolik/WarnFlux/internal/plugins/sources/snapshotutil"
)

// parseLocalTime parses an IMGW local timestamp in Europe/Warsaw.
func parseLocalTime(s string) (time.Time, error) {
	return snapshotutil.ParseWarsawLocal(s)
}

// normalizeText trims and collapses internal whitespace deterministically.
func normalizeText(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

// normalizeKey normalizes identity-relevant text: trim, collapse
// whitespace, lowercase.
func normalizeKey(s string) string {
	return strings.ToLower(strings.Join(strings.Fields(s), " "))
}

// severityFromLevel maps the shared IMGW degree scale. IMGW exposes 1, 2
// and 3; anything unexpected is "unknown" (never a panic or a feed
// rejection). known is false for values that should be logged.
func severityFromLevel(level string, drought bool) (severity string, known bool) {
	switch strings.TrimSpace(level) {
	case "1":
		return "moderate", true
	case "2":
		return "severe", true
	case "3":
		return "extreme", true
	case "-1":
		// Hydrological drought is explicitly ungraded (bezstopniowe).
		return "unknown", true
	default:
		return "unknown", false
	}
}

// probabilityPct parses the provider probability string (0..100).
func probabilityPct(s string) (int, bool) {
	v, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil || v < 0 || v > 100 {
		return 0, false
	}
	return v, true
}

// buildDescription assembles the deterministic human-readable description:
// the provider body text plus, when available, the IMGW probability and a
// non-placeholder comment. Provider publication times are deliberately not
// included (they would change the fingerprint every poll without changing
// hazard content).
func buildDescription(body, komentarz string, probability int, hasProbability bool) string {
	var b strings.Builder
	b.WriteString(normalizeText(body))
	if hasProbability {
		fmt.Fprintf(&b, "\n\nIMGW probability: %d%%.", probability)
	}
	kom := normalizeText(komentarz)
	if kom != "" && !strings.EqualFold(kom, "brak") && !strings.EqualFold(kom, "brak.") {
		fmt.Fprintf(&b, "\nComment: %s", kom)
	}
	return b.String()
}

// hydroSourceID derives the stable hydrological warning identity:
// sha256(year(data_od) \x00 numer \x00 normalized biuro). Mutable fields
// (przebieg, komentarz, probability, data_do) are deliberately excluded so
// ordinary content updates keep the same identity.
func hydroSourceID(w hydroWarning) (string, error) {
	year := yearOfDataOd(w.DataOd)
	if year == "" || strings.TrimSpace(w.Numer) == "" || normalizeKey(w.Biuro) == "" {
		return "", fmt.Errorf("hydro warning lacks required identity fields (data_od, numer, biuro)")
	}
	sum := sha256.Sum256([]byte(year + "\x00" + strings.TrimSpace(w.Numer) + "\x00" + normalizeKey(w.Biuro)))
	return "hydro:" + hex.EncodeToString(sum[:]), nil
}

// yearOfDataOd extracts the year of a valid data_od timestamp ("" when
// malformed).
func yearOfDataOd(dataOd string) string {
	t, err := parseLocalTime(dataOd)
	if err != nil {
		return ""
	}
	return strconv.Itoa(t.Year())
}

// parseExpires parses a provider expiry string; IMGW uses year >= 9999 for
// effectively indefinite warnings (hydrological drought), which becomes
// nil. An empty string is also nil; any other malformed value is an error.
func parseExpires(s string) (*time.Time, error) {
	if strings.TrimSpace(s) == "" {
		return nil, nil
	}
	t, err := parseLocalTime(s)
	if err != nil {
		return nil, err
	}
	if t.Year() >= 9999 {
		return nil, nil
	}
	return &t, nil
}

// meteoAreas normalizes TERYT codes into deterministic area labels.
func meteoAreas(teryt []string) ([]string, error) {
	areas := make([]string, 0, len(teryt))
	for _, raw := range teryt {
		code := strings.TrimSpace(raw)
		if code == "" {
			continue
		}
		if len(code) > 32 {
			return nil, fmt.Errorf("TERYT code %q is implausibly long", code)
		}
		areas = append(areas, "teryt:"+code)
	}
	return sortedUnique(areas), nil
}

// hydroAreas normalizes wojewodztwo / basin codes / descriptions into
// deterministic area labels. Basin codes are never geo-resolved.
func hydroAreas(obszary []hydroArea) []string {
	var areas []string
	for _, a := range obszary {
		woj := normalizeText(a.Wojewodztwo)
		if woj != "" {
			areas = append(areas, "wojewodztwo:"+woj)
		}
		for _, raw := range a.KodZlewni {
			code := strings.TrimSpace(raw)
			if code != "" {
				areas = append(areas, "zlewnia:"+code)
			}
		}
		opis := normalizeText(a.Opis)
		// Avoid duplicating the voivodeship when opis already starts with
		// it (the feed often prefixes the description with the region name).
		var combo string
		switch {
		case woj == "":
			combo = opis
		case opis == "":
			combo = woj
		case strings.HasPrefix(strings.ToLower(opis), strings.ToLower(woj)):
			combo = opis
		default:
			combo = woj + ", " + opis
		}
		combo = strings.TrimSpace(combo)
		if combo != "" {
			areas = append(areas, "obszar:"+combo)
		}
	}
	return sortedUnique(areas)
}

// sortedUnique returns the deduplicated, lexicographically sorted list.
func sortedUnique(in []string) []string {
	sort.Strings(in)
	out := in[:0]
	var prev string
	for i, s := range in {
		if i > 0 && s == prev {
			continue
		}
		out = append(out, s)
		prev = s
	}
	return out
}

// normalizeMeteo converts one meteorological provider item into a
// canonical HazardEvent. A non-nil error means the item could not be
// safely identified/normalized (the feed snapshot becomes incomplete).
func normalizeMeteo(w meteoWarning, sourceURL string) (core.HazardEvent, error) {
	id := strings.TrimSpace(w.ID)
	if id == "" {
		return core.HazardEvent{}, fmt.Errorf("meteo warning has an empty id")
	}
	event := normalizeText(w.NazwaZdarzenia)
	if event == "" {
		return core.HazardEvent{}, fmt.Errorf("meteo warning %q has no event name", id)
	}
	effective, err := parseLocalTime(w.ObowiazujeOd)
	if err != nil {
		return core.HazardEvent{}, fmt.Errorf("meteo warning %q: %w", id, err)
	}
	expires, err := parseExpires(w.ObowiazujeDo)
	if err != nil {
		return core.HazardEvent{}, fmt.Errorf("meteo warning %q: %w", id, err)
	}
	areas, err := meteoAreas(w.Teryt)
	if err != nil {
		return core.HazardEvent{}, fmt.Errorf("meteo warning %q: %w", id, err)
	}
	severity, known := severityFromLevel(w.Stopien, false)
	if !known {
		slog.Warn("unexpected IMGW meteo degree", "id", id, "stopien", w.Stopien)
	}
	probability, hasProbability := probabilityPct(w.Prawdopodobienstwo)
	if !hasProbability && strings.TrimSpace(w.Prawdopodobienstwo) != "" {
		slog.Debug("IMGW meteo probability unparsable", "id", id, "value", w.Prawdopodobienstwo)
	}

	ev := core.HazardEvent{
		Source:           sourceMeteo,
		SourceID:         id,
		Category:         "met",
		Event:            event,
		Severity:         severity,
		ProviderSeverity: strings.TrimSpace(w.Stopien),
		Headline:         event,
		Description:      buildDescription(w.Tresc, w.Komentarz, probability, hasProbability),
		EffectiveAt:      &effective,
		ExpiresAt:        expires,
		Areas:            areas,
		Status:           core.StatusActive,
		SourceURL:        sourceURL,
	}
	if err := ev.Validate(); err != nil {
		return core.HazardEvent{}, fmt.Errorf("meteo warning %q is oversized/invalid: %w", id, err)
	}
	return ev, nil
}

// normalizeHydro converts one hydrological provider item into a canonical
// HazardEvent. A non-nil error means the item could not be safely
// identified/normalized (the feed snapshot becomes incomplete).
func normalizeHydro(w hydroWarning, sourceURL string) (core.HazardEvent, error) {
	id, err := hydroSourceID(w)
	if err != nil {
		return core.HazardEvent{}, err
	}
	event := normalizeText(w.Zdarzenie)
	if event == "" {
		return core.HazardEvent{}, fmt.Errorf("hydro warning %q has no event name", id)
	}
	effective, err := parseLocalTime(w.DataOd)
	if err != nil {
		return core.HazardEvent{}, fmt.Errorf("hydro warning %q: %w", id, err)
	}
	expires, err := parseExpires(w.DataDo)
	if err != nil {
		return core.HazardEvent{}, fmt.Errorf("hydro warning %q: %w", id, err)
	}
	// stopień -1 is the documented ungraded hydrological drought.
	drought := strings.TrimSpace(w.Stopien) == "-1"
	severity, known := severityFromLevel(w.Stopien, drought)
	if !known {
		slog.Warn("unexpected IMGW hydro degree", "id", id, "stopien", w.Stopien)
	}
	probability, hasProbability := probabilityPct(w.Prawdopodobienstwo)
	if !hasProbability && strings.TrimSpace(w.Prawdopodobienstwo) != "" {
		slog.Debug("IMGW hydro probability unparsable", "id", id, "value", w.Prawdopodobienstwo)
	}

	ev := core.HazardEvent{
		Source:           sourceHydro,
		SourceID:         id,
		Category:         "met",
		Event:            event,
		Severity:         severity,
		ProviderSeverity: strings.TrimSpace(w.Stopien),
		Headline:         event,
		Description:      buildDescription(w.Przebieg, w.Komentarz, probability, hasProbability),
		EffectiveAt:      &effective,
		ExpiresAt:        expires,
		Areas:            hydroAreas(w.Obszary),
		Status:           core.StatusActive,
		SourceURL:        sourceURL,
	}
	if err := ev.Validate(); err != nil {
		return core.HazardEvent{}, fmt.Errorf("hydro warning %q is oversized/invalid: %w", id, err)
	}
	return ev, nil
}
