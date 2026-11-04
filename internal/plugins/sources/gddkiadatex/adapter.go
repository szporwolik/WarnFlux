package gddkiadatex

import (
	"fmt"
	"strings"

	"github.com/szporwolik/WarnFlux/internal/core"
	"github.com/szporwolik/WarnFlux/internal/severity"
)

// sourceName is the normalized source namespace for routing and display. It is
// deliberately distinct from the retired XML feed ("gddkia"): the KPD dataset
// has different identities, so the two must never reconcile against each other.
const sourceName = "gddkia-datex"

// eventNames translates the DATEX record types the national dataset actually
// carries into the short names the station shows (and routes on).
var eventNames = map[string]string{
	"MaintenanceWorks":                  "Road works",
	"GeneralObstruction":                "Obstruction",
	"Accident":                          "Accident",
	"InfrastructureDamageObstruction":   "Infrastructure damage",
	"VehicleObstruction":                "Vehicle obstruction",
	"Obstruction":                       "Obstruction",
	"RoadOrCarriagewayOrLaneManagement": "Lane management",
	"AbnormalTraffic":                   "Abnormal traffic",
	"TrafficElement":                    "Traffic",
	"WeatherRelatedRoadConditions":      "Weather conditions",
	"NonWeatherRelatedRoadConditions":   "Road conditions",
	"PoorEnvironmentConditions":         "Environment",
	"SignalFault":                       "Signal fault",
	"PublicEvent":                       "Public event",
	"ReroutingManagement":               "Diversion",
	"SpeedManagement":                   "Speed management",
	"WrongWayDriving":                   "Wrong-way driving",
	"VehicleFire":                       "Vehicle fire",
	"DisabledVehicle":                   "Disabled vehicle",
	"Queue":                             "Queue",
}

// typeSeverity gives a record type the severity the operator expects, used when
// the provider does not send one (the national obstruction profile has no
// <severity> element at all — verified against the live response: 519 records,
// zero severity elements). An explicit provider value always wins.
//
// The scale reflects what the closure means for the driver:
//   - an accident is an incident in progress on the carriageway → severe;
//   - an obstruction blocks part of the road → moderate;
//   - planned works are an inconvenience with warning signs → minor.
//
// A station that disagrees edits this table; guessing anything finer from the
// free-text comment would be inventing data.
var typeSeverity = map[string]string{
	"Accident":                          severity.Severe,
	"VehicleFire":                       severity.Severe,
	"WrongWayDriving":                   severity.Severe,
	"InfrastructureDamageObstruction":   severity.Severe,
	"GeneralObstruction":                severity.Moderate,
	"Obstruction":                       severity.Moderate,
	"VehicleObstruction":                severity.Moderate,
	"AbnormalTraffic":                   severity.Moderate,
	"WeatherRelatedRoadConditions":      severity.Moderate,
	"NonWeatherRelatedRoadConditions":   severity.Moderate,
	"MaintenanceWorks":                  severity.Minor,
	"RoadOrCarriagewayOrLaneManagement": severity.Minor,
	"SpeedManagement":                   severity.Minor,
	"ReroutingManagement":               severity.Minor,
}

// severities maps the DATEX severity vocabulary onto the canonical scale.
var severities = map[string]string{
	"lowest":  severity.Minor,
	"low":     severity.Minor,
	"medium":  severity.Moderate,
	"high":    severity.Severe,
	"highest": severity.Extreme,
}

// normalize converts one situation record into a canonical hazard event. The
// record id is stable across polls, so the same obstruction keeps one identity
// (and one message number) for its whole life.
func normalize(rec record, sourceURL string) (core.HazardEvent, error) {
	id := strings.TrimSpace(rec.ID)
	if id == "" {
		return core.HazardEvent{}, fmt.Errorf("record has no id")
	}
	lat, lon, ok := rec.coordinates()
	if !ok {
		return core.HazardEvent{}, fmt.Errorf("record has no point geometry")
	}
	status := strings.ToLower(strings.TrimSpace(rec.Validity.Status))
	if status != "" && status != "active" {
		return core.HazardEvent{}, fmt.Errorf("record is not active (%s)", status)
	}

	// xsi:type arrives with the document's namespace prefix ("ns5:MaintenanceWorks").
	kind := localType(rec.Type)
	name := eventNames[kind]
	if name == "" {
		name = humaniseKind(kind)
	}
	sev := severities[strings.ToLower(strings.TrimSpace(rec.Severity))]
	if sev == "" {
		sev = typeSeverity[kind]
	}
	if sev == "" {
		sev = severity.Moderate
	}

	road := rec.road()
	headline := name
	switch {
	case road != "" && rec.kilometre() != "":
		// The kilometre is what an operator reads on the road, so it belongs in
		// the headline next to the number ("km 425.798").
		headline = fmt.Sprintf("%s — road %s, km %s", name, road, rec.kilometre())
	case road != "":
		headline = fmt.Sprintf("%s — road %s", name, road)
	}
	desc := rec.text()
	if desc == "" {
		desc = headline
	}
	// A planned closure is not a minor inconvenience: the provider has no
	// field for it, but the comment says so in words. Narrow and documented on
	// purpose — only a total closure raises planned works to moderate.
	if sev == severity.Minor && mentionsClosure(desc) {
		sev = severity.Moderate
	}

	providerSeverity := kind
	if sub := rec.subtype(); sub != "" {
		providerSeverity = kind + "/" + sub
	}
	ev := core.HazardEvent{
		Source:           sourceName,
		SourceID:         id,
		Category:         "road",
		Event:            name,
		Severity:         sev,
		Certainty:        certainty(rec.Probability),
		ProviderSeverity: providerSeverity,
		Headline:         headline,
		Description:      desc,
		Latitude:         &lat,
		Longitude:        &lon,
		Status:           core.StatusActive,
		SourceURL:        sourceURL,
		Path:             rec.segment(),
	}
	if road != "" {
		ev.Areas = []string{roadArea(road)}
	}
	if t, ok := parseTime(rec.Validity.Spec.Start); ok {
		ev.EffectiveAt = &t
	} else if t, ok := parseTime(rec.CreationTime); ok {
		ev.EffectiveAt = &t
	}
	if t, ok := parseTime(rec.Validity.Spec.End); ok {
		ev.ExpiresAt = &t
	}
	if err := ev.Validate(); err != nil {
		return core.HazardEvent{}, fmt.Errorf("record %s is invalid: %w", id, err)
	}
	return ev, nil
}

// closurePhrases are the ways the provider writes "the road is closed" in the
// free-text comment. Matched on the folded text (lowercase, diacritics kept:
// the source is Polish).
var closurePhrases = []string{
	"całkowite zamknięcie",
	"całkowitym zamknięciem",
	"całkowitego zamknięcia",
	"pełne zamknięcie",
	"droga zamknięta",
	"jezdnia zamknięta",
	"closed in both directions",
}

// offCarriagewayMarkers are the ways the comment says the closed thing is NOT
// the carriageway the record sits on: a street running under the motorway, a
// pavement, a cycle path, a municipal road. KPD attaches such works to the
// bridge or the motorway they belong to, so the record is about the A4 while
// the traffic that actually stops is on the street below — local news, not a
// closed carriageway.
var offCarriagewayMarkers = []string{
	"ul.", "ulicy", "ulica ", "chodnik", "ścieżk", "drogi gminnej", "drodze gminnej",
	"pod obiektami", "pod autostradą", "pod drogą",
}

// closureWindow is how far around the closure phrase the comment is read when
// looking for markers of a closure that is not of the carriageway. KPD writes
// the object and the closing in one breath ("całkowite zamknięcie odcinka
// ul. Nad Serafą pod obiektami autostrady A4"), so a sentence-split would cut
// exactly at the "ul." that identifies it.
const closureWindow = 120

// mentionsClosure reports whether the comment announces a total closure OF THE
// CARRIAGEWAY. A closure of a street under the motorway, of a pavement or of a
// municipal road is local news: KPD attaches such works to the bridge or the
// motorway they belong to, so the record says "A4" while the traffic that stops
// is on the street below.
func mentionsClosure(text string) bool {
	t := []rune(strings.ToLower(text))
	for _, p := range closurePhrases {
		phrase := []rune(p)
		for at := indexRunes(t, phrase); at >= 0; {
			from := at - closureWindow
			if from < 0 {
				from = 0
			}
			to := at + len(phrase) + closureWindow
			if to > len(t) {
				to = len(t)
			}
			if !containsAny(string(t[from:to]), offCarriagewayMarkers) {
				return true
			}
			next := indexRunes(t[at+len(phrase):], phrase)
			if next < 0 {
				break
			}
			at += len(phrase) + next
		}
	}
	return false
}

// indexRunes is strings.Index for two rune slices.
func indexRunes(haystack, needle []rune) int {
	if len(needle) == 0 || len(haystack) < len(needle) {
		return -1
	}
	for i := 0; i+len(needle) <= len(haystack); i++ {
		match := true
		for j := range needle {
			if haystack[i+j] != needle[j] {
				match = false
				break
			}
		}
		if match {
			return i
		}
	}
	return -1
}

// containsAny reports whether the folded text carries any of the needles.
func containsAny(text string, needles []string) bool {
	for _, n := range needles {
		if strings.Contains(text, n) {
			return true
		}
	}
	return false
}

// certainty maps the DATEX probability vocabulary onto the CAP-style wording
// the rest of the station uses.
func certainty(p string) string {
	switch strings.ToLower(strings.TrimSpace(p)) {
	case "certain":
		return "observed"
	case "probable", "riskOf":
		return "likely"
	case "improbable":
		return "unlikely"
	default:
		return ""
	}
}

// localType drops a namespace prefix from an xsi:type value.
func localType(t string) string {
	if i := strings.LastIndex(t, ":"); i >= 0 {
		return strings.TrimSpace(t[i+1:])
	}
	return strings.TrimSpace(t)
}

// humaniseKind turns "MaintenanceWorks" into "Maintenance works" for a type the
// table does not name yet, so a new provider vocabulary still reads as words.
func humaniseKind(kind string) string {
	if kind == "" {
		return "Road obstruction"
	}
	var b strings.Builder
	for i, r := range kind {
		if i > 0 && r >= 'A' && r <= 'Z' {
			b.WriteByte(' ')
		}
		b.WriteRune(r)
	}
	words := strings.Fields(b.String())
	if len(words) == 0 {
		return "Road obstruction"
	}
	// Sentence case, the style the rest of the station's event names use
	// ("Road works", "Abnormal traffic").
	words[0] = strings.ToUpper(words[0][:1]) + strings.ToLower(words[0][1:])
	for i := 1; i < len(words); i++ {
		words[i] = strings.ToLower(words[i])
	}
	return strings.Join(words, " ")
}

// roadArea normalizes a road designation into a deterministic area token.
func roadArea(road string) string {
	return "droga:" + strings.ToLower(strings.TrimSpace(road))
}
