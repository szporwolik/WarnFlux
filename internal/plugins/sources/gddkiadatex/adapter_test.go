package gddkiadatex

import (
	"os"
	"strings"
	"testing"

	"github.com/szporwolik/WarnFlux/internal/severity"
)

// normalize turns a real record into a routable event: identity, severity,
// coordinates, validity and the road area the routing matrix uses.
func TestNormalizeFromTheServiceFixture(t *testing.T) {
	raw, err := os.ReadFile("testdata/snapshot.xml")
	if err != nil {
		t.Fatal(err)
	}
	doc, err := parseResponse(raw)
	if err != nil {
		t.Fatal(err)
	}
	rec := doc.Payload.Situations[0].Records[0]
	ev, err := normalize(rec, "https://kpd.gddkia.gov.pl/addiod/ws_v34/snapshotPull")
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}
	if ev.Source != "gddkia-datex" {
		t.Fatalf("source = %q, want gddkia-datex (it must never reconcile against the old feed)", ev.Source)
	}
	if ev.SourceID != rec.ID {
		t.Fatalf("source id = %q, want the record id %q", ev.SourceID, rec.ID)
	}
	if ev.Category != "road" || ev.Event == "" {
		t.Fatalf("category/event = %q/%q", ev.Category, ev.Event)
	}
	if ev.Severity == "" {
		t.Fatal("no severity")
	}
	if ev.Latitude == nil || ev.Longitude == nil {
		t.Fatal("no coordinates")
	}
	if ev.Description == "" || ev.Headline == "" {
		t.Fatal("no text")
	}
	if len(ev.Areas) == 0 {
		t.Fatal("no area token (the routing matrix keys on it)")
	}
	if ev.EffectiveAt == nil || ev.ExpiresAt == nil {
		t.Fatalf("validity not mapped: effective=%v expires=%v", ev.EffectiveAt, ev.ExpiresAt)
	}
	if !ev.ExpiresAt.After(*ev.EffectiveAt) {
		t.Fatalf("expiry %s is not after the start %s", ev.ExpiresAt, ev.EffectiveAt)
	}
}

// A record without geometry cannot be placed on the map, and one whose validity
// has ended must not be ingested at all.
func TestNormalizeRejectsUnusableRecords(t *testing.T) {
	base := record{ID: "x"}
	base.Location.Point.Coordinates.Lat = 50
	base.Location.Point.Coordinates.Lon = 20

	noPoint := base
	noPoint.Location.Point.Coordinates.Lat = 0
	noPoint.Location.Point.Coordinates.Lon = 0
	if _, err := normalize(noPoint, ""); err == nil {
		t.Fatal("normalize accepted a record without coordinates")
	}

	ended := base
	ended.Validity.Status = "definedByValidityTimeSpec"
	if _, err := normalize(ended, ""); err == nil {
		t.Fatal("normalize accepted a record that is not active")
	}

	noID := base
	noID.ID = ""
	if _, err := normalize(noID, ""); err == nil {
		t.Fatal("normalize accepted a record without an id")
	}
}

// The type table covers what the national dataset carries today; anything new
// still reads as words instead of a raw CamelCase token.
func TestHumaniseKind(t *testing.T) {
	if got := humaniseKind("WinterDrivingManagement"); got != "Winter driving management" {
		t.Fatalf("humaniseKind = %q", got)
	}
	if got := localType("ns5:Accident"); got != "Accident" {
		t.Fatalf("localType = %q", got)
	}
	if got := roadArea(" S19 "); got != "droga:s19" {
		t.Fatalf("roadArea = %q", got)
	}
}

// The kilometre, the subtype and the provider's certainty are all in the
// response; they belong in the event so an operator does not have to open the
// raw record.
func TestNormalizeCarriesKilometreSubtypeAndCertainty(t *testing.T) {
	raw, err := os.ReadFile("testdata/snapshot.xml")
	if err != nil {
		t.Fatal(err)
	}
	doc, err := parseResponse(raw)
	if err != nil {
		t.Fatal(err)
	}
	rec := doc.Payload.Situations[0].Records[0]
	if rec.kilometre() == "" {
		t.Fatal("fixture has no kilometre")
	}
	ev, err := normalize(rec, "")
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}
	// The event is a stable KIND the i18n layer renders per language, and the
	// headline is the language-free location — that is what keeps a Polish
	// Discord post from reading "Vehicle obstruction — Vehicle obstruction".
	if ev.Event != "MaintenanceWorks" {
		t.Errorf("event = %q, want the DATEX kind", ev.Event)
	}
	if strings.Contains(ev.Headline, "works") || strings.Contains(ev.Headline, "Road") {
		t.Errorf("headline carries an English label: %q", ev.Headline)
	}
	if !strings.Contains(ev.Headline, "km "+rec.kilometre()) {
		t.Fatalf("headline = %q, want the kilometre %q", ev.Headline, rec.kilometre())
	}
	if !strings.Contains(ev.ProviderSeverity, "/maintenanceWork") {
		t.Fatalf("provider severity = %q, want the subtype appended", ev.ProviderSeverity)
	}
	if ev.Certainty != "observed" {
		t.Fatalf("certainty = %q, want observed for probabilityOfOccurrence=certain", ev.Certainty)
	}
}

// The national obstruction profile sends no <severity>, so the type decides:
// an accident is severe, an obstruction moderate, planned works minor. An
// explicit provider severity still wins.
func TestSeverityFromRecordType(t *testing.T) {
	mk := func(kind, provSeverity string) record {
		r := record{ID: "x", Type: kind, Severity: provSeverity}
		r.Location.Point.Coordinates.Lat = 50
		r.Location.Point.Coordinates.Lon = 20
		return r
	}
	cases := []struct{ kind, want string }{
		{"Accident", severity.Severe},
		{"VehicleFire", severity.Severe},
		{"GeneralObstruction", severity.Moderate},
		{"MaintenanceWorks", severity.Minor},
		{"SomethingNew", severity.Moderate},
	}
	for _, c := range cases {
		ev, err := normalize(mk(c.kind, ""), "")
		if err != nil {
			t.Fatalf("%s: %v", c.kind, err)
		}
		if ev.Severity != c.want {
			t.Fatalf("%s severity = %q, want %q", c.kind, ev.Severity, c.want)
		}
	}

	// The provider's own scale overrides the table when present.
	ev, err := normalize(mk("MaintenanceWorks", "high"), "")
	if err != nil {
		t.Fatal(err)
	}
	if ev.Severity != severity.Severe {
		t.Fatalf("severity = %q, want the provider's high -> severe", ev.Severity)
	}
}

// TestSeverityIgnoresClosuresUnderTheCarriageway pins the 2026-11-04
// false positive from production: KPD attached the renovation of the bridges
// over the Serafa river to the A4 record, but the thing that closes is the
// street running UNDER the motorway. The motorway record must stay minor —
// otherwise the panel shows moderate exactly like a real tunnel closure.
func TestSeverityIgnoresClosuresUnderTheCarriageway(t *testing.T) {
	withComment := func(text string) record {
		r := record{ID: "x", Type: "MaintenanceWorks"}
		r.Location.Point.Coordinates.Lat = 50
		r.Location.Point.Coordinates.Lon = 20
		r.Comments = append(r.Comments, struct {
			Text struct {
				Values []localised `xml:"values>value"`
			} `xml:"comment"`
			Type string `xml:"commentType"`
		}{})
		r.Comments[0].Text.Values = []localised{{Lang: "PL", Value: text}}
		return r
	}
	// The real comment, line wraps included.
	street := "Remont dwóch mostów nad rzeką Serafą w Krakowie w km 425+798 autostrady A4”, od dnia \n" +
		"29 października 2026 r.\n zostanie wprowadzona czasowa organizacja ruchu obejmująca \n" +
		"całkowite zamknięcie odcinka ul. Nad Serafą pod obiektami autostrady A4\n" +
		". Planowany termin obowiązywania utrudnień to \n10 grudnia 2026 r.\n"
	ev, err := normalize(withComment(street), "")
	if err != nil {
		t.Fatal(err)
	}
	if ev.Severity != severity.Minor {
		t.Errorf("severity = %q, want minor: the closed street is not the motorway", ev.Severity)
	}

	// The tunnel closures stay moderate: the carriageway itself is closed.
	tunnel := "Utrudnienia związane z pracami serwisowymi w tunelu. Całkowite zamknięcie drogi od węzła Zielonki do węzła Węgrzce i od węzła Węgrzce do węzła Zielonki. Ruch przekierowany na objazdy."
	ev, err = normalize(withComment(tunnel), "")
	if err != nil {
		t.Fatal(err)
	}
	if ev.Severity != severity.Moderate {
		t.Errorf("severity = %q, want moderate for a closed carriageway", ev.Severity)
	}
}

// Planned works are minor, but a total closure described in the comment is not
// a minor inconvenience: the provider has no field for it, so the wording
// raises the severity — narrowly and predictably.
func TestSeverityRaisesPlannedClosure(t *testing.T) {
	mk := func(text string) record {
		r := record{ID: "x", Type: "MaintenanceWorks"}
		r.Location.Point.Coordinates.Lat = 50
		r.Location.Point.Coordinates.Lon = 20
		r.Comments = append(r.Comments, struct {
			Text struct {
				Values []localised `xml:"values>value"`
			} `xml:"comment"`
			Type string `xml:"commentType"`
		}{})
		r.Comments[0].Text.Values = []localised{{Lang: "PL", Value: text}}
		return r
	}
	ev, err := normalize(mk("Zawężenie jezdni, ruch wahadłowy."), "")
	if err != nil {
		t.Fatal(err)
	}
	if ev.Severity != severity.Minor {
		t.Fatalf("severity = %q, want minor for lane narrowing", ev.Severity)
	}
	ev, err = normalize(mk("Prace serwisowe w tunelu. Całkowite zamknięcie drogi od węzła Węgrzce."), "")
	if err != nil {
		t.Fatal(err)
	}
	if ev.Severity != severity.Moderate {
		t.Fatalf("severity = %q, want moderate for a total closure", ev.Severity)
	}
}

// LegacyHeadline rewrites only the text this adapter used to write: the type
// glued to the location. Everything else must survive untouched, because the
// rewrite runs over stored rows the station did not author.
func TestLegacyHeadlineRewritesOnlyTheOldRoadShape(t *testing.T) {
	cases := []struct{ in, want string }{
		{"Road works — road A4, km 8.7", "A4 · km 8.7"},
		{"Vehicle obstruction — road A4, km 435.88", "A4 · km 435.88"},
		{"Road works — road S52c", "S52c"},
		{"A4 · km 435.88", "A4 · km 435.88"},
		{"Gęsta mgła", "Gęsta mgła"},
		{"", ""},
		{"Road works — road A4, km ", "Road works — road A4, km "},
	}
	for _, c := range cases {
		if got := LegacyHeadline(c.in); got != c.want {
			t.Errorf("LegacyHeadline(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}
