package gddkiadatex

import (
	"os"
	"testing"
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
