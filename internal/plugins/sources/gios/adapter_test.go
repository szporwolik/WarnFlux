package gios

import (
	"strings"
	"testing"
)

func TestNormalizeChemicalRelease(t *testing.T) {
	rec := sampleRecords()[1] // emisja (wyciek), Skawina
	ev, err := normalize(rec, 49.975, 19.828, "https://dane.gios.gov.pl/api/powazne-awarie")
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}
	if ev.Source != "gios" {
		t.Errorf("source = %q", ev.Source)
	}
	if ev.Event != "Chemical release" || ev.Category != "chemical" || ev.Severity != "severe" {
		t.Errorf("chemical mapping: event=%q category=%q severity=%q", ev.Event, ev.Category, ev.Severity)
	}
	if ev.Latitude == nil || ev.Longitude == nil || *ev.Latitude != 49.975 || *ev.Longitude != 19.828 {
		t.Errorf("coordinates = (%v, %v)", ev.Latitude, ev.Longitude)
	}
	if ev.EffectiveAt == nil {
		t.Error("effective date missing")
	}
	if !strings.HasPrefix(ev.Headline, "Skawina — ") {
		t.Errorf("headline = %q", ev.Headline)
	}
	areas := strings.Join(ev.Areas, ",")
	if !strings.Contains(areas, "wojewodztwo:małopolskie") || !strings.Contains(areas, "powiat:krakowski") {
		t.Errorf("areas = %v", ev.Areas)
	}
	if err := ev.Validate(); err != nil {
		t.Errorf("Validate: %v", err)
	}
}

func TestNormalizeIndustrialAccident(t *testing.T) {
	rec := sampleRecords()[0] // pożar, Oświęcim
	ev, err := normalize(rec, 50.0358, 19.21, "u")
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}
	if ev.Event != "Industrial accident" || ev.Category != "industrial" || ev.Severity != "moderate" {
		t.Errorf("accident mapping: event=%q category=%q severity=%q", ev.Event, ev.Category, ev.Severity)
	}
	if !strings.Contains(ev.Description, "Źródło zdarzenia: proces przemysłowy") {
		t.Errorf("description = %q", ev.Description)
	}
}

func TestNormalizeStableIdentity(t *testing.T) {
	rec := sampleRecords()[0]
	a, err := normalize(rec, 50.0, 19.2, "u")
	if err != nil {
		t.Fatal(err)
	}
	b, err := normalize(rec, 50.001, 19.201, "u")
	if err != nil {
		t.Fatal(err)
	}
	if a.Key() != b.Key() {
		t.Errorf("identity not stable across geocode drift: %q vs %q", a.Key(), b.Key())
	}
	// A changed record becomes a new identity.
	rec2 := rec
	rec2.RodzajZdarzenia = "wybuch"
	c, err := normalize(rec2, 50.0, 19.2, "u")
	if err != nil {
		t.Fatal(err)
	}
	if a.Key() == c.Key() {
		t.Error("changed record kept the same identity")
	}
}

func TestNormalizeRejects(t *testing.T) {
	rec := sampleRecords()[0]
	rec.Miejscowosc = " "
	if _, err := normalize(rec, 50.0, 19.2, "u"); err == nil {
		t.Error("missing locality accepted")
	}
	rec.Miejscowosc = "Oświęcim"
	if _, err := normalize(rec, 91.0, 19.2, "u"); err == nil {
		t.Error("out-of-range latitude accepted")
	}
	// Unparseable date must not fail the record.
	rec.Data = "?"
	ev, err := normalize(rec, 50.0, 19.2, "u")
	if err != nil {
		t.Fatalf("bad date should be tolerated: %v", err)
	}
	if ev.EffectiveAt != nil {
		t.Error("unparseable date produced an effective time")
	}
}
