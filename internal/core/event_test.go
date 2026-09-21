package core

import (
	"math"
	"strings"
	"testing"
	"time"
)

func TestEventKeyStable(t *testing.T) {
	a := HazardEvent{Source: "gdacs", SourceID: "EQ-1456789"}.Key()
	b := HazardEvent{Source: "gdacs", SourceID: "EQ-1456789"}.Key()
	if a != b {
		t.Errorf("same source + source ID must produce the same key: %q != %q", a, b)
	}
	if a != "gdacs:EQ-1456789" {
		t.Errorf("key = %q, want %q", a, "gdacs:EQ-1456789")
	}
}

func TestEventKeyNoCollision(t *testing.T) {
	if EventKey("gdacs", "EQ-1") == EventKey("usgs", "EQ-1") {
		t.Error("different sources must not collide")
	}
	if EventKey("gdacs", "EQ-1") == EventKey("gdacs", "EQ-2") {
		t.Error("different source IDs must not collide")
	}
}

func TestNormalizeDefaultsStatus(t *testing.T) {
	var e HazardEvent
	e.Normalize()
	if e.Status != StatusActive {
		t.Errorf("status = %q, want %q", e.Status, StatusActive)
	}

	e.Status = StatusCancelled
	e.Normalize()
	if e.Status != StatusCancelled {
		t.Error("explicit status must not be overwritten")
	}
}

func TestValidate(t *testing.T) {
	valid := HazardEvent{Source: "meteoalarm", SourceID: "2.49", Event: "Rain"}
	if err := valid.Validate(); err != nil {
		t.Errorf("minimal valid event rejected: %v", err)
	}

	cases := []struct {
		name  string
		event HazardEvent
		want  string
	}{
		{"missing source", HazardEvent{SourceID: "2.49", Event: "Rain"}, "source"},
		{"missing source id", HazardEvent{Source: "meteoalarm", Event: "Rain"}, "source_id"},
		{"missing event", HazardEvent{Source: "meteoalarm", SourceID: "2.49"}, "event"},
		{"invalid status", HazardEvent{Source: "meteoalarm", SourceID: "2.49", Event: "Rain", Status: "gone"}, "status"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.event.Validate()
			if err == nil {
				t.Fatal("expected validation error, got nil")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q should mention %q", err, tc.want)
			}
		})
	}
}

func TestValidateOptionalFieldsMayBeAbsent(t *testing.T) {
	// Optional CAP-like fields must not be required.
	e := HazardEvent{Source: "imgw", SourceID: "123", Event: "Storm"}
	if err := e.Validate(); err != nil {
		t.Errorf("event without optional fields rejected: %v", err)
	}
}

func TestStatusValues(t *testing.T) {
	if StatusActive != "active" || StatusCancelled != "cancelled" || StatusExpired != "expired" {
		t.Error("status constants have unexpected values")
	}
}

func TestChangeTypeString(t *testing.T) {
	if ChangeNew.String() != "new" || ChangeUpdated.String() != "updated" ||
		ChangeCancelled.String() != "cancelled" || ChangeExpired.String() != "expired" {
		t.Error("change type String() mismatch")
	}
}

func TestEffectiveAndExpiryPointers(t *testing.T) {
	eff := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	exp := eff.Add(time.Hour)
	e := HazardEvent{Source: "s", SourceID: "1", Event: "E", EffectiveAt: &eff, ExpiresAt: &exp}
	if err := e.Validate(); err != nil {
		t.Errorf("event with times rejected: %v", err)
	}
}

func TestNormalizeZeroTimesBecomeNil(t *testing.T) {
	zero := time.Time{}
	e := HazardEvent{
		Source:      "s",
		SourceID:    "1",
		Event:       "E",
		EffectiveAt: &zero,
		ExpiresAt:   &zero,
	}
	e.Normalize()
	if e.EffectiveAt != nil {
		t.Error("zero EffectiveAt should become nil")
	}
	if e.ExpiresAt != nil {
		t.Error("zero ExpiresAt should become nil (must not cause instant expiration)")
	}

	// Real timestamps must be preserved.
	eff := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	e2 := HazardEvent{Source: "s", SourceID: "1", Event: "E", EffectiveAt: &eff}
	e2.Normalize()
	if e2.EffectiveAt == nil || !e2.EffectiveAt.Equal(eff) {
		t.Error("non-zero EffectiveAt must be preserved")
	}
}

func TestNormalizeCanonicalizesSourceAndAreas(t *testing.T) {
	e := HazardEvent{
		Source:   "  MeteoAlarm-EU ",
		SourceID: "  abc ",
		Event:    "E",
		Areas:    []string{" DE-NW ", "", "de-nw", "DE-NW", "de-rp"},
	}
	e.Normalize()
	if e.Source != "meteoalarm-eu" {
		t.Errorf("source = %q, want %q", e.Source, "meteoalarm-eu")
	}
	if e.SourceID != "abc" {
		t.Errorf("source id = %q, want %q", e.SourceID, "abc")
	}
	want := []string{"DE-NW", "de-nw", "de-rp"}
	if len(e.Areas) != len(want) {
		t.Fatalf("areas = %v, want %v", e.Areas, want)
	}
	for i := range want {
		if e.Areas[i] != want[i] {
			t.Errorf("areas = %v, want %v", e.Areas, want)
		}
	}
}

func TestValidateSourceCanonical(t *testing.T) {
	valid := []string{"meteoalarm", "gdacs", "imgw-weather", "nws.office_x", "a", "trailing-"}
	for _, s := range valid {
		if err := ValidateSource(s); err != nil {
			t.Errorf("source %q rejected: %v", s, err)
		}
	}
	invalid := []string{"", "MeteoAlarm", "has space", "has:colon", "-leading"}
	for _, s := range invalid {
		if err := ValidateSource(s); err == nil {
			t.Errorf("source %q should be rejected", s)
		}
	}
}

func TestValidateSourceIDBounds(t *testing.T) {
	if err := ValidateSourceID("EQ-1456789"); err != nil {
		t.Errorf("valid source id rejected: %v", err)
	}
	if err := ValidateSourceID("  "); err == nil {
		t.Error("blank source id should be rejected")
	}
	if err := ValidateSourceID(strings.Repeat("x", maxSourceIDLength+1)); err == nil {
		t.Error("oversized source id should be rejected")
	}
}

func TestValidateCoordinates(t *testing.T) {
	valid := []struct {
		lat, lon float64
	}{
		{50.06, 19.94},
		{-90, -180},
		{90, 180},
		{0, 0},
	}
	for _, c := range valid {
		lat, lon := c.lat, c.lon
		e := HazardEvent{Source: "s", SourceID: "1", Event: "E", Latitude: &lat, Longitude: &lon}
		if err := e.Validate(); err != nil {
			t.Errorf("valid coordinates %v rejected: %v", c, err)
		}
	}

	nan := math.NaN()
	inf := math.Inf(1)
	lat91, lon181, latOne, lonOne := 91.0, 181.0, 10.0, 10.0
	invalid := []HazardEvent{
		{Source: "s", SourceID: "1", Event: "E", Latitude: &nan, Longitude: &lonOne},
		{Source: "s", SourceID: "1", Event: "E", Latitude: &latOne, Longitude: &inf},
		{Source: "s", SourceID: "1", Event: "E", Latitude: &lat91, Longitude: &lonOne},
		{Source: "s", SourceID: "1", Event: "E", Latitude: &latOne, Longitude: &lon181},
		{Source: "s", SourceID: "1", Event: "E", Latitude: &latOne}, // only one
	}
	for _, e := range invalid {
		if err := e.Validate(); err == nil {
			t.Errorf("invalid coordinates accepted: %+v", e)
		}
	}
}

func TestCloneIsDeep(t *testing.T) {
	eff := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	lat, lon := 50.0, 19.0
	e := HazardEvent{
		Source:      "s",
		SourceID:    "1",
		Event:       "E",
		EffectiveAt: &eff,
		Latitude:    &lat,
		Longitude:   &lon,
		Areas:       []string{"DE-NW"},
	}

	cp := e.Clone()
	*cp.Latitude = 99
	*cp.EffectiveAt = cp.EffectiveAt.Add(time.Hour)
	cp.Areas[0] = "PL-MA"

	if *e.Latitude != 50.0 {
		t.Error("mutating clone changed the original latitude")
	}
	if !e.EffectiveAt.Equal(eff) {
		t.Error("mutating clone changed the original EffectiveAt")
	}
	if e.Areas[0] != "DE-NW" {
		t.Error("mutating clone changed the original areas")
	}
}

func TestEventKeyDelimiterSafety(t *testing.T) {
	// Sources can never contain ":", so the first colon in a key is
	// always the source/sourceID boundary.
	if EventKey("a-b", "c") == EventKey("a", "b-c") {
		t.Error("delimiter collision")
	}
	if EventKey("x.y_z", "1") == EventKey("x.y", "z_1") {
		t.Error("delimiter collision")
	}
}
