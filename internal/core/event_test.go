package core

import (
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
