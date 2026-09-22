package core

import (
	"math"
	"testing"
)

func journalEvent() HazardEvent {
	return HazardEvent{Source: "src", SourceID: "1", Event: "Flood", Status: StatusActive}
}

func TestValidateJournalChangeAccepts(t *testing.T) {
	valid := []struct {
		ct     ChangeType
		status EventStatus
	}{
		{ChangeNew, StatusActive},
		{ChangeUpdated, StatusActive},
		// An updated change may carry any valid lifecycle state.
		{ChangeUpdated, StatusCancelled},
		{ChangeUpdated, StatusExpired},
		{ChangeCancelled, StatusCancelled},
		{ChangeExpired, StatusExpired},
	}
	for _, c := range valid {
		e := journalEvent()
		e.Status = c.status
		if err := ValidateJournalChange(c.ct, e); err != nil {
			t.Errorf("(%s,%s) rejected: %v", c.ct, c.status, err)
		}
	}

	e := journalEvent()
	lat, lon := 51.5, -0.1
	e.Latitude, e.Longitude = &lat, &lon
	if err := ValidateJournalChange(ChangeNew, e); err != nil {
		t.Errorf("valid coordinates rejected: %v", err)
	}
	edge := journalEvent()
	lat2, lon2 := -90.0, 180.0
	edge.Latitude, edge.Longitude = &lat2, &lon2
	if err := ValidateJournalChange(ChangeNew, edge); err != nil {
		t.Errorf("boundary coordinates rejected: %v", err)
	}
}

func TestValidateJournalChangeRejects(t *testing.T) {
	cases := []struct {
		name string
		ct   ChangeType
		mut  func(*HazardEvent)
	}{
		{"empty source", ChangeNew, func(e *HazardEvent) { e.Source = "" }},
		{"empty source_id", ChangeNew, func(e *HazardEvent) { e.SourceID = "" }},
		{"empty event", ChangeNew, func(e *HazardEvent) { e.Event = "" }},
		{"invalid status", ChangeNew, func(e *HazardEvent) { e.Status = "banana" }},
		{"latitude without longitude", ChangeNew, func(e *HazardEvent) {
			v := 1.0
			e.Latitude = &v
		}},
		{"longitude without latitude", ChangeNew, func(e *HazardEvent) {
			v := 1.0
			e.Longitude = &v
		}},
		{"NaN latitude", ChangeNew, func(e *HazardEvent) {
			v := math.NaN()
			e.Latitude, e.Longitude = &v, &v
		}},
		{"latitude 91", ChangeNew, func(e *HazardEvent) {
			v, w := 91.0, 1.0
			e.Latitude, e.Longitude = &v, &w
		}},
		{"longitude 181", ChangeNew, func(e *HazardEvent) {
			v, w := 1.0, 181.0
			e.Latitude, e.Longitude = &v, &w
		}},
		{"cancelled change with active snapshot", ChangeCancelled, nil},
		{"expired change with active snapshot", ChangeExpired, nil},
		{"cancelled change with expired snapshot", ChangeCancelled, func(e *HazardEvent) {
			e.Status = StatusExpired
		}},
		{"unknown change type", ChangeType("banana"), nil},
	}
	for _, c := range cases {
		e := journalEvent()
		if c.mut != nil {
			c.mut(&e)
		}
		if err := ValidateJournalChange(c.ct, e); err == nil {
			t.Errorf("%s: expected rejection, got nil", c.name)
		}
	}
}
