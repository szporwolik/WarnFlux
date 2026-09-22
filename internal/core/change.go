package core

import (
	"fmt"
	"math"
)

// ChangeType describes what changed about an event so outputs can act on
// meaningful transitions only.
type ChangeType string

const (
	// ChangeNew is emitted when a previously unknown event is ingested.
	ChangeNew ChangeType = "new"
	// ChangeUpdated is emitted when an existing event's content changes.
	ChangeUpdated ChangeType = "updated"
	// ChangeCancelled is emitted when a source cancels an event.
	ChangeCancelled ChangeType = "cancelled"
	// ChangeExpired is emitted when the expiration worker expires an event.
	ChangeExpired ChangeType = "expired"
)

// String returns the change type as its wire name.
func (t ChangeType) String() string { return string(t) }

// ChangeTypeOf returns the ChangeType for a wire value and whether it is a
// known enum member. Unknown values are corruption and must be rejected by
// storage before reaching outputs.
func ChangeTypeOf(s string) (ChangeType, bool) {
	switch s {
	case "new":
		return ChangeNew, true
	case "updated":
		return ChangeUpdated, true
	case "cancelled":
		return ChangeCancelled, true
	case "expired":
		return ChangeExpired, true
	default:
		return "", false
	}
}

// ValidateJournalChange is the STABLE, minimal semantic validator for
// journal snapshots read back from storage. It detects corruption only:
// structurally impossible states must never reach outputs. It must not
// re-apply provider policy, and it must stay frozen so historical journal
// data remains readable even if future business rules become stricter.
func ValidateJournalChange(ct ChangeType, event HazardEvent) error {
	switch ct {
	case ChangeNew, ChangeUpdated, ChangeCancelled, ChangeExpired:
	default:
		return fmt.Errorf("unknown change type %q", ct)
	}
	if event.Source == "" {
		return fmt.Errorf("empty source")
	}
	if event.SourceID == "" {
		return fmt.Errorf("empty source_id")
	}
	if event.Event == "" {
		return fmt.Errorf("empty event")
	}
	switch event.Status {
	case StatusActive, StatusCancelled, StatusExpired:
	default:
		return fmt.Errorf("invalid status %q", event.Status)
	}
	hasLat, hasLon := event.Latitude != nil, event.Longitude != nil
	if hasLat != hasLon {
		return fmt.Errorf("coordinates must be both present or both absent")
	}
	if hasLat {
		if math.IsNaN(*event.Latitude) || math.IsInf(*event.Latitude, 0) ||
			*event.Latitude < -90 || *event.Latitude > 90 {
			return fmt.Errorf("latitude out of range")
		}
		if math.IsNaN(*event.Longitude) || math.IsInf(*event.Longitude, 0) ||
			*event.Longitude < -180 || *event.Longitude > 180 {
			return fmt.Errorf("longitude out of range")
		}
	}
	// Change-type vs snapshot-state consistency.
	switch ct {
	case ChangeCancelled:
		if event.Status != StatusCancelled {
			return fmt.Errorf("cancelled change carries status %q", event.Status)
		}
	case ChangeExpired:
		if event.Status != StatusExpired {
			return fmt.Errorf("expired change carries status %q", event.Status)
		}
	}
	return nil
}

// EventChange is a meaningful state transition that outputs should receive.
// Duplicates intentionally produce no EventChange.
type EventChange struct {
	// ID is the durable journal change ID (0 when the change does not come
	// from the journal, e.g. in tests).
	ID int64

	Type  ChangeType
	Event HazardEvent
}

// Clone returns a deep copy of the change so outputs can never mutate data
// shared with the core or with other outputs.
func (c EventChange) Clone() EventChange {
	return EventChange{ID: c.ID, Type: c.Type, Event: c.Event.Clone()}
}
