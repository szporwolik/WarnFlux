// Package core holds the normalized hazard event model shared by all
// sources, the ingestion pipeline and outputs.
package core

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// EventStatus is the lifecycle state of an event as persisted by WarnFlux.
//
// "updated" is deliberately not a status: it is a change type reported when
// an active event's content changes.
type EventStatus string

const (
	// StatusActive is the normal state of a live event.
	StatusActive EventStatus = "active"
	// StatusCancelled means a source explicitly withdrew the event.
	StatusCancelled EventStatus = "cancelled"
	// StatusExpired means the event's ExpiresAt time has passed. It is set
	// only by the expiration worker, never by ingestion.
	StatusExpired EventStatus = "expired"
)

// HazardEvent is the normalized representation of a hazard or emergency
// alert produced by a source adapter.
type HazardEvent struct {
	// Source identifies the originating provider, e.g. "meteoalarm".
	Source string
	// SourceID is the provider-specific identifier of the event. Together
	// with Source it forms the stable event identity.
	SourceID string

	Category  string
	Event     string
	Severity  string
	Urgency   string
	Certainty string

	Headline    string
	Description string
	Instruction string

	EffectiveAt *time.Time
	ExpiresAt   *time.Time

	Latitude  *float64
	Longitude *float64

	Areas []string

	Status EventStatus

	SourceURL string

	// ReceivedAt is when the source delivered this event. It is ingestion
	// metadata and is not part of the content fingerprint.
	ReceivedAt time.Time
	// UpdatedAt is when the persisted content last changed. It is ingestion
	// metadata and is not part of the content fingerprint.
	UpdatedAt time.Time
}

// EventKey builds the stable logical identity of an event from its source
// and source-specific ID, e.g. "meteoalarm:2.49.0.1.616.0.DEU...".
func EventKey(source, sourceID string) string {
	return source + ":" + sourceID
}

// Key returns the stable logical identity of the event.
func (e HazardEvent) Key() string {
	return EventKey(e.Source, e.SourceID)
}

// Normalize fills in defaults so the event can be persisted. Zero-value
// time pointers are treated as absent: a zero ExpiresAt must not cause an
// event to expire immediately.
func (e *HazardEvent) Normalize() {
	if e.Status == "" {
		e.Status = StatusActive
	}
	if e.EffectiveAt != nil && e.EffectiveAt.IsZero() {
		e.EffectiveAt = nil
	}
	if e.ExpiresAt != nil && e.ExpiresAt.IsZero() {
		e.ExpiresAt = nil
	}
}

// Validate checks the fields required for persistence. Optional CAP-like
// fields may be empty.
func (e HazardEvent) Validate() error {
	if strings.TrimSpace(e.Source) == "" {
		return errors.New("event source is required")
	}
	if strings.TrimSpace(e.SourceID) == "" {
		return errors.New("event source_id is required")
	}
	if strings.TrimSpace(e.Event) == "" {
		return errors.New("event type is required")
	}
	switch e.Status {
	// The empty status is accepted: Normalize defaults it to active.
	case "", StatusActive, StatusCancelled:
		return nil
	default:
		return fmt.Errorf("invalid event status %q", e.Status)
	}
}
