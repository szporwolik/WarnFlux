// Package core holds the normalized hazard event model shared by all
// sources, the ingestion pipeline and outputs.
package core

import (
	"errors"
	"fmt"
	"math"
	"regexp"
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

// sourcePattern constrains source names to a safe namespace: lowercase
// alphanumeric plus dot, underscore and hyphen. Because the separator ":"
// can never appear in a source name, two (source, sourceID) pairs cannot
// map to the same logical key.
var sourcePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,63}$`)

// maxSourceIDLength bounds provider-specific IDs.
const maxSourceIDLength = 512

// EventKey builds the stable logical identity of an event from its source
// and source-specific ID, e.g. "meteoalarm:2.49.0.1.616.0.DEU...". Callers
// must pass a normalized (lowercase) source name.
func EventKey(source, sourceID string) string {
	return source + ":" + sourceID
}

// Key returns the stable logical identity of the event.
func (e HazardEvent) Key() string {
	return EventKey(e.Source, e.SourceID)
}

// ValidateSource checks a canonical source name. Callers should normalize
// (lowercase + trim) before validating.
func ValidateSource(source string) error {
	if strings.TrimSpace(source) == "" {
		return errors.New("event source is required")
	}
	if !sourcePattern.MatchString(source) {
		return fmt.Errorf("event source %q must match [a-z0-9][a-z0-9._-]* (max 64 chars, lowercase)", source)
	}
	return nil
}

// ValidateSourceID checks a provider-specific event ID. Only harmless outer
// whitespace is ever removed; the value itself is never transformed.
func ValidateSourceID(sourceID string) error {
	if strings.TrimSpace(sourceID) == "" {
		return errors.New("event source_id is required")
	}
	if len(sourceID) > maxSourceIDLength {
		return fmt.Errorf("event source_id exceeds %d characters", maxSourceIDLength)
	}
	return nil
}

// Normalize fills in defaults so the event can be persisted: the source
// name is canonicalized, areas are trimmed/deduplicated, zero-value time
// pointers become nil and an empty status becomes active.
func (e *HazardEvent) Normalize() {
	e.Source = strings.ToLower(strings.TrimSpace(e.Source))
	e.SourceID = strings.TrimSpace(e.SourceID)
	if e.Status == "" {
		e.Status = StatusActive
	}
	if e.EffectiveAt != nil && e.EffectiveAt.IsZero() {
		e.EffectiveAt = nil
	}
	if e.ExpiresAt != nil && e.ExpiresAt.IsZero() {
		e.ExpiresAt = nil
	}
	e.Areas = normalizeAreas(e.Areas)
}

// normalizeAreas trims, drops empty entries and collapses duplicates into a
// fresh slice, so callers never share the original backing array.
func normalizeAreas(areas []string) []string {
	if len(areas) == 0 {
		return nil
	}
	seen := make(map[string]bool, len(areas))
	out := make([]string, 0, len(areas))
	for _, area := range areas {
		area = strings.TrimSpace(area)
		if area == "" || seen[area] {
			continue
		}
		seen[area] = true
		out = append(out, area)
	}
	return out
}

// Clone returns a deep copy of the event: slices and pointer fields are
// copied, so mutating the result can never affect the original.
func (e HazardEvent) Clone() HazardEvent {
	cp := e
	if e.EffectiveAt != nil {
		t := *e.EffectiveAt
		cp.EffectiveAt = &t
	}
	if e.ExpiresAt != nil {
		t := *e.ExpiresAt
		cp.ExpiresAt = &t
	}
	if e.Latitude != nil {
		v := *e.Latitude
		cp.Latitude = &v
	}
	if e.Longitude != nil {
		v := *e.Longitude
		cp.Longitude = &v
	}
	cp.Areas = append([]string(nil), e.Areas...)
	return cp
}

// Validate checks the fields required for persistence. Optional CAP-like
// fields may be empty. Coordinates must be finite, within range, and either
// both or neither present.
func (e HazardEvent) Validate() error {
	if err := ValidateSource(e.Source); err != nil {
		return err
	}
	if err := ValidateSourceID(e.SourceID); err != nil {
		return err
	}
	if strings.TrimSpace(e.Event) == "" {
		return errors.New("event type is required")
	}
	switch e.Status {
	// The empty status is accepted: Normalize defaults it to active.
	case "", StatusActive, StatusCancelled:
	default:
		return fmt.Errorf("invalid event status %q", e.Status)
	}
	if (e.Latitude == nil) != (e.Longitude == nil) {
		return errors.New("event latitude and longitude must both be set or both be absent")
	}
	if e.Latitude != nil {
		if math.IsNaN(*e.Latitude) || math.IsInf(*e.Latitude, 0) {
			return fmt.Errorf("event latitude must be finite, got %v", *e.Latitude)
		}
		if *e.Latitude < -90 || *e.Latitude > 90 {
			return fmt.Errorf("event latitude out of range [-90, 90]: %v", *e.Latitude)
		}
	}
	if e.Longitude != nil {
		if math.IsNaN(*e.Longitude) || math.IsInf(*e.Longitude, 0) {
			return fmt.Errorf("event longitude must be finite, got %v", *e.Longitude)
		}
		if *e.Longitude < -180 || *e.Longitude > 180 {
			return fmt.Errorf("event longitude out of range [-180, 180]: %v", *e.Longitude)
		}
	}
	return nil
}
