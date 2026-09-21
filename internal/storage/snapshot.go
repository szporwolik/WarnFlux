package storage

import (
	"time"

	"github.com/szporwolik/WarnFlux/internal/core"
)

// EventSnapshot is the immutable internal representation of a HazardEvent
// at the moment a journal change was written. The changes journal stores
// one snapshot per transition; the events table remains the current-state
// table. Field order is fixed and no maps are involved, so JSON marshaling
// is deterministic.
//
// SnapshotOf / Event are deep copies in both directions: neither the live
// event nor a reconstructed one can mutate a snapshot through shared
// pointers.
type EventSnapshot struct {
	Source      string           `json:"source"`
	SourceID    string           `json:"source_id"`
	Category    string           `json:"category"`
	Event       string           `json:"event"`
	Severity    string           `json:"severity"`
	Urgency     string           `json:"urgency"`
	Certainty   string           `json:"certainty"`
	Headline    string           `json:"headline"`
	Description string           `json:"description"`
	Instruction string           `json:"instruction"`
	EffectiveAt *time.Time       `json:"effective_at,omitempty"`
	ExpiresAt   *time.Time       `json:"expires_at,omitempty"`
	Latitude    *float64         `json:"latitude,omitempty"`
	Longitude   *float64         `json:"longitude,omitempty"`
	Areas       []string         `json:"areas"`
	Status      core.EventStatus `json:"status"`
	SourceURL   string           `json:"source_url"`
	ReceivedAt  time.Time        `json:"received_at"`
	UpdatedAt   time.Time        `json:"updated_at"`
}

// SnapshotOf deep-copies an event into its immutable snapshot form.
func SnapshotOf(e core.HazardEvent) EventSnapshot {
	s := EventSnapshot{
		Source:      e.Source,
		SourceID:    e.SourceID,
		Category:    e.Category,
		Event:       e.Event,
		Severity:    e.Severity,
		Urgency:     e.Urgency,
		Certainty:   e.Certainty,
		Headline:    e.Headline,
		Description: e.Description,
		Instruction: e.Instruction,
		Areas:       append([]string(nil), e.Areas...),
		Status:      e.Status,
		SourceURL:   e.SourceURL,
		ReceivedAt:  e.ReceivedAt,
		UpdatedAt:   e.UpdatedAt,
	}
	if e.EffectiveAt != nil {
		t := *e.EffectiveAt
		s.EffectiveAt = &t
	}
	if e.ExpiresAt != nil {
		t := *e.ExpiresAt
		s.ExpiresAt = &t
	}
	if e.Latitude != nil {
		v := *e.Latitude
		s.Latitude = &v
	}
	if e.Longitude != nil {
		v := *e.Longitude
		s.Longitude = &v
	}
	return s
}

// ToEvent reconstructs the HazardEvent as it was at snapshot time.
func (s EventSnapshot) ToEvent() core.HazardEvent {
	e := core.HazardEvent{
		Source:      s.Source,
		SourceID:    s.SourceID,
		Category:    s.Category,
		Event:       s.Event,
		Severity:    s.Severity,
		Urgency:     s.Urgency,
		Certainty:   s.Certainty,
		Headline:    s.Headline,
		Description: s.Description,
		Instruction: s.Instruction,
		Areas:       append([]string(nil), s.Areas...),
		Status:      s.Status,
		SourceURL:   s.SourceURL,
		ReceivedAt:  s.ReceivedAt,
		UpdatedAt:   s.UpdatedAt,
	}
	if s.EffectiveAt != nil {
		t := *s.EffectiveAt
		e.EffectiveAt = &t
	}
	if s.ExpiresAt != nil {
		t := *s.ExpiresAt
		e.ExpiresAt = &t
	}
	if s.Latitude != nil {
		v := *s.Latitude
		e.Latitude = &v
	}
	if s.Longitude != nil {
		v := *s.Longitude
		e.Longitude = &v
	}
	return e
}
