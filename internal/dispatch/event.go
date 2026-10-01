// Package dispatch owns the canonical dispatch event model and the single
// bounded ingress every MQTT receiver feeds:
//
//	receiver callback -> canonical Event -> bounded ingress queue
//	    -> (future rule engine) -> selected ActionPlugins
//
// The ingress is deliberately passive: nothing consumes events today, so
// a full queue drops the event with a counter and a log line. Durable
// rules, action jobs, deduplication and retry are a later task.
package dispatch

import (
	"time"

	"github.com/szporwolik/WarnFlux/internal/core"
)

// EventKind classifies a canonical dispatch event.
type EventKind string

// The two canonical inputs every future rule sees.
const (
	// EventHazardTransition is a structured transition parsed from the
	// WarnFlux public <prefix>/events MQTT contract.
	EventHazardTransition EventKind = "hazard_transition"
	// EventMQTTMessage is a raw MQTT frame received through a generic
	// subscription. The payload is an opaque byte copy; no JSON, UTF-8 or
	// other interpretation is assumed.
	EventMQTTMessage EventKind = "mqtt_message"
)

// Origin identifies where a canonical event came from.
type Origin struct {
	// Type is the transport class, currently always "mqtt".
	Type string
	// ReceiverID is the configured receiver ID (stable identity for rules).
	ReceiverID string
}

// TransitionType classifies a hazard transition on the /events stream.
type TransitionType string

// Valid transition types published by WarnFlux.
const (
	TransitionNew       TransitionType = "hazard_new"
	TransitionUpdated   TransitionType = "hazard_updated"
	TransitionCancelled TransitionType = "hazard_cancelled"
	TransitionExpired   TransitionType = "hazard_expired"
)

// Hazard is the compact hazard block of one transition.
type Hazard struct {
	EventKey string
	Source   string
	SourceID string
	Event    string
	Severity string
	// ProviderSeverity is the raw provider-scale value (diagnostics only).
	ProviderSeverity string
	Urgency          string
	Certainty        string
	Headline         string
	Areas            []string

	// Description and Instruction are free-text event metadata carried
	// on the wire; optional for every producer.
	Description string
	Instruction string

	// Latitude/Longitude are the optional event coordinates (compose map
	// picker, geo-located sources); nil when the event has no point.
	Latitude  *float64
	Longitude *float64

	EffectiveAt *time.Time
	ExpiresAt   *time.Time

	ReceivedAt time.Time
	UpdatedAt  time.Time
}

// HazardTransition is the canonical form of one WarnFlux /events
// message. ChangeID, Key, Source and Timestamp are the stable identity
// fields for future durable deduplication (the /events stream is
// at-least-once).
type HazardTransition struct {
	Type      TransitionType
	Key       string
	Source    string
	ChangeID  int64
	Timestamp time.Time
	// Publisher is the persistent UUID of the producing WarnFlux
	// instance (empty for legacy publishers): deduplication includes it,
	// so independent instances can never suppress each other.
	Publisher string
	Hazard    Hazard
}

// MQTTMessage is a deep-copied raw MQTT frame. Payload never aliases the
// Paho-owned buffer: it is copied before the callback returns.
type MQTTMessage struct {
	Topic     string
	QoS       byte
	Retained  bool
	Duplicate bool
	Payload   []byte
}

// Event is the canonical dispatch event. Exactly one of Hazard or MQTT is
// non-nil depending on Kind.
type Event struct {
	Kind       EventKind
	ReceivedAt time.Time

	Origin Origin

	Hazard *HazardTransition
	MQTT   *MQTTMessage

	// InboxID is the durable inbox row this event was accepted through
	// (0 = no inbox attached): the routing engine acknowledges it once
	// the event has been evaluated, so a crash between acceptance and
	// evaluation re-delivers the event after a restart.
	InboxID int64
}

// EventForJournalChange converts one journal change into the canonical
// dispatch transition — the SAME shape the MQTT receiver parses from the
// /events stream, so the direct local copy and the broker loopback carry
// identical dedup identities (publisher + change ID). It is the single
// constructor shared by the receiver conversion (EventFromChange) and by
// storage, which persists the canonical transition into the durable inbox
// INSIDE the journal transaction.
func EventForJournalChange(changeType core.ChangeType, changeID int64, publisher string, event core.HazardEvent, receiverID string, now time.Time) Event {
	var typ TransitionType
	switch changeType {
	case core.ChangeNew:
		typ = TransitionNew
	case core.ChangeUpdated:
		typ = TransitionUpdated
	case core.ChangeCancelled:
		typ = TransitionCancelled
	case core.ChangeExpired:
		typ = TransitionExpired
	}

	ev := event
	return Event{
		Kind:       EventHazardTransition,
		ReceivedAt: now,
		Origin:     Origin{Type: "local", ReceiverID: receiverID},
		Hazard: &HazardTransition{
			Type:      typ,
			Key:       ev.Key(),
			Source:    ev.Source,
			ChangeID:  changeID,
			Publisher: publisher,
			Timestamp: ev.UpdatedAt,
			Hazard: Hazard{
				EventKey:         ev.Key(),
				Source:           ev.Source,
				SourceID:         ev.SourceID,
				Event:            ev.Event,
				Severity:         ev.Severity,
				ProviderSeverity: ev.ProviderSeverity,
				Urgency:          ev.Urgency,
				Certainty:        ev.Certainty,
				Headline:         ev.Headline,
				Description:      ev.Description,
				Instruction:      ev.Instruction,
				Areas:            append([]string(nil), ev.Areas...),
				Latitude:         ev.Latitude,
				Longitude:        ev.Longitude,
				EffectiveAt:      ev.EffectiveAt,
				ExpiresAt:        ev.ExpiresAt,
				ReceivedAt:       ev.ReceivedAt,
				UpdatedAt:        ev.UpdatedAt,
			},
		},
	}
}

// Clone returns a deep copy of the event (payload included).
func (e Event) Clone() Event {
	c := e
	if e.Hazard != nil {
		h := *e.Hazard
		h.Hazard = e.Hazard.Hazard
		if e.Hazard.Hazard.Areas != nil {
			h.Hazard.Areas = append([]string(nil), e.Hazard.Hazard.Areas...)
		}
		if e.Hazard.Hazard.EffectiveAt != nil {
			t := *e.Hazard.Hazard.EffectiveAt
			h.Hazard.EffectiveAt = &t
		}
		if e.Hazard.Hazard.ExpiresAt != nil {
			t := *e.Hazard.Hazard.ExpiresAt
			h.Hazard.ExpiresAt = &t
		}
		c.Hazard = &h
	}
	if e.MQTT != nil {
		m := *e.MQTT
		m.Payload = append([]byte(nil), e.MQTT.Payload...)
		c.MQTT = &m
	}
	return c
}
