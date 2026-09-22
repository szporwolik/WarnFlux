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

import "time"

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
	Headline string
	Areas    []string

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
