package core

import (
	"encoding/json"
	"fmt"
	"regexp"
	"time"
)

// MaxInformationPayloadBytes bounds the payload of an information message.
// Information is auxiliary latest-state data (weather snapshots, not
// HazardEvents); a generous cap keeps retained MQTT topics and memory
// bounded without limiting legitimate content.
const MaxInformationPayloadBytes = 256 << 10

// informationSlugRE is the shape of safe informational identifiers. They
// become MQTT topic segments, so they must be lowercase slugs that cannot
// collide with MQTT wildcards or path separators.
var informationSlugRE = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,63}$`)

// InformationMessage is a non-hazard, latest-state informational message
// produced by a source plugin and forwarded to information-capable outputs
// (currently the retained MQTT information topics). It is deliberately NOT
// part of the HazardEvent database tables, the durable change journal or
// the output cursors: a lost information snapshot is acceptable, a lost
// committed HazardEvent is not.
type InformationMessage struct {
	// Source names the producing integration type, e.g. "openmeteo".
	Source string
	// ProducerID is the configured source plugin INSTANCE ID (e.g.
	// "weather-home"). It is stamped by the manager when the message
	// leaves the source boundary; plugins must not choose it.
	ProducerID string
	// Key is the stable identity of the information object within the
	// producer, e.g. the configured location ID.
	Key string
	// Kind names the information type, e.g. "weather".
	Kind string
	// GeneratedAt is when WarnFlux generated this snapshot.
	GeneratedAt time.Time
	// ValidUntil optionally bounds freshness (application metadata, not a
	// provider guarantee); consumers use it to reject stale retained data.
	ValidUntil *time.Time
	// Payload is provider-normalized information JSON (the complete wire
	// document), owned by the message.
	Payload json.RawMessage
}

// Clone returns a deep copy so ownership can cross package boundaries
// without aliasing the payload.
func (m InformationMessage) Clone() InformationMessage {
	m.Payload = append(json.RawMessage(nil), m.Payload...)
	if m.ValidUntil != nil {
		t := *m.ValidUntil
		m.ValidUntil = &t
	}
	return m
}

// Validate enforces the information-message bounds: safe slugs for source,
// key and kind (they become MQTT topic segments), a non-zero generation
// time, and a valid, size-capped JSON payload.
func (m InformationMessage) Validate() error {
	if !informationSlugRE.MatchString(m.Source) {
		return fmt.Errorf("source must be a lowercase slug matching %s, got %q", informationSlugRE, m.Source)
	}
	if !informationSlugRE.MatchString(m.ProducerID) {
		return fmt.Errorf("producer_id must be a lowercase slug matching %s, got %q", informationSlugRE, m.ProducerID)
	}
	if !informationSlugRE.MatchString(m.Key) {
		return fmt.Errorf("key must be a lowercase slug matching %s, got %q", informationSlugRE, m.Key)
	}
	if !informationSlugRE.MatchString(m.Kind) {
		return fmt.Errorf("kind must be a lowercase slug matching %s, got %q", informationSlugRE, m.Kind)
	}
	if m.GeneratedAt.IsZero() {
		return fmt.Errorf("generated_at must be non-zero")
	}
	if m.ValidUntil != nil && (!m.ValidUntil.After(m.GeneratedAt) || m.ValidUntil.IsZero()) {
		return fmt.Errorf("valid_until must be after generated_at")
	}
	if len(m.Payload) > MaxInformationPayloadBytes {
		return fmt.Errorf("payload is %d bytes, maximum %d", len(m.Payload), MaxInformationPayloadBytes)
	}
	if !json.Valid(m.Payload) {
		return fmt.Errorf("payload is not valid JSON")
	}
	return nil
}
