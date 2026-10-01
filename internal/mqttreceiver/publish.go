package mqttreceiver

import (
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/szporwolik/WarnFlux/internal/dispatch/state"
	"github.com/szporwolik/WarnFlux/internal/mqttpolicy"
)

// publishTimeout bounds a single broker publish (connect-level recovery is
// paho's job; a stuck publish must not block the web handler forever).
const publishTimeout = 10 * time.Second

// PublishActive publishes a retained active-hazard document on the WarnFlux
// prefix of the first connected receiver. The topic is derived from the
// protocol rules: <prefix>/active/<source>/<sha256(event_key)>. The
// receiving ingestor (usually this same broker, via the subscription)
// mirrors the document into the active state.
func (m *Manager) PublishActive(source string, h state.Hazard) error {
	if !mqttpolicy.Allowed(mqttpolicy.CatActive) {
		return nil // masked by the publish policy
	}
	r, err := m.publishReceiver()
	if err != nil {
		return err
	}

	now := time.Now().UTC()
	if h.ReceivedAt.IsZero() {
		h.ReceivedAt = now
	}
	if h.UpdatedAt.IsZero() {
		h.UpdatedAt = now
	}

	wh := ActivePayload{
		SchemaVersion: WireSchemaVersion,
		Type:          TypeActiveHazard,
		EventKey:      h.EventKey,
		Event: HazardPayload{
			Source:      h.Source,
			SourceID:    h.SourceID,
			Category:    h.Category,
			Event:       h.Event,
			Severity:    h.Severity,
			Urgency:     h.Urgency,
			Certainty:   h.Certainty,
			Headline:    h.Headline,
			Description: h.Description,
			Instruction: h.Instruction,
			Latitude:    h.Latitude,
			Longitude:   h.Longitude,
			EffectiveAt: wireTimePtr(h.EffectiveAt),
			ExpiresAt:   wireTimePtr(h.ExpiresAt),
			Areas:       h.Areas,
			Status:      h.Status,
			ReceivedAt:  h.ReceivedAt.Format(time.RFC3339),
			UpdatedAt:   h.UpdatedAt.Format(time.RFC3339),
		},
	}
	payload, err := json.Marshal(wh)
	if err != nil {
		return err
	}
	return r.publish(activeTopic(r.cfg.WF.TopicPrefix, source, h.EventKey), 1, true, payload)
}

// ExpireActive removes a retained active hazard by publishing an empty
// retained payload on its topic — the protocol's delete operation.
func (m *Manager) ExpireActive(source, eventKey string) error {
	if !mqttpolicy.Allowed(mqttpolicy.CatActive) {
		return nil // masked by the publish policy
	}
	r, err := m.publishReceiver()
	if err != nil {
		return err
	}
	return r.publish(activeTopic(r.cfg.WF.TopicPrefix, source, eventKey), 1, true, nil)
}

// activeTopic is the canonical active topic for a source + event key.
func activeTopic(prefix, source, eventKey string) string {
	return prefix + "/active/" + source + "/" + TopicHash(eventKey)
}

// PublishRaw publishes one arbitrary document under the WarnFlux topic
// prefix of the first connected receiver: <prefix>/<suffix>, QoS 1. A nil
// payload with retained=true is the retained-topic delete. The suffix must
// be a plain topic path (no wildcards); callers build their own layout —
// the APRS hub publishes its station/packet/message feeds through it.
// The suffix also picks the publish-mask category: a masked family is
// skipped without an error (the policy, not the publisher, decides).
func (m *Manager) PublishRaw(suffix string, retained bool, payload []byte) error {
	if cat := rawCategory(suffix); cat != 0 && !mqttpolicy.Allowed(cat) {
		slog.Debug("mqtt publish masked by policy", "suffix", suffix)
		return nil
	}
	r, err := m.publishReceiver()
	if err != nil {
		return err
	}
	return r.publish(r.cfg.WF.TopicPrefix+"/"+suffix, 1, retained, payload)
}

// rawCategory maps a PublishRaw suffix onto its publish-mask category;
// 0 means "not classified" (always allowed — unknown future feeds stay
// on until explicitly categorized).
func rawCategory(suffix string) mqttpolicy.Category {
	switch {
	case suffix == "events":
		// Routed APRS/Meshtastic messages republished as canonical events.
		return mqttpolicy.CatEvents
	case strings.HasPrefix(suffix, "aprs/stations/"):
		return mqttpolicy.CatAPRSStations
	case strings.HasPrefix(suffix, "aprs/bulletins/"):
		return mqttpolicy.CatAPRSBulletins
	case suffix == "aprs/packets":
		return mqttpolicy.CatAPRSPackets
	case suffix == "aprs/messages":
		return mqttpolicy.CatAPRSMessages
	case strings.HasPrefix(suffix, "meshtastic/stations/"):
		return mqttpolicy.CatMeshtasticStations
	case suffix == "meshtastic/messages":
		return mqttpolicy.CatMeshtasticMessages
	case strings.HasPrefix(suffix, "info/"):
		return mqttpolicy.CatInfo
	}
	return 0
}

// wireTimePtr renders an optional time as an RFC 3339 wire string.
func wireTimePtr(t *time.Time) *string {
	if t == nil {
		return nil
	}
	s := t.UTC().Format(time.RFC3339)
	return &s
}

// publishReceiver returns the first enabled WarnFlux-mode receiver with a
// live connection, or an error when none exists.
func (m *Manager) publishReceiver() (*Receiver, error) {
	for _, r := range m.receivers {
		if r.cfg.Enabled && r.cfg.WF.Enabled && r.isConnected() {
			return r, nil
		}
	}
	return nil, errors.New("no connected WarnFlux receiver to publish on")
}

// isConnected reports whether the underlying paho client is connected.
func (r *Receiver) isConnected() bool {
	return r.client != nil && r.client.IsConnected()
}

// publish sends one message through the receiver's client (bounded wait).
func (r *Receiver) publish(topic string, qos byte, retained bool, payload []byte) error {
	if !r.isConnected() {
		return errors.New("receiver not connected")
	}
	tok := r.client.Publish(topic, qos, retained, payload)
	if !tok.WaitTimeout(publishTimeout) {
		return errors.New("publish timed out")
	}
	return tok.Error()
}
