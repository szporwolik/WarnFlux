package mqttreceiver

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/szporwolik/WarnFlux/internal/dispatch"
	"github.com/szporwolik/WarnFlux/internal/dispatch/state"
)

// Ingestor is the MQTT message callback for one receiver. It is the single
// entry point for every message that receiver receives.
//
// The callback is deliberately FAST and never blocks: it validates the
// topic, bounds the payload, deep-copies the bytes, decodes known Router
// wire formats, updates the mirrored state and offers canonical events to
// the dispatch ingress (non-blocking). It never waits for actions, never
// queries SQLite, never renders HTML and never performs blocking I/O.
type Ingestor struct {
	receiverID string
	prefix     string
	wfEnabled  bool
	filters    []string

	state   *state.State
	ingress *dispatch.Ingress
	stats   *Stats
	logger  *slog.Logger
}

// NewIngestor builds the message ingestor for one receiver.
func NewIngestor(receiverID string, wfEnabled bool, prefix string, filters []string,
	st *state.State, ingress *dispatch.Ingress, stats *Stats, logger *slog.Logger) *Ingestor {
	return &Ingestor{
		receiverID: receiverID,
		prefix:     prefix,
		wfEnabled:  wfEnabled,
		filters:    filters,
		state:      st,
		ingress:    ingress,
		stats:      stats,
		logger:     logger,
	}
}

// HandleMessage implements paho's MessageHandler.
//
// Routing precedence: when WarnFlux mode is enabled, topics inside the
// configured prefix are ALWAYS treated as protocol topics — a malformed
// protocol frame is rejected and counted, never reclassified as a raw
// generic event, even when generic subscriptions overlap.
func (in *Ingestor) HandleMessage(_ mqtt.Client, msg mqtt.Message) {
	now := time.Now()
	if in.stats != nil {
		in.stats.Messages.Add(1)
	}

	topic := msg.Topic()
	payload := msg.Payload()

	if len(payload) > MaxPayload {
		if in.stats != nil {
			in.stats.Oversized.Add(1)
		}
		in.logger.Warn("receiver: oversized message ignored",
			"receiver", in.receiverID, "topic", topic, "bytes", len(payload), "limit", MaxPayload)
		return
	}

	if in.wfEnabled && strings.HasPrefix(topic, in.prefix+"/") {
		parsed := ParseTopic(in.prefix, topic)
		switch parsed.Kind {
		case TopicActive:
			in.handleActive(parsed, topic, payload)
		case TopicInfo:
			in.handleInfo(parsed, topic, payload, now)
		case TopicStatus:
			in.handleStatus(topic, payload, now)
		case TopicEvents:
			in.handleEvent(topic, payload, now)
		default:
			// Inside the protocol namespace but not a valid protocol
			// topic: reject, never fall through to generic handling.
			in.reject(topic, "malformed warnflux protocol topic", nil)
		}
		return
	}

	if in.matchesFilter(topic) {
		in.handleGeneric(msg, topic, payload, now)
		return
	}
	in.logger.Debug("receiver: unsubscribed topic ignored", "receiver", in.receiverID, "topic", topic)
}

func (in *Ingestor) matchesFilter(topic string) bool {
	for _, f := range in.filters {
		if MatchFilter(topic, f) {
			return true
		}
	}
	return false
}

// handleGeneric feeds a raw subscribed frame into the canonical ingress as
// a generic MQTT event. The payload is deep-copied: the canonical event
// never aliases the Paho-owned buffer.
func (in *Ingestor) handleGeneric(msg mqtt.Message, topic string, payload []byte, now time.Time) {
	ev := dispatch.Event{
		Kind:       dispatch.EventMQTTMessage,
		ReceivedAt: now,
		Origin:     dispatch.Origin{Type: "mqtt", ReceiverID: in.receiverID},
		MQTT: &dispatch.MQTTMessage{
			Topic:     topic,
			QoS:       msg.Qos(),
			Retained:  msg.Retained(),
			Duplicate: msg.Duplicate(),
			Payload:   append([]byte(nil), payload...),
		},
	}
	if !in.ingress.Enqueue(ev) {
		if in.stats != nil {
			in.stats.Dropped.Add(1)
		}
		in.logger.Warn("receiver: dispatch intake full, generic event dropped",
			"receiver", in.receiverID, "topic", topic)
	}
}

// handleActive stores or removes a retained active hazard (per receiver
// namespace). A zero-length retained payload removes the hazard.
func (in *Ingestor) handleActive(pt ParsedTopic, topic string, payload []byte) {
	if len(payload) == 0 {
		in.state.DeleteActive(in.receiverID, topic)
		in.logger.Debug("receiver: active hazard removed (empty retained payload)",
			"receiver", in.receiverID, "topic", topic)
		return
	}

	var wh ActivePayload
	if err := json.Unmarshal(payload, &wh); err != nil {
		in.reject(topic, "active: invalid JSON", err)
		return
	}
	if err := validateActive(&wh); err != nil {
		in.reject(topic, "active", err)
		return
	}
	if TopicHash(wh.EventKey) != pt.Hash {
		in.reject(topic, "active: event_key does not match topic hash", nil)
		return
	}

	h := state.Hazard{
		EventKey:    wh.EventKey,
		Source:      wh.Event.Source,
		SourceID:    wh.Event.SourceID,
		Category:    wh.Event.Category,
		Event:       wh.Event.Event,
		Severity:    wh.Event.Severity,
		Urgency:     wh.Event.Urgency,
		Certainty:   wh.Event.Certainty,
		Headline:    wh.Event.Headline,
		Description: wh.Event.Description,
		Instruction: wh.Event.Instruction,
		EffectiveAt: optTime(wh.Event.EffectiveAt),
		ExpiresAt:   optTime(wh.Event.ExpiresAt),
		Areas:       wh.Event.Areas,
		Status:      wh.Event.Status,
		ReceivedAt:  parseTime(wh.Event.ReceivedAt),
		UpdatedAt:   parseTime(wh.Event.UpdatedAt),
	}
	if err := in.state.AddOrUpdateActive(in.receiverID, topic, h); err != nil {
		in.logger.Warn("receiver: active hazard rejected",
			"receiver", in.receiverID, "topic", topic, "error", err)
	}
}

// handleInfo stores a retained informational message. Canonical weather
// documents are decoded into a typed snapshot.
func (in *Ingestor) handleInfo(pt ParsedTopic, topic string, payload []byte, now time.Time) {
	if len(payload) == 0 {
		// The contract never publishes empty info payloads; ignore defensively.
		return
	}

	entry := state.InfoEntry{
		Source:     pt.InfoSource,
		ProducerID: pt.ProducerID,
		Key:        pt.Key,
		Kind:       pt.InfoKind,
		ReceivedAt: now,
	}

	if pt.InfoKind == TypeWeather {
		var ww wireWeather
		if err := json.Unmarshal(payload, &ww); err != nil {
			in.reject(topic, "info: weather invalid JSON", err)
			return
		}
		if ww.SchemaVersion != WireSchemaVersion {
			in.reject(topic, "info: weather", errSchemaVersion)
			return
		}
		if ww.Type != TypeWeather {
			in.reject(topic, "info: weather", errType)
			return
		}
		entry.Weather = toWeather(&ww)
	}

	if err := in.state.AddOrUpdateInfo(in.receiverID, topic, entry); err != nil {
		in.logger.Warn("receiver: info entry rejected",
			"receiver", in.receiverID, "topic", topic, "error", err)
	}
}

// handleStatus stores the latest WarnFlux status of this receiver.
func (in *Ingestor) handleStatus(topic string, payload []byte, now time.Time) {
	if len(payload) == 0 {
		// Empty retained status: keep last known status.
		in.logger.Debug("receiver: empty status payload ignored", "receiver", in.receiverID, "topic", topic)
		return
	}
	var ws wireStatus
	if err := json.Unmarshal(payload, &ws); err != nil {
		in.reject(topic, "status: invalid JSON", err)
		return
	}
	if ws.SchemaVersion != WireSchemaVersion {
		in.reject(topic, "status", errSchemaVersion)
		return
	}
	if ws.Service != ServiceName {
		in.reject(topic, "status", errService)
		return
	}
	if ws.State != RouterStateRunning && ws.State != RouterStateOffline {
		in.reject(topic, "status", errBadState)
		return
	}
	in.state.SetRouterStatus(in.receiverID, state.RouterStatus{
		Valid:           true,
		ReceivedAt:      now,
		Service:         ws.Service,
		State:           ws.State,
		GeneratedAt:     parseTime(ws.GeneratedAt),
		Version:         ws.Version,
		UptimeSeconds:   ws.UptimeSeconds,
		DatabaseHealthy: ws.DatabaseHealthy,
		PendingChanges:  ws.PendingChanges,
	})
}

// handleEvent feeds strictly validated hazard transitions into the
// canonical dispatch ingress. It never infers actions.
func (in *Ingestor) handleEvent(topic string, payload []byte, now time.Time) {
	if len(payload) == 0 {
		return
	}
	we, err := ParseEventPayload(payload)
	if err != nil {
		in.reject(topic, "events", err)
		return
	}

	var typ dispatch.TransitionType
	switch we.ChangeType {
	case ChangeNew:
		typ = dispatch.TransitionNew
	case ChangeUpdated:
		typ = dispatch.TransitionUpdated
	case ChangeCancelled:
		typ = dispatch.TransitionCancelled
	case ChangeExpired:
		typ = dispatch.TransitionExpired
	}

	ts := parseTime(we.Event.UpdatedAt)
	if ts.IsZero() {
		ts = parseTime(we.Event.ReceivedAt)
	}
	if ts.IsZero() {
		ts = now
	}

	ev := dispatch.Event{
		Kind:       dispatch.EventHazardTransition,
		ReceivedAt: now,
		Origin:     dispatch.Origin{Type: "mqtt", ReceiverID: in.receiverID},
		Hazard: &dispatch.HazardTransition{
			Type:      typ,
			Key:       we.EventKey,
			Source:    we.Event.Source,
			ChangeID:  we.ChangeID,
			Timestamp: ts,
			Hazard: dispatch.Hazard{
				EventKey:    we.EventKey,
				Source:      we.Event.Source,
				SourceID:    we.Event.SourceID,
				Event:       we.Event.Event,
				Severity:    we.Event.Severity,
				Urgency:     we.Event.Urgency,
				Certainty:   we.Event.Certainty,
				Headline:    we.Event.Headline,
				Areas:       append([]string(nil), we.Event.Areas...),
				EffectiveAt: optTime(we.Event.EffectiveAt),
				ExpiresAt:   optTime(we.Event.ExpiresAt),
				ReceivedAt:  parseTime(we.Event.ReceivedAt),
				UpdatedAt:   parseTime(we.Event.UpdatedAt),
			},
		},
	}

	// Non-blocking offer: a full dispatch queue drops the event instead of
	// slowing down MQTT ingestion.
	if !in.ingress.Enqueue(ev) {
		if in.stats != nil {
			in.stats.Dropped.Add(1)
		}
		in.logger.Warn("receiver: dispatch intake full, transition dropped",
			"receiver", in.receiverID, "type", typ, "event_key", we.EventKey)
	}
}

// reject records a malformed message and continues.
func (in *Ingestor) reject(topic, what string, err error) {
	if in.stats != nil {
		in.stats.Malformed.Add(1)
	}
	if err != nil {
		in.logger.Warn("receiver: malformed message ignored",
			"receiver", in.receiverID, "topic", topic, "reason", what, "error", err)
		return
	}
	in.logger.Warn("receiver: malformed message ignored",
		"receiver", in.receiverID, "topic", topic, "reason", what)
}

// toWeather converts the canonical weather wire document into the compact
// in-memory snapshot used by the dashboard.
func toWeather(ww *wireWeather) *state.Weather {
	w := &state.Weather{
		GeneratedAt:  parseTime(ww.GeneratedAt),
		ValidUntil:   optTime(&ww.ValidUntil),
		ProviderName: ww.Provider.Name,
		LocationID:   ww.Location.ID,
		LocationName: ww.Location.Name,
		Latitude:     ww.Location.Latitude,
		Longitude:    ww.Location.Longitude,
	}
	if ww.Current != nil {
		w.TemperatureC = ww.Current.TemperatureC
		w.Condition = ww.Current.Condition
		w.HumidityPct = ww.Current.RelativeHumidityPct
		w.WindSpeedKmh = ww.Current.WindSpeedKmh
		w.WindDirectionDeg = ww.Current.WindDirectionDeg
		w.WindGustsKmh = ww.Current.WindGustsKmh
		w.PressureMSLHpa = ww.Current.PressureMSLHpa
	}
	return w
}

// validateActive checks the mandatory envelope fields of an active payload.
func validateActive(wh *ActivePayload) error {
	if wh.SchemaVersion != WireSchemaVersion {
		return fmt.Errorf("%w: got %d", errSchemaVersion, wh.SchemaVersion)
	}
	if wh.Type != TypeActiveHazard {
		return errType
	}
	if wh.EventKey == "" {
		return errEmptyKey
	}
	return nil
}

// Stats are the per-receiver ingest counters (atomics; safe for concurrent
// callback use).
type Stats struct {
	Messages  atomic.Int64
	Malformed atomic.Int64
	Oversized atomic.Int64
	Dropped   atomic.Int64
}
