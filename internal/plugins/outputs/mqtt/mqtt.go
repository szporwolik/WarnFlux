// Package mqtt implements the single built-in MQTT output: it publishes
// meaningful EventChange values (non-retained event stream) and an optional
// retained application status topic (heartbeat). This replaces the legacy
// top-level mqtt configuration and internal/mqtt package.
package mqtt

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
	"gopkg.in/yaml.v3"

	"github.com/szporwolik/WarnFlux/internal/core"
	"github.com/szporwolik/WarnFlux/internal/plugin"
)

// Type is the plugin type name used in the YAML configuration.
const Type = "mqtt"

// wireSchemaVersion is the version of the MQTT wire schema described below.
const wireSchemaVersion = 1

const connectTimeout = 10 * time.Second

// maxReconnectInterval caps paho's automatic reconnection backoff (default
// would be 10 minutes). Hazard delivery must recover promptly after the
// broker returns: without this cap, an extended outage makes the next
// reconnect attempt minutes away and the durable journal stays undelivered
// that much longer.
const maxReconnectInterval = 30 * time.Second

// Active-view rehydration bounds: each republish after a (re)connect waits
// at most activeRehydrateTimeout for the broker acknowledgment, and the
// convergence loop (concurrent Handle updates vs. rehydration snapshot)
// runs at most maxRehydratePasses iterations.
const (
	activeRehydrateTimeout = 10 * time.Second
	maxRehydratePasses     = 4
)

// Input bounds: sane high caps so configuration cannot force pathological
// memory/network behavior, not tiny policy limits.
const (
	maxClientIDBytes      = 256
	maxTopicPrefixBytes   = 256
	maxPasswordFileBytes  = 64 * 1024
	offlinePublishTimeout = time.Second
)

// Config is the plugin-specific configuration. Credentials are never
// logged.
type Config struct {
	Broker   string `yaml:"broker"`
	ClientID string `yaml:"client_id"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	// PasswordFile, when set, reads the broker password from a Docker
	// secret / mounted file (e.g. /run/secrets/mqtt_password). Mutually
	// exclusive with password.
	PasswordFile string `yaml:"password_file"`
	TopicPrefix  string `yaml:"topic_prefix"`
	// QoS is a pointer so an omitted value defaults to 1 while an explicit
	// 0 is rejected (QoS 0 would downgrade hazard delivery to best effort).
	QoS *byte `yaml:"qos"`
	// HeartbeatInterval publishes the retained status topic periodically.
	// Zero disables the heartbeat.
	HeartbeatInterval time.Duration `yaml:"heartbeat_interval"`
}

// Output publishes EventChange values to MQTT and, optionally, the retained
// application status topic. Paho owns the authoritative connection state
// (IsConnectionOpen distinguishes an active connection from reconnect mode);
// no parallel boolean is maintained here.
type Output struct {
	cfg Config
	qos byte

	mu     sync.Mutex
	client mqttClient

	// activeCache is the desired retained active view: one entry per
	// currently ACTIVE hazard (never cancelled/expired events, never
	// /events history). It exists only so a broker reconnect or a fresh
	// broker can be rehydrated without a new provider update; the
	// authoritative state is always SQLite.
	activeMu    sync.Mutex
	activeSeq   uint64
	activeCache map[string]activeCacheEntry

	// pendingDeletes are retained topics that MUST NOT exist: cancelled
	// or expired hazards whose retained DELETE has not yet been confirmed
	// by the broker (transient failures). Memory scales with currently
	// unresolved deletes only — never event history, never persisted. A
	// later rehydrate pass or reconnect retries every entry until the
	// broker confirms the deletion. A key is never authoritative in both
	// sets: reactivation removes its pending delete.
	pendingDeletes map[string]activeDeleteEntry

	// rehydrateMu serializes active-state rehydration passes (at most one
	// runs at a time per output).
	rehydrateMu sync.Mutex
}

// activeCacheEntry is one desired retained active payload plus the cache
// generation it was written with (convergence guard for rehydration).
type activeCacheEntry struct {
	key     string
	topic   string
	payload []byte
	seq     uint64
}

// activeDeleteEntry is one unresolved retained-topic deletion plus the
// generation it was registered with.
type activeDeleteEntry struct {
	key   string
	topic string
	seq   uint64
}

// mqttClient is the minimal paho client surface used by this plugin. It is
// an interface only so tests can substitute a deterministic fake; the only
// production implementation is paho.Client.
type mqttClient interface {
	IsConnectionOpen() bool
	Connect() paho.Token
	Disconnect(quiesce uint)
	Publish(topic string, qos byte, retained bool, payload any) paho.Token
	OptionsReader() paho.ClientOptionsReader
}

// New decodes and validates the plugin-specific configuration. The broker
// connection is established lazily on the first delivery, so an unreachable
// broker is a runtime failure (isolated by the framework), not a startup
// error.
func New(node *yaml.Node) (plugin.OutputPlugin, error) {
	var cfg Config
	if err := plugin.DecodeConfig(node, &cfg); err != nil {
		return nil, err
	}
	broker := strings.TrimSpace(cfg.Broker)
	if broker == "" {
		return nil, fmt.Errorf("broker must not be empty")
	}
	if strings.ContainsAny(broker, " \t\n") {
		return nil, fmt.Errorf("broker must not contain whitespace, got %q", cfg.Broker)
	}
	cfg.Broker = broker
	clientID := strings.TrimSpace(cfg.ClientID)
	if clientID == "" {
		return nil, fmt.Errorf("client_id is required: every WarnFlux instance needs its own MQTT client ID (two instances sharing one ID will kick each other off the broker)")
	}
	if len(clientID) > maxClientIDBytes {
		return nil, fmt.Errorf("client_id is %d bytes, maximum %d", len(clientID), maxClientIDBytes)
	}
	cfg.ClientID = clientID
	// Normalize the prefix and reject values that would corrupt topics.
	prefix, err := normalizeTopicPrefix(cfg.TopicPrefix)
	if err != nil {
		return nil, err
	}
	if len(prefix) > maxTopicPrefixBytes {
		return nil, fmt.Errorf("topic_prefix is %d bytes, maximum %d", len(prefix), maxTopicPrefixBytes)
	}
	cfg.TopicPrefix = prefix
	// QoS 0 is rejected: it would downgrade hazard event delivery to best
	// effort, contradicting WarnFlux's at-least-once journal semantics.
	// An omitted qos defaults to 1.
	qos := byte(1)
	if cfg.QoS != nil {
		if *cfg.QoS == 0 || *cfg.QoS > 2 {
			return nil, fmt.Errorf("qos must be 1 or 2 for at-least-once hazard delivery, got %d", *cfg.QoS)
		}
		qos = *cfg.QoS
	}
	if cfg.Password != "" && cfg.PasswordFile != "" {
		return nil, fmt.Errorf("password and password_file are mutually exclusive")
	}
	if cfg.PasswordFile != "" {
		if info, err := os.Stat(cfg.PasswordFile); err != nil {
			return nil, fmt.Errorf("stat password_file: %w", err)
		} else if info.Size() > maxPasswordFileBytes {
			return nil, fmt.Errorf("password_file is %d bytes, maximum %d", info.Size(), maxPasswordFileBytes)
		}
		data, err := os.ReadFile(cfg.PasswordFile)
		if err != nil {
			return nil, fmt.Errorf("read password_file: %w", err)
		}
		cfg.Password = strings.TrimRight(string(data), "\r\n")
	}
	if cfg.HeartbeatInterval < 0 {
		return nil, fmt.Errorf("heartbeat_interval must not be negative, got %s", cfg.HeartbeatInterval)
	}

	routePahoLogs()
	// Retained Last Will: if this process disappears without disconnecting,
	// the broker publishes a retained "offline" status on <prefix>/status,
	// so consumers can distinguish a dead instance from a stale heartbeat.
	// The will payload is fixed at plugin construction; its generated_at
	// therefore reflects when the MQTT output instance configured its will,
	// not each reconnect. state=offline is authoritative regardless of the
	// timestamp.
	willPayload, err := json.Marshal(offlineWireStatus(time.Now()))
	if err != nil {
		return nil, fmt.Errorf("marshal last will: %w", err)
	}

	out := &Output{cfg: cfg, qos: qos, activeCache: make(map[string]activeCacheEntry), pendingDeletes: make(map[string]activeDeleteEntry)}
	// The on-connect hook MUST be installed in the options BEFORE
	// paho.NewClient: paho copies the ClientOptions struct by value
	// (c.options = *o), so mutating the options afterwards never reaches
	// the production client and reconnect rehydration is silently lost.
	out.client = paho.NewClient(newClientOptions(cfg, qos, willPayload, func(paho.Client) {
		out.rehydrateOnConnect()
	}))
	return out, nil
}

// newClientOptions builds the complete paho client options for this
// plugin, including the on-connect hook. The hook is installed here —
// before paho.NewClient copies the options — and every caller of this
// helper is guaranteed to receive it (see the regression test).
func newClientOptions(cfg Config, qos byte, willPayload []byte, onConnect func(paho.Client)) *paho.ClientOptions {
	opts := paho.NewClientOptions().
		AddBroker(cfg.Broker).
		SetClientID(cfg.ClientID).
		SetCleanSession(true).
		SetAutoReconnect(true).
		SetConnectTimeout(connectTimeout).
		SetMaxReconnectInterval(maxReconnectInterval).
		SetConnectionLostHandler(func(_ paho.Client, err error) {
			slog.Warn("mqtt connection lost", "error", err)
		})
	opts.SetWill(cfg.TopicPrefix+"/status", string(willPayload), qos, true)
	opts.SetOnConnectHandler(onConnect)
	if cfg.Username != "" {
		opts.SetUsername(cfg.Username)
		if cfg.Password != "" {
			opts.SetPassword(cfg.Password)
		}
	}
	return opts
}

// Name returns the plugin type name.
func (o *Output) Name() string { return Type }

// StatusInterval reports the heartbeat interval (zero disables it).
func (o *Output) StatusInterval() time.Duration { return o.cfg.HeartbeatInterval }

// Handle publishes the change as JSON to <topic_prefix>/events with
// retain=false and the configured QoS, and then materializes the retained
// active view (<topic_prefix>/active/...). Both MQTT operations must
// succeed before nil is returned: the output worker acknowledges the
// journal change only on nil, so an /active failure keeps the cursor in
// place (a retry may republish /events — accepted at-least-once behavior)
// instead of silently diverging the retained view. The wire schema is
// deliberately explicit (see wireEvent): internal Go structs are never
// marshaled directly.
func (o *Output) Handle(ctx context.Context, change core.EventChange) error {
	if err := o.ensureConnected(ctx); err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	msg := toWireEvent(change)
	payload, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal change: %w", err)
	}

	topic := o.cfg.TopicPrefix + "/events"
	token := o.client.Publish(topic, o.qos, false, payload)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-token.Done():
	}
	if err := token.Error(); err != nil {
		return fmt.Errorf("publish to %s: %w", topic, err)
	}

	// The current event status is the authoritative decision for the
	// active view (not the change type alone): cancelled/expired delete
	// the retained topic, active replaces it, anything else is ignored.
	if err := o.updateActiveState(ctx, change.Event); err != nil {
		return fmt.Errorf("active state: %w", err)
	}
	return nil
}

// updateActiveState materializes one event's active-view state. It runs
// only after the /events publish succeeded. The desired cache is updated
// BEFORE the network publish so a reconnect in the middle of the operation
// rehydrates what SHOULD exist.
func (o *Output) updateActiveState(ctx context.Context, event core.HazardEvent) error {
	topic := o.activeTopic(event.Source, event.Key())
	switch event.Status {
	case core.StatusActive:
		payload, err := o.activePayload(event)
		if err != nil {
			return err
		}
		o.activeMu.Lock()
		o.activeSeq++
		o.activeCache[event.Key()] = activeCacheEntry{key: event.Key(), topic: topic, payload: payload, seq: o.activeSeq}
		// Reactivation: any older pending delete for this key is stale.
		delete(o.pendingDeletes, event.Key())
		o.activeMu.Unlock()
		return o.publishActive(ctx, topic, payload)
	case core.StatusCancelled, core.StatusExpired:
		// Register the desired ABSENCE before the network publish: if the
		// DELETE fails, a later reconnect rehydrates from pendingDeletes
		// and the deletion is never forgotten.
		o.activeMu.Lock()
		delete(o.activeCache, event.Key())
		o.activeSeq++
		delSeq := o.activeSeq
		o.pendingDeletes[event.Key()] = activeDeleteEntry{key: event.Key(), topic: topic, seq: delSeq}
		o.activeMu.Unlock()
		// A zero-length retained payload deletes the retained topic, so
		// late subscribers never see this hazard under /active/# again.
		if err := o.publishActive(ctx, topic, []byte{}); err != nil {
			// The pending delete stays registered for later recovery; the
			// error keeps the journal change unacknowledged upstream.
			return err
		}
		o.activeMu.Lock()
		if cur, ok := o.pendingDeletes[event.Key()]; ok && cur.seq == delSeq {
			delete(o.pendingDeletes, event.Key())
		}
		o.activeMu.Unlock()
		return nil
	default:
		// Unknown lifecycle state: /events already carried the
		// transition; the active view only models the three known states.
		return nil
	}
}

// SeedActiveState implements plugin.ActiveStateSeeder for startup
// reconstruction: it registers one desired active payload in the in-memory
// cache ONLY — no network I/O, no blocking — so the SQLite-backed startup
// seeding cannot serialize on a slow broker. The actual publication happens
// later in one bounded rehydration pass (see RehydrateActiveState) or on
// the next (re)connect.
func (o *Output) SeedActiveState(event core.HazardEvent) error {
	if event.Status != core.StatusActive {
		return nil
	}
	topic := o.activeTopic(event.Source, event.Key())
	payload, err := o.activePayload(event)
	if err != nil {
		return err
	}
	o.activeMu.Lock()
	o.activeSeq++
	o.activeCache[event.Key()] = activeCacheEntry{key: event.Key(), topic: topic, payload: payload, seq: o.activeSeq}
	// Reactivation: any older pending delete for this key is stale.
	delete(o.pendingDeletes, event.Key())
	o.activeMu.Unlock()
	return nil
}

// RehydrateActiveState implements plugin.ActiveStateRehydrater: it
// triggers one bounded background rehydration pass and returns
// immediately, so the output worker enters normal journal delivery without
// waiting for the network.
func (o *Output) RehydrateActiveState() {
	o.rehydrateOnConnect()
}

// activeTopic maps an event to its retained active topic:
// <topic_prefix>/active/<source>/<sha256(event_key)>.
func (o *Output) activeTopic(source, eventKey string) string {
	return o.cfg.TopicPrefix + "/active/" + source + "/" + activeID(eventKey)
}

// activeID derives the MQTT-safe final topic level from the logical event
// key: 64 lowercase hex characters. Raw event keys may contain anything
// (slashes, MQTT wildcards, Unicode, long strings) and never appear in the
// topic; the full key stays inside the payload.
func activeID(eventKey string) string {
	sum := sha256.Sum256([]byte(eventKey))
	return hex.EncodeToString(sum[:])
}

// activePayload builds the canonical active_hazard wire document. The event
// block reuses the exact same wireHazardEvent mapping as /events — there is
// no second, slightly different hazard JSON definition.
func (o *Output) activePayload(event core.HazardEvent) ([]byte, error) {
	payload, err := json.Marshal(wireActiveHazard{
		SchemaVersion: wireSchemaVersion,
		Type:          "active_hazard",
		EventKey:      event.Key(),
		Event:         wireHazardEventOf(event),
	})
	if err != nil {
		return nil, fmt.Errorf("marshal active state: %w", err)
	}
	return payload, nil
}

// publishActive performs one retained publish with a bounded wait.
func (o *Output) publishActive(ctx context.Context, topic string, payload []byte) error {
	if err := o.ensureConnected(ctx); err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	token := o.client.Publish(topic, o.qos, true, payload)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-token.Done():
	}
	if err := token.Error(); err != nil {
		return fmt.Errorf("publish to %s: %w", topic, err)
	}
	return nil
}

// rehydrateOnConnect is the paho OnConnect hook: it rehydrates the
// retained active view in the background so the connection callback never
// blocks paho.
func (o *Output) rehydrateOnConnect() {
	go o.rehydrateActive()
}

// rehydrateActive republishes the whole desired active cache as retained
// topics and retries every pending retained deletion, e.g. after a broker
// restart lost its retained state or after a transient DELETE failure. At
// most one pass set runs at a time; the active-state mutex is never held
// during network waits (snapshots are copied first). After every republish
// the pass re-checks the desired state: a changed generation republishes
// the newer value, and an entry that DISAPPEARED (cancelled/expired during
// the stale publish) is registered as a pending delete and deleted after
// the stale publish — a cancelled hazard can never be resurrected by a
// stale snapshot, and a failed delete is never forgotten.
func (o *Output) rehydrateActive() {
	o.rehydrateMu.Lock()
	defer o.rehydrateMu.Unlock()
	for pass := 0; pass < maxRehydratePasses; pass++ {
		activeSnapshot := o.activeSnapshot()
		deleteSnapshot := o.pendingDeleteSnapshot()
		if len(activeSnapshot) == 0 && len(deleteSnapshot) == 0 {
			return
		}
		dirty := false
		for _, e := range activeSnapshot {
			ctx, cancel := context.WithTimeout(context.Background(), activeRehydrateTimeout)
			err := o.publishActive(ctx, e.topic, e.payload)
			cancel()
			if err != nil {
				slog.Warn("active state rehydration publish failed", "topic", e.topic, "error", err)
			}
			o.activeMu.Lock()
			current, ok := o.activeCache[e.key]
			o.activeMu.Unlock()
			switch {
			case !ok:
				// Desired state became ABSENT while the stale snapshot
				// publish was in flight (cancel/expire): ensure a pending
				// delete exists and delete the retained topic AFTER the
				// stale publish. A failed corrective delete stays
				// registered and is retried on the next pass/reconnect.
				d := o.registerPendingDelete(e.key, e.topic)
				o.attemptDelete(d.key, d.topic, d.seq)
				dirty = true
			case current.seq != e.seq:
				dirty = true
			}
		}
		for _, d := range deleteSnapshot {
			o.attemptDelete(d.key, d.topic, d.seq)
			o.activeMu.Lock()
			_, activeNow := o.activeCache[d.key]
			o.activeMu.Unlock()
			if activeNow {
				// Reactivation raced the delete publish: the active value
				// must win — repeat the pass so it is republished.
				dirty = true
			}
		}
		if !dirty {
			return
		}
	}
	slog.Warn("active state rehydration did not converge within the iteration bound; a later update or reconnect republishes", "passes", maxRehydratePasses)
}

// registerPendingDelete records that a retained topic must NOT exist. It is
// called whenever a desired-absent topic is discovered (cancel/expire, or a
// corrective delete during rehydration), so a failed DELETE is never
// forgotten.
func (o *Output) registerPendingDelete(key, topic string) activeDeleteEntry {
	o.activeMu.Lock()
	defer o.activeMu.Unlock()
	o.activeSeq++
	d := activeDeleteEntry{key: key, topic: topic, seq: o.activeSeq}
	o.pendingDeletes[key] = d
	return d
}

// attemptDelete publishes one retained zero-length delete and, on success,
// drops the pending-delete registration if it still carries the same
// generation (a newer mutation may have replaced or removed it). On failure
// the registration stays so a later reconnect retries it.
func (o *Output) attemptDelete(key, topic string, seq uint64) {
	ctx, cancel := context.WithTimeout(context.Background(), activeRehydrateTimeout)
	err := o.publishActive(ctx, topic, []byte{})
	cancel()
	if err != nil {
		slog.Warn("active state delete failed; retried on the next reconnect", "topic", topic, "error", err)
		return
	}
	o.activeMu.Lock()
	if cur, ok := o.pendingDeletes[key]; ok && cur.seq == seq {
		delete(o.pendingDeletes, key)
	}
	o.activeMu.Unlock()
}

// pendingDeleteSnapshot copies the unresolved deletions without holding the
// active-state mutex during any network operation.
func (o *Output) pendingDeleteSnapshot() []activeDeleteEntry {
	o.activeMu.Lock()
	defer o.activeMu.Unlock()
	out := make([]activeDeleteEntry, 0, len(o.pendingDeletes))
	for _, d := range o.pendingDeletes {
		out = append(out, d)
	}
	return out
}

// activeSnapshot copies the desired active cache without holding the
// active-state mutex during any network operation.
func (o *Output) activeSnapshot() []activeCacheEntry {
	o.activeMu.Lock()
	defer o.activeMu.Unlock()
	out := make([]activeCacheEntry, 0, len(o.activeCache))
	for _, e := range o.activeCache {
		e.payload = append([]byte(nil), e.payload...)
		out = append(out, e)
	}
	return out
}

// PublishStatus publishes the retained application status topic
// (<topic_prefix>/status). Retention means the latest known status is
// available to new subscribers.
func (o *Output) PublishStatus(ctx context.Context, status plugin.Status) error {
	if err := o.ensureConnected(ctx); err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	msg := toWireStatus(status, time.Now())
	payload, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal status: %w", err)
	}

	topic := o.cfg.TopicPrefix + "/status"
	token := o.client.Publish(topic, o.qos, true, payload)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-token.Done():
	}
	if err := token.Error(); err != nil {
		return fmt.Errorf("publish to %s: %w", topic, err)
	}
	return nil
}

// informationTopic maps an information message to its retained MQTT topic:
// <topic_prefix>/info/<source>/<producer_id>/<key>/<kind>. The producer
// segment (configured source instance ID) prevents collisions between two
// instances of the same plugin with the same location. All segments are
// validated safe slugs, so the topic can never collide with MQTT
// wildcards or the hazard /events and /status topics.
func (o *Output) informationTopic(message core.InformationMessage) string {
	return o.cfg.TopicPrefix + "/info/" + message.Source + "/" + message.ProducerID + "/" + message.Key + "/" + message.Kind
}

// PublishInformation publishes a non-hazard informational snapshot as a
// RETAINED message on the information topic with the configured QoS. It is
// auxiliary latest-state delivery: failures are returned to the output
// worker, logged, and never touch the hazard journal, cursors or failure
// counters. The payload is the message's complete wire document (already
// normalized by the producer); nothing is wrapped or re-marshaled here.
func (o *Output) PublishInformation(ctx context.Context, message core.InformationMessage) error {
	if err := message.Validate(); err != nil {
		return fmt.Errorf("invalid information message: %w", err)
	}
	if err := o.ensureConnected(ctx); err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	topic := o.informationTopic(message)
	// Paho accepts string/[]byte/bytes.Buffer payloads only; convert the
	// RawMessage explicitly.
	token := o.client.Publish(topic, o.qos, true, []byte(message.Payload))
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-token.Done():
	}
	if err := token.Error(); err != nil {
		return fmt.Errorf("publish to %s: %w", topic, err)
	}
	return nil
}

// Close publishes a retained offline status (bounded wait) and disconnects
// cleanly. A graceful DISCONNECT also cancels the broker-side last will, so
// the offline state is published exactly once. Failure to publish must not
// hang shutdown.
func (o *Output) Close() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.client.IsConnectionOpen() {
		topic := o.cfg.TopicPrefix + "/status"
		payload, err := json.Marshal(offlineWireStatus(time.Now()))
		if err != nil {
			slog.Warn("marshal graceful offline status failed", "error", err)
		} else {
			token := o.client.Publish(topic, o.qos, true, payload)
			select {
			case <-token.Done():
				// The publish completed; report transport errors instead
				// of leaving the retained status stale silently.
				if err := token.Error(); err != nil {
					slog.Warn("graceful offline status publish failed", "topic", topic, "error", err)
				}
			case <-time.After(offlinePublishTimeout):
				slog.Warn("graceful offline status publish timed out", "topic", topic)
			}
		}
	}
	o.client.Disconnect(250)
	return nil
}

// ensureConnected waits until an active broker connection exists (or ctx
// expires). Paho's own state is authoritative: with AutoReconnect enabled
// it returns to reconnect mode on loss and re-establishes by itself; a
// Connect() call during reconnect mode completes immediately as a no-op.
// The durable journal provides delivery retry, so MQTT does not need
// offline persistence of its own.
func (o *Output) ensureConnected(ctx context.Context) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.client.IsConnectionOpen() {
		return nil
	}
	token := o.client.Connect()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-token.Done():
	}
	if err := token.Error(); err != nil {
		return err
	}
	return nil
}

// ---- wire schema ----
//
// The MQTT payloads below are the public contract. Fields marked STABLE are
// guaranteed to remain present with the same meaning in future releases;
// additional fields may be added at any time.

// wireEvent is the event stream payload (<topic_prefix>/events).
type wireEvent struct {
	SchemaVersion int             `json:"schema_version"` // STABLE
	ChangeID      int64           `json:"change_id"`      // STABLE: journal ID (unique within ONE WarnFlux database)
	ChangeType    string          `json:"change_type"`    // STABLE: new|updated|cancelled|expired
	EventKey      string          `json:"event_key"`      // STABLE: source:source_id — the logical upstream event
	Event         wireHazardEvent `json:"event"`
}

// wireActiveHazard is the retained active-view payload
// (<topic_prefix>/active/<source>/<sha256(event_key)>). It is a small
// wrapper around the SAME wireHazardEvent used by /events: an active-state
// snapshot is not a journal transition, but the hazard field semantics are
// identical.
type wireActiveHazard struct {
	SchemaVersion int             `json:"schema_version"` // STABLE
	Type          string          `json:"type"`           // STABLE: "active_hazard"
	EventKey      string          `json:"event_key"`      // STABLE: source:source_id
	Event         wireHazardEvent `json:"event"`
}

type wireHazardEvent struct {
	Source      string   `json:"source"`    // STABLE
	SourceID    string   `json:"source_id"` // STABLE
	Category    string   `json:"category"`
	Event       string   `json:"event"` // STABLE
	Severity    string   `json:"severity"`
	Urgency     string   `json:"urgency"`
	Certainty   string   `json:"certainty"`
	Headline    string   `json:"headline"`
	Description string   `json:"description"`
	Instruction string   `json:"instruction"`
	EffectiveAt *string  `json:"effective_at,omitempty"`
	ExpiresAt   *string  `json:"expires_at,omitempty"`
	Latitude    *float64 `json:"latitude,omitempty"`
	Longitude   *float64 `json:"longitude,omitempty"`
	Areas       []string `json:"areas"`
	Status      string   `json:"status"` // STABLE: active|cancelled|expired
	SourceURL   string   `json:"source_url"`
	ReceivedAt  string   `json:"received_at"`
	UpdatedAt   string   `json:"updated_at"`
}

// wireStatus is the retained status payload (<topic_prefix>/status).
type wireStatus struct {
	SchemaVersion           int               `json:"schema_version"` // STABLE
	Service                 string            `json:"service"`        // STABLE
	State                   string            `json:"state"`          // STABLE: running
	GeneratedAt             string            `json:"generated_at"`   // STABLE: RFC3339 UTC observation time
	Version                 string            `json:"version"`
	UptimeSeconds           int64             `json:"uptime_seconds"`
	DatabaseHealthy         bool              `json:"database_healthy"`
	PendingChanges          int               `json:"pending_changes"`
	OldestPendingAgeSeconds int64             `json:"oldest_pending_age_seconds"`
	Sources                 []wirePluginState `json:"sources"`
	Outputs                 []wirePluginState `json:"outputs"`
}

type wirePluginState struct {
	ID                  string `json:"id"`
	Type                string `json:"type"`
	State               string `json:"state"`
	ConsecutiveFailures int    `json:"consecutive_failures"`
	RestartCount        int    `json:"restart_count"`
	LastError           string `json:"last_error,omitempty"`
}

func toWireEvent(change core.EventChange) wireEvent {
	return wireEvent{
		SchemaVersion: wireSchemaVersion,
		ChangeID:      change.ID,
		ChangeType:    string(change.Type),
		EventKey:      change.Event.Key(),
		Event:         wireHazardEventOf(change.Event),
	}
}

// wireHazardEventOf is the single hazard wire mapping shared by the /events
// stream and the /active retained view.
func wireHazardEventOf(event core.HazardEvent) wireHazardEvent {
	return wireHazardEvent{
		Source:      event.Source,
		SourceID:    event.SourceID,
		Category:    event.Category,
		Event:       event.Event,
		Severity:    event.Severity,
		Urgency:     event.Urgency,
		Certainty:   event.Certainty,
		Headline:    event.Headline,
		Description: event.Description,
		Instruction: event.Instruction,
		EffectiveAt: wireTime(event.EffectiveAt),
		ExpiresAt:   wireTime(event.ExpiresAt),
		Latitude:    event.Latitude,
		Longitude:   event.Longitude,
		Areas:       event.Areas,
		Status:      string(event.Status),
		SourceURL:   event.SourceURL,
		ReceivedAt:  formatWireTime(event.ReceivedAt),
		UpdatedAt:   formatWireTime(event.UpdatedAt),
	}
}

func toWireStatus(status plugin.Status, now time.Time) wireStatus {
	out := wireStatus{
		SchemaVersion:           wireSchemaVersion,
		Service:                 "warnflux",
		State:                   "running",
		GeneratedAt:             now.UTC().Format(time.RFC3339),
		Version:                 status.Version,
		UptimeSeconds:           int64(status.Uptime.Seconds()),
		DatabaseHealthy:         status.DatabaseHealthy,
		PendingChanges:          status.PendingChanges,
		OldestPendingAgeSeconds: int64(status.OldestPendingAge.Seconds()),
	}
	for _, p := range status.Sources {
		out.Sources = append(out.Sources, wirePluginState{
			ID:                  p.ID,
			Type:                p.Type,
			State:               string(p.State),
			ConsecutiveFailures: p.ConsecutiveFailures,
			RestartCount:        p.RestartCount,
			LastError:           p.LastError,
		})
	}
	for _, p := range status.Outputs {
		out.Outputs = append(out.Outputs, wirePluginState{
			ID:                  p.ID,
			Type:                p.Type,
			State:               string(p.State),
			ConsecutiveFailures: p.ConsecutiveFailures,
			RestartCount:        p.RestartCount,
			LastError:           p.LastError,
		})
	}
	return out
}

func wireTime(t *time.Time) *string {
	if t == nil {
		return nil
	}
	s := t.UTC().Format(time.RFC3339Nano)
	return &s
}

func formatWireTime(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}

// normalizeTopicPrefix trims whitespace and trailing slashes (so
// "warnflux/" does not become "warnflux//events"), defaults an
// empty prefix to "warnflux", and rejects values that would corrupt
// the topics WarnFlux publishes to.
func normalizeTopicPrefix(raw string) (string, error) {
	prefix := strings.TrimRight(strings.TrimSpace(raw), "/")
	if prefix == "" {
		return "warnflux", nil
	}
	if strings.ContainsAny(prefix, "+#") || strings.ContainsRune(prefix, 0) {
		return "", fmt.Errorf("topic_prefix must not contain '+', '#' or NUL characters")
	}
	return prefix, nil
}

// offlineWireStatus builds the wire payload for the offline state. It is
// shared by the retained last will and the graceful shutdown publication,
// so both use exactly the same core wire semantics.
func offlineWireStatus(now time.Time) wireStatus {
	return wireStatus{
		SchemaVersion: wireSchemaVersion,
		Service:       "warnflux",
		State:         "offline",
		GeneratedAt:   now.UTC().Format(time.RFC3339),
	}
}

// routePahoLogs silences paho's verbose protocol debug output and routes
// warnings and errors through slog. This is the single MQTT logging
// adapter in the repository.
func routePahoLogs() {
	paho.DEBUG = discardLogger{}
	paho.WARN = slogAdapter{level: slog.LevelWarn}
	paho.ERROR = slogAdapter{level: slog.LevelError}
	paho.CRITICAL = slogAdapter{level: slog.LevelError}
}

type discardLogger struct{}

func (discardLogger) Println(_ ...any)          {}
func (discardLogger) Printf(_ string, _ ...any) {}

type slogAdapter struct{ level slog.Level }

func (a slogAdapter) Println(v ...any) {
	slog.Log(context.Background(), a.level, fmt.Sprint(v...))
}

func (a slogAdapter) Printf(format string, v ...any) {
	slog.Log(context.Background(), a.level, fmt.Sprintf(format, v...))
}

// Register registers the MQTT output plugin type.
func Register(reg *plugin.Registry) error {
	return reg.RegisterOutput(Type, New)
}
