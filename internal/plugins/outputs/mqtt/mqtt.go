// Package mqtt implements the single built-in MQTT output: it publishes
// meaningful EventChange values (non-retained event stream) and an optional
// retained application status topic (heartbeat). This replaces the legacy
// top-level mqtt configuration and internal/mqtt package.
package mqtt

import (
	"context"
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
	client paho.Client
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
	opts.SetWill(cfg.TopicPrefix+"/status", string(willPayload), qos, true)
	if cfg.Username != "" {
		opts.SetUsername(cfg.Username)
		if cfg.Password != "" {
			opts.SetPassword(cfg.Password)
		}
	}
	return &Output{cfg: cfg, qos: qos, client: paho.NewClient(opts)}, nil
}

// Name returns the plugin type name.
func (o *Output) Name() string { return Type }

// StatusInterval reports the heartbeat interval (zero disables it).
func (o *Output) StatusInterval() time.Duration { return o.cfg.HeartbeatInterval }

// Handle publishes the change as JSON to <topic_prefix>/events with
// retain=false and the configured QoS. The wire schema is deliberately
// explicit (see wireEvent): internal Go structs are never marshaled
// directly.
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
	return nil
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
	event := change.Event
	return wireEvent{
		SchemaVersion: wireSchemaVersion,
		ChangeID:      change.ID,
		ChangeType:    string(change.Type),
		EventKey:      event.Key(),
		Event: wireHazardEvent{
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
		},
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
