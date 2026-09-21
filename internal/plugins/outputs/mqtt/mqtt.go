// Package mqtt implements the built-in MQTT output plugin: it publishes
// meaningful EventChange values to the broker as JSON.
package mqtt

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
	"gopkg.in/yaml.v3"

	"warnflux/internal/core"
	"warnflux/internal/plugin"
)

// Type is the plugin type name used in the YAML configuration.
const Type = "mqtt"

const connectTimeout = 10 * time.Second

// Config is the plugin-specific configuration. Credentials are never
// logged.
type Config struct {
	Broker      string `yaml:"broker"`
	ClientID    string `yaml:"client_id"`
	Username    string `yaml:"username"`
	Password    string `yaml:"password"`
	TopicPrefix string `yaml:"topic_prefix"`
	QoS         byte   `yaml:"qos"`
}

// Output publishes EventChange values to MQTT.
type Output struct {
	cfg Config

	mu        sync.Mutex
	client    paho.Client
	connected bool
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
	if cfg.Broker == "" {
		return nil, fmt.Errorf("broker must not be empty")
	}
	if cfg.ClientID == "" {
		cfg.ClientID = "warnflux-events"
	}
	if cfg.TopicPrefix == "" {
		cfg.TopicPrefix = "warnflux"
	}
	if cfg.QoS > 2 {
		return nil, fmt.Errorf("qos must be 0, 1 or 2, got %d", cfg.QoS)
	}

	routePahoLogs()
	opts := paho.NewClientOptions().
		AddBroker(cfg.Broker).
		SetClientID(cfg.ClientID).
		SetCleanSession(true).
		SetAutoReconnect(true).
		SetConnectTimeout(connectTimeout).
		SetConnectionLostHandler(func(_ paho.Client, err error) {
			slog.Warn("mqtt output connection lost", "error", err)
		})
	if cfg.Username != "" {
		opts.SetUsername(cfg.Username)
		if cfg.Password != "" {
			opts.SetPassword(cfg.Password)
		}
	}
	return &Output{cfg: cfg, client: paho.NewClient(opts)}, nil
}

// Name returns the plugin type name.
func (o *Output) Name() string { return Type }

// Handle publishes the change as JSON to <topic_prefix>/events with
// retain=false and the configured QoS. The context is bounded by the
// framework's runtime timeout.
func (o *Output) Handle(ctx context.Context, change core.EventChange) error {
	if err := o.ensureConnected(ctx); err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	payload, err := json.Marshal(struct {
		ChangeType core.ChangeType  `json:"change_type"`
		Event      core.HazardEvent `json:"event"`
	}{change.Type, change.Event})
	if err != nil {
		return fmt.Errorf("marshal change: %w", err)
	}

	topic := o.cfg.TopicPrefix + "/events"
	token := o.client.Publish(topic, o.cfg.QoS, false, payload)
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

// Close releases the broker connection cleanly on shutdown.
func (o *Output) Close() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.connected {
		o.client.Disconnect(250)
		o.connected = false
	}
	return nil
}

func (o *Output) ensureConnected(ctx context.Context) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.connected {
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
	o.connected = true
	return nil
}

// routePahoLogs silences paho's verbose protocol debug output and routes
// warnings and errors through slog.
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
