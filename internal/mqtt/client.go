// Package mqtt provides the MQTT client connection and the heartbeat
// publisher for WarnFlux.
package mqtt

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"

	"warnflux/internal/config"
)

const connectTimeout = 10 * time.Second

// pingPayload is the JSON document published to <topic_prefix>/status/ping.
type pingPayload struct {
	Type      string    `json:"type"`
	Service   string    `json:"service"`
	Version   string    `json:"version"`
	Timestamp time.Time `json:"timestamp"`
}

// Client is a thin wrapper around the paho MQTT client.
type Client struct {
	cfg    config.MQTT
	logger *slog.Logger
	client paho.Client
}

// New creates a Client for the given configuration. It also routes the
// paho library's internal logging through slog.
func New(cfg config.MQTT, logger *slog.Logger) *Client {
	routePahoLogs(logger)

	opts := paho.NewClientOptions().
		AddBroker(cfg.Broker).
		SetClientID(cfg.ClientID).
		SetCleanSession(true).
		SetAutoReconnect(true).
		SetConnectTimeout(connectTimeout).
		SetOnConnectHandler(func(paho.Client) {
			logger.Info("MQTT connected", "broker", cfg.Broker)
		}).
		SetConnectionLostHandler(func(_ paho.Client, err error) {
			logger.Warn("MQTT disconnected", "error", err)
		})

	// Credentials are optional and must never be logged.
	if cfg.Username != "" {
		opts.SetUsername(cfg.Username)
		if cfg.Password != "" {
			opts.SetPassword(cfg.Password)
		}
	}

	return &Client{
		cfg:    cfg,
		logger: logger,
		client: paho.NewClient(opts),
	}
}

// Connect establishes the broker connection, honouring ctx cancellation.
func (c *Client) Connect(ctx context.Context) error {
	c.logger.Info("connecting to MQTT broker", "broker", c.cfg.Broker, "client_id", c.cfg.ClientID)

	token := c.client.Connect()
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

// Disconnect closes the broker connection, waiting up to timeout for
// in-flight messages to complete.
func (c *Client) Disconnect(timeout time.Duration) {
	c.logger.Info("disconnecting from MQTT broker")
	c.client.Disconnect(uint(timeout.Milliseconds()))
	c.logger.Info("MQTT disconnected")
}

// PingLoop publishes the ping message immediately and then every interval
// until ctx is cancelled.
func (c *Client) PingLoop(ctx context.Context, interval time.Duration, version string) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	c.publishPing(ctx, version)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.publishPing(ctx, version)
		}
	}
}

// publishPing publishes a single heartbeat message to
// <topic_prefix>/status/ping with the configured QoS and retain=false.
func (c *Client) publishPing(ctx context.Context, version string) {
	payload := pingPayload{
		Type:      "ping",
		Service:   "warnflux",
		Version:   version,
		Timestamp: time.Now().UTC().Truncate(time.Second),
	}
	data, err := json.Marshal(payload)
	if err != nil {
		c.logger.Error("failed to marshal ping payload", "error", err)
		return
	}

	topic := c.cfg.TopicPrefix + "/status/ping"
	token := c.client.Publish(topic, c.cfg.QoS, false, data)
	select {
	case <-ctx.Done():
		return
	case <-token.Done():
	}
	if err := token.Error(); err != nil {
		if ctx.Err() == nil {
			c.logger.Warn("failed to publish ping", "topic", topic, "error", err)
		}
		return
	}
	c.logger.Info("ping published", "topic", topic, "qos", c.cfg.QoS)
}

// routePahoLogs redirects paho's internal logging to slog and silences its
// verbose debug output (protocol-level dumps to stderr).
//
// paho exposes package-level logger variables, so this mutates process-wide
// state. That is fine here: WarnFlux runs a single MQTT client.
func routePahoLogs(logger *slog.Logger) {
	paho.DEBUG = discardLogger{}
	paho.WARN = slogAdapter{logger: logger, level: slog.LevelWarn}
	paho.ERROR = slogAdapter{logger: logger, level: slog.LevelError}
	paho.CRITICAL = slogAdapter{logger: logger, level: slog.LevelError}
}

type discardLogger struct{}

func (discardLogger) Println(_ ...any)          {}
func (discardLogger) Printf(_ string, _ ...any) {}

type slogAdapter struct {
	logger *slog.Logger
	level  slog.Level
}

func (a slogAdapter) Println(v ...any) {
	a.logger.Log(context.Background(), a.level, fmt.Sprint(v...))
}

func (a slogAdapter) Printf(format string, v ...any) {
	a.logger.Log(context.Background(), a.level, fmt.Sprintf(format, v...))
}
