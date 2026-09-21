// Package config loads and validates the WarnFlux YAML configuration.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"slices"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	// DefaultConfigPath is used when no --config flag is provided.
	DefaultConfigPath = "config.yaml"

	defaultLogLevel     = "info"
	defaultClientID     = "warnflux"
	defaultTopicPrefix  = "warnflux"
	defaultQoS          = byte(1)
	defaultPingInterval = 30 * time.Second
)

// validLogLevels are the accepted values for app.log_level.
var validLogLevels = []string{"debug", "info", "warn", "error"}

// Config is the fully defaulted, validated application configuration.
type Config struct {
	App  App
	MQTT MQTT
}

// App holds general application settings.
type App struct {
	LogLevel string
}

// MQTT holds broker connection and publishing settings.
type MQTT struct {
	Enabled      bool
	Broker       string
	ClientID     string
	Username     string
	Password     string
	TopicPrefix  string
	QoS          byte
	PingInterval time.Duration
}

// fileConfig mirrors the YAML layout. The QoS pointer distinguishes
// "not set" from an explicit 0 so defaults can be applied correctly.
type fileConfig struct {
	App  fileApp  `yaml:"app"`
	MQTT fileMQTT `yaml:"mqtt"`
}

type fileApp struct {
	LogLevel string `yaml:"log_level"`
}

type fileMQTT struct {
	Enabled      bool           `yaml:"enabled"`
	Broker       string         `yaml:"broker"`
	ClientID     string         `yaml:"client_id"`
	Username     string         `yaml:"username"`
	Password     string         `yaml:"password"`
	TopicPrefix  string         `yaml:"topic_prefix"`
	QoS          *byte          `yaml:"qos"`
	PingInterval *time.Duration `yaml:"ping_interval"`
}

// Load reads the YAML file at path, applies defaults and validates it.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config file %q: %w", path, err)
	}

	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true) // reject misspelled or unknown keys

	var file fileConfig
	if err := dec.Decode(&file); err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("parse config file %q: %w", path, err)
	}

	cfg := file.toConfig()
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config file %q: %w", path, err)
	}
	return &cfg, nil
}

// toConfig converts the decoded file representation into a Config with
// defaults applied for missing values.
func (f fileConfig) toConfig() Config {
	cfg := Config{
		App: App{LogLevel: defaultLogLevel},
		MQTT: MQTT{
			Enabled:      f.MQTT.Enabled,
			Broker:       f.MQTT.Broker,
			ClientID:     defaultClientID,
			Username:     f.MQTT.Username,
			Password:     f.MQTT.Password,
			TopicPrefix:  defaultTopicPrefix,
			QoS:          defaultQoS,
			PingInterval: defaultPingInterval,
		},
	}

	if level := strings.TrimSpace(f.App.LogLevel); level != "" {
		cfg.App.LogLevel = strings.ToLower(level)
	}
	if id := strings.TrimSpace(f.MQTT.ClientID); id != "" {
		cfg.MQTT.ClientID = id
	}
	if prefix := strings.TrimSpace(f.MQTT.TopicPrefix); prefix != "" {
		cfg.MQTT.TopicPrefix = prefix
	}
	if f.MQTT.QoS != nil {
		cfg.MQTT.QoS = *f.MQTT.QoS
	}
	if f.MQTT.PingInterval != nil {
		cfg.MQTT.PingInterval = *f.MQTT.PingInterval
	}
	return cfg
}

// Validate checks that the configuration is usable.
func (c Config) Validate() error {
	if !slices.Contains(validLogLevels, c.App.LogLevel) {
		return fmt.Errorf("app.log_level must be one of %s, got %q",
			strings.Join(validLogLevels, ", "), c.App.LogLevel)
	}
	if c.MQTT.QoS > 2 {
		return fmt.Errorf("mqtt.qos must be 0, 1 or 2, got %d", c.MQTT.QoS)
	}
	if c.MQTT.PingInterval <= 0 {
		return fmt.Errorf("mqtt.ping_interval must be greater than 0, got %s", c.MQTT.PingInterval)
	}
	if c.MQTT.Enabled {
		if strings.TrimSpace(c.MQTT.Broker) == "" {
			return errors.New("mqtt.broker must be set when mqtt.enabled is true")
		}
		if strings.TrimSpace(c.MQTT.ClientID) == "" {
			return errors.New("mqtt.client_id must not be empty")
		}
		if strings.TrimSpace(c.MQTT.TopicPrefix) == "" {
			return errors.New("mqtt.topic_prefix must not be empty")
		}
	}
	return nil
}

// SlogLevel returns the configured log level as a slog.Level.
func (a App) SlogLevel() slog.Level {
	switch a.LogLevel {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
