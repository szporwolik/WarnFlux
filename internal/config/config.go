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

	defaultLogLevel           = "info"
	defaultClientID           = "warnflux"
	defaultTopicPrefix        = "warnflux"
	defaultQoS                = byte(1)
	defaultPingInterval       = 30 * time.Second
	defaultExpirationInterval = time.Minute
	defaultStorageDriver      = "sqlite"
	defaultStoragePath        = "warnflux.db"
	defaultLogFile            = ""
	defaultLogMaxSizeMB       = 10
	defaultLogMaxBackups      = 5
	defaultRestart            = true
	defaultStartupTimeout     = 15 * time.Second
	defaultShutdownTimeout    = 10 * time.Second
	defaultOutputTimeout      = 10 * time.Second
	defaultFailureThreshold   = 5
)

// validLogLevels are the accepted values for app.log_level.
var validLogLevels = []string{"debug", "info", "warn", "error"}

// Config is the fully defaulted, validated application configuration.
type Config struct {
	App     App
	MQTT    MQTT
	Storage Storage
	Sources []Source
	Outputs []Output
}

// Source is one configured source plugin instance.
type Source struct {
	ID      string
	Type    string
	Enabled bool
	Runtime SourceRuntime
	// Config is the raw plugin-specific configuration; the plugin decodes
	// it into its own typed struct.
	Config *yaml.Node
}

// SourceRuntime holds common supervision options for a source instance.
type SourceRuntime struct {
	Restart         bool
	StartupTimeout  time.Duration
	ShutdownTimeout time.Duration
}

// Output is one configured output plugin instance.
type Output struct {
	ID      string
	Type    string
	Enabled bool
	Runtime OutputRuntime
	// Config is the raw plugin-specific configuration; the plugin decodes
	// it into its own typed struct.
	Config *yaml.Node
}

// OutputRuntime holds common supervision options for an output instance.
type OutputRuntime struct {
	Timeout          time.Duration
	FailureThreshold int
}

// App holds general application settings.
type App struct {
	LogLevel           string
	ExpirationInterval time.Duration

	// LogFile is the optional rotating log file. Empty means stdout only.
	LogFile string
	// LogMaxSizeMB is the rotation threshold in megabytes.
	LogMaxSizeMB int
	// LogMaxBackups is how many rotated files are retained.
	LogMaxBackups int
}

// Storage holds persistence settings.
type Storage struct {
	Driver string
	Path   string
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
	App     fileApp      `yaml:"app"`
	MQTT    fileMQTT     `yaml:"mqtt"`
	Storage fileStorage  `yaml:"storage"`
	Sources []fileSource `yaml:"sources"`
	Outputs []fileOutput `yaml:"outputs"`
}

type fileSource struct {
	ID      string             `yaml:"id"`
	Type    string             `yaml:"type"`
	Enabled bool               `yaml:"enabled"`
	Runtime *fileSourceRuntime `yaml:"runtime"`
	Config  *rawPluginConfig   `yaml:"config"`
}

type fileSourceRuntime struct {
	Restart         *bool          `yaml:"restart"`
	StartupTimeout  *time.Duration `yaml:"startup_timeout"`
	ShutdownTimeout *time.Duration `yaml:"shutdown_timeout"`
}

type fileOutput struct {
	ID      string             `yaml:"id"`
	Type    string             `yaml:"type"`
	Enabled bool               `yaml:"enabled"`
	Runtime *fileOutputRuntime `yaml:"runtime"`
	Config  *rawPluginConfig   `yaml:"config"`
}

type fileOutputRuntime struct {
	Timeout          *time.Duration `yaml:"timeout"`
	FailureThreshold *int           `yaml:"failure_threshold"`
}

// rawPluginConfig captures the plugin-specific configuration subtree as a
// YAML node without interpreting it. Strict decoding cannot validate
// arbitrary mappings against yaml.Node directly, so this small wrapper
// stores the subtree via UnmarshalYAML.
type rawPluginConfig struct {
	node yaml.Node
}

// UnmarshalYAML captures the raw configuration subtree.
func (r *rawPluginConfig) UnmarshalYAML(value *yaml.Node) error {
	r.node = *value
	return nil
}

// pluginConfigNode returns the raw node, or nil when no config was given.
func pluginConfigNode(raw *rawPluginConfig) *yaml.Node {
	if raw == nil {
		return nil
	}
	return &raw.node
}

type fileApp struct {
	LogLevel           string         `yaml:"log_level"`
	ExpirationInterval *time.Duration `yaml:"expiration_interval"`
	LogFile            string         `yaml:"log_file"`
	LogMaxSizeMB       *int           `yaml:"log_max_size_mb"`
	LogMaxBackups      *int           `yaml:"log_max_backups"`
}

type fileStorage struct {
	Driver string `yaml:"driver"`
	Path   string `yaml:"path"`
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
		App: App{
			LogLevel:           defaultLogLevel,
			ExpirationInterval: defaultExpirationInterval,
			LogFile:            defaultLogFile,
			LogMaxSizeMB:       defaultLogMaxSizeMB,
			LogMaxBackups:      defaultLogMaxBackups,
		},
		Storage: Storage{
			Driver: defaultStorageDriver,
			Path:   defaultStoragePath,
		},
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
	if f.App.ExpirationInterval != nil {
		cfg.App.ExpirationInterval = *f.App.ExpirationInterval
	}
	if file := strings.TrimSpace(f.App.LogFile); file != "" {
		cfg.App.LogFile = file
	}
	if f.App.LogMaxSizeMB != nil {
		cfg.App.LogMaxSizeMB = *f.App.LogMaxSizeMB
	}
	if f.App.LogMaxBackups != nil {
		cfg.App.LogMaxBackups = *f.App.LogMaxBackups
	}
	if driver := strings.TrimSpace(f.Storage.Driver); driver != "" {
		cfg.Storage.Driver = strings.ToLower(driver)
	}
	if path := strings.TrimSpace(f.Storage.Path); path != "" {
		cfg.Storage.Path = path
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

	cfg.Sources = make([]Source, 0, len(f.Sources))
	for _, s := range f.Sources {
		inst := Source{
			ID:      strings.TrimSpace(s.ID),
			Type:    strings.TrimSpace(s.Type),
			Enabled: s.Enabled,
			Config:  pluginConfigNode(s.Config),
			Runtime: SourceRuntime{
				Restart:         defaultRestart,
				StartupTimeout:  defaultStartupTimeout,
				ShutdownTimeout: defaultShutdownTimeout,
			},
		}
		if s.Runtime != nil {
			if s.Runtime.Restart != nil {
				inst.Runtime.Restart = *s.Runtime.Restart
			}
			if s.Runtime.StartupTimeout != nil {
				inst.Runtime.StartupTimeout = *s.Runtime.StartupTimeout
			}
			if s.Runtime.ShutdownTimeout != nil {
				inst.Runtime.ShutdownTimeout = *s.Runtime.ShutdownTimeout
			}
		}
		cfg.Sources = append(cfg.Sources, inst)
	}

	cfg.Outputs = make([]Output, 0, len(f.Outputs))
	for _, o := range f.Outputs {
		inst := Output{
			ID:      strings.TrimSpace(o.ID),
			Type:    strings.TrimSpace(o.Type),
			Enabled: o.Enabled,
			Config:  pluginConfigNode(o.Config),
			Runtime: OutputRuntime{
				Timeout:          defaultOutputTimeout,
				FailureThreshold: defaultFailureThreshold,
			},
		}
		if o.Runtime != nil {
			if o.Runtime.Timeout != nil {
				inst.Runtime.Timeout = *o.Runtime.Timeout
			}
			if o.Runtime.FailureThreshold != nil {
				inst.Runtime.FailureThreshold = *o.Runtime.FailureThreshold
			}
		}
		cfg.Outputs = append(cfg.Outputs, inst)
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
	if c.App.ExpirationInterval <= 0 {
		return fmt.Errorf("app.expiration_interval must be greater than 0, got %s", c.App.ExpirationInterval)
	}
	if c.App.LogMaxSizeMB <= 0 {
		return fmt.Errorf("app.log_max_size_mb must be greater than 0, got %d", c.App.LogMaxSizeMB)
	}
	if c.App.LogMaxBackups < 0 {
		return fmt.Errorf("app.log_max_backups must not be negative, got %d", c.App.LogMaxBackups)
	}
	if c.Storage.Driver != "sqlite" {
		return fmt.Errorf("storage.driver must be %q, got %q", "sqlite", c.Storage.Driver)
	}
	if strings.TrimSpace(c.Storage.Path) == "" {
		return errors.New("storage.path must not be empty")
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

	// Plugin instances: every instance needs a unique ID and a type; a
	// unique ID namespace is shared between sources and outputs.
	seen := make(map[string]bool, len(c.Sources)+len(c.Outputs))
	for i, s := range c.Sources {
		if s.ID == "" {
			return fmt.Errorf("sources[%d].id must not be empty", i)
		}
		if s.Type == "" {
			return fmt.Errorf("source %q: type must not be empty", s.ID)
		}
		if seen[s.ID] {
			return fmt.Errorf("duplicate plugin id %q", s.ID)
		}
		seen[s.ID] = true
		if s.Runtime.StartupTimeout <= 0 {
			return fmt.Errorf("source %q: runtime.startup_timeout must be greater than 0", s.ID)
		}
		if s.Runtime.ShutdownTimeout <= 0 {
			return fmt.Errorf("source %q: runtime.shutdown_timeout must be greater than 0", s.ID)
		}
	}
	for i, o := range c.Outputs {
		if o.ID == "" {
			return fmt.Errorf("outputs[%d].id must not be empty", i)
		}
		if o.Type == "" {
			return fmt.Errorf("output %q: type must not be empty", o.ID)
		}
		if seen[o.ID] {
			return fmt.Errorf("duplicate plugin id %q", o.ID)
		}
		seen[o.ID] = true
		if o.Runtime.Timeout <= 0 {
			return fmt.Errorf("output %q: runtime.timeout must be greater than 0", o.ID)
		}
		if o.Runtime.FailureThreshold <= 0 {
			return fmt.Errorf("output %q: runtime.failure_threshold must be greater than 0", o.ID)
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
