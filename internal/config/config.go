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
	defaultExpirationInterval = time.Minute
	defaultChangeRetention    = 24 * time.Hour
	defaultStorageDriver      = "sqlite"
	// An empty default means "not provided"; the application resolves a
	// dev/debug location next to the executable at startup, with a
	// warning, instead of failing.
	defaultStoragePath      = ""
	defaultLogFile          = ""
	defaultLogMaxSizeMB     = 10
	defaultLogMaxBackups    = 5
	defaultRestart          = true
	defaultShutdownTimeout  = 10 * time.Second
	defaultOutputTimeout    = 10 * time.Second
	defaultFailureThreshold = 5
)

// validLogLevels are the accepted values for app.log_level.
var validLogLevels = []string{"debug", "info", "warn", "error"}

// Config is the fully defaulted, validated application configuration.
type Config struct {
	App     App
	Storage Storage
	Sources []Source
	Outputs []Output
}

// App holds general application settings.
type App struct {
	LogLevel           string
	ExpirationInterval time.Duration
	// ChangeRetention is how long acknowledged journal records are kept
	// before cleanup deletes them.
	ChangeRetention time.Duration

	// LogFile is the optional rotating log file. Empty means stdout only.
	LogFile string
	// LogMaxSizeMB is the rotation threshold in megabytes.
	LogMaxSizeMB int
	// LogMaxBackups is how many rotated files are retained.
	LogMaxBackups int
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
// There is intentionally no "startup timeout": no readiness contract
// exists, and a fake one would mislead operators.
type SourceRuntime struct {
	Restart         bool
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

// Storage holds persistence settings. Path may be left empty in the
// configuration: the application then resolves a dev/debug location next
// to the executable (with a warning) instead of failing startup.
type Storage struct {
	Driver string
	Path   string
}

// fileConfig mirrors the YAML layout.
type fileConfig struct {
	App     fileApp      `yaml:"app"`
	Storage fileStorage  `yaml:"storage"`
	Sources []fileSource `yaml:"sources"`
	Outputs []fileOutput `yaml:"outputs"`
}

type fileApp struct {
	LogLevel           string         `yaml:"log_level"`
	ExpirationInterval *time.Duration `yaml:"expiration_interval"`
	ChangeRetention    *time.Duration `yaml:"change_retention"`
	LogFile            string         `yaml:"log_file"`
	LogMaxSizeMB       *int           `yaml:"log_max_size_mb"`
	LogMaxBackups      *int           `yaml:"log_max_backups"`
}

type fileStorage struct {
	Driver string `yaml:"driver"`
	Path   string `yaml:"path"`
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

// Load reads the YAML file at path, applies defaults and validates it.
// Additional trailing YAML documents are rejected: a configuration file
// must contain exactly one document.
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

	var trailing any
	if err := dec.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, fmt.Errorf("parse config file %q: multiple YAML documents are not allowed", path)
		}
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
			ChangeRetention:    defaultChangeRetention,
			LogFile:            defaultLogFile,
			LogMaxSizeMB:       defaultLogMaxSizeMB,
			LogMaxBackups:      defaultLogMaxBackups,
		},
		Storage: Storage{
			Driver: defaultStorageDriver,
			Path:   defaultStoragePath,
		},
	}

	if level := strings.TrimSpace(f.App.LogLevel); level != "" {
		cfg.App.LogLevel = strings.ToLower(level)
	}
	if f.App.ExpirationInterval != nil {
		cfg.App.ExpirationInterval = *f.App.ExpirationInterval
	}
	if f.App.ChangeRetention != nil {
		cfg.App.ChangeRetention = *f.App.ChangeRetention
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

	cfg.Sources = make([]Source, 0, len(f.Sources))
	for _, s := range f.Sources {
		inst := Source{
			ID:      strings.TrimSpace(s.ID),
			Type:    strings.TrimSpace(s.Type),
			Enabled: s.Enabled,
			Config:  pluginConfigNode(s.Config),
			Runtime: SourceRuntime{
				Restart:         defaultRestart,
				ShutdownTimeout: defaultShutdownTimeout,
			},
		}
		if s.Runtime != nil {
			if s.Runtime.Restart != nil {
				inst.Runtime.Restart = *s.Runtime.Restart
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
	if c.App.ExpirationInterval <= 0 {
		return fmt.Errorf("app.expiration_interval must be greater than 0, got %s", c.App.ExpirationInterval)
	}
	if c.App.ChangeRetention <= 0 {
		return fmt.Errorf("app.change_retention must be greater than 0, got %s", c.App.ChangeRetention)
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
	// An empty storage.path is valid here: the application resolves it at
	// startup (dev/debug fallback next to the binary, with a warning).

	// Plugin instances: every instance needs a unique ID and a type; the ID
	// namespace is shared between sources and outputs.
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
