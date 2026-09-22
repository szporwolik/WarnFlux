// Package config loads and validates the WarnFlux YAML configuration.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"regexp"
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
	defaultEventRetention     = 30 * 24 * time.Hour
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

	defaultDispatchQueueSize = 1024

	defaultReceiverConnectTimeout = 10 * time.Second
	defaultReceiverKeepAlive      = 30 * time.Second
	defaultWFPrefix               = "warnflux" // WarnFlux MQTT protocol namespace
	defaultSubscriptionQoS        = 1

	defaultWebListen = ":8080"
	defaultWebTitle  = "WarnFlux"

	defaultActionQueueSize       = 128
	defaultActionCallTimeout     = 10 * time.Second
	defaultActionShutdownTimeout = 10 * time.Second

	// Bounds against absurd runtime values.
	maxPluginQueueSize   = 100_000
	maxRuntimeTimeout    = 24 * time.Hour
	maxMQTTClientIDBytes = 256
	maxTopicPrefixBytes  = 256
	maxSubTopicBytes     = 1024
)

// Plugin instance IDs are durable identities (output cursors are keyed by
// output ID), so they use one canonical lowercase slug format. Only outer
// whitespace is trimmed by the loader; uppercase is rejected rather than
// silently lowercased, because IDs are persisted.
var pluginIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,63}$`)

// validLogLevels are the accepted values for app.log_level.
var validLogLevels = []string{"debug", "info", "warn", "error"}

// Config is the fully defaulted, validated application configuration.
type Config struct {
	App      App
	Storage  Storage
	Sources  []Source
	Outputs  []Output
	Dispatch Dispatch
	Web      Web
	Actions  []Action
}

// App holds general application settings.
type App struct {
	LogLevel           string
	ExpirationInterval time.Duration
	// ChangeRetention is how long acknowledged journal records are kept
	// before cleanup deletes them.
	ChangeRetention time.Duration
	// EventRetention is how long cancelled/expired current-state records
	// are kept in the events table before cleanup deletes them. Active
	// events are never cleaned up.
	EventRetention time.Duration

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

// Dispatch holds the canonical dispatch ingress and the MQTT receiver
// subsystem configuration. Receivers are INPUT clients: they consume MQTT
// frames for dispatch. They are deliberately independent from
// outputs[].type=mqtt (the Router publisher).
type Dispatch struct {
	// QueueSize is the bounded canonical dispatch intake queue capacity.
	QueueSize int
	// Receivers are the independent MQTT receiver connections.
	Receivers []Receiver
}

// Receiver is one independent MQTT receiver connection.
type Receiver struct {
	ID           string
	Enabled      bool
	Broker       string
	ClientID     string
	Username     string
	Password     string
	PasswordFile string

	ConnectTimeout time.Duration
	KeepAlive      time.Duration

	// WarnFlux mode subscribes the strict WarnFlux protocol topics.
	WF ReceiverWF
	// Subscriptions are additional generic MQTT topic filters.
	Subscriptions []ReceiverSubscription
}

// ReceiverWF configures the WarnFlux MQTT protocol mode of one
// receiver.
type ReceiverWF struct {
	Enabled     bool
	TopicPrefix string
}

// ReceiverSubscription is one generic MQTT topic filter with standard MQTT
// wildcard semantics.
type ReceiverSubscription struct {
	Topic string
	QoS   int
}

// Web holds the authenticated admin UI configuration.
type Web struct {
	Enabled bool
	Listen  string
	// Title is the application name: browser title and footer.
	Title string
	// Name is the human-readable system name shown next to the logo on
	// the login page and in the sidebar header. Empty falls back to the
	// title.
	Name string
	// Header1 is the primary header line shown next to the logo (sidebar)
	// and as the login title. Empty falls back to the name.
	Header1 string
	// Header2 is an optional subtitle shown under Header1 in the sidebar
	// and on the login page. Empty hides it.
	Header2 string
	Auth    WebAuth
}

// WebAuth holds the single admin account for the web UI. Password and
// PasswordFile are mutually exclusive.
type WebAuth struct {
	Username string
	Password string
	// PasswordFile, when set, reads the admin password from a Docker
	// secret / mounted file. Mutually exclusive with password.
	PasswordFile string
	// SecureCookie marks session cookies Secure (for TLS-terminated
	// deployments).
	SecureCookie bool
}

// Action is one configured ActionPlugin instance. ActionPlugins are
// explicitly invoked by future dispatch rules; they never automatically
// receive MQTT events.
type Action struct {
	ID      string
	Type    string
	Enabled bool
	Runtime ActionRuntime
	// Config is the raw action-specific configuration; the action factory
	// decodes it into its own typed struct.
	Config *yaml.Node
}

// ActionRuntime holds common supervision options for an action instance.
type ActionRuntime struct {
	QueueSize       int
	CallTimeout     time.Duration
	ShutdownTimeout time.Duration
}

// fileConfig mirrors the YAML layout.
type fileConfig struct {
	App      fileApp       `yaml:"app"`
	Storage  fileStorage   `yaml:"storage"`
	Sources  []fileSource  `yaml:"sources"`
	Outputs  []fileOutput  `yaml:"outputs"`
	Dispatch *fileDispatch `yaml:"dispatch"`
	Web      *fileWeb      `yaml:"web"`
	Actions  []fileAction  `yaml:"actions"`
}

type fileDispatch struct {
	QueueSize *int           `yaml:"queue_size"`
	Receivers []fileReceiver `yaml:"mqtt_receivers"`
}

type fileReceiver struct {
	ID             string                     `yaml:"id"`
	Enabled        bool                       `yaml:"enabled"`
	Broker         string                     `yaml:"broker"`
	ClientID       string                     `yaml:"client_id"`
	Username       string                     `yaml:"username"`
	Password       string                     `yaml:"password"`
	PasswordFile   string                     `yaml:"password_file"`
	ConnectTimeout *time.Duration             `yaml:"connect_timeout"`
	KeepAlive      *time.Duration             `yaml:"keep_alive"`
	WF             *fileReceiverWF            `yaml:"warnflux"`
	Subscriptions  []fileReceiverSubscription `yaml:"subscriptions"`
}

type fileReceiverWF struct {
	Enabled     *bool  `yaml:"enabled"`
	TopicPrefix string `yaml:"topic_prefix"`
}

type fileReceiverSubscription struct {
	Topic string `yaml:"topic"`
	QoS   *int   `yaml:"qos"`
}

type fileWeb struct {
	Enabled bool         `yaml:"enabled"`
	Listen  string       `yaml:"listen"`
	Title   string       `yaml:"title"`
	Name    string       `yaml:"name"`
	Header1 string       `yaml:"header1"`
	Header2 string       `yaml:"header2"`
	Auth    *fileWebAuth `yaml:"auth"`
}

type fileWebAuth struct {
	Username     string `yaml:"username"`
	Password     string `yaml:"password"`
	PasswordFile string `yaml:"password_file"`
	SecureCookie bool   `yaml:"secure_cookie"`
}

type fileAction struct {
	ID      string             `yaml:"id"`
	Type    string             `yaml:"type"`
	Enabled bool               `yaml:"enabled"`
	Runtime *fileActionRuntime `yaml:"runtime"`
	Config  *rawPluginConfig   `yaml:"config"`
}

type fileActionRuntime struct {
	QueueSize       *int           `yaml:"queue_size"`
	CallTimeout     *time.Duration `yaml:"call_timeout"`
	ShutdownTimeout *time.Duration `yaml:"shutdown_timeout"`
}

type fileApp struct {
	LogLevel           string         `yaml:"log_level"`
	ExpirationInterval *time.Duration `yaml:"expiration_interval"`
	ChangeRetention    *time.Duration `yaml:"change_retention"`
	EventRetention     *time.Duration `yaml:"event_retention"`
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

// maxConfigFileBytes bounds the configuration file read at startup.
// The file is local-trust input (an administrator controls it), but
// os.ReadFile would otherwise accept arbitrary sizes; 1 MiB is far beyond
// any realistic WarnFlux configuration.
const maxConfigFileBytes = 1 << 20

// Load reads the YAML file at path, applies defaults and validates it.
// Additional trailing YAML documents are rejected: a configuration file
// must contain exactly one document.
func Load(path string) (*Config, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("read config file %q: %w", path, err)
	}
	if info.Size() > maxConfigFileBytes {
		return nil, fmt.Errorf("read config file %q: size %d bytes exceeds the maximum of %d bytes", path, info.Size(), maxConfigFileBytes)
	}
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
			EventRetention:     defaultEventRetention,
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
	if f.App.EventRetention != nil {
		cfg.App.EventRetention = *f.App.EventRetention
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

	cfg.Dispatch = Dispatch{QueueSize: defaultDispatchQueueSize}
	if f.Dispatch != nil {
		if f.Dispatch.QueueSize != nil {
			cfg.Dispatch.QueueSize = *f.Dispatch.QueueSize
		}
		for _, r := range f.Dispatch.Receivers {
			inst := Receiver{
				ID:             strings.TrimSpace(r.ID),
				Enabled:        r.Enabled,
				Broker:         strings.TrimSpace(r.Broker),
				ClientID:       strings.TrimSpace(r.ClientID),
				Username:       strings.TrimSpace(r.Username),
				Password:       r.Password,
				PasswordFile:   strings.TrimSpace(r.PasswordFile),
				ConnectTimeout: defaultReceiverConnectTimeout,
				KeepAlive:      defaultReceiverKeepAlive,
			}
			if r.ConnectTimeout != nil {
				inst.ConnectTimeout = *r.ConnectTimeout
			}
			if r.KeepAlive != nil {
				inst.KeepAlive = *r.KeepAlive
			}
			if r.WF != nil {
				inst.WF = ReceiverWF{
					Enabled:     r.WF.Enabled == nil || *r.WF.Enabled,
					TopicPrefix: defaultWFPrefix,
				}
				if p := strings.TrimSpace(r.WF.TopicPrefix); p != "" {
					inst.WF.TopicPrefix = p
				}
			}
			for _, s := range r.Subscriptions {
				inst.Subscriptions = append(inst.Subscriptions, ReceiverSubscription{
					Topic: s.Topic,
					QoS:   defaultSubscriptionQoS,
				})
				if s.QoS != nil {
					inst.Subscriptions[len(inst.Subscriptions)-1].QoS = *s.QoS
				}
			}
			cfg.Dispatch.Receivers = append(cfg.Dispatch.Receivers, inst)
		}
	}

	cfg.Web = Web{
		Enabled: f.Web != nil && f.Web.Enabled,
		Listen:  defaultWebListen,
		Title:   defaultWebTitle,
		Name:    defaultWebTitle,
		Header1: defaultWebTitle,
	}
	if f.Web != nil {
		if l := strings.TrimSpace(f.Web.Listen); l != "" {
			cfg.Web.Listen = l
		}
		if t := strings.TrimSpace(f.Web.Title); t != "" {
			cfg.Web.Title = t
		}
		if n := strings.TrimSpace(f.Web.Name); n != "" {
			cfg.Web.Name = n
		} else {
			cfg.Web.Name = cfg.Web.Title
		}
		if h := strings.TrimSpace(f.Web.Header1); h != "" {
			cfg.Web.Header1 = h
		} else {
			cfg.Web.Header1 = cfg.Web.Name
		}
		cfg.Web.Header2 = strings.TrimSpace(f.Web.Header2)
		if f.Web.Auth != nil {
			cfg.Web.Auth = WebAuth{
				Username:     f.Web.Auth.Username,
				Password:     f.Web.Auth.Password,
				PasswordFile: strings.TrimSpace(f.Web.Auth.PasswordFile),
				SecureCookie: f.Web.Auth.SecureCookie,
			}
		}
	}

	cfg.Actions = make([]Action, 0, len(f.Actions))
	for _, a := range f.Actions {
		inst := Action{
			ID:      strings.TrimSpace(a.ID),
			Type:    strings.TrimSpace(a.Type),
			Enabled: a.Enabled,
			Config:  pluginConfigNode(a.Config),
			Runtime: ActionRuntime{
				QueueSize:       defaultActionQueueSize,
				CallTimeout:     defaultActionCallTimeout,
				ShutdownTimeout: defaultActionShutdownTimeout,
			},
		}
		if a.Runtime != nil {
			if a.Runtime.QueueSize != nil {
				inst.Runtime.QueueSize = *a.Runtime.QueueSize
			}
			if a.Runtime.CallTimeout != nil {
				inst.Runtime.CallTimeout = *a.Runtime.CallTimeout
			}
			if a.Runtime.ShutdownTimeout != nil {
				inst.Runtime.ShutdownTimeout = *a.Runtime.ShutdownTimeout
			}
		}
		cfg.Actions = append(cfg.Actions, inst)
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
	if c.App.EventRetention <= 0 {
		return fmt.Errorf("app.event_retention must be greater than 0, got %s", c.App.EventRetention)
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
		if !pluginIDPattern.MatchString(s.ID) {
			return fmt.Errorf("sources[%d].id %q must match %s (lowercase slug; plugin IDs are durable identities)", i, s.ID, pluginIDPattern)
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
		if !pluginIDPattern.MatchString(o.ID) {
			return fmt.Errorf("outputs[%d].id %q must match %s (lowercase slug; the output id is its durable consumer identity)", i, o.ID, pluginIDPattern)
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
	if c.Dispatch.QueueSize < 1 || c.Dispatch.QueueSize > maxPluginQueueSize {
		return fmt.Errorf("dispatch.queue_size must be between 1 and %d, got %d", maxPluginQueueSize, c.Dispatch.QueueSize)
	}
	receiverSeen := make(map[string]bool, len(c.Dispatch.Receivers))
	for i, r := range c.Dispatch.Receivers {
		field := fmt.Sprintf("dispatch.mqtt_receivers[%d]", i)
		if !pluginIDPattern.MatchString(r.ID) {
			return fmt.Errorf("%s.id %q must match %s (lowercase slug)", field, r.ID, pluginIDPattern)
		}
		if receiverSeen[r.ID] {
			return fmt.Errorf("dispatch.mqtt_receivers: duplicate receiver id %q", r.ID)
		}
		receiverSeen[r.ID] = true
		if r.ConnectTimeout <= 0 || r.ConnectTimeout > maxRuntimeTimeout {
			return fmt.Errorf("%s.connect_timeout must be >0 and at most %s, got %s", field, maxRuntimeTimeout, r.ConnectTimeout)
		}
		if r.KeepAlive <= 0 || r.KeepAlive > maxRuntimeTimeout {
			return fmt.Errorf("%s.keep_alive must be >0 and at most %s, got %s", field, maxRuntimeTimeout, r.KeepAlive)
		}
		if r.Password != "" && r.PasswordFile != "" {
			return fmt.Errorf("%s: password and password_file are mutually exclusive", field)
		}
		if r.WF.Enabled {
			p := r.WF.TopicPrefix
			if strings.TrimSpace(p) == "" || strings.ContainsAny(p, "+#") {
				return fmt.Errorf("%s.warnflux.topic_prefix must be a non-empty MQTT topic segment without '+' or '#'", field)
			}
			if p != strings.Trim(p, "/") {
				return fmt.Errorf("%s.warnflux.topic_prefix must not start or end with '/'", field)
			}
			if len(p) > maxTopicPrefixBytes {
				return fmt.Errorf("%s.warnflux.topic_prefix is %d bytes, maximum %d", field, len(p), maxTopicPrefixBytes)
			}
		}
		for j, s := range r.Subscriptions {
			sub := fmt.Sprintf("%s.subscriptions[%d]", field, j)
			if err := validateTopicFilter(s.Topic); err != nil {
				return fmt.Errorf("%s.topic: %w", sub, err)
			}
			if s.QoS < 0 || s.QoS > 2 {
				return fmt.Errorf("%s.qos must be 0, 1 or 2, got %d", sub, s.QoS)
			}
		}
		if !r.Enabled {
			continue
		}
		if r.Broker == "" {
			return fmt.Errorf("%s.broker is required for an enabled receiver", field)
		}
		if strings.ContainsAny(r.Broker, " \t\n") {
			return fmt.Errorf("%s.broker must not contain whitespace, got %q", field, r.Broker)
		}
		if r.ClientID == "" {
			return fmt.Errorf("%s.client_id is required: the receiver client ID must differ from the MQTT output client_id when both connect to the same broker", field)
		}
		if len(r.ClientID) > maxMQTTClientIDBytes {
			return fmt.Errorf("%s.client_id is %d bytes, maximum %d", field, len(r.ClientID), maxMQTTClientIDBytes)
		}
		if !r.WF.Enabled && len(r.Subscriptions) == 0 {
			return fmt.Errorf("%s: enabled receiver needs warnflux mode or at least one subscription", field)
		}
	}
	if c.Web.Enabled {
		if c.Web.Listen == "" {
			return fmt.Errorf("web.listen must not be empty when the web UI is enabled")
		}
		if strings.TrimSpace(c.Web.Auth.Username) == "" {
			return fmt.Errorf("web.auth.username is required when the web UI is enabled")
		}
		if c.Web.Auth.Password == "" && c.Web.Auth.PasswordFile == "" {
			return fmt.Errorf("web.auth.password or web.auth.password_file is required when the web UI is enabled")
		}
		if c.Web.Auth.Password != "" && c.Web.Auth.PasswordFile != "" {
			return fmt.Errorf("web.auth.password and web.auth.password_file are mutually exclusive")
		}
	}
	for i, a := range c.Actions {
		if !pluginIDPattern.MatchString(a.ID) {
			return fmt.Errorf("actions[%d].id %q must match %s (lowercase slug; plugin IDs are durable identities)", i, a.ID, pluginIDPattern)
		}
		if a.Type == "" {
			return fmt.Errorf("action %q: type must not be empty", a.ID)
		}
		if seen[a.ID] {
			return fmt.Errorf("duplicate plugin id %q (one ID namespace across sources, outputs and actions)", a.ID)
		}
		seen[a.ID] = true
		if a.Runtime.QueueSize < 1 || a.Runtime.QueueSize > maxPluginQueueSize {
			return fmt.Errorf("action %q: runtime.queue_size must be between 1 and %d, got %d", a.ID, maxPluginQueueSize, a.Runtime.QueueSize)
		}
		if a.Runtime.CallTimeout <= 0 || a.Runtime.CallTimeout > maxRuntimeTimeout {
			return fmt.Errorf("action %q: runtime.call_timeout must be >0 and at most %s, got %s", a.ID, maxRuntimeTimeout, a.Runtime.CallTimeout)
		}
		if a.Runtime.ShutdownTimeout <= 0 || a.Runtime.ShutdownTimeout > maxRuntimeTimeout {
			return fmt.Errorf("action %q: runtime.shutdown_timeout must be >0 and at most %s, got %s", a.ID, maxRuntimeTimeout, a.Runtime.ShutdownTimeout)
		}
	}
	return nil
}

// validateTopicFilter checks a generic MQTT subscription filter: non-empty,
// bounded, no control characters, '#' only as the last segment.
func validateTopicFilter(topic string) error {
	if topic == "" {
		return fmt.Errorf("must not be empty")
	}
	if len(topic) > maxSubTopicBytes {
		return fmt.Errorf("is %d bytes, maximum %d", len(topic), maxSubTopicBytes)
	}
	for _, r := range topic {
		if r < 0x20 {
			return fmt.Errorf("must not contain control characters")
		}
	}
	segs := strings.Split(topic, "/")
	for i, seg := range segs {
		if strings.Contains(seg, "#") && seg != "#" {
			return fmt.Errorf("'#' wildcard must occupy an entire level")
		}
		if seg == "#" && i != len(segs)-1 {
			return fmt.Errorf("'#' must be the last level")
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
