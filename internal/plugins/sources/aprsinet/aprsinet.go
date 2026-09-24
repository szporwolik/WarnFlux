// Package aprsinet implements the APRS-IS source plugin: it connects to an
// APRS-IS server, subscribes to packets around the configured position
// (gridsquare + radius) and feeds every parsed packet into the shared APRS
// hub, which merges them with other backends (aprs-radio later) and
// publishes the merged station state to MQTT.
//
// The plugin also implements aprs.Transmitter: while connected it can
// inject APRS text messages back into the APRS-IS network, so WarnFlux can
// reach hams directly (e.g. via the aprs action plugin).
package aprsinet

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/szporwolik/WarnFlux/internal/aprs"
	"github.com/szporwolik/WarnFlux/internal/plugin"
)

// Type is the plugin type name used in the YAML configuration.
const Type = "aprs-inet"

// Defaults and bounds.
const (
	DefaultServer         = "rotate.aprs2.net:14580"
	defaultConnectTimeout = 15 * time.Second
	defaultReadTimeout    = 10 * time.Minute
	minConnectTimeout     = time.Second
	maxConnectTimeout     = time.Minute
	minReadTimeout        = 30 * time.Second
	maxReadTimeout        = time.Hour
	maxPasscodeFileBytes  = 64 * 1024
	maxFilterBytes        = 512
	writeTimeout          = 10 * time.Second

	minReconnectDelay = 2 * time.Second
	maxReconnectDelay = 2 * time.Minute
)

// healthySessionReset is how long a session must run before a drop resets
// the reconnect backoff to the minimum. A connection that survived this
// long proved the uplink works, so recovery after a single drop must be
// fast instead of inheriting a historical failure backoff. Tests shrink it.
var healthySessionReset = time.Minute

// Config is the plugin-specific configuration.
type Config struct {
	// Callsign is the login callsign. Empty inherits the hub identity
	// (aprs.callsign).
	Callsign string `yaml:"callsign"`
	// Passcode is the APRS-IS validation code (numeric). Mutually
	// exclusive with PasscodeFile. Required for validated connections.
	Passcode string `yaml:"passcode"`
	// PasscodeFile reads the passcode from a file (e.g. a Docker secret).
	PasscodeFile string `yaml:"passcode_file"`
	// Server is the APRS-IS host:port. Defaults to rotate.aprs2.net:14580.
	Server string `yaml:"server"`
	// Filter overrides the automatic range filter
	// ("r/<lat>/<lon>/<radius>"). Advanced setups only.
	Filter string `yaml:"filter"`
	// ConnectTimeout bounds one dial attempt.
	ConnectTimeout time.Duration `yaml:"connect_timeout"`
	// ReadTimeout bounds one read from the server; exceeding it forces a
	// reconnect (servers drop idle connections).
	ReadTimeout time.Duration `yaml:"read_timeout"`
}

// Source is the APRS-IS source plugin instance.
type Source struct {
	cfg   Config
	hub   *aprs.Hub
	login string // resolved login callsign
	pass  string // resolved passcode (never logged)
	// filter is the resolved APRS-IS filter string.
	filter string

	logger *slog.Logger

	// connMu guards the live connection; writeMu serializes writes.
	connMu  sync.Mutex
	writeMu sync.Mutex
	conn    net.Conn
	ready   atomic.Bool

	// counters for the health/stats reporters.
	packets   atomic.Int64
	messages  atomic.Int64
	positions atomic.Int64
}

// errNotReady is returned when Send is called while disconnected.
var errNotReady = errors.New("aprs-inet is not connected")

// Register adds the aprs-inet source factory to the plugin registry.
func Register(reg *plugin.Registry, hub *aprs.Hub) error {
	return reg.RegisterSource(Type, func(node *yaml.Node) (plugin.SourcePlugin, error) {
		return New(node, hub)
	})
}

// New decodes and validates the plugin configuration.
func New(node *yaml.Node, hub *aprs.Hub) (plugin.SourcePlugin, error) {
	if hub == nil || !hub.Enabled() {
		return nil, errors.New("aprs-inet: the APRS hub is disabled (set aprs.enabled: true)")
	}
	var cfg Config
	if err := plugin.DecodeConfig(node, &cfg); err != nil {
		return nil, err
	}
	if cfg.ConnectTimeout == 0 {
		cfg.ConnectTimeout = defaultConnectTimeout
	}
	if cfg.ReadTimeout == 0 {
		cfg.ReadTimeout = defaultReadTimeout
	}
	if cfg.ConnectTimeout < minConnectTimeout || cfg.ConnectTimeout > maxConnectTimeout {
		return nil, fmt.Errorf("aprs-inet: connect_timeout must be between %s and %s, got %s", minConnectTimeout, maxConnectTimeout, cfg.ConnectTimeout)
	}
	if cfg.ReadTimeout < minReadTimeout || cfg.ReadTimeout > maxReadTimeout {
		return nil, fmt.Errorf("aprs-inet: read_timeout must be between %s and %s, got %s", minReadTimeout, maxReadTimeout, cfg.ReadTimeout)
	}
	if cfg.Passcode != "" && cfg.PasscodeFile != "" {
		return nil, errors.New("aprs-inet: passcode and passcode_file are mutually exclusive")
	}

	login := aprs.NormalizeCallsign(cfg.Callsign)
	if login == "" {
		login = hub.Callsign()
	}
	if !aprs.ValidCallsign(login) {
		return nil, fmt.Errorf("aprs-inet: callsign %q is not a valid APRS callsign", cfg.Callsign)
	}

	pass := strings.TrimSpace(cfg.Passcode)
	if pass == "" && cfg.PasscodeFile != "" {
		info, err := os.Stat(cfg.PasscodeFile)
		if err != nil {
			return nil, fmt.Errorf("aprs-inet: stat passcode_file: %w", err)
		}
		if info.Size() > maxPasscodeFileBytes {
			return nil, fmt.Errorf("aprs-inet: passcode_file is %d bytes, maximum %d", info.Size(), maxPasscodeFileBytes)
		}
		data, err := os.ReadFile(cfg.PasscodeFile)
		if err != nil {
			return nil, fmt.Errorf("aprs-inet: read passcode_file: %w", err)
		}
		pass = strings.TrimRight(string(data), "\r\n")
	}
	if pass == "" {
		return nil, errors.New("aprs-inet: passcode is required (the APRS-IS validation code for the callsign)")
	}

	server := strings.TrimSpace(cfg.Server)
	if server == "" {
		server = DefaultServer
	}
	if _, _, err := net.SplitHostPort(server); err != nil {
		return nil, fmt.Errorf("aprs-inet: server must be host:port, got %q", server)
	}

	filter := strings.TrimSpace(cfg.Filter)
	if filter == "" {
		filter = fmt.Sprintf("r/%.4f/%.4f/%d", hub.CenterLat(), hub.CenterLon(), int(hub.RadiusKM()))
	}
	if len(filter) > maxFilterBytes {
		return nil, fmt.Errorf("aprs-inet: filter is %d bytes, maximum %d", len(filter), maxFilterBytes)
	}

	return &Source{
		cfg:    cfg,
		hub:    hub,
		login:  login,
		pass:   pass,
		filter: filter,
		logger: slog.Default(),
	}, nil
}

// Name returns the plugin type name.
func (s *Source) Name() string { return Type }

// Run connects to APRS-IS and reads packets until the context is
// cancelled, reconnecting with backoff after failures.
func (s *Source) Run(ctx context.Context, emit plugin.Emitter) error {
	health, _ := emit.(plugin.SourceHealthReporter)
	stats, _ := emit.(plugin.SourceStatsReporter)

	s.logger.Info("aprs-inet plugin started",
		"server", s.cfg.Server, "login", s.login, "filter", s.filter)

	delay := minReconnectDelay
	for {
		sessionStart := time.Now()
		err := s.session(ctx)
		if ctx.Err() != nil {
			return nil
		}
		// A session that ran healthily proves the uplink works: after a
		// drop, recover at the minimum delay instead of inheriting a
		// historical failure backoff.
		delay = resetBackoff(delay, time.Since(sessionStart))
		if health != nil {
			health.ReportSourceDegraded(err)
		}
		s.logger.Warn("aprs-inet: session ended, reconnecting",
			"server", s.cfg.Server, "error", err, "retry_in", delay)

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(delay):
		}
		if delay *= 2; delay > maxReconnectDelay {
			delay = maxReconnectDelay
		}
		if stats != nil {
			stats.ReportSourceStats(s.summary())
		}
	}
}

// resetBackoff returns the reconnect delay after one session end: a
// healthy session (healthySessionReset or longer) resets it to the
// minimum; a short failed session keeps the current (growing) delay.
func resetBackoff(delay, sessionDuration time.Duration) time.Duration {
	if sessionDuration >= healthySessionReset {
		return minReconnectDelay
	}
	return delay
}

// session runs one connection lifetime: dial, login, read, disconnect.
func (s *Source) session(ctx context.Context) error {
	dialer := net.Dialer{Timeout: s.cfg.ConnectTimeout}
	conn, err := dialer.DialContext(ctx, "tcp", s.cfg.Server)
	if err != nil {
		return fmt.Errorf("dial %s: %w", s.cfg.Server, err)
	}

	// Login (APRS-IS handshake): the filter limits the server-side feed
	// to the configured range; messages addressed to our callsign are
	// delivered regardless of the filter. aprsc requires the version to
	// be a separate token after the software name.
	login := fmt.Sprintf("user %s pass %s vers WarnFlux %s filter %s\r\n",
		s.login, s.pass, s.hub.Version(), s.filter)
	if err := writeAll(conn, login, writeTimeout); err != nil {
		conn.Close()
		return fmt.Errorf("write login: %w", err)
	}

	s.connMu.Lock()
	s.conn = conn
	s.connMu.Unlock()
	s.ready.Store(true)
	s.hub.AddTransmitter(Type, s)
	defer func() {
		s.ready.Store(false)
		s.hub.RemoveTransmitter(Type)
		s.connMu.Lock()
		s.conn = nil
		s.connMu.Unlock()
		conn.Close()
	}()

	s.logger.Info("aprs-inet: connected", "server", s.cfg.Server)

	sc := bufio.NewScanner(conn)
	sc.Buffer(make([]byte, 64*1024), 64*1024)
	for sc.Scan() {
		conn.SetReadDeadline(time.Now().Add(s.cfg.ReadTimeout))
		line := strings.TrimRight(sc.Text(), "\r")
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		s.packets.Add(1)
		p := aprs.ParseFeedLine(line, time.Now())
		if p.Kind == aprs.KindMessage {
			s.messages.Add(1)
		}
		if p.Kind == aprs.KindPosition || p.Kind == aprs.KindObject {
			s.positions.Add(1)
		}
		s.hub.Observe(p, Type)
	}
	if err := sc.Err(); err != nil {
		return fmt.Errorf("read from %s: %w", s.cfg.Server, err)
	}
	return errors.New("server closed the connection")
}

// summary renders the one-line stats report for the web health page.
func (s *Source) summary() string {
	return fmt.Sprintf("%d packets / %d positions / %d messages / %d stations",
		s.packets.Load(), s.positions.Load(), s.messages.Load(), s.hub.Stats().Stations)
}

// Ready reports whether the APRS-IS connection is live.
func (s *Source) Ready() bool { return s.ready.Load() }

// Send transmits one APRS text message to the addressed callsign via
// APRS-IS. APRS-IS routes the message through the nearest i-gate to the
// target station. The write is bounded by the write deadline; ctx is
// honoured through the connection deadline rather than a goroutine leak.
func (s *Source) Send(_ context.Context, to, text string) error {
	s.connMu.Lock()
	conn := s.conn
	s.connMu.Unlock()
	if conn == nil || !s.ready.Load() {
		return errNotReady
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return writeAll(conn, aprs.EncodeMessageLine(s.login, to, text)+"\r\n", writeTimeout)
}

// writeAll writes the whole line (short lines; io.WriteString is enough).
func writeAll(conn net.Conn, line string, timeout time.Duration) error {
	conn.SetWriteDeadline(time.Now().Add(timeout))
	for len(line) > 0 {
		n, err := conn.Write([]byte(line))
		if err != nil {
			return err
		}
		line = line[n:]
	}
	return nil
}
