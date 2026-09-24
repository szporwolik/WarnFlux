// Package aprsradio implements the KISS radio backend: it connects to a
// KISS TNC server (TCP, e.g. Direwolf, aprsd or a hardware TNC bridge),
// feeds every decoded frame into the shared APRS hub and acts as the hub's
// outbound transmitter — APRS messages are encoded as AX.25 UI frames and
// pushed through the TNC to the digipeater network.
package aprsradio

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/szporwolik/WarnFlux/internal/aprs"
	"github.com/szporwolik/WarnFlux/internal/plugin"
)

// Type is the plugin type name used in the YAML configuration.
const Type = aprs.BackendRadio

// Defaults and bounds.
const (
	defaultConnectTimeout = 15 * time.Second
	defaultReadTimeout    = 10 * time.Minute
	minConnectTimeout     = time.Second
	maxConnectTimeout     = time.Minute
	minReadTimeout        = 5 * time.Second
	maxReadTimeout        = time.Hour
	writeTimeout          = 10 * time.Second
	// defaultMaxFrame caps one decoded KISS frame; real APRS frames are
	// far smaller, this only bounds pathological input.
	defaultMaxFrame = 2048

	minReconnectDelay = 2 * time.Second
	maxReconnectDelay = 2 * time.Minute
)

// healthySessionReset is how long a session must run before a drop resets
// the reconnect backoff to the minimum (see aprsinet). The radio is the
// primary network when the uplink is down, so fast recovery matters even
// more here. Tests shrink it.
var healthySessionReset = time.Minute

// idleTimeoutFactor scales ReadTimeout into the "no frames at all"
// threshold that ends the session. A single read timeout is expected on a
// quiet channel — it only resets the deadline. Package-level so tests can
// shrink it.
var idleTimeoutFactor = 3

// Config is the plugin-specific configuration.
type Config struct {
	// Server is the KISS server address host:port (plain TCP).
	Server string `yaml:"server"`
	// Path is the digipeater path used for outbound frames, e.g.
	// ["WIDE1-1"]; empty disables digipeating (direct only).
	Path []string `yaml:"path"`
	// ConnectTimeout bounds one dial attempt.
	ConnectTimeout time.Duration `yaml:"connect_timeout"`
	// ReadTimeout bounds one read from the TNC. A timeout is NOT an
	// error: a quiet radio channel sends nothing for long stretches, so
	// the session resets the deadline and keeps reading. Only when no
	// frame at all arrives for idleTimeoutFactor × ReadTimeout does the
	// session reconnect (TCP keepalive still catches dead peers).
	ReadTimeout time.Duration `yaml:"read_timeout"`
	// MaxFrameBytes caps one decoded KISS frame.
	MaxFrameBytes int `yaml:"max_frame_bytes"`
}

// Source is the KISS radio backend plugin instance.
type Source struct {
	cfg    Config
	hub    *aprs.Hub
	logger *slog.Logger

	connMu  sync.Mutex
	writeMu sync.Mutex
	conn    net.Conn
	ready   atomic.Bool

	rxPackets atomic.Int64
	lastRx    atomic.Int64
}

// errNotReady is returned when Send is called while disconnected.
var errNotReady = errors.New("aprs-radio is not connected")

// Register adds the aprs-radio source factory to the plugin registry.
func Register(reg *plugin.Registry, hub *aprs.Hub) error {
	return reg.RegisterSource(Type, func(node *yaml.Node) (plugin.SourcePlugin, error) {
		return New(node, hub)
	})
}

// New decodes and validates the plugin configuration.
func New(node *yaml.Node, hub *aprs.Hub) (plugin.SourcePlugin, error) {
	if hub == nil || !hub.Enabled() {
		return nil, errors.New("aprs-radio: the APRS hub is disabled (set aprs.enabled: true)")
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
	if cfg.MaxFrameBytes == 0 {
		cfg.MaxFrameBytes = defaultMaxFrame
	}
	if cfg.ConnectTimeout < minConnectTimeout || cfg.ConnectTimeout > maxConnectTimeout {
		return nil, fmt.Errorf("aprs-radio: connect_timeout must be between %s and %s, got %s", minConnectTimeout, maxConnectTimeout, cfg.ConnectTimeout)
	}
	if cfg.ReadTimeout < minReadTimeout || cfg.ReadTimeout > maxReadTimeout {
		return nil, fmt.Errorf("aprs-radio: read_timeout must be between %s and %s, got %s", minReadTimeout, maxReadTimeout, cfg.ReadTimeout)
	}
	if cfg.MaxFrameBytes < 64 || cfg.MaxFrameBytes > 64*1024 {
		return nil, fmt.Errorf("aprs-radio: max_frame_bytes must be between 64 and 65536, got %d", cfg.MaxFrameBytes)
	}
	if _, _, err := net.SplitHostPort(strings.TrimSpace(cfg.Server)); err != nil {
		return nil, fmt.Errorf("aprs-radio: server must be host:port, got %q", cfg.Server)
	}
	for i, p := range cfg.Path {
		cfg.Path[i] = aprs.NormalizeCallsign(p)
		if !aprs.ValidCallsign(cfg.Path[i]) {
			return nil, fmt.Errorf("aprs-radio: path entry %q is not a valid APRS callsign", p)
		}
	}
	return &Source{cfg: cfg, hub: hub, logger: slog.Default()}, nil
}

// Name returns the plugin type name.
func (s *Source) Name() string { return Type }

// Ready reports whether the KISS connection is live (the hub checks this
// before routing outbound messages here).
func (s *Source) Ready() bool { return s.ready.Load() }

// Run connects to the KISS server and pumps frames into the hub until the
// context is cancelled, reconnecting with backoff after failures.
func (s *Source) Run(ctx context.Context, emit plugin.Emitter) error {
	health, _ := emit.(plugin.SourceHealthReporter)
	stats, _ := emit.(plugin.SourceStatsReporter)

	s.logger.Info("aprs-radio plugin started",
		"server", s.cfg.Server, "path", strings.Join(s.cfg.Path, ","))

	// Closing the connection on cancellation unblocks the in-flight read
	// so the session ends promptly.
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-ctx.Done():
			s.closeConn()
		case <-stop:
		}
	}()

	delay := minReconnectDelay
	for {
		sessionStart := time.Now()
		err := s.session(ctx)
		if ctx.Err() != nil {
			return nil
		}
		// A session that ran healthily proves the link works: after a
		// drop, recover at the minimum delay instead of inheriting a
		// historical failure backoff.
		delay = resetBackoff(delay, time.Since(sessionStart))
		if health != nil {
			health.ReportSourceDegraded(err)
		}
		s.logger.Warn("aprs-radio: session ended, reconnecting",
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

// closeConn tears down the live connection (idempotent).
func (s *Source) closeConn() {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	if s.conn != nil {
		s.conn.Close()
	}
}

// session runs one connection lifetime: dial, register as transmitter,
// decode frames, deregister on exit.
func (s *Source) session(ctx context.Context) error {
	dialer := net.Dialer{Timeout: s.cfg.ConnectTimeout, KeepAlive: 30 * time.Second}
	conn, err := dialer.DialContext(ctx, "tcp", s.cfg.Server)
	if err != nil {
		return fmt.Errorf("dial %s: %w", s.cfg.Server, err)
	}

	s.connMu.Lock()
	s.conn = conn
	s.connMu.Unlock()
	s.ready.Store(true)
	s.lastRx.Store(time.Now().UnixNano())
	s.hub.AddTransmitter(Type, s)
	defer func() {
		s.ready.Store(false)
		s.hub.RemoveTransmitter(Type)
		s.connMu.Lock()
		s.conn = nil
		s.connMu.Unlock()
		conn.Close()
	}()

	s.logger.Info("aprs-radio: connected", "server", s.cfg.Server)

	dec := &aprs.KISSDecoder{}
	reader := bufio.NewReaderSize(conn, 4096)
	buf := make([]byte, 1024)
	for {
		_ = conn.SetReadDeadline(time.Now().Add(s.cfg.ReadTimeout))
		n, err := reader.Read(buf)
		if n > 0 {
			s.lastRx.Store(time.Now().UnixNano())
			for _, frame := range dec.Feed(buf[:n]) {
				if len(frame) == 0 || len(frame) > s.cfg.MaxFrameBytes {
					continue
				}
				s.handleFrame(frame)
			}
		}
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				// A quiet channel has nothing to say: stay connected.
				// Only a completely silent TNC for the whole idle
				// window ends the session.
				idle := s.idleLimit()
				if time.Since(time.Unix(0, s.lastRx.Load())) < idle {
					s.logger.Debug("aprs-radio: no frames from the TNC, keeping the connection",
						"server", s.cfg.Server, "idle_limit", idle)
					continue
				}
				return fmt.Errorf("aprs-radio: no frames from %s for %s", s.cfg.Server, idle)
			}
			return fmt.Errorf("read from %s: %w", s.cfg.Server, err)
		}
	}
}

// idleLimit is how long the session tolerates a completely silent TNC
// before reconnecting.
func (s *Source) idleLimit() time.Duration {
	return time.Duration(idleTimeoutFactor) * s.cfg.ReadTimeout
}

// handleFrame decodes one KISS data frame and feeds the parsed packet into
// the hub. Non-UI frames and garbage are ignored by the parser/hub.
func (s *Source) handleFrame(frame []byte) {
	src, dst, digis, info, ok := aprs.DecodeUIFrame(frame)
	if !ok || len(info) == 0 {
		return
	}
	line := aprs.FrameToFeedLine(src, dst, digis, info)
	p := aprs.ParseFeedLine(line, time.Now())
	if p.Src == "" {
		return
	}
	s.rxPackets.Add(1)
	s.hub.Observe(p, Type)
}

// Send transmits one APRS text message as an AX.25 UI frame through the
// KISS connection. The text already carries the {id} ack suffix when the
// hub asked for ack tracking.
func (s *Source) Send(_ context.Context, to, text string) error {
	s.connMu.Lock()
	conn := s.conn
	s.connMu.Unlock()
	if conn == nil || !s.ready.Load() {
		return errNotReady
	}
	info := fmt.Sprintf(":%-9s:%s", aprs.NormalizeCallsign(to), text)
	frame, err := aprs.BuildUIFrame(s.hub.Callsign(), to, s.cfg.Path, []byte(info))
	if err != nil {
		return fmt.Errorf("build frame: %w", err)
	}
	kiss := aprs.EncodeKISS(frame)

	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	_ = conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	for len(kiss) > 0 {
		n, err := conn.Write(kiss)
		if err != nil {
			return fmt.Errorf("write to %s: %w", s.cfg.Server, err)
		}
		kiss = kiss[n:]
	}
	return nil
}

// summary renders the one-line stats report for the web health page.
func (s *Source) summary() string {
	age := time.Since(time.Unix(0, s.lastRx.Load())).Truncate(time.Second)
	return fmt.Sprintf("%d packets / %d stations / last rx %s ago", s.rxPackets.Load(), s.hub.Stats().Stations, age)
}

var _ plugin.SourcePlugin = (*Source)(nil)
