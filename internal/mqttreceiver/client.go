package mqttreceiver

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/szporwolik/WarnFlux/internal/config"
	"github.com/szporwolik/WarnFlux/internal/dispatch"
	"github.com/szporwolik/WarnFlux/internal/dispatch/state"
)

// subscribeTimeout bounds subscription setup inside OnConnect.
const subscribeTimeout = 10 * time.Second

// maxReconnectInterval caps paho's automatic reconnection backoff for
// receivers (same value the MQTT output uses): prompt recovery matters
// because receivers feed the dispatch ingress.
const maxReconnectInterval = 30 * time.Second

// Inbound-stall watchdog defaults. A wedged delivery path is otherwise
// silent: the connection stays "connected" and the subscriptions look
// fine while no message ever arrives again (the broker can stop sending
// without the client noticing). The app publishes status/heartbeat
// documents every 30s, so a connected receiver that heard nothing for
// stallThreshold has a broken inbound stream.
const (
	stallCheckInterval  = 30 * time.Second
	stallThreshold      = 3 * time.Minute
	stallEscalateAfter  = 2                // resets before the clean-session escalation
	stallEscalateWindow = 10 * time.Minute // repeated stalls outside this window restart the escalation
	stallConnectTimeout = 15 * time.Second // bound for the reset Connect attempt
)

// maxPasswordFileBytes bounds the receiver password file read.
const maxPasswordFileBytes = 64 * 1024

// mqttClient is the minimal paho surface used by a receiver. It is an
// interface only so tests can substitute a deterministic fake; the only
// production implementation is paho.Client.
type mqttClient interface {
	Connect() mqtt.Token
	Disconnect(quiesce uint)
	Subscribe(topic string, qos byte, callback mqtt.MessageHandler) mqtt.Token
	Unsubscribe(topics ...string) mqtt.Token
	Publish(topic string, qos byte, retained bool, payload interface{}) mqtt.Token
	IsConnected() bool
}

// Status is a point-in-time view of one receiver connection. Broker
// credentials are never included (the URL is sanitized at construction).
type Status struct {
	ID            string
	Enabled       bool
	Broker        string
	WFEnabled     bool
	WFPrefix      string
	Subscriptions int
	Connected     bool
	LastConnect   time.Time
	LastMessage   time.Time
	LastError     string
	Messages      int64
	Malformed     int64
	Oversized     int64
	Dropped       int64
	StallResets   int64
}

// Receiver owns one independent MQTT client: its connection, subscriptions,
// ingestor, reconnect lifecycle and health. Receiver failures never affect
// other receivers or the rest of the application.
type Receiver struct {
	cfg      config.Receiver
	client   mqttClient
	ingestor *Ingestor
	stats    *Stats
	logger   *slog.Logger

	mu     sync.Mutex
	status Status

	// connMu serializes Connect calls (startup and stall-monitor session
	// resets) so the client pointer is never swapped concurrently.
	connMu sync.Mutex
	// done closes on Disconnect and stops the stall monitor.
	done     chan struct{}
	doneOnce sync.Once
	// cleanOnce forces the next Connect to use a clean session: the
	// broker discards the persistent session (queued messages, in-flight
	// state and subscriptions) and the receiver starts from a clean slate.
	cleanOnce bool
	// stallResets counts stall-monitor session resets (visible in the
	// receiver status and metrics).
	stallResets int64
	// resync re-publishes the local-first state (panel communications,
	// EMCOM networks) after a (re)connect, so broker outages never lose
	// them permanently.
	resync func()

	// browseMu serializes the temporary browse subscriptions the web UI
	// opens (one at a time per receiver).
	browseMu sync.Mutex
}

// NewReceiver builds the receiver and its paho client without connecting.
// Password files are read here (like the MQTT output) so a missing secret
// fails receiver construction, never runtime delivery.
func NewReceiver(cfg config.Receiver, st *state.State, ingress *dispatch.Ingress, logger *slog.Logger, traffic *TrafficBuffer) (*Receiver, error) {
	if cfg.PasswordFile != "" {
		if info, err := os.Stat(cfg.PasswordFile); err != nil {
			return nil, fmt.Errorf("receiver %q: stat password_file: %w", cfg.ID, err)
		} else if info.Size() > maxPasswordFileBytes {
			return nil, fmt.Errorf("receiver %q: password_file is %d bytes, maximum %d", cfg.ID, info.Size(), maxPasswordFileBytes)
		}
		data, err := os.ReadFile(cfg.PasswordFile)
		if err != nil {
			return nil, fmt.Errorf("receiver %q: read password_file: %w", cfg.ID, err)
		}
		cfg.Password = strings.TrimRight(string(data), "\r\n")
	}

	var filters []string
	if cfg.WF.Enabled {
		filters = Subscriptions(cfg.WF.TopicPrefix)
	}
	for _, sub := range cfg.Subscriptions {
		filters = append(filters, sub.Topic)
	}

	stats := &Stats{}
	r := &Receiver{
		cfg:    cfg,
		stats:  stats,
		logger: logger,
		done:   make(chan struct{}),
		status: Status{
			ID:            cfg.ID,
			Enabled:       cfg.Enabled,
			Broker:        sanitizeBroker(cfg.Broker),
			WFEnabled:     cfg.WF.Enabled,
			WFPrefix:      cfg.WF.TopicPrefix,
			Subscriptions: len(uniqueTopics(filters)),
		},
	}
	r.ingestor = NewIngestor(cfg.ID, cfg.WF.Enabled, cfg.WF.TopicPrefix,
		subscriptionFilters(cfg), st, ingress, stats, logger, traffic)
	return r, nil
}

func subscriptionFilters(cfg config.Receiver) []string {
	out := make([]string, 0, len(cfg.Subscriptions))
	for _, s := range cfg.Subscriptions {
		out = append(out, s.Topic)
	}
	return out
}

// SetResync installs the post-(re)connect state sync callback (local-first
// producers re-publish their current state through it).
func (r *Receiver) SetResync(fn func()) {
	r.mu.Lock()
	r.resync = fn
	r.mu.Unlock()
}

// Connect performs the first connection attempt (bounded by
// connect_timeout) and installs the reconnect-safe subscribe handlers.
// After that, paho reconnects automatically in the background; a failed
// initial attempt is an error here, never fatal to the process.
func (r *Receiver) Connect(ctx context.Context) error {
	r.connMu.Lock()
	defer r.connMu.Unlock()

	// A stall-monitor escalation may request one clean-session connect:
	// the flag is consumed here so the next auto-reconnect goes back to
	// the configured session mode.
	r.mu.Lock()
	cleanOnce := r.cleanOnce
	r.cleanOnce = false
	r.mu.Unlock()

	opts := mqtt.NewClientOptions().
		AddBroker(r.cfg.Broker).
		SetClientID(r.cfg.ClientID).
		SetKeepAlive(r.cfg.KeepAlive).
		SetConnectTimeout(r.cfg.ConnectTimeout).
		SetCleanSession(r.cfg.CleanSession || cleanOnce).
		SetOrderMatters(false).
		SetAutoReconnect(true).
		SetConnectRetry(true).
		SetMaxReconnectInterval(maxReconnectInterval).
		SetAutoAckDisabled(true). // ack only after the durable inbox write
		SetDefaultPublishHandler(r.messageHandler())

	if r.cfg.Username != "" {
		opts.SetUsername(r.cfg.Username)
	}
	if r.cfg.Password != "" {
		opts.SetPassword(r.cfg.Password)
	}

	// With CleanSession(true) the broker forgets subscriptions on
	// disconnect; OnConnect re-establishes them after every (re)connection
	// either way (idempotent for persistent sessions).
	opts.SetOnConnectHandler(func(_ mqtt.Client) {
		if err := r.subscribe(); err != nil {
			r.logger.Error("receiver: subscription setup failed", "receiver", r.cfg.ID, "error", err)
			r.setConnected(false, err.Error())
			return
		}
		r.setConnected(true, "")
		mode := "persistent session"
		if r.cfg.CleanSession {
			mode = "clean session"
		}
		r.logger.Info("receiver: connected and subscribed",
			"receiver", r.cfg.ID, "broker", sanitizeBroker(r.cfg.Broker), "session", mode)
		r.mu.Lock()
		resync := r.resync
		r.mu.Unlock()
		if resync != nil {
			resync()
		}
	})
	opts.SetConnectionLostHandler(func(_ mqtt.Client, err error) {
		r.setConnected(false, err.Error())
		r.logger.Warn("receiver: connection lost, reconnecting", "receiver", r.cfg.ID, "error", err)
	})

	r.client = mqtt.NewClient(opts)

	token := r.client.Connect()
	if !token.WaitTimeout(r.cfg.ConnectTimeout) {
		r.setConnected(false, "initial connection attempt timed out (reconnecting in background)")
		return &ConnectError{ID: r.cfg.ID, Timeout: r.cfg.ConnectTimeout}
	}
	if err := token.Error(); err != nil {
		r.setConnected(false, err.Error())
		return &ConnectError{ID: r.cfg.ID, Err: err}
	}
	return nil
}

// messageHandler wraps the ingestor callback: it stamps the last-message
// time per receiver, then delegates. The MQTT acknowledgment follows the
// ingestor's decision: the message is ACKed only when its events were
// durably accepted (or deliberately consumed); a rejected event is left
// unacknowledged so the broker redelivers it on the next (re)connect.
func (r *Receiver) messageHandler() mqtt.MessageHandler {
	return func(c mqtt.Client, msg mqtt.Message) {
		now := time.Now()
		r.mu.Lock()
		r.status.LastMessage = now
		r.mu.Unlock()
		if r.ingestor.HandleMessage(c, msg) {
			msg.Ack()
		}
	}
}

func (r *Receiver) setConnected(connected bool, lastErr string) {
	r.mu.Lock()
	r.status.Connected = connected
	r.status.LastError = lastErr
	if connected {
		r.status.LastConnect = time.Now()
	}
	r.mu.Unlock()
}

// subscribe sets up the full subscription set of this receiver (WarnFlux
// protocol topics at QoS 1 plus the configured generic subscriptions).
func (r *Receiver) subscribe() error {
	topics := make(map[string]byte)
	if r.cfg.WF.Enabled {
		for _, t := range Subscriptions(r.cfg.WF.TopicPrefix) {
			topics[t] = 1
		}
	}
	for _, s := range r.cfg.Subscriptions {
		if q, ok := topics[s.Topic]; !ok || byte(s.QoS) > q {
			topics[s.Topic] = byte(s.QoS)
		}
	}
	for topic, qos := range topics {
		token := r.client.Subscribe(topic, qos, nil)
		if !token.WaitTimeout(subscribeTimeout) {
			return &SubscribeError{Topic: topic}
		}
		if err := token.Error(); err != nil {
			return &SubscribeError{Topic: topic, Err: err}
		}
	}
	return nil
}

// StartStallMonitor watches the inbound stream and resets the MQTT
// session when messages stop arriving while the client claims to be
// connected. The first reset preserves the persistent session (queued
// and redelivered backlog is kept); repeated stalls inside a short
// window escalate to a clean-session reset, which discards the
// broker-side session state entirely. interval and threshold exist for
// tests; production uses the package defaults.
func (r *Receiver) StartStallMonitor(interval, threshold time.Duration) {
	if interval <= 0 {
		interval = stallCheckInterval
	}
	if threshold <= 0 {
		threshold = stallThreshold
	}
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		stalls := 0
		var firstStallAt time.Time
		for {
			select {
			case <-r.done:
				return
			case <-t.C:
			}

			r.mu.Lock()
			connected, last := r.status.Connected, r.status.LastMessage
			r.mu.Unlock()
			if !firstStallAt.IsZero() && time.Since(firstStallAt) > stallEscalateWindow {
				stalls = 0
				firstStallAt = time.Time{}
			}
			if connected && !last.IsZero() && time.Since(last) < threshold {
				// Traffic flowing: healthy, restart the escalation.
				stalls = 0
				firstStallAt = time.Time{}
				continue
			}
			if !connected || last.IsZero() {
				// Reconnecting or no traffic yet: keep the escalation state
				// so a reset-in-progress still counts towards escalation.
				continue
			}
			if firstStallAt.IsZero() {
				firstStallAt = time.Now()
			}
			stalls++
			clean := stalls >= stallEscalateAfter
			r.logger.Warn("receiver: inbound stream stalled; resetting MQTT session",
				"receiver", r.cfg.ID, "last_message", last.Format(time.RFC3339),
				"threshold", threshold, "stalls", stalls, "clean_session", clean)
			r.resetSession(clean)
			r.mu.Lock()
			r.stallResets++
			r.mu.Unlock()
		}
	}()
}

// resetSession disconnects the current client and opens a fresh
// connection. With clean=true the broker discards the persistent session
// (queued messages, in-flight state, prior subscriptions) and the
// receiver starts from a clean slate. An explicit Disconnect stops the
// old client's automatic reconnection, so exactly one client owns the
// connection afterwards.
func (r *Receiver) resetSession(clean bool) {
	r.mu.Lock()
	r.cleanOnce = clean
	r.mu.Unlock()

	// Swap the client under the connection and browse locks so a
	// concurrent startup Connect or Browse never races the pointer.
	r.connMu.Lock()
	r.browseMu.Lock()
	if r.client != nil {
		r.client.Disconnect(250)
	}
	r.client = nil
	r.browseMu.Unlock()
	r.connMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), stallConnectTimeout)
	defer cancel()
	if err := r.Connect(ctx); err != nil {
		// Not fatal: paho keeps retrying the new client in the background.
		r.logger.Warn("receiver: session reset failed (background retry pending)",
			"receiver", r.cfg.ID, "error", err)
	}
}

// StopIntake unsubscribes from all topics so no new messages reach the
// ingress. The TCP connection stays up; Disconnect tears it down.
func (r *Receiver) StopIntake() {
	r.connMu.Lock()
	defer r.connMu.Unlock()
	r.stopIntake()
}

func (r *Receiver) stopIntake() {
	if r.client == nil {
		return
	}
	r.client.Unsubscribe(uniqueTopics(allTopics(r.cfg))...)
	r.setConnected(false, "")
}

// Disconnect unsubscribes and disconnects cleanly (bounded wait).
func (r *Receiver) Disconnect() {
	if r.done != nil {
		r.doneOnce.Do(func() { close(r.done) })
	}
	r.connMu.Lock()
	defer r.connMu.Unlock()
	if r.client == nil {
		return
	}
	r.stopIntake()
	r.client.Disconnect(250)
}

// Status returns a point-in-time copy of the receiver status.
func (r *Receiver) Status() Status {
	r.mu.Lock()
	defer r.mu.Unlock()
	s := r.status
	s.Messages = r.stats.Messages.Load()
	s.Malformed = r.stats.Malformed.Load()
	s.Oversized = r.stats.Oversized.Load()
	s.Dropped = r.stats.Dropped.Load()
	s.StallResets = r.stallResets
	return s
}

func allTopics(cfg config.Receiver) []string {
	out := make([]string, 0, 4+len(cfg.Subscriptions))
	if cfg.WF.Enabled {
		out = append(out, Subscriptions(cfg.WF.TopicPrefix)...)
	}
	for _, s := range cfg.Subscriptions {
		out = append(out, s.Topic)
	}
	return out
}

func uniqueTopics(topics []string) []string {
	seen := make(map[string]bool, len(topics))
	out := make([]string, 0, len(topics))
	for _, t := range topics {
		if !seen[t] {
			seen[t] = true
			out = append(out, t)
		}
	}
	return out
}

// sanitizeBroker removes credentials (userinfo) from a broker URL for
// display/logging. URLs without userinfo are returned unchanged.
func sanitizeBroker(broker string) string {
	scheme := ""
	rest := broker
	if idx := strings.Index(broker, "://"); idx >= 0 {
		scheme = broker[:idx+3]
		rest = broker[idx+3:]
	}
	if at := strings.LastIndex(rest, "@"); at >= 0 {
		rest = rest[at+1:]
	}
	return scheme + rest
}

// ConnectError reports a failed or timed-out initial connection attempt.
type ConnectError struct {
	ID      string
	Timeout time.Duration
	Err     error
}

func (e *ConnectError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("receiver %q: initial connection failed: %v", e.ID, e.Err)
	}
	return fmt.Sprintf("receiver %q: initial connection attempt timed out (reconnecting in background)", e.ID)
}

// SubscribeError reports a failed subscription.
type SubscribeError struct {
	Topic string
	Err   error
}

func (e *SubscribeError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("receiver: subscribe %s: %v", e.Topic, e.Err)
	}
	return "receiver: subscribe " + e.Topic + ": timeout"
}
