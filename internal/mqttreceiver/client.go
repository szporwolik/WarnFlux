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

// Connect performs the first connection attempt (bounded by
// connect_timeout) and installs the reconnect-safe subscribe handlers.
// After that, paho reconnects automatically in the background; a failed
// initial attempt is an error here, never fatal to the process.
func (r *Receiver) Connect(ctx context.Context) error {
	opts := mqtt.NewClientOptions().
		AddBroker(r.cfg.Broker).
		SetClientID(r.cfg.ClientID).
		SetKeepAlive(r.cfg.KeepAlive).
		SetConnectTimeout(r.cfg.ConnectTimeout).
		SetCleanSession(true).
		SetOrderMatters(false).
		SetAutoReconnect(true).
		SetConnectRetry(true).
		SetMaxReconnectInterval(maxReconnectInterval).
		SetDefaultPublishHandler(r.messageHandler())

	if r.cfg.Username != "" {
		opts.SetUsername(r.cfg.Username)
	}
	if r.cfg.Password != "" {
		opts.SetPassword(r.cfg.Password)
	}

	// With CleanSession the broker forgets subscriptions on disconnect;
	// OnConnect re-establishes them after every (re)connection.
	opts.SetOnConnectHandler(func(_ mqtt.Client) {
		if err := r.subscribe(); err != nil {
			r.logger.Error("receiver: subscription setup failed", "receiver", r.cfg.ID, "error", err)
			r.setConnected(false, err.Error())
			return
		}
		r.setConnected(true, "")
		r.logger.Info("receiver: connected and subscribed",
			"receiver", r.cfg.ID, "broker", sanitizeBroker(r.cfg.Broker))
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
// time per receiver, then delegates. The callback itself stays fast and
// non-blocking.
func (r *Receiver) messageHandler() mqtt.MessageHandler {
	return func(c mqtt.Client, msg mqtt.Message) {
		now := time.Now()
		r.mu.Lock()
		r.status.LastMessage = now
		r.mu.Unlock()
		r.ingestor.HandleMessage(c, msg)
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

// StopIntake unsubscribes from all topics so no new messages reach the
// ingress. The TCP connection stays up; Disconnect tears it down.
func (r *Receiver) StopIntake() {
	if r.client == nil {
		return
	}
	r.client.Unsubscribe(uniqueTopics(allTopics(r.cfg))...)
	r.setConnected(false, "")
}

// Disconnect unsubscribes and disconnects cleanly (bounded wait).
func (r *Receiver) Disconnect() {
	if r.client == nil {
		return
	}
	r.StopIntake()
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
