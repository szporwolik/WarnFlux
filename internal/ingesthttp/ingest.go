// Package ingesthttp implements the public HTTP ingest endpoints: one
// API-key-protected POST handler per configured instance. Each endpoint
// accepts hazard messages in two shapes:
//
//   - the canonical WarnFlux MQTT /events wire payload (validated and
//     re-published verbatim semantics), or
//   - a simplified builder form ({severity, headline, event, ...}) that is
//     assembled into a proper /events wire payload with the instance ID as
//     the event source.
//
// Accepted messages are published to <topic_prefix>/events on the
// configured broker; the regular receiver → dispatch ingress → routing
// flow then treats them exactly like messages from any upstream WarnFlux
// instance, and every other consumer on the broker sees them too.
package ingesthttp

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/netip"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/szporwolik/WarnFlux/internal/config"
	"github.com/szporwolik/WarnFlux/internal/mqttreceiver"
	"github.com/szporwolik/WarnFlux/internal/storage"
)

const (
	// maxSecretFileBytes bounds the API key / password file reads.
	maxSecretFileBytes = 64 * 1024
	// connectTimeout bounds the initial broker connection attempt.
	connectTimeout = 10 * time.Second
	// publishTimeout bounds one publish operation.
	publishTimeout = 5 * time.Second
	// maxReconnectInterval caps paho's automatic reconnection backoff.
	maxReconnectInterval = 30 * time.Second
)

// publisher is the minimal paho surface used by an ingest endpoint. It is
// an interface only so tests can substitute a deterministic fake; the only
// production implementation is paho.Client.
type publisher interface {
	Connect() mqtt.Token
	Disconnect(quiesce uint)
	Publish(topic string, qos byte, retained bool, payload any) mqtt.Token
	IsConnected() bool
}

// Instance is one configured HTTP ingest endpoint: an API-key check plus
// a broker publisher. It is safe for concurrent use.
type Instance struct {
	cfg         config.IngestHTTP
	client      publisher
	logger      *slog.Logger
	previousKey string
	allowed     []netip.Prefix
	limiter     *rateLimiter
	metrics     counters
	started     atomic.Bool
	initGuard   sync.Once
}

// New reads the configured secret files and builds the instance without
// connecting. A missing or oversized secret file or an invalid CIDR is a
// construction error, never a runtime surprise.
func New(cfg config.IngestHTTP, logger *slog.Logger) (*Instance, error) {
	if cfg.APIKeyFile != "" {
		data, err := readSecret(cfg.APIKeyFile, "api_key_file")
		if err != nil {
			return nil, fmt.Errorf("ingest_http %q: %w", cfg.ID, err)
		}
		cfg.APIKey = data
	}
	if cfg.PreviousKeyFile != "" {
		data, err := readSecret(cfg.PreviousKeyFile, "previous_key_file")
		if err != nil {
			return nil, fmt.Errorf("ingest_http %q: %w", cfg.ID, err)
		}
		cfg.PreviousKey = data
	}
	if cfg.PasswordFile != "" {
		data, err := readSecret(cfg.PasswordFile, "password_file")
		if err != nil {
			return nil, fmt.Errorf("ingest_http %q: %w", cfg.ID, err)
		}
		cfg.Password = data
	}
	allowed, err := parseAllowedCIDRs(cfg.AllowedCIDRs)
	if err != nil {
		return nil, fmt.Errorf("ingest_http %q: %w", cfg.ID, err)
	}
	rate := cfg.RateLimitPerMinute
	if rate == 0 {
		rate = defaultRateLimitPerMinute
	}
	in := &Instance{
		cfg:         cfg,
		logger:      logger,
		previousKey: cfg.PreviousKey,
		allowed:     allowed,
		limiter:     newRateLimiter(rate),
	}
	return in, nil
}

// ensureInit lazily completes instances built directly (tests): a missing
// limiter falls back to the default rate and an unparsed allowlist is
// parsed once.
func (in *Instance) ensureInit() {
	in.initGuard.Do(func() {
		if in.limiter == nil && in.cfg.RateLimitPerMinute >= 0 {
			rate := in.cfg.RateLimitPerMinute
			if rate == 0 {
				rate = defaultRateLimitPerMinute
			}
			in.limiter = newRateLimiter(rate)
		}
		if in.allowed == nil && len(in.cfg.AllowedCIDRs) > 0 {
			if parsed, err := parseAllowedCIDRs(in.cfg.AllowedCIDRs); err == nil {
				in.allowed = parsed
			}
		}
	})
}

func readSecret(path, what string) (string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return "", fmt.Errorf("stat %s: %w", what, err)
	}
	if info.Size() > maxSecretFileBytes {
		return "", fmt.Errorf("%s is %d bytes, maximum %d", what, info.Size(), maxSecretFileBytes)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read %s: %w", what, err)
	}
	return strings.TrimRight(string(data), "\r\n"), nil
}

// Resolve merges the instance's empty broker settings with the fallback
// (the primary MQTT output's settings) and applies the final defaults.
// Explicit per-instance values always win, so an endpoint can still be
// pointed at another broker as an override.
func Resolve(cfg config.IngestHTTP, fallback config.IngestHTTP) config.IngestHTTP {
	if cfg.Broker == "" {
		cfg.Broker = fallback.Broker
	}
	if cfg.Username == "" {
		cfg.Username = fallback.Username
	}
	if cfg.Password == "" && cfg.PasswordFile == "" {
		cfg.Password = fallback.Password
		cfg.PasswordFile = fallback.PasswordFile
	}
	if cfg.TopicPrefix == "" {
		if fallback.TopicPrefix != "" {
			cfg.TopicPrefix = fallback.TopicPrefix
		} else {
			cfg.TopicPrefix = "warnflux"
		}
	}
	if cfg.ClientID == "" {
		cfg.ClientID = "warnflux-ingest-" + cfg.ID
	}
	return cfg
}

// Start connects to the broker (bounded by connectTimeout). A failed
// initial attempt is a non-fatal error: paho keeps retrying in the
// background and the endpoint answers 503 until the connection is up.
func (in *Instance) Start() error {
	opts := mqtt.NewClientOptions().
		AddBroker(in.cfg.Broker).
		SetClientID(in.cfg.ClientID).
		SetKeepAlive(60 * time.Second).
		SetConnectTimeout(connectTimeout).
		SetCleanSession(true).
		SetOrderMatters(false).
		SetAutoReconnect(true).
		SetConnectRetry(true).
		SetMaxReconnectInterval(maxReconnectInterval).
		SetConnectionLostHandler(func(_ mqtt.Client, err error) {
			in.logger.Warn("ingest_http: broker connection lost, reconnecting",
				"instance", in.cfg.ID, "error", err)
		})

	if in.cfg.Username != "" {
		opts.SetUsername(in.cfg.Username)
	}
	if in.cfg.Password != "" {
		opts.SetPassword(in.cfg.Password)
	}

	in.client = mqtt.NewClient(opts)
	token := in.client.Connect()
	if !token.WaitTimeout(connectTimeout) {
		return fmt.Errorf("initial connection attempt timed out (reconnecting in background)")
	}
	if err := token.Error(); err != nil {
		return err
	}
	in.started.Store(true)
	in.logger.Info("ingest_http: connected", "instance", in.cfg.ID, "topic", in.eventsTopic())
	return nil
}

// Close disconnects the broker client.
func (in *Instance) Close() {
	if in.client != nil {
		in.client.Disconnect(250)
	}
}

// ID returns the configured instance ID (also the builder-mode source).
func (in *Instance) ID() string { return in.cfg.ID }

// Counters returns a point-in-time snapshot of the request metrics.
func (in *Instance) Counters() Counters { return in.metrics.snapshot() }

// Connected reports the current broker connection state (false before
// Start and during reconnect windows).
func (in *Instance) Connected() bool {
	return in.started.Load() && in.client != nil && in.client.IsConnected()
}

// Started reports whether the endpoint attempted its initial broker
// connection (true after Start even when the broker is down).
func (in *Instance) Started() bool { return in.started.Load() }

func (in *Instance) eventsTopic() string {
	return in.cfg.TopicPrefix + "/events"
}

// ServeHTTP handles one ingest request: method check, source allowlist,
// API key check, rate limit, body parse/validation (wire or builder form)
// and broker publish. Every request is audited with its request id,
// source IP and result.
func (in *Instance) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	in.ensureInit()
	reqID := requestID(r.Header.Get("X-Request-ID"))
	w.Header().Set("X-Request-ID", reqID)
	ip := remoteIP(r.RemoteAddr)
	ipText := "unknown"
	if ip.IsValid() {
		ipText = ip.String()
	}

	audit := func(result string, status int, eventKey string, bytes int) {
		in.logger.Info("ingest_http: request",
			"instance", in.cfg.ID, "request_id", reqID, "remote", ipText,
			"result", result, "status", status, "bytes", bytes,
			"event_key", eventKey)
	}

	if r.Method != http.MethodPost {
		in.metrics.rejected.Add(1)
		in.fail(w, http.StatusMethodNotAllowed, "only POST is allowed")
		audit("rejected", http.StatusMethodNotAllowed, "", 0)
		return
	}
	if !addrAllowed(in.allowed, ip) {
		in.metrics.forbidden.Add(1)
		in.fail(w, http.StatusForbidden, "source address is not allowed")
		audit("forbidden", http.StatusForbidden, "", 0)
		return
	}
	if !in.authorized(r) {
		in.metrics.authFailed.Add(1)
		in.fail(w, http.StatusUnauthorized, "missing or invalid api key")
		audit("auth_failed", http.StatusUnauthorized, "", 0)
		return
	}
	if ok, wait := in.limiter.allow(time.Now()); !ok {
		in.metrics.rateLimited.Add(1)
		w.Header().Set("Retry-After", fmt.Sprintf("%d", int(wait.Seconds())+1))
		in.fail(w, http.StatusTooManyRequests, "rate limit exceeded; retry later")
		audit("rate_limited", http.StatusTooManyRequests, "", 0)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, mqttreceiver.MaxPayload+1))
	if err != nil {
		in.metrics.rejected.Add(1)
		in.fail(w, http.StatusBadRequest, "could not read request body")
		audit("rejected", http.StatusBadRequest, "", 0)
		return
	}
	if len(body) > mqttreceiver.MaxPayload {
		in.metrics.rejected.Add(1)
		in.fail(w, http.StatusRequestEntityTooLarge, "payload too large")
		audit("rejected", http.StatusRequestEntityTooLarge, "", len(body))
		return
	}

	payload, eventKey, err := in.buildPayload(body)
	if err != nil {
		in.metrics.rejected.Add(1)
		in.fail(w, http.StatusBadRequest, err.Error())
		audit("rejected", http.StatusBadRequest, "", len(body))
		return
	}

	if in.client == nil || !in.client.IsConnected() {
		in.fail(w, http.StatusServiceUnavailable, "broker is not connected; retry in a moment")
		audit("broker_unavailable", http.StatusServiceUnavailable, eventKey, len(body))
		return
	}

	token := in.client.Publish(in.eventsTopic(), 1, false, payload)
	if !token.WaitTimeout(publishTimeout) {
		in.fail(w, http.StatusServiceUnavailable, "broker publish timed out")
		audit("broker_unavailable", http.StatusServiceUnavailable, eventKey, len(body))
		return
	}
	if err := token.Error(); err != nil {
		in.logger.Warn("ingest_http: publish failed", "instance", in.cfg.ID,
			"request_id", reqID, "error", err)
		in.fail(w, http.StatusServiceUnavailable, "broker publish failed")
		audit("broker_unavailable", http.StatusServiceUnavailable, eventKey, len(body))
		return
	}

	in.metrics.accepted.Add(1)
	audit("accepted", http.StatusAccepted, eventKey, len(body))
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_, _ = w.Write([]byte(`{"accepted":true,"topic":"` + in.eventsTopic() + `","event_key":"` + eventKey + `","request_id":"` + reqID + `"}`))
}

// authorized checks the bearer token against the current and previous
// keys in constant time (both are accepted during rotation).
func (in *Instance) authorized(r *http.Request) bool {
	const prefix = "Bearer "
	auth := strings.TrimSpace(r.Header.Get("Authorization"))
	if !strings.HasPrefix(auth, prefix) {
		return false
	}
	presented := []byte(strings.TrimSpace(strings.TrimPrefix(auth, prefix)))
	if len(presented) > 0 && subtle.ConstantTimeCompare(presented, []byte(in.cfg.APIKey)) == 1 {
		return true
	}
	if len(presented) > 0 && in.previousKey != "" &&
		subtle.ConstantTimeCompare(presented, []byte(in.previousKey)) == 1 {
		return true
	}
	return false
}

func (in *Instance) fail(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(`{"error":` + strconv.Quote(msg) + `}`))
}

// builderPayload is the simplified alert form: a scraper posts these
// fields and the endpoint assembles a canonical /events wire payload.
type builderPayload struct {
	Transition  string   `json:"transition"`
	SourceID    string   `json:"source_id"`
	Category    string   `json:"category"`
	Event       string   `json:"event"`
	Severity    string   `json:"severity"`
	Urgency     string   `json:"urgency"`
	Certainty   string   `json:"certainty"`
	Headline    string   `json:"headline"`
	Description string   `json:"description"`
	Instruction string   `json:"instruction"`
	EffectiveAt *string  `json:"effective_at,omitempty"`
	ExpiresAt   *string  `json:"expires_at,omitempty"`
	Latitude    *float64 `json:"latitude,omitempty"`
	Longitude   *float64 `json:"longitude,omitempty"`
	Areas       []string `json:"areas"`
	SourceURL   string   `json:"source_url"`
}

// buildPayload decides between the wire form (a schema_version key is
// present) and the builder form, and returns the canonical JSON payload
// plus its event key.
func (in *Instance) buildPayload(body []byte) ([]byte, string, error) {
	var probe struct {
		SchemaVersion *int `json:"schema_version"`
	}
	if err := json.Unmarshal(body, &probe); err != nil {
		return nil, "", fmt.Errorf("invalid JSON body")
	}
	if probe.SchemaVersion != nil {
		// Full wire form: validate with the receiver's own parser and
		// re-publish the canonical shape (unknown fields dropped).
		we, err := mqttreceiver.ParseEventPayload(body)
		if err != nil {
			return nil, "", err
		}
		canonical, err := json.Marshal(we)
		if err != nil {
			return nil, "", fmt.Errorf("encode event: %w", err)
		}
		return canonical, we.EventKey, nil
	}
	return in.buildFromBuilder(body)
}

// buildFromBuilder assembles a canonical /events wire payload from the
// simplified alert form. The instance ID is the event source.
func (in *Instance) buildFromBuilder(body []byte) ([]byte, string, error) {
	var b builderPayload
	if err := json.Unmarshal(body, &b); err != nil {
		return nil, "", fmt.Errorf("invalid JSON body")
	}
	if err := validateBuilder(&b); err != nil {
		return nil, "", err
	}

	now := time.Now().UTC().Format(time.RFC3339)
	sourceID := strings.TrimSpace(b.SourceID)
	if sourceID == "" {
		// No stable ID: hash the raw body so an identical re-post yields
		// the same event key (deduplicates naturally), while any field
		// change yields a fresh alert.
		sum := sha256.Sum256(body)
		sourceID = hex.EncodeToString(sum[:])[:16]
	}

	we := mqttreceiver.EventPayload{
		SchemaVersion: mqttreceiver.WireSchemaVersion,
		ChangeType:    b.Transition,
		EventKey:      in.cfg.ID + ":" + sourceID,
		Event: mqttreceiver.HazardPayload{
			Source:      in.cfg.ID,
			SourceID:    sourceID,
			Category:    strings.TrimSpace(b.Category),
			Event:       strings.TrimSpace(b.Event),
			Severity:    strings.ToLower(strings.TrimSpace(b.Severity)),
			Urgency:     strings.ToLower(strings.TrimSpace(b.Urgency)),
			Certainty:   strings.ToLower(strings.TrimSpace(b.Certainty)),
			Headline:    strings.TrimSpace(b.Headline),
			Description: strings.TrimSpace(b.Description),
			Instruction: strings.TrimSpace(b.Instruction),
			EffectiveAt: b.EffectiveAt,
			ExpiresAt:   b.ExpiresAt,
			Latitude:    b.Latitude,
			Longitude:   b.Longitude,
			Areas:       b.Areas,
			Status:      "active",
			SourceURL:   strings.TrimSpace(b.SourceURL),
			ReceivedAt:  now,
			UpdatedAt:   now,
		},
	}

	canonical, err := json.Marshal(we)
	if err != nil {
		return nil, "", fmt.Errorf("encode event: %w", err)
	}
	return canonical, we.EventKey, nil
}

// validateBuilder enforces the builder form contract.
func validateBuilder(b *builderPayload) error {
	switch strings.ToLower(strings.TrimSpace(b.Transition)) {
	case "", "new":
		b.Transition = mqttreceiver.ChangeNew
	case "updated":
		b.Transition = mqttreceiver.ChangeUpdated
	case "cancelled":
		b.Transition = mqttreceiver.ChangeCancelled
	case "expired":
		b.Transition = mqttreceiver.ChangeExpired
	default:
		return fmt.Errorf("invalid transition %q (new, updated, cancelled or expired)", b.Transition)
	}

	sev := strings.ToLower(strings.TrimSpace(b.Severity))
	if !storage.ValidSeverity(sev) {
		return fmt.Errorf("severity is required and must be one of unknown, minor, moderate, severe, extreme")
	}
	b.Severity = sev

	if strings.TrimSpace(b.Event) == "" && strings.TrimSpace(b.Headline) == "" {
		return fmt.Errorf("event or headline is required")
	}
	for field, v := range map[string]string{
		"event": b.Event, "headline": b.Headline, "urgency": b.Urgency,
		"certainty": b.Certainty, "source_id": b.SourceID,
	} {
		if len(v) > 256 {
			return fmt.Errorf("%s is too long (maximum 256 characters)", field)
		}
	}
	if strings.TrimSpace(b.SourceID) != b.SourceID {
		return fmt.Errorf("source_id must not have leading or trailing whitespace")
	}
	return nil
}
