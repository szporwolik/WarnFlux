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
// Accepted messages are dispatched into the LOCAL ingress first (the
// durable inbox — SQLite + radio suffice, the endpoint never depends on
// the broker connection) and their broker publication is persisted into
// the durable OUTBOX before the 202: a background worker publishes the
// rows to <topic_prefix>/events whenever the broker is connected and
// mirrors the retained active view <prefix>/active/<source>/<hash>, with
// at-least-once semantics. A broker outage therefore never rejects an
// accepted request nor loses its cross-instance sync. The MQTT publish
// mask controls ONLY the broker synchronization (applied at publish
// time): a fully masked category defers the sync, never the local
// delivery.
package ingesthttp

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
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
	"github.com/szporwolik/WarnFlux/internal/dispatch"
	"github.com/szporwolik/WarnFlux/internal/mqttpolicy"
	"github.com/szporwolik/WarnFlux/internal/mqttreceiver"
	"github.com/szporwolik/WarnFlux/internal/severity"
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

// Outbox is the durable broker-sync backlog for HTTP ingest: an accepted
// request persists its intended broker publication BEFORE answering 202,
// and the background worker deletes the row only after the broker
// confirmed it — so a broker outage never loses the cross-instance sync
// of an accepted request. Implemented by the SQLite store.
type Outbox interface {
	AppendOutbox(ctx context.Context, instanceID, topic string, payload []byte) (int64, error)
	PendingOutbox(ctx context.Context, instanceID string, limit int) ([]storage.OutboxItem, error)
	AckOutbox(ctx context.Context, id int64) error
	OutboxCount(ctx context.Context) (int, error)
}

// Acceptor is the atomic local acceptance backend for HTTP ingest (the
// SQLite store): one transaction persists the durable inbox row, the
// lifecycle record and the durable outbox row together, so a rejected
// commit accepts nothing and a crash can never leave an accepted local
// notification without its MQTT sync.
type Acceptor interface {
	CommitIngest(ctx context.Context, instanceID, topic string, payload []byte, ev dispatch.Event) (int64, error)
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

	// ingress is the optional local dispatch ingress: accepted messages
	// are dispatched into it (the durable inbox) BEFORE any broker I/O,
	// so the MQTT publish mask and a broker outage never lose the local
	// pipeline. Wired in main; nil in mirror-only constructions.
	ingress *dispatch.Ingress

	// outbox is the optional durable broker-sync backlog: the accepted
	// payload is persisted BEFORE the 202 and a background worker
	// publishes it whenever the broker is connected (mask applied at
	// publish time). nil disables the outbox (tests).
	outbox Outbox
	// wake nudges the outbox worker right after an append so a connected
	// broker receives the request without waiting for the tick.
	wake chan struct{}

	// acceptor is the optional ATOMIC acceptance backend: inbox row +
	// lifecycle + outbox row commit in one transaction before the event
	// is handed to the live worker (P1). Wired in main; nil falls back
	// to the legacy two-step acceptance (mirror-only constructions and
	// tests).
	acceptor Acceptor

	// publisherID is the persistent instance UUID stamped onto every
	// builder-mode event (wired from the storage store): with it the
	// routing engine records each transition in the lifecycle ledger
	// and the pre-transmission gate blocks jobs a later cancellation
	// supersedes. Empty = legacy publisher-less form (fail-open).
	publisherID string
	// lastChangeID is the monotonic version source for builder events:
	// versions must strictly increase so a cancellation always
	// supersedes the jobs it retires, even within one millisecond.
	lastChangeID atomic.Int64
}

// SetIngress attaches the local-first dispatch ingress. It must be set
// before the endpoint starts serving requests (wired in main).
func (in *Instance) SetIngress(g *dispatch.Ingress) {
	in.ingress = g
}

// SetAcceptor attaches the atomic acceptance backend (the SQLite
// store). When set, every accepted request commits its inbox row, its
// lifecycle record and its outbox row in ONE transaction before the
// live handoff; without it the endpoint uses the legacy two-step
// acceptance (ingress + outbox).
func (in *Instance) SetAcceptor(a Acceptor) {
	in.acceptor = a
}

// SetOutbox attaches the durable broker-sync backlog. It must be set
// before the endpoint starts serving requests (wired in main).
func (in *Instance) SetOutbox(o Outbox) {
	in.outbox = o
	if in.wake == nil {
		in.wake = make(chan struct{}, 1)
	}
}

// SetPublisherID attaches the persistent instance UUID stamped onto
// builder-mode events (wired in main from the storage store). Without
// it builder events stay publisher-less and version-less — the legacy
// form routing skips in the lifecycle ledger.
func (in *Instance) SetPublisherID(id string) {
	in.publisherID = strings.TrimSpace(id)
}

// nextChangeID returns the next strictly-increasing version for a
// builder event: wall-clock milliseconds, bumped past the last issued
// value so two posts within the same millisecond still order correctly.
func (in *Instance) nextChangeID() int64 {
	for {
		last := in.lastChangeID.Load()
		next := time.Now().UnixMilli()
		if next <= last {
			next = last + 1
		}
		if in.lastChangeID.CompareAndSwap(last, next) {
			return next
		}
	}
}

// Disabled returns the explicit-error handler for an ingest input the
// operator disabled in the configuration: the route stays registered so
// a misconfigured sender gets a clear answer instead of a misleading
// 404 that hides the configuration mistake.
func Disabled(logger *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		logger.Warn("ingest_http: request to a disabled input",
			"method", r.Method, "remote", r.RemoteAddr, "path", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"error":"ingest input disabled by configuration"}`))
	})
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
// background and, until the connection is up, accepted requests pile up
// in the durable outbox and are published by the outbox worker once the
// broker returns.
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

	// LOCAL-FIRST acceptance. With the atomic backend attached, the
	// durable inbox row, the lifecycle record and the durable outbox row
	// commit in ONE transaction (P1): a rejected commit accepts NOTHING
	// (the 503 carries no side effects) and a crash can never leave an
	// accepted local notification without its MQTT sync. After the
	// commit the event is handed to the live worker with its inbox id —
	// the ingress never re-appends it. Without the backend the legacy
	// two-step acceptance applies (mirror-only constructions, tests).
	if in.acceptor != nil {
		we, err := mqttreceiver.ParseEventPayload(payload)
		if err != nil {
			// Impossible after buildPayload; defensive only.
			in.metrics.rejected.Add(1)
			in.fail(w, http.StatusBadRequest, "invalid event payload")
			audit("rejected", http.StatusBadRequest, eventKey, len(body))
			return
		}
		ev := mqttreceiver.EventFromWire(we, in.cfg.ID, time.Now())
		inboxID, err := in.acceptor.CommitIngest(r.Context(), in.cfg.ID, in.eventsTopic(), payload, ev)
		if err != nil {
			in.metrics.rejected.Add(1)
			msg := "local durable acceptance unavailable; retry later"
			if errors.Is(err, storage.ErrOutboxFull) {
				msg = "durable outbox full (pending backlog over capacity); retry later"
				in.logger.Warn("ingest_http: durable outbox at capacity — explicit backpressure, nothing dropped",
					"instance", in.cfg.ID, "request_id", reqID, "event_key", eventKey)
			}
			in.fail(w, http.StatusServiceUnavailable, msg)
			audit("accept_rejected", http.StatusServiceUnavailable, eventKey, len(body))
			return
		}
		ev.InboxID = inboxID
		if in.ingress != nil {
			switch in.ingress.Enqueue(ev) {
			case dispatch.Rejected:
				// The durable row is committed: inbox recovery still
				// delivers the event even though the live handoff was
				// refused.
				in.logger.Warn("ingest_http: live handoff refused; the event stays in the durable inbox",
					"instance", in.cfg.ID, "request_id", reqID, "event_key", eventKey)
			case dispatch.AcceptedEmergency:
				// Unreachable with an inbox id attached; kept for
				// completeness.
				in.logger.Warn("ingest_http: accepted WITHOUT durable storage (emergency mode; lost on restart)",
					"instance", in.cfg.ID, "request_id", reqID, "event_key", eventKey)
			}
		}
		in.wakeOutbox()
	} else if in.ingress != nil {
		// LEGACY two-step acceptance (no atomic backend attached).
		we, err := mqttreceiver.ParseEventPayload(payload)
		if err != nil {
			// Impossible after buildPayload; defensive only.
			in.metrics.rejected.Add(1)
			in.fail(w, http.StatusBadRequest, "invalid event payload")
			audit("rejected", http.StatusBadRequest, eventKey, len(body))
			return
		}
		ev := mqttreceiver.EventFromWire(we, in.cfg.ID, time.Now())
		switch in.ingress.Enqueue(ev) {
		case dispatch.Rejected:
			in.fail(w, http.StatusServiceUnavailable, "local dispatch is unavailable; retry later")
			audit("local_dispatch_rejected", http.StatusServiceUnavailable, eventKey, len(body))
			return
		case dispatch.AcceptedEmergency:
			in.logger.Warn("ingest_http: accepted WITHOUT durable storage (emergency mode; lost on restart)",
				"instance", in.cfg.ID, "request_id", reqID, "event_key", eventKey)
		}
	}

	// Durable broker sync: the atomic path already committed the outbox
	// row above; the legacy path records the intended /events
	// publication (and the active-view mirror) here, BEFORE answering
	// 202. The background worker publishes it whenever the broker is
	// connected — the publish mask is applied at publish time — so
	// acceptance never depends on the broker and an outage never loses
	// the cross-instance sync of an accepted request.
	if in.acceptor == nil && in.outbox != nil {
		if _, err := in.outbox.AppendOutbox(r.Context(), in.cfg.ID, in.eventsTopic(), payload); err != nil {
			in.metrics.rejected.Add(1)
			msg := "durable outbox unavailable; retry later"
			if errors.Is(err, storage.ErrOutboxFull) {
				msg = "durable outbox full (pending backlog over capacity); retry later"
				in.logger.Warn("ingest_http: durable outbox at capacity — explicit backpressure, nothing dropped",
					"instance", in.cfg.ID, "request_id", reqID, "event_key", eventKey)
			}
			in.fail(w, http.StatusServiceUnavailable, msg)
			audit("outbox_unavailable", http.StatusServiceUnavailable, eventKey, len(body))
			return
		}
		in.wakeOutbox()
	}

	in.metrics.accepted.Add(1)
	audit("accepted", http.StatusAccepted, eventKey, len(body))
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_, _ = w.Write([]byte(`{"accepted":true,"topic":"` + in.eventsTopic() + `","event_key":"` + eventKey + `","request_id":"` + reqID + `"}`))
}

// RunOutbox drives the durable broker sync in the background: pending
// rows are published (the publish mask applied at publish time) whenever
// the broker is connected, and each row is deleted only after the broker
// confirmed — at-least-once, retried on the next tick otherwise. Rows
// whose categories are fully masked are deferred, not dropped: the mask
// is an operational cut and the sync resumes when the admin re-enables
// the category. It returns when ctx is done.
func (in *Instance) RunOutbox(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			in.drainOutbox(ctx)
		case <-in.wake:
			in.drainOutbox(ctx)
		}
	}
}

// drainOutbox performs one bounded pass over the pending rows: a
// disconnected broker, a failed publish or a failed acknowledgement stops
// the pass — every unfinished row stays pending and is retried on the
// next tick.
func (in *Instance) drainOutbox(ctx context.Context) {
	if in.outbox == nil || in.client == nil || !in.client.IsConnected() {
		return // paho's background reconnect flips this
	}
	items, err := in.outbox.PendingOutbox(ctx, in.cfg.ID, 32)
	if err != nil {
		in.logger.Warn("ingest_http: outbox poll failed", "instance", in.cfg.ID, "error", err)
		return
	}
	for _, it := range items {
		eventsAllowed := mqttpolicy.Allowed(mqttpolicy.CatEvents)
		activeAllowed := mqttpolicy.Allowed(mqttpolicy.CatActive)
		if !eventsAllowed && !activeAllowed {
			// Fully masked: the row waits for the mask instead of being
			// dropped silently.
			continue
		}
		if eventsAllowed {
			token := in.client.Publish(it.Topic, 1, false, it.Payload)
			if !token.WaitTimeout(publishTimeout) {
				in.logger.Warn("ingest_http: outbox publish timed out", "instance", in.cfg.ID, "row", it.ID)
				return
			}
			if err := token.Error(); err != nil {
				in.logger.Warn("ingest_http: outbox publish failed", "instance", in.cfg.ID, "row", it.ID, "error", err)
				return
			}
		}
		if activeAllowed {
			if err := in.publishActive(it.Payload); err != nil {
				in.logger.Warn("ingest_http: outbox active publish failed",
					"instance", in.cfg.ID, "row", it.ID, "error", err)
				return
			}
		}
		if err := in.outbox.AckOutbox(ctx, it.ID); err != nil {
			in.logger.Warn("ingest_http: outbox ack failed", "instance", in.cfg.ID, "row", it.ID, "error", err)
			return
		}
	}
}

// wakeOutbox nudges the outbox worker immediately after an append so a
// connected broker receives the request without waiting for the tick.
func (in *Instance) wakeOutbox() {
	if in.wake == nil {
		return
	}
	select {
	case in.wake <- struct{}{}:
	default:
	}
}

// publishActive mirrors one accepted transition into the retained active
// view (<prefix>/active/<source>/<sha256(event_key)>): a document for
// new/updated transitions, an empty retained payload (delete) for
// cancelled/expired ones. The /events journal alone feeds only the
// routing engine, never the public state mirror.
func (in *Instance) publishActive(payload []byte) error {
	we, err := mqttreceiver.ParseEventPayload(payload)
	if err != nil {
		return err // impossible after buildPayload; defensive only
	}
	topic := in.cfg.TopicPrefix + "/active/" + we.Event.Source + "/" +
		mqttreceiver.TopicHash(we.EventKey)

	var retained []byte
	switch we.ChangeType {
	case mqttreceiver.ChangeNew, mqttreceiver.ChangeUpdated:
		doc, err := json.Marshal(mqttreceiver.ActivePayload{
			SchemaVersion: mqttreceiver.WireSchemaVersion,
			Type:          mqttreceiver.TypeActiveHazard,
			EventKey:      we.EventKey,
			Event:         we.Event,
		})
		if err != nil {
			return err
		}
		retained = doc
	case mqttreceiver.ChangeCancelled, mqttreceiver.ChangeExpired:
		retained = nil // retained delete
	default:
		return nil
	}

	token := in.client.Publish(topic, 1, true, retained)
	if !token.WaitTimeout(publishTimeout) {
		return fmt.Errorf("active view publish timed out")
	}
	return token.Error()
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
			Status:      builderEventStatus(b.Transition),
			SourceURL:   strings.TrimSpace(b.SourceURL),
			ReceivedAt:  now,
			UpdatedAt:   now,
		},
	}

	// Persistent identity + versioning (P1): builder events carry the
	// instance UUID and a strictly increasing version, so the routing
	// engine records every transition in the lifecycle ledger and the
	// pre-transmission gate blocks jobs a later update or cancellation
	// supersedes — previously builder events were publisher-less and
	// version-less, and lifecycle skipped them entirely (an old alert
	// stayed deliverable after its cancellation).
	if in.publisherID != "" {
		we.Publisher = in.publisherID
		we.ChangeID = in.nextChangeID()
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
	if !severity.Valid(sev) {
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

// builderEventStatus maps the canonical transition onto the wire event
// status, so the outbox retention can trust event.status again — the
// simplified form previously wrote "active" even for cancellations and
// retention read exactly that field (reported P1).
func builderEventStatus(transition string) string {
	switch transition {
	case mqttreceiver.ChangeCancelled:
		return "cancelled"
	case mqttreceiver.ChangeExpired:
		return "expired"
	default:
		return "active"
	}
}
