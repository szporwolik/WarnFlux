package ingesthttp

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/szporwolik/WarnFlux/internal/config"
	"github.com/szporwolik/WarnFlux/internal/mqttreceiver"
)

type fakeToken struct {
	err     error
	timeout bool
}

func (t *fakeToken) Wait() bool                     { return true }
func (t *fakeToken) WaitTimeout(time.Duration) bool { return !t.timeout }
func (t *fakeToken) Done() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}
func (t *fakeToken) Error() error { return t.err }

type fakePublish struct {
	topic    string
	qos      byte
	retained bool
	payload  string
}

type fakePublisher struct {
	mu        sync.Mutex
	connected bool
	pubErr    error
	timeout   bool
	published []fakePublish
}

func (f *fakePublisher) Connect() mqtt.Token {
	f.mu.Lock()
	f.connected = true
	f.mu.Unlock()
	return &fakeToken{}
}
func (f *fakePublisher) Disconnect(uint) {}
func (f *fakePublisher) Publish(topic string, qos byte, retained bool, payload any) mqtt.Token {
	f.mu.Lock()
	defer f.mu.Unlock()
	data, _ := payload.([]byte)
	f.published = append(f.published, fakePublish{topic: topic, qos: qos, retained: retained, payload: string(data)})
	return &fakeToken{err: f.pubErr, timeout: f.timeout}
}
func (f *fakePublisher) IsConnected() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.connected
}

func testInstance(t *testing.T, pub *fakePublisher) *Instance {
	t.Helper()
	return &Instance{
		cfg: config.IngestHTTP{
			ID:          "news",
			APIKey:      "test-key-1234567890abcdef",
			TopicPrefix: "warnflux",
		},
		client: pub,
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func post(t *testing.T, inst *Instance, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/ingest/news", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer test-key-1234567890abcdef")
	rec := httptest.NewRecorder()
	inst.ServeHTTP(rec, req)
	return rec
}

func lastPublish(t *testing.T, pub *fakePublisher) fakePublish {
	t.Helper()
	pub.mu.Lock()
	defer pub.mu.Unlock()
	if len(pub.published) == 0 {
		t.Fatal("no publish recorded")
	}
	return pub.published[len(pub.published)-1]
}

func TestAuthRejectsMissingOrWrongKey(t *testing.T) {
	pub := &fakePublisher{connected: true}
	inst := testInstance(t, pub)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/ingest/news", strings.NewReader(`{"severity":"severe"}`))
	rec := httptest.NewRecorder()
	inst.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("missing key = %d, want 401", rec.Code)
	}

	req = httptest.NewRequest(http.MethodPost, "/api/v1/ingest/news", strings.NewReader(`{"severity":"severe"}`))
	req.Header.Set("Authorization", "Bearer wrong-key")
	rec = httptest.NewRecorder()
	inst.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("wrong key = %d, want 401", rec.Code)
	}
	if pub.published != nil {
		t.Fatalf("unauthorized request published: %+v", pub.published)
	}
}

func TestMethodNotAllowed(t *testing.T) {
	inst := testInstance(t, &fakePublisher{connected: true})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/ingest/news", nil)
	req.Header.Set("Authorization", "Bearer test-key-1234567890abcdef")
	rec := httptest.NewRecorder()
	inst.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET = %d, want 405", rec.Code)
	}
}

func TestBuilderModePublishesCanonicalWireEvent(t *testing.T) {
	pub := &fakePublisher{connected: true}
	inst := testInstance(t, pub)

	rec := post(t, inst, `{"severity":"severe","headline":"Pożar w lesie","event":"Pożar","areas":["gmina Niepołomice"]}`)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("builder post = %d, want 202 (body %s)", rec.Code, rec.Body.String())
	}

	p := lastPublish(t, pub)
	if p.topic != "warnflux/events" || p.qos != 1 || p.retained {
		t.Errorf("publish = %+v, want warnflux/events qos 1 non-retained", p)
	}
	var we mqttreceiver.EventPayload
	if err := json.Unmarshal([]byte(p.payload), &we); err != nil {
		t.Fatalf("published payload is not valid wire JSON: %v", err)
	}
	if we.SchemaVersion != mqttreceiver.WireSchemaVersion || we.ChangeType != mqttreceiver.ChangeNew {
		t.Errorf("envelope = schema %d type %q, want 1/new", we.SchemaVersion, we.ChangeType)
	}
	if we.Event.Source != "news" || !strings.HasPrefix(we.EventKey, "news:") {
		t.Errorf("identity = source %q key %q, want source news, key news:...", we.Event.Source, we.EventKey)
	}
	if we.Event.Severity != "severe" || we.Event.Headline != "Pożar w lesie" || we.Event.Event != "Pożar" {
		t.Errorf("event fields = %+v", we.Event)
	}
	if we.Event.Status != "active" || we.Event.ReceivedAt == "" || we.Event.UpdatedAt == "" {
		t.Errorf("status/timestamps = %q/%q/%q", we.Event.Status, we.Event.ReceivedAt, we.Event.UpdatedAt)
	}
}

func TestBuilderDeduplicatesIdenticalPosts(t *testing.T) {
	pub := &fakePublisher{connected: true}
	inst := testInstance(t, pub)
	body := `{"severity":"moderate","headline":"Zalana droga","source_id":"scraper-7"}`

	if rec := post(t, inst, body); rec.Code != http.StatusAccepted {
		t.Fatalf("first post = %d", rec.Code)
	}
	first := lastPublish(t, pub)
	if rec := post(t, inst, body); rec.Code != http.StatusAccepted {
		t.Fatalf("second post = %d", rec.Code)
	}
	second := lastPublish(t, pub)

	var w1, w2 mqttreceiver.EventPayload
	_ = json.Unmarshal([]byte(first.payload), &w1)
	_ = json.Unmarshal([]byte(second.payload), &w2)
	if w1.EventKey != "news:scraper-7" || w2.EventKey != w1.EventKey {
		t.Errorf("event keys = %q / %q, want stable news:scraper-7", w1.EventKey, w2.EventKey)
	}

	// A changed body without a source_id gets a fresh (hashed) key.
	if rec := post(t, inst, `{"severity":"moderate","headline":"Zalana droga 2"}`); rec.Code != http.StatusAccepted {
		t.Fatalf("third post = %d", rec.Code)
	}
	var w3 mqttreceiver.EventPayload
	_ = json.Unmarshal([]byte(lastPublish(t, pub).payload), &w3)
	if !strings.HasPrefix(w3.EventKey, "news:") || w3.EventKey == w1.EventKey {
		t.Errorf("hashed event key = %q, want news:<hash> different from %q", w3.EventKey, w1.EventKey)
	}
}

func TestBuilderValidationErrors(t *testing.T) {
	pub := &fakePublisher{connected: true}
	inst := testInstance(t, pub)
	cases := []struct {
		name string
		body string
		want string
	}{
		{"missing severity", `{"headline":"x"}`, "severity"},
		{"invalid severity", `{"severity":"orange","headline":"x"}`, "severity"},
		{"missing event and headline", `{"severity":"severe"}`, "event or headline"},
		{"bad transition", `{"severity":"severe","headline":"x","transition":"bogus"}`, "transition"},
		{"bad json", `{not json`, "JSON"},
	}
	for _, c := range cases {
		rec := post(t, inst, c.body)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("%s: status = %d, want 400", c.name, rec.Code)
			continue
		}
		if !strings.Contains(rec.Body.String(), c.want) {
			t.Errorf("%s: body = %s, want mention of %q", c.name, rec.Body.String(), c.want)
		}
	}
	if pub.published != nil {
		t.Errorf("invalid posts published: %+v", pub.published)
	}
}

func TestWireModePassThroughNormalized(t *testing.T) {
	pub := &fakePublisher{connected: true}
	inst := testInstance(t, pub)

	wire := `{"schema_version":1,"change_id":42,"change_type":"updated","event_key":"imgw-meteo:123","extra_junk":"dropped","event":{"source":"imgw-meteo","source_id":"123","event":"Wiatr","severity":"severe","status":"active","received_at":"2026-09-22T10:00:00Z","updated_at":"2026-09-22T10:05:00Z"}}`
	rec := post(t, inst, wire)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("wire post = %d, want 202 (body %s)", rec.Code, rec.Body.String())
	}

	p := lastPublish(t, pub)
	if p.topic != "warnflux/events" {
		t.Errorf("topic = %q, want warnflux/events", p.topic)
	}
	if strings.Contains(p.payload, "extra_junk") {
		t.Errorf("unknown fields must be dropped: %s", p.payload)
	}
	var we mqttreceiver.EventPayload
	if err := json.Unmarshal([]byte(p.payload), &we); err != nil {
		t.Fatalf("published payload invalid: %v", err)
	}
	if we.EventKey != "imgw-meteo:123" || we.ChangeID != 42 || we.ChangeType != "updated" || we.Event.Source != "imgw-meteo" {
		t.Errorf("wire event = %+v", we)
	}
}

func TestWireModeRejectsInvalid(t *testing.T) {
	pub := &fakePublisher{connected: true}
	inst := testInstance(t, pub)
	cases := []string{
		`{"schema_version":9,"change_type":"new","event_key":"a:1","event":{}}`,
		`{"schema_version":1,"change_type":"bogus","event_key":"a:1","event":{}}`,
		`{"schema_version":1,"change_type":"new","event":{}}`,
	}
	for _, body := range cases {
		rec := post(t, inst, body)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("wire post %s = %d, want 400", body, rec.Code)
		}
	}
	if pub.published != nil {
		t.Errorf("invalid wire posts published: %+v", pub.published)
	}
}

func TestBrokerUnavailable(t *testing.T) {
	pub := &fakePublisher{connected: false}
	inst := testInstance(t, pub)
	if rec := post(t, inst, `{"severity":"severe","headline":"x"}`); rec.Code != http.StatusServiceUnavailable {
		t.Errorf("disconnected broker = %d, want 503", rec.Code)
	}
}

func TestPublishFailure(t *testing.T) {
	pub := &fakePublisher{connected: true, pubErr: io.ErrUnexpectedEOF}
	inst := testInstance(t, pub)
	if rec := post(t, inst, `{"severity":"severe","headline":"x"}`); rec.Code != http.StatusServiceUnavailable {
		t.Errorf("publish error = %d, want 503", rec.Code)
	}

	pub = &fakePublisher{connected: true, timeout: true}
	inst = testInstance(t, pub)
	if rec := post(t, inst, `{"severity":"severe","headline":"x"}`); rec.Code != http.StatusServiceUnavailable {
		t.Errorf("publish timeout = %d, want 503", rec.Code)
	}
}

func TestOversizedBody(t *testing.T) {
	pub := &fakePublisher{connected: true}
	inst := testInstance(t, pub)
	big := `{"severity":"severe","headline":"` + strings.Repeat("x", mqttreceiver.MaxPayload) + `"}`
	rec := post(t, inst, big)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized = %d, want 413", rec.Code)
	}
}

// TestResolve pins the broker inheritance: empty instance settings come
// from the primary mqtt output; explicit values always win.
func TestResolve(t *testing.T) {
	fallback := config.IngestHTTP{
		Broker:       "tcp://main:1883",
		ClientID:     "warnflux-main-out",
		Username:     "main-user",
		Password:     "main-pass",
		TopicPrefix:  "warnflux",
		PasswordFile: "",
	}

	got := Resolve(config.IngestHTTP{ID: "news"}, fallback)
	if got.Broker != fallback.Broker || got.Username != fallback.Username ||
		got.Password != fallback.Password || got.TopicPrefix != fallback.TopicPrefix {
		t.Errorf("inherited = %+v, want fallback values", got)
	}
	if got.ClientID != "warnflux-ingest-news" {
		t.Errorf("client_id = %q, want default warnflux-ingest-news", got.ClientID)
	}

	override := config.IngestHTTP{
		ID:          "news",
		Broker:      "tcp://other:1883",
		TopicPrefix: "e2etest",
	}
	got = Resolve(override, fallback)
	if got.Broker != "tcp://other:1883" || got.TopicPrefix != "e2etest" {
		t.Errorf("overrides lost: %+v", got)
	}
	if got.Username != fallback.Username || got.Password != fallback.Password {
		t.Errorf("credentials should still inherit: %+v", got)
	}
}

// TestKeyRotation pins the two-key window: the current and the previous
// key both authenticate; anything else fails and counts as auth_failed.
func TestKeyRotation(t *testing.T) {
	pub := &fakePublisher{connected: true}
	inst := testInstance(t, pub)
	inst.previousKey = "previous-key-1234567890abc"

	for _, key := range []string{"test-key-1234567890abcdef", "previous-key-1234567890abc"} {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/ingest/news", strings.NewReader(`{"severity":"severe","headline":"x"}`))
		req.Header.Set("Authorization", "Bearer "+key)
		rec := httptest.NewRecorder()
		inst.ServeHTTP(rec, req)
		if rec.Code != http.StatusAccepted {
			t.Errorf("key %q = %d, want 202", key, rec.Code)
		}
	}

	req := httptest.NewRequest(http.MethodPost, "/api/v1/ingest/news", strings.NewReader(`{"severity":"severe","headline":"x"}`))
	req.Header.Set("Authorization", "Bearer neither-of-them")
	rec := httptest.NewRecorder()
	inst.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("unknown key = %d, want 401", rec.Code)
	}
	if c := inst.Counters(); c.AuthFailed != 1 || c.Accepted != 2 {
		t.Errorf("counters = %+v, want accepted=2 auth_failed=1", c)
	}
}

// TestAllowedCIDRs pins the optional source allowlist: matching sources
// pass, others get 403 with the forbidden counter.
func TestAllowedCIDRs(t *testing.T) {
	pub := &fakePublisher{connected: true}
	inst := testInstance(t, pub)
	inst.cfg.AllowedCIDRs = []string{"192.0.2.0/24", "2001:db8::1"}
	inst.ensureInit()

	allowed := httptest.NewRequest(http.MethodPost, "/api/v1/ingest/news", strings.NewReader(`{"severity":"severe","headline":"x"}`))
	allowed.RemoteAddr = "192.0.2.7:5555"
	allowed.Header.Set("Authorization", "Bearer test-key-1234567890abcdef")
	rec := httptest.NewRecorder()
	inst.ServeHTTP(rec, allowed)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("allowed source = %d, want 202", rec.Code)
	}

	blocked := httptest.NewRequest(http.MethodPost, "/api/v1/ingest/news", strings.NewReader(`{"severity":"severe","headline":"x"}`))
	blocked.RemoteAddr = "198.51.100.9:5555"
	blocked.Header.Set("Authorization", "Bearer test-key-1234567890abcdef")
	rec = httptest.NewRecorder()
	inst.ServeHTTP(rec, blocked)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("blocked source = %d, want 403", rec.Code)
	}
	if c := inst.Counters(); c.Forbidden != 1 {
		t.Errorf("counters = %+v, want forbidden=1", c)
	}
}

// TestRateLimit pins the token bucket: the third request inside the
// window is rejected with 429 + Retry-After and counted.
func TestRateLimit(t *testing.T) {
	pub := &fakePublisher{connected: true}
	inst := testInstance(t, pub)
	inst.cfg.RateLimitPerMinute = 2
	inst.ensureInit()

	for i := 0; i < 2; i++ {
		if rec := post(t, inst, `{"severity":"severe","headline":"x"}`); rec.Code != http.StatusAccepted {
			t.Fatalf("request %d = %d, want 202", i+1, rec.Code)
		}
	}
	rec := post(t, inst, `{"severity":"severe","headline":"x"}`)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("third request = %d, want 429", rec.Code)
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Error("429 response missing Retry-After header")
	}
	if c := inst.Counters(); c.Accepted != 2 || c.RateLimited != 1 {
		t.Errorf("counters = %+v, want accepted=2 rate_limited=1", c)
	}
}

// TestRateLimitUnlimited pins the negative-rate escape hatch.
func TestRateLimitUnlimited(t *testing.T) {
	pub := &fakePublisher{connected: true}
	inst := testInstance(t, pub)
	inst.cfg.RateLimitPerMinute = -1
	inst.ensureInit()
	for i := 0; i < 200; i++ {
		if rec := post(t, inst, `{"severity":"severe","headline":"x"}`); rec.Code != http.StatusAccepted {
			t.Fatalf("request %d = %d, want 202 with limiting disabled", i+1, rec.Code)
		}
	}
}

// TestRequestID pins the audit/response request id: generated when absent,
// echoed when the client supplies one.
func TestRequestID(t *testing.T) {
	pub := &fakePublisher{connected: true}
	inst := testInstance(t, pub)

	rec := post(t, inst, `{"severity":"severe","headline":"x"}`)
	if rec.Header().Get("X-Request-ID") == "" {
		t.Error("response missing generated X-Request-ID")
	}
	if !strings.Contains(rec.Body.String(), "request_id") {
		t.Errorf("accepted body missing request id: %s", rec.Body.String())
	}

	req := httptest.NewRequest(http.MethodPost, "/api/v1/ingest/news", strings.NewReader(`{"severity":"severe","headline":"x"}`))
	req.Header.Set("Authorization", "Bearer test-key-1234567890abcdef")
	req.Header.Set("X-Request-ID", "scraper-run-42")
	rec = httptest.NewRecorder()
	inst.ServeHTTP(rec, req)
	if got := rec.Header().Get("X-Request-ID"); got != "scraper-run-42" {
		t.Errorf("X-Request-ID = %q, want echoed client value", got)
	}
}

// TestInvalidCIDRRejectedAtNew pins the fail-at-startup contract.
func TestInvalidCIDRRejectedAtNew(t *testing.T) {
	_, err := New(config.IngestHTTP{
		ID: "news", APIKey: "supersecret-key-123",
		AllowedCIDRs: []string{"not-a-cidr"},
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err == nil || !strings.Contains(err.Error(), "not-a-cidr") {
		t.Fatalf("New with bad CIDR = %v, want rejection", err)
	}
}

// TestPreviousKeyFile pins the file-based rotation key.
func TestPreviousKeyFile(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/prev.key"
	if err := os.WriteFile(path, []byte("previous-key-1234567890abc\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	inst, err := New(config.IngestHTTP{
		ID: "news", APIKey: "current-key-1234567890ab",
		PreviousKeyFile: path,
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	inst.client = &fakePublisher{connected: true}
	inst.started.Store(true)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/ingest/news", strings.NewReader(`{"severity":"severe","headline":"x"}`))
	req.Header.Set("Authorization", "Bearer previous-key-1234567890abc")
	rec := httptest.NewRecorder()
	inst.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("previous key from file = %d, want 202", rec.Code)
	}
}
