package mqtt

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"

	"warnflux/internal/config"
)

// fakeToken is a paho.Token that completes immediately with the given error.
type fakeToken struct {
	done chan struct{}
	err  error
}

func newFakeToken(err error) *fakeToken {
	done := make(chan struct{})
	close(done)
	return &fakeToken{done: done, err: err}
}

// newBlockingToken returns a token that never completes; used to test
// context cancellation.
func newBlockingToken() *fakeToken {
	return &fakeToken{done: make(chan struct{})}
}

func (t *fakeToken) Wait() bool                     { return t.err == nil }
func (t *fakeToken) WaitTimeout(time.Duration) bool { return t.err == nil }
func (t *fakeToken) Done() <-chan struct{}          { return t.done }
func (t *fakeToken) Error() error                   { return t.err }

// published records a single Publish call.
type published struct {
	topic    string
	qos      byte
	retained bool
	payload  []byte
}

// fakeClient implements paho.Client and records published messages.
type fakeClient struct {
	connectToken paho.Token
	publishErr   error
	disconnectMs uint

	mu        sync.Mutex
	published []published
}

func (f *fakeClient) AddRoute(string, paho.MessageHandler) {}
func (f *fakeClient) IsConnected() bool                    { return true }
func (f *fakeClient) IsConnectionOpen() bool               { return true }

func (f *fakeClient) Connect() paho.Token {
	if f.connectToken != nil {
		return f.connectToken
	}
	return newFakeToken(nil)
}

func (f *fakeClient) Disconnect(quiesce uint) { f.disconnectMs = quiesce }

func (f *fakeClient) Publish(topic string, qos byte, retained bool, payload any) paho.Token {
	f.mu.Lock()
	defer f.mu.Unlock()
	data, _ := payload.([]byte)
	f.published = append(f.published, published{topic: topic, qos: qos, retained: retained, payload: data})
	return newFakeToken(f.publishErr)
}

func (f *fakeClient) Subscribe(string, byte, paho.MessageHandler) paho.Token {
	return newFakeToken(nil)
}

func (f *fakeClient) SubscribeMultiple(map[string]byte, paho.MessageHandler) paho.Token {
	return newFakeToken(nil)
}

func (f *fakeClient) Unsubscribe(...string) paho.Token { return newFakeToken(nil) }

func (f *fakeClient) OptionsReader() paho.ClientOptionsReader { return paho.ClientOptionsReader{} }

func (f *fakeClient) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.published)
}

func (f *fakeClient) last() published {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.published[len(f.published)-1]
}

func testLogger() (*slog.Logger, *bytes.Buffer) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	return logger, &buf
}

func testClient(f *fakeClient, prefix string) *Client {
	logger, _ := testLogger()
	return &Client{
		cfg: config.MQTT{
			Enabled:      true,
			Broker:       "tcp://localhost:1883",
			ClientID:     "warnflux",
			TopicPrefix:  prefix,
			QoS:          1,
			PingInterval: 30 * time.Second,
		},
		logger: logger,
		client: f,
	}
}

func TestPublishPing(t *testing.T) {
	f := &fakeClient{}
	c := testClient(f, "warnflux")

	before := time.Now().UTC()
	c.publishPing(context.Background(), "dev")

	if f.count() != 1 {
		t.Fatalf("publish count = %d, want 1", f.count())
	}
	got := f.last()
	if got.topic != "warnflux/status/ping" {
		t.Errorf("topic = %q, want %q", got.topic, "warnflux/status/ping")
	}
	if got.qos != 1 {
		t.Errorf("qos = %d, want 1", got.qos)
	}
	if got.retained {
		t.Error("retained = true, want false")
	}

	var payload struct {
		Type      string    `json:"type"`
		Service   string    `json:"service"`
		Version   string    `json:"version"`
		Timestamp time.Time `json:"timestamp"`
	}
	if err := json.Unmarshal(got.payload, &payload); err != nil {
		t.Fatalf("payload is not valid JSON: %v", err)
	}
	if payload.Type != "ping" || payload.Service != "warnflux" || payload.Version != "dev" {
		t.Errorf("unexpected payload: %+v", payload)
	}
	if payload.Timestamp.IsZero() ||
		payload.Timestamp.Before(before.Add(-2*time.Second)) ||
		payload.Timestamp.After(before.Add(2*time.Second)) {
		t.Errorf("timestamp %s not a plausible current UTC time (before %s)", payload.Timestamp, before)
	}
	if _, offset := payload.Timestamp.Zone(); offset != 0 {
		t.Errorf("timestamp should be UTC, got offset %d", offset)
	}
}

func TestPublishPingFailureIsLogged(t *testing.T) {
	f := &fakeClient{publishErr: errors.New("broker unavailable")}
	logger, buf := testLogger()
	c := &Client{
		cfg:    config.MQTT{TopicPrefix: "warnflux", QoS: 1},
		logger: logger,
		client: f,
	}

	c.publishPing(context.Background(), "dev")

	if f.count() != 1 {
		t.Fatalf("publish count = %d, want 1", f.count())
	}
	if !bytes.Contains(buf.Bytes(), []byte("failed to publish ping")) {
		t.Errorf("log should contain the failure, got: %s", buf.String())
	}
}

func TestPingLoop(t *testing.T) {
	f := &fakeClient{}
	c := testClient(f, "warnflux")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.PingLoop(ctx, 10*time.Millisecond, "dev")
	}()

	// The first ping is immediate; wait until a few ticks have passed.
	deadline := time.Now().Add(500 * time.Millisecond)
	for f.count() < 3 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if f.count() < 3 {
		t.Fatalf("publish count = %d, want at least 3 within 500ms", f.count())
	}

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("PingLoop did not stop after context cancellation")
	}

	// No further pings after cancellation.
	after := f.count()
	time.Sleep(30 * time.Millisecond)
	if f.count() != after {
		t.Errorf("publish count grew after cancellation: %d -> %d", after, f.count())
	}
}

func TestConnect(t *testing.T) {
	f := &fakeClient{}
	c := testClient(f, "warnflux")
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
}

func TestConnectError(t *testing.T) {
	f := &fakeClient{connectToken: newFakeToken(errors.New("connection refused"))}
	c := testClient(f, "warnflux")
	if err := c.Connect(context.Background()); err == nil {
		t.Fatal("Connect should return the broker error, got nil")
	}
}

func TestConnectContextCancelled(t *testing.T) {
	f := &fakeClient{connectToken: newBlockingToken()}
	c := testClient(f, "warnflux")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := c.Connect(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("Connect error = %v, want context.Canceled", err)
	}
}

func TestDisconnect(t *testing.T) {
	f := &fakeClient{}
	c := testClient(f, "warnflux")

	c.Disconnect(250 * time.Millisecond)

	if f.disconnectMs != 250 {
		t.Errorf("disconnect quiesce = %d ms, want 250", f.disconnectMs)
	}
}

func TestNewWiresOptions(t *testing.T) {
	cfg := config.MQTT{
		Enabled:     true,
		Broker:      "tcp://broker.example.com:1883",
		ClientID:    "my-client",
		Username:    "alice",
		Password:    "secret",
		TopicPrefix: "warnflux",
		QoS:         2,
	}

	c := New(cfg, slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)))

	opts := c.client.OptionsReader()
	servers := opts.Servers()
	if len(servers) != 1 || servers[0].String() != cfg.Broker {
		t.Errorf("brokers = %v, want %q", servers, cfg.Broker)
	}
	if opts.ClientID() != "my-client" {
		t.Errorf("client id = %q, want %q", opts.ClientID(), "my-client")
	}
	if opts.Username() != "alice" || opts.Password() != "secret" {
		t.Error("credentials were not applied to the client options")
	}
	if !opts.CleanSession() {
		t.Error("clean session should be enabled")
	}
	if !opts.AutoReconnect() {
		t.Error("auto reconnect should be enabled")
	}
	if opts.ConnectTimeout() <= 0 {
		t.Error("connect timeout should be set")
	}
}

func TestNewWithoutCredentials(t *testing.T) {
	cfg := config.MQTT{Broker: "tcp://localhost:1883"}

	c := New(cfg, slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)))

	opts := c.client.OptionsReader()
	if opts.Username() != "" || opts.Password() != "" {
		t.Error("credentials should be empty when not configured")
	}
}
