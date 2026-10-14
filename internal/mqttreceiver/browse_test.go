package mqttreceiver

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/szporwolik/WarnFlux/internal/config"
)

// testToken is a paho token fake that always succeeds.
type testToken struct{}

func (t *testToken) Wait() bool                       { return true }
func (t *testToken) WaitTimeout(_ time.Duration) bool { return true }
func (t *testToken) Error() error                     { return nil }
func (t *testToken) Done() <-chan struct{}            { ch := make(chan struct{}); close(ch); return ch }

// browseClient is a minimal mqttClient fake for Browse tests.
type browseClient struct {
	mu        sync.Mutex
	subTopic  string
	handler   mqtt.MessageHandler
	unsubbed  bool
	connected bool
	// connects/disconnects count the lifecycle calls, so a test can prove
	// which client was used.
	connects    int
	disconnects int
	clientID    string
	cleanSess   bool
	// subscribeTimeout makes Subscribe report a broker that never acks.
	subscribeTimeout bool
	// connectTimeout makes Connect report a broker that never answers.
	connectTimeout bool
}

// timeoutToken is a paho token fake that never completes.
type timeoutToken struct{}

func (t *timeoutToken) Wait() bool                     { return false }
func (t *timeoutToken) WaitTimeout(time.Duration) bool { return false }
func (t *timeoutToken) Error() error                   { return nil }
func (t *timeoutToken) Done() <-chan struct{}          { return make(chan struct{}) }

func newBrowseClient() *browseClient { return &browseClient{connected: true} }

func (c *browseClient) Connect() mqtt.Token {
	c.mu.Lock()
	c.connects++
	stalled := c.connectTimeout
	c.mu.Unlock()
	if stalled {
		return &timeoutToken{}
	}
	return &testToken{}
}
func (c *browseClient) Disconnect(uint) {
	c.mu.Lock()
	c.disconnects++
	c.mu.Unlock()
}
func (c *browseClient) Subscribe(topic string, _ byte, cb mqtt.MessageHandler) mqtt.Token {
	if c.subscribeTimeout {
		return &timeoutToken{}
	}
	c.mu.Lock()
	c.subTopic = topic
	c.handler = cb
	c.unsubbed = false
	c.mu.Unlock()
	return &testToken{}
}
func (c *browseClient) Unsubscribe(...string) mqtt.Token {
	c.mu.Lock()
	c.unsubbed = true
	c.mu.Unlock()
	return &testToken{}
}
func (c *browseClient) Publish(string, byte, bool, interface{}) mqtt.Token { return &testToken{} }
func (c *browseClient) IsConnected() bool                                  { return c.connected }

// connectionAttempts reports how many connects this client saw.
func (c *browseClient) connectionAttempts() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.connects
}

// deliver simulates the broker pushing one message to the current handler.
func (c *browseClient) deliver(msg mqtt.Message) {
	c.mu.Lock()
	h := c.handler
	c.mu.Unlock()
	if h != nil {
		h(nil, msg)
	}
}

// newBrowseReceiverPair builds one receiver with TWO distinct clients: the
// receiver's own (prod) and the throwaway one a browse must use (browse).
// They must never be the same object — that was the reported P1.
func newBrowseReceiverPair(prod, browse *browseClient) *Receiver {
	r := &Receiver{
		cfg:    config.Receiver{ID: "t", Enabled: true, ClientID: "warnflux-t"},
		client: prod,
		logger: testLogger(),
	}
	r.newBrowseClient = func(o *mqtt.ClientOptions) mqttClient {
		browse.mu.Lock()
		browse.clientID = o.ClientID
		browse.cleanSess = o.CleanSession
		browse.mu.Unlock()
		return browse
	}
	return r
}

func newBrowseReceiver(c mqttClient) *Receiver {
	prod, _ := c.(*browseClient)
	if prod == nil {
		prod = newBrowseClient()
	}
	return newBrowseReceiverPair(prod, newBrowseClient())
}

func TestReceiverBrowseCollectsMessages(t *testing.T) {
	prod, probe := newBrowseClient(), newBrowseClient()
	r := newBrowseReceiverPair(prod, probe)

	done := make(chan []BrowseEntry, 1)
	errs := make(chan error, 1)
	go func() {
		entries, err := r.Browse(context.Background(), "aprs/#", 200*time.Millisecond, 10)
		if err != nil {
			errs <- err
			return
		}
		done <- entries
	}()

	time.Sleep(20 * time.Millisecond)
	probe.deliver(&testMessage{topic: "aprs/stations/SP9XYZ-7", payload: []byte("{}"), retained: true})
	probe.deliver(&testMessage{topic: "aprs/packets", payload: []byte("hello")})

	select {
	case err := <-errs:
		t.Fatalf("Browse failed: %v", err)
	case entries := <-done:
		if len(entries) != 2 {
			t.Fatalf("got %d entries, want 2", len(entries))
		}
		if entries[0].Topic != "aprs/stations/SP9XYZ-7" || !entries[0].Retained {
			t.Errorf("first entry = %+v, want retained station message", entries[0])
		}
		if entries[0].Size != 2 || entries[0].Payload != "{}" {
			t.Errorf("first entry size/payload = %d/%q, want 2/{}", entries[0].Size, entries[0].Payload)
		}
		if entries[1].Topic != "aprs/packets" || entries[1].Retained {
			t.Errorf("second entry = %+v, want non-retained packet message", entries[1])
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Browse did not return in time")
	}
	probe.mu.Lock()
	defer probe.mu.Unlock()
	if !probe.unsubbed {
		t.Error("temporary subscription was not removed")
	}
	if probe.disconnects != 1 {
		t.Errorf("browse client disconnects = %d, want 1", probe.disconnects)
	}
	if probe.clientID != "warnflux-t-browse" || !probe.cleanSess {
		t.Errorf("browse client id/session = %q/%v, want the throwaway id and a clean session",
			probe.clientID, probe.cleanSess)
	}
	// The receiver's own client was never used for the browse: its
	// unsubscribe is what used to kill a production subscription.
	prod.mu.Lock()
	defer prod.mu.Unlock()
	if prod.connects != 0 || prod.unsubbed || prod.subTopic != "" {
		t.Errorf("the receiver's client was touched: connects=%d unsub=%v topic=%q",
			prod.connects, prod.unsubbed, prod.subTopic)
	}
}

func TestReceiverBrowseTruncatesPayload(t *testing.T) {
	prod, probe := newBrowseClient(), newBrowseClient()
	r := newBrowseReceiverPair(prod, probe)

	go func() {
		_, _ = r.Browse(context.Background(), "#", 100*time.Millisecond, 10)
	}()
	time.Sleep(20 * time.Millisecond)
	big := strings.Repeat("x", browsePayloadLimit+500)
	probe.deliver(&testMessage{topic: "big", payload: []byte(big)})
	time.Sleep(150 * time.Millisecond)

	// Deliver after unsubscribe to keep it simple: assert via a second
	// browse instead — run one more and inspect the payload through the
	// returned entries.
	entries := make(chan []BrowseEntry, 1)
	go func() {
		got, _ := r.Browse(context.Background(), "#", 50*time.Millisecond, 10)
		entries <- got
	}()
	time.Sleep(10 * time.Millisecond)
	probe.deliver(&testMessage{topic: "big", payload: []byte(big)})
	got := <-entries
	if len(got) != 1 {
		t.Fatalf("got %d entries, want 1", len(got))
	}
	if got[0].Size != len(big) {
		t.Errorf("size = %d, want %d (full size reported)", got[0].Size, len(big))
	}
	if len(got[0].Payload) != browsePayloadLimit+len("…") {
		t.Errorf("payload len = %d, want truncated to %d", len(got[0].Payload), browsePayloadLimit+len("…"))
	}
}

func TestReceiverBrowseCapsEntries(t *testing.T) {
	prod, probe := newBrowseClient(), newBrowseClient()
	r := newBrowseReceiverPair(prod, probe)

	go func() {
		_, _ = r.Browse(context.Background(), "#", 150*time.Millisecond, 3)
	}()
	time.Sleep(20 * time.Millisecond)
	for i := 0; i < 10; i++ {
		probe.deliver(&testMessage{topic: "t", payload: []byte("p")})
	}
	time.Sleep(200 * time.Millisecond)

	entries := make(chan []BrowseEntry, 1)
	go func() {
		got, _ := r.Browse(context.Background(), "#", 50*time.Millisecond, 3)
		entries <- got
	}()
	time.Sleep(10 * time.Millisecond)
	for i := 0; i < 10; i++ {
		probe.deliver(&testMessage{topic: "t", payload: []byte("p")})
	}
	got := <-entries
	if len(got) != 3 {
		t.Fatalf("got %d entries, want 3 (capped)", len(got))
	}
}

func TestReceiverBrowseErrors(t *testing.T) {
	c := newBrowseClient()
	r := newBrowseReceiver(c)

	if _, err := r.Browse(context.Background(), "   ", time.Second, 10); err == nil {
		t.Error("empty topic should fail")
	}
	c.connected = false
	if _, err := r.Browse(context.Background(), "#", time.Second, 10); err == nil {
		t.Error("disconnected receiver should fail")
	}
}

func TestManagerBrowseFallback(t *testing.T) {
	off := newBrowseClient()
	off.connected = false
	on, onProbe := newBrowseClient(), newBrowseClient()
	m := &Manager{
		receivers: []*Receiver{
			newBrowseReceiver(off),
			newBrowseReceiverPair(on, onProbe),
		},
		logger: testLogger(),
	}

	done := make(chan []BrowseEntry, 1)
	go func() {
		got, _ := m.Browse(context.Background(), "", "a/b", 100*time.Millisecond, 5)
		done <- got
	}()
	time.Sleep(20 * time.Millisecond)
	onProbe.deliver(&testMessage{topic: "a/b", payload: []byte("v"), retained: true})
	got := <-done
	if len(got) != 1 || got[0].Topic != "a/b" {
		t.Fatalf("fallback browse entries = %+v", got)
	}

	if _, err := m.Browse(context.Background(), "missing", "a/b", time.Second, 5); err == nil {
		t.Error("unknown receiver should fail")
	}
}

// assertProbeDropped pins that a browse always disposed of its throwaway
// client and removed its temporary subscription.
func assertProbeDropped(t *testing.T, probe *browseClient) {
	t.Helper()
	probe.mu.Lock()
	defer probe.mu.Unlock()
	if !probe.unsubbed {
		t.Error("the temporary browse subscription was not removed")
	}
	if probe.disconnects != 1 {
		t.Errorf("browse client disconnects = %d, want 1 (the session must not outlive the browse)", probe.disconnects)
	}
}

// assertProdUntouched pins the reported P1: a browse must never use the
// receiver's own client, because unsubscribing on it tore a production
// subscription down.
func assertProdUntouched(t *testing.T, prod *browseClient) {
	t.Helper()
	prod.mu.Lock()
	defer prod.mu.Unlock()
	if prod.connects != 0 || prod.unsubbed || prod.subTopic != "" {
		t.Errorf("the receiver's client was touched: connects=%d unsub=%v topic=%q",
			prod.connects, prod.unsubbed, prod.subTopic)
	}
}

// TestReceiverBrowseCleansUpOnCancelAndTimeout pins the acceptance check of
// the reported P1 for the two endings that used to be the dangerous ones: a
// browse the operator cancels mid-window and a subscribe that times out.
// Neither may touch the receiver's own client, and both must leave the
// throwaway client disconnected.
func TestReceiverBrowseCleansUpOnCancelAndTimeout(t *testing.T) {
	// Cancelled while the collection window is still open.
	prod, probe := newBrowseClient(), newBrowseClient()
	r := newBrowseReceiverPair(prod, probe)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := r.Browse(ctx, "aprs/#", 5*time.Second, 10)
		done <- err
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("cancelled browse = %v, want no error", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a cancelled browse did not return")
	}
	assertProbeDropped(t, probe)
	assertProdUntouched(t, prod)

	// The broker never acknowledges the temporary subscription.
	prodSlow, probeSlow := newBrowseClient(), newBrowseClient()
	probeSlow.subscribeTimeout = true
	rSlow := newBrowseReceiverPair(prodSlow, probeSlow)
	if _, err := rSlow.Browse(context.Background(), "aprs/#", time.Second, 10); err == nil {
		t.Fatal("a subscribe timeout must be reported to the caller")
	}
	assertProbeDropped(t, probeSlow)
	assertProdUntouched(t, prodSlow)
}

// browseFactoryReceiver builds a receiver whose throwaway clients are all
// fake and recorded, so a test can count how many browses ever connected.
func browseFactoryReceiver(t *testing.T) (*Receiver, func() []*browseClient) {
	t.Helper()
	r := &Receiver{
		cfg:    config.Receiver{ID: "t", Enabled: true, ClientID: "warnflux-t"},
		client: newBrowseClient(),
		logger: testLogger(),
	}
	var mu sync.Mutex
	var probes []*browseClient
	r.newBrowseClient = func(*mqtt.ClientOptions) mqttClient {
		c := newBrowseClient()
		mu.Lock()
		probes = append(probes, c)
		mu.Unlock()
		return c
	}
	return r, func() []*browseClient {
		mu.Lock()
		defer mu.Unlock()
		return append([]*browseClient(nil), probes...)
	}
}

// waitForCond polls cond until it holds or the timeout expires.
func waitForCond(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// TestQueuedBrowseLeavesTheQueueOnCancel pins the reported P2: a browse that
// waits for the single browse slot must leave the queue when its request is
// cancelled, and must never connect after it was abandoned. A series of
// closed previews used to queue up and connect one by one, loading the broker
// and delaying the browses behind them.
func TestQueuedBrowseLeavesTheQueueOnCancel(t *testing.T) {
	r, probes := browseFactoryReceiver(t)

	// One browse holds the slot for its collection window.
	holding := make(chan error, 1)
	go func() {
		_, err := r.Browse(context.Background(), "aprs/#", 600*time.Millisecond, 10)
		holding <- err
	}()
	waitForCond(t, 2*time.Second, "the holding browse to connect", func() bool {
		got := probes()
		return len(got) == 1 && got[0].connectionAttempts() == 1
	})

	// A second request queues behind it and is cancelled while it waits.
	ctx, cancel := context.WithCancel(context.Background())
	queued := make(chan error, 1)
	go func() {
		_, err := r.Browse(ctx, "aprs/#", time.Second, 10)
		queued <- err
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-queued:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("the cancelled queued browse = %v, want the cancellation", err)
		}
	case <-time.After(400 * time.Millisecond):
		t.Fatal("the cancelled browse stayed in the queue instead of leaving it")
	}
	if got := probes(); len(got) != 1 {
		t.Fatalf("the abandoned request opened %d browse clients, want none", len(got)-1)
	}

	// The holder finishes on its own; the abandoned request must not have
	// connected behind it.
	if err := <-holding; err != nil {
		t.Fatalf("the holding browse failed: %v", err)
	}
	for i, probe := range probes() {
		if probe.connectionAttempts() != 1 {
			t.Errorf("browse client %d connected %d times, want one (only the holder)", i, probe.connectionAttempts())
		}
	}
}

// TestBrowseConnectHonorsCancellation pins the third part of the reported
// P2: waiting for the broker's answer must not outlive the request. Before
// the fix the connect token was awaited with a bare timeout, so a closed
// preview kept a connection attempt — and a goroutine — until the broker
// answered or the timeout expired.
func TestBrowseConnectHonorsCancellation(t *testing.T) {
	prod, probe := newBrowseClient(), newBrowseClient()
	probe.connectTimeout = true
	r := newBrowseReceiverPair(prod, probe)
	r.cfg.ConnectTimeout = 5 * time.Second

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := r.Browse(ctx, "aprs/#", time.Second, 10)
		done <- err
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("the connect wait = %v, want the cancellation", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a browse whose connect never answers ignored the cancellation")
	}
	probe.mu.Lock()
	disconnects := probe.disconnects
	probe.mu.Unlock()
	if disconnects != 1 {
		t.Errorf("the abandoned connect was disconnected %d times, want 1 (no half-open session)", disconnects)
	}
	assertProdUntouched(t, prod)
}
