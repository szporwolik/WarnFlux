package mqttreceiver

import (
	"context"
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
}

func newBrowseClient() *browseClient { return &browseClient{connected: true} }

func (c *browseClient) Connect() mqtt.Token { return &testToken{} }
func (c *browseClient) Disconnect(uint)     {}
func (c *browseClient) Subscribe(topic string, _ byte, cb mqtt.MessageHandler) mqtt.Token {
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

// deliver simulates the broker pushing one message to the current handler.
func (c *browseClient) deliver(msg mqtt.Message) {
	c.mu.Lock()
	h := c.handler
	c.mu.Unlock()
	if h != nil {
		h(nil, msg)
	}
}

func newBrowseReceiver(c mqttClient) *Receiver {
	return &Receiver{
		cfg:    config.Receiver{ID: "t", Enabled: true},
		client: c,
		logger: testLogger(),
	}
}

func TestReceiverBrowseCollectsMessages(t *testing.T) {
	c := newBrowseClient()
	r := newBrowseReceiver(c)

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
	c.deliver(&testMessage{topic: "aprs/stations/SP9XYZ-7", payload: []byte("{}"), retained: true})
	c.deliver(&testMessage{topic: "aprs/packets", payload: []byte("hello")})

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
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.unsubbed {
		t.Error("temporary subscription was not removed")
	}
}

func TestReceiverBrowseTruncatesPayload(t *testing.T) {
	c := newBrowseClient()
	r := newBrowseReceiver(c)

	go func() {
		_, _ = r.Browse(context.Background(), "#", 100*time.Millisecond, 10)
	}()
	time.Sleep(20 * time.Millisecond)
	big := strings.Repeat("x", browsePayloadLimit+500)
	c.deliver(&testMessage{topic: "big", payload: []byte(big)})
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
	c.deliver(&testMessage{topic: "big", payload: []byte(big)})
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
	c := newBrowseClient()
	r := newBrowseReceiver(c)

	go func() {
		_, _ = r.Browse(context.Background(), "#", 150*time.Millisecond, 3)
	}()
	time.Sleep(20 * time.Millisecond)
	for i := 0; i < 10; i++ {
		c.deliver(&testMessage{topic: "t", payload: []byte("p")})
	}
	time.Sleep(200 * time.Millisecond)

	entries := make(chan []BrowseEntry, 1)
	go func() {
		got, _ := r.Browse(context.Background(), "#", 50*time.Millisecond, 3)
		entries <- got
	}()
	time.Sleep(10 * time.Millisecond)
	for i := 0; i < 10; i++ {
		c.deliver(&testMessage{topic: "t", payload: []byte("p")})
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
	on := newBrowseClient()
	m := &Manager{
		receivers: []*Receiver{
			newBrowseReceiver(off),
			newBrowseReceiver(on),
		},
		logger: testLogger(),
	}

	done := make(chan []BrowseEntry, 1)
	go func() {
		got, _ := m.Browse(context.Background(), "", "a/b", 100*time.Millisecond, 5)
		done <- got
	}()
	time.Sleep(20 * time.Millisecond)
	on.deliver(&testMessage{topic: "a/b", payload: []byte("v"), retained: true})
	got := <-done
	if len(got) != 1 || got[0].Topic != "a/b" {
		t.Fatalf("fallback browse entries = %+v", got)
	}

	if _, err := m.Browse(context.Background(), "missing", "a/b", time.Second, 5); err == nil {
		t.Error("unknown receiver should fail")
	}
}
