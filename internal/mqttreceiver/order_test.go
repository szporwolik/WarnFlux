package mqttreceiver

import (
	"io"
	"log/slog"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/szporwolik/WarnFlux/internal/config"
	"github.com/szporwolik/WarnFlux/internal/dispatch"
	"github.com/szporwolik/WarnFlux/internal/dispatch/state"
)

// TestReceiverKeepsFrameOrder is the regression test for a resurrected hazard:
// with order-matters off paho runs one goroutine per frame, so an update that
// is still being applied can land AFTER the zero-length retained payload that
// deleted the same topic — the alarm comes back in the application state (the
// test that found it observed exactly that). Frames of one topic therefore have
// to be handled in the order the broker delivered them.
//
// The clock is not part of this test: the delete payload carries no timestamp,
// so no state-side staleness check can repair an out-of-order apply.
func TestReceiverKeepsFrameOrder(t *testing.T) {
	var got *mqtt.ClientOptions
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	r, err := NewReceiver(config.Receiver{
		ID: "order", Enabled: true,
		Broker: "tcp://127.0.0.1:1", ClientID: "warnflux-test-order",
		ConnectTimeout: 50 * time.Millisecond, KeepAlive: 30 * time.Second,
		WF: config.ReceiverWF{Enabled: true, TopicPrefix: "warnflux"},
	}, state.New(), dispatch.NewIngress(64), logger, nil)
	if err != nil {
		t.Fatalf("NewReceiver: %v", err)
	}
	t.Cleanup(r.Disconnect)
	r.newClient = func(opts *mqtt.ClientOptions) mqttClient {
		got = opts
		return &noopClient{}
	}

	// The initial connect fails (nothing listens on port 1); the options are
	// built before any network call, which is all this test needs.
	_ = r.Connect(t.Context())
	if got == nil {
		t.Fatal("Connect never built the client options")
	}
	if !got.Order {
		t.Error("client options disable ordered dispatch: a slow update could resurrect a deleted alarm")
	}
}

// noopClient satisfies the receiver's client interface without a broker.
type noopClient struct{}

func (n *noopClient) Connect() mqtt.Token                                { return doneToken{} }
func (n *noopClient) Disconnect(uint)                                    {}
func (n *noopClient) IsConnected() bool                                  { return false }
func (n *noopClient) Publish(string, byte, bool, interface{}) mqtt.Token { return doneToken{} }
func (n *noopClient) Subscribe(string, byte, mqtt.MessageHandler) mqtt.Token {
	return doneToken{}
}
func (n *noopClient) Unsubscribe(...string) mqtt.Token { return doneToken{} }

// doneToken is an already-finished token whose Error is nil.
type doneToken struct{}

func (doneToken) Wait() bool                     { return true }
func (doneToken) WaitTimeout(time.Duration) bool { return true }
func (d doneToken) Done() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}
func (doneToken) Error() error { return nil }
