package mqttreceiver

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// Browse bounds for the temporary subscriptions the web UI opens to
// inspect retained state and broker status ($SYS).
const (
	// BrowseDefaultWindow is the collection window when the caller does
	// not ask for a specific one.
	BrowseDefaultWindow = 3 * time.Second
	// BrowseMaxWindow caps a single browse: Mosquitto publishes $SYS
	// topics every 10 seconds by default, so a longer window is useful,
	// but never unbounded.
	BrowseMaxWindow = 15 * time.Second
	// BrowseMaxEntries caps how many messages one browse may collect so a
	// busy topic can never flood the web UI.
	BrowseMaxEntries = 250
)

// browsePayloadLimit truncates payloads served to the UI; the size field
// always reflects the full payload.
const browsePayloadLimit = 4096

// browseDisconnectQuiesce is how long the throwaway browse client may
// spend draining before it is dropped; a browse must never outlive its
// request.
const browseDisconnectQuiesce = 250 * time.Millisecond

// BrowseEntry is one message collected by a temporary browse
// subscription. Payload is truncated for JSON friendliness.
type BrowseEntry struct {
	Topic    string    `json:"topic"`
	QoS      byte      `json:"qos"`
	Retained bool      `json:"retained"`
	Size     int       `json:"size"`
	Payload  string    `json:"payload"`
	At       time.Time `json:"at"`
}

// Browse collects messages of one topic for an operator: it CONNECTS A
// THROWAWAY CLIENT of its own, subscribes that one, holds it for the
// collection window and disconnects it — the receiver's own client is
// never touched.
//
// Sharing the receiver's client used to mean that browsing a filter the
// receiver watches in production ended with Unsubscribe(topic) on that
// same client: delivery for the filter stopped while the connection still
// looked healthy, and only a resubscribe brought it back (reported P1).
// The browsed messages carry their own handler on the throwaway client,
// so they never reach the dispatch ingress either. Only one browse may run
// per receiver at a time.
func (r *Receiver) Browse(ctx context.Context, topic string, window time.Duration, max int) ([]BrowseEntry, error) {
	topic = strings.TrimSpace(topic)
	if topic == "" {
		return nil, fmt.Errorf("empty topic")
	}
	r.browseMu.Lock()
	defer r.browseMu.Unlock()
	if !r.isConnected() {
		return nil, fmt.Errorf("receiver %q is not connected", r.cfg.ID)
	}
	if max <= 0 || max > BrowseMaxEntries {
		max = BrowseMaxEntries
	}

	client, err := r.connectBrowseClient()
	if err != nil {
		return nil, err
	}
	// Always clean up: the subscription and the session die with this
	// client, whatever the outcome (cancel and timeout included).
	defer client.Disconnect(uint(browseDisconnectQuiesce.Milliseconds()))

	var (
		mu      sync.Mutex
		entries []BrowseEntry
	)
	handler := func(_ mqtt.Client, msg mqtt.Message) {
		payload := string(msg.Payload())
		if len(payload) > browsePayloadLimit {
			payload = payload[:browsePayloadLimit] + "…"
		}
		mu.Lock()
		if len(entries) < max {
			entries = append(entries, BrowseEntry{
				Topic:    msg.Topic(),
				QoS:      msg.Qos(),
				Retained: msg.Retained(),
				Size:     len(msg.Payload()),
				Payload:  payload,
				At:       time.Now(),
			})
		}
		mu.Unlock()
	}

	// Subscribe at QoS 1 so the reported QoS matches what the broker
	// stores for the retained message (delivery QoS is min(pub, sub)).
	token := client.Subscribe(topic, 1, handler)
	if !token.WaitTimeout(subscribeTimeout) {
		client.Unsubscribe(topic)
		return nil, &SubscribeError{Topic: topic}
	}
	if err := token.Error(); err != nil {
		client.Unsubscribe(topic)
		return nil, &SubscribeError{Topic: topic, Err: err}
	}

	if window <= 0 {
		window = BrowseDefaultWindow
	}
	if window > BrowseMaxWindow {
		window = BrowseMaxWindow
	}
	timer := time.NewTimer(window)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
	client.Unsubscribe(topic)

	mu.Lock()
	out := entries
	mu.Unlock()
	return out, nil
}

// Browse opens a temporary subscription on one receiver. receiverID may
// be empty to use the first connected receiver.
func (m *Manager) Browse(ctx context.Context, receiverID, topic string, window time.Duration, max int) ([]BrowseEntry, error) {
	var fallback *Receiver
	for _, r := range m.receivers {
		if r.cfg.ID == receiverID && r.cfg.Enabled {
			return r.Browse(ctx, topic, window, max)
		}
		if fallback == nil && r.cfg.Enabled && r.isConnected() {
			fallback = r
		}
	}
	if receiverID == "" && fallback != nil {
		return fallback.Browse(ctx, topic, window, max)
	}
	if receiverID != "" {
		return nil, fmt.Errorf("receiver %q not found or disabled", receiverID)
	}
	return nil, fmt.Errorf("no connected MQTT receiver")
}

// connectBrowseClient builds and connects the throwaway client of one
// browse session: its own client id (so it can never displace the
// receiver's session on the broker), a clean session (nothing survives
// the browse) and no auto-reconnect (a browse is short-lived by design).
func (r *Receiver) connectBrowseClient() (mqttClient, error) {
	opts := mqtt.NewClientOptions().
		AddBroker(r.cfg.Broker).
		SetClientID(browseClientID(r.cfg.ClientID)).
		SetKeepAlive(r.cfg.KeepAlive).
		SetConnectTimeout(r.cfg.ConnectTimeout).
		SetCleanSession(true).
		SetOrderMatters(false)
	if r.cfg.Username != "" {
		opts.SetUsername(r.cfg.Username)
	}
	if r.cfg.Password != "" {
		opts.SetPassword(r.cfg.Password)
	}
	factory := r.newBrowseClient
	if factory == nil {
		factory = func(o *mqtt.ClientOptions) mqttClient { return mqtt.NewClient(o) }
	}
	client := factory(opts)
	token := client.Connect()
	if !token.WaitTimeout(r.cfg.ConnectTimeout) {
		client.Disconnect(0)
		return nil, fmt.Errorf("browse connect for receiver %q timed out", r.cfg.ID)
	}
	if err := token.Error(); err != nil {
		client.Disconnect(0)
		return nil, fmt.Errorf("browse connect for receiver %q: %w", r.cfg.ID, err)
	}
	return client, nil
}

// browseClientID derives the throwaway client's id from the receiver's.
// It must differ: the broker drops the existing session when a second
// client connects with the same id.
func browseClientID(base string) string {
	if base == "" {
		base = "warnflux-browse"
	}
	return base + "-browse"
}
