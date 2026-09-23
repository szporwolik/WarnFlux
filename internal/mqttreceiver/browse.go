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

// Browse temporarily subscribes the receiver to topic, collects up to max
// messages during window, then unsubscribes. The temporary subscription
// carries its own handler, so browsed messages never reach the dispatch
// ingress. Only one browse may run per receiver at a time.
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
	token := r.client.Subscribe(topic, 1, handler)
	if !token.WaitTimeout(subscribeTimeout) {
		r.client.Unsubscribe(topic)
		return nil, &SubscribeError{Topic: topic}
	}
	if err := token.Error(); err != nil {
		r.client.Unsubscribe(topic)
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
	r.client.Unsubscribe(topic)

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
