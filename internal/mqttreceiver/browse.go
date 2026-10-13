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
// per receiver at a time, and the whole session is bounded by ctx: waiting
// for that slot, waiting for the broker's connect and subscription
// acknowledgements, and the collection window all give up as soon as the
// request is gone. A preview the operator closed must not keep its place in
// the queue (nor connect a minute later), because a series of closed
// previews would otherwise load the broker and delay every browse behind
// them (reported P2).
func (r *Receiver) Browse(ctx context.Context, topic string, window time.Duration, max int) ([]BrowseEntry, error) {
	topic = strings.TrimSpace(topic)
	if topic == "" {
		return nil, fmt.Errorf("empty topic")
	}
	// Waiting for the slot honors the request: a preview the operator
	// closed must leave the queue instead of connecting minutes later,
	// holding up the browses behind it.
	if err := r.lockBrowse(ctx); err != nil {
		return nil, err
	}
	defer r.browseMu.Unlock()
	// The request can die in the window between taking the slot and using
	// it: a cancelled one never opens a connection at all.
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !r.isConnected() {
		return nil, fmt.Errorf("receiver %q is not connected", r.cfg.ID)
	}
	if max <= 0 || max > BrowseMaxEntries {
		max = BrowseMaxEntries
	}

	client, err := r.connectBrowseClient(ctx)
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
	if !awaitToken(ctx, token, subscribeTimeout) {
		client.Unsubscribe(topic)
		if err := ctx.Err(); err != nil {
			return nil, err
		}
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
func (r *Receiver) connectBrowseClient(ctx context.Context) (mqttClient, error) {
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
	if !awaitToken(ctx, token, r.cfg.ConnectTimeout) {
		client.Disconnect(0)
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("browse connect for receiver %q timed out", r.cfg.ID)
	}
	if err := token.Error(); err != nil {
		client.Disconnect(0)
		return nil, fmt.Errorf("browse connect for receiver %q: %w", r.cfg.ID, err)
	}
	return client, nil
}

// lockBrowse takes the single browse slot of one receiver, giving up when
// the request is cancelled while it waits. The abandoned waiter still takes
// the lock the moment it is free and hands it straight back, so no slot is
// ever leaked — it simply never connects a client.
func (r *Receiver) lockBrowse(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if r.browseMu.TryLock() {
		return nil
	}
	locked := make(chan struct{})
	go func() {
		r.browseMu.Lock()
		close(locked)
	}()
	select {
	case <-locked:
		return nil
	case <-ctx.Done():
		go func() {
			<-locked
			r.browseMu.Unlock()
		}()
		return ctx.Err()
	}
}

// awaitToken waits for a paho token, whichever comes first: the broker's
// answer, the timeout or the request being cancelled. A browse (and its
// connection attempt) must never outlive the request that asked for it.
func awaitToken(ctx context.Context, token mqtt.Token, timeout time.Duration) bool {
	// A zero timeout means "no timeout" (the paho convention), so the
	// timer is left unarmed rather than firing immediately.
	var expired <-chan time.Time
	if timeout > 0 {
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		expired = timer.C
	}
	select {
	case <-token.Done():
		return true
	case <-expired:
		return false
	case <-ctx.Done():
		return false
	}
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
