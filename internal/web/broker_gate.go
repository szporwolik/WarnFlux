package web

import (
	"errors"
	"log/slog"
	"sync"

	"github.com/szporwolik/WarnFlux/internal/dispatch/state"
)

// brokerGate is the ONE path through which the station publishes (and
// retires) retained broker documents, from the panel and from the state
// resync alike.
//
// Two publications of the same message used to race: the resync read an
// ACTIVE communication, the operator expired it (the retained tombstone
// went out), and the delayed resync publish then put the active document
// straight back on the broker — which served an alarm the station had
// already retired, even though the local record was correct (reported
// P1). The gate therefore
//
//   - SERIALIZES the publications of one message (its own lock), so two
//     of them can never interleave into a state neither of them meant, and
//   - ORDERS them by the version of the local record they were built
//     from: a publication older than what was already sent for that
//     message is dropped instead of resurrecting it.
//
// A version of 0 means "no version identity" (an installation without
// local storage, a document another station published): such a
// publication is serialized but never dropped and never raises the bar.
type brokerGate struct {
	pub composePublisher

	mu   sync.Mutex
	keys map[string]*gateKey
}

// gateKey is one message's gate: its lock plus the newest version that
// went to the broker.
type gateKey struct {
	mu      sync.Mutex
	version int64
}

// gateKeyCap bounds the gate table. Only the pending races matter (a
// publication is always built and sent within one call), so keys are
// dropped once the table grows past the cap.
const gateKeyCap = 4096

// errNoBroker is returned when the station has no MQTT publisher.
var errNoBroker = errors.New("no broker publisher configured")

// newBrokerGate builds the publication gate around the raw publisher
// (nil = no broker configured).
func newBrokerGate(pub composePublisher) *brokerGate {
	return &brokerGate{pub: pub, keys: map[string]*gateKey{}}
}

// key returns the gate of one message, creating it on first use. The
// message key is "<family>:<source>:<event key>" for retained hazards and
// the document suffix for retained info documents — one entry per
// document, never per family.
func (g *brokerGate) key(name string) *gateKey {
	g.mu.Lock()
	defer g.mu.Unlock()
	k, ok := g.keys[name]
	if !ok {
		if len(g.keys) >= gateKeyCap {
			// The table only exists for races in flight; dropping it is
			// safe and keeps the map bounded over years of messages.
			g.keys = map[string]*gateKey{}
		}
		k = &gateKey{}
		g.keys[name] = k
	}
	return k
}

// publish runs fn under the message's gate unless the version was already
// superseded. Recording the version even when fn fails is deliberate: the
// failure is retried by the caller with the SAME version (equal is never
// dropped), while an older publication stays out.
func (g *brokerGate) publish(name string, version int64, fn func() error) error {
	if g.pub == nil {
		return errNoBroker
	}
	k := g.key(name)
	k.mu.Lock()
	defer k.mu.Unlock()
	if version > 0 && version < k.version {
		slog.Debug("broker publication superseded by a newer version",
			"document", name, "version", version, "published", k.version)
		return nil
	}
	if version > 0 {
		k.version = version
	}
	return fn()
}

// publishActive publishes (or republishes) a retained active-hazard
// document for the version of the local record it was built from.
func (g *brokerGate) publishActive(source string, h state.Hazard, version int64) error {
	return g.publish("active:"+source+":"+h.EventKey, version,
		func() error { return g.pub.PublishActive(source, h) })
}

// retireActive removes a retained active-hazard document. The removal is
// ordered like any other publication: a stand-down superseded by a newer
// version must not take a live alarm down.
func (g *brokerGate) retireActive(source, eventKey string, version int64) error {
	return g.publish("active:"+source+":"+eventKey, version,
		func() error { return g.pub.ExpireActive(source, eventKey) })
}

// publishInfo publishes one retained state document (the EMCOM networks)
// under its topic suffix.
func (g *brokerGate) publishInfo(suffix string, payload []byte, version int64) error {
	return g.publish("info:"+suffix, version,
		func() error { return g.pub.PublishRaw(suffix, true, payload) })
}
