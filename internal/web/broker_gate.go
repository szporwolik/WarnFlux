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
	// seq stamps the keys by last use, so the oldest idle one goes first
	// when the table needs room. Guarded by mu.
	seq uint64
}

// gateKey is one message's gate: its lock plus the newest version that
// went to the broker.
type gateKey struct {
	mu      sync.Mutex
	version int64

	// refs counts the publications currently using this key and seq its
	// last use — both guarded by brokerGate.mu. A key in use is never
	// dropped: two publications of one message must always meet on ONE
	// lock (reported P2).
	refs int
	seq  uint64
}

// gateKeyCap bounds the IDLE, never-versioned keys the gate keeps: a
// publication without a version identity (an installation without local
// storage, a document another station published) has nothing to remember,
// so its entry only serves the race in flight and stays bounded over
// years of messages. The entries that REMEMBER a version are the ordering
// guard itself and are never dropped — losing one would let a delayed
// publication resurrect a retired alarm (reported P2) — so the table
// mirrors the published documents, not the messages.
const gateKeyCap = 4096

// errNoBroker is returned when the station has no MQTT publisher.
var errNoBroker = errors.New("no broker publisher configured")

// newBrokerGate builds the publication gate around the raw publisher
// (nil = no broker configured).
func newBrokerGate(pub composePublisher) *brokerGate {
	return &brokerGate{pub: pub, keys: map[string]*gateKey{}}
}

// key returns the gate of one message, creating it on first use, and
// marks it in use — the caller MUST release() it when done. The message
// key is "<family>:<source>:<event key>" for retained hazards and the
// document suffix for retained info documents — one entry per document,
// never per family.
func (g *brokerGate) key(name string) *gateKey {
	g.mu.Lock()
	defer g.mu.Unlock()
	k, ok := g.keys[name]
	if !ok {
		g.makeRoom()
		k = &gateKey{}
		g.keys[name] = k
	}
	g.seq++
	k.seq = g.seq
	k.refs++
	return k
}

// release ends one use of a key.
func (g *brokerGate) release(k *gateKey) {
	g.mu.Lock()
	k.refs--
	g.mu.Unlock()
}

// makeRoom drops ONE idle key that remembers no version when the table is
// full. The caller holds g.mu. Replacing the table wholesale — as the cap
// used to do — threw away the remembered versions and the locks of
// publications still in flight, so a delayed publication could put a
// retired alarm back on the broker (reported P2).
func (g *brokerGate) makeRoom() {
	if len(g.keys) < gateKeyCap {
		return
	}
	victim, oldest, found := "", uint64(0), false
	for name, k := range g.keys {
		if k.refs > 0 || k.version != 0 {
			continue
		}
		if !found || k.seq < oldest {
			victim, oldest, found = name, k.seq, true
		}
	}
	if found {
		delete(g.keys, victim)
	}
}

// publish runs fn under the message's gate unless the version was already
// superseded. Recording the version even when fn fails is deliberate: the
// failure is retried by the caller with the SAME version (equal is never
// dropped), while an older publication stays out. The bar lives as long
// as the station does: it is the only thing that keeps a publication
// built from an older record out.
func (g *brokerGate) publish(name string, version int64, fn func() error) error {
	if g.pub == nil {
		return errNoBroker
	}
	k := g.key(name)
	defer g.release(k)
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
