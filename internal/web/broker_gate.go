package web

import (
	"context"
	"errors"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/szporwolik/WarnFlux/internal/dispatch/state"
	"github.com/szporwolik/WarnFlux/internal/storage"
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
	// ledger is the DURABLE version bar (nil = memory only). The gate
	// consults it when it holds no bar for a document — the entry was
	// evicted from a full table, or the process restarted — so the
	// ordering rule survives memory pressure instead of living in RAM
	// only (reported P2). Without it a versioned entry is never evicted:
	// dropping a bar nothing else can restore would let a delayed
	// publication resurrect a retired alarm.
	ledger gateLedger

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

// gateKeyCap bounds the IDLE keys the gate keeps. A publication without a
// version identity (an installation without local storage, a document
// another station published) has nothing to remember, so its entry only
// serves the race in flight. An entry that REMEMBERS a version is the
// ordering guard itself: it may only be dropped where the durable ledger
// can answer for it (reported P2).
const gateKeyCap = 4096

// gateEvictBatch is how many least recently used entries one eviction pass
// frees: the scan cost is amortized over the batch instead of being paid
// on every insertion once the table is full (reported P2).
const gateEvictBatch = gateKeyCap / 8

// gateLedgerTimeout bounds one durable version lookup: a slow or stuck
// store must never block a publication (fail-open, like an unknown
// document).
const gateLedgerTimeout = 2 * time.Second

// gateLedger is the durable lifecycle ledger the gate falls back to. It is
// satisfied by the SQLite store (LifecycleSuperseded: "a newer version of
// this message is already recorded").
type gateLedger interface {
	LifecycleSuperseded(ctx context.Context, publisher, eventKey string, changeID int64) (bool, error)
}

// errNoBroker is returned when the station has no MQTT publisher.
var errNoBroker = errors.New("no broker publisher configured")

// newBrokerGate builds the publication gate around the raw publisher
// (nil = no broker configured) and the durable version ledger (nil = the
// gate orders from memory only).
func newBrokerGate(pub composePublisher, ledger gateLedger) *brokerGate {
	return &brokerGate{pub: pub, ledger: ledger, keys: map[string]*gateKey{}}
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

// makeRoom frees a batch of the least recently used evictable keys when
// the table is full. The caller holds g.mu. Replacing the table wholesale
// — as the cap used to do — threw away the remembered versions and the
// locks of publications still in flight, so a delayed publication could
// put a retired alarm back on the broker (reported P2).
func (g *brokerGate) makeRoom() {
	if len(g.keys) < gateKeyCap {
		return
	}
	type entry struct {
		name string
		seq  uint64
	}
	cand := make([]entry, 0, len(g.keys))
	for name, k := range g.keys {
		if g.evictable(name, k) {
			cand = append(cand, entry{name: name, seq: k.seq})
		}
	}
	if len(cand) == 0 {
		return
	}
	sort.Slice(cand, func(i, j int) bool { return cand[i].seq < cand[j].seq })
	for _, e := range cand[:min(gateEvictBatch, len(cand))] {
		delete(g.keys, e.name)
	}
}

// evictable reports whether one entry may be dropped: never while it is in
// use, always when it remembers nothing, and — for an entry that DOES
// remember a version — only when the durable ledger can restore the bar,
// which the gate can ask for the retained hazard documents (their event
// key is the ledger key). Info documents have no ledger identity of their
// own, so those few entries stay (bounded by the configured networks).
func (g *brokerGate) evictable(name string, k *gateKey) bool {
	if k.refs > 0 {
		return false
	}
	if k.version == 0 {
		return true
	}
	return g.ledger != nil && strings.HasPrefix(name, "active:")
}

// superseded asks the durable ledger whether a newer version of the
// publication's message is already recorded. An unavailable ledger, an
// unknown document or an error all answer "no" (fail-open: the gate only
// drops what it can prove is stale).
func (g *brokerGate) superseded(name string, version int64) bool {
	if g.ledger == nil || version <= 0 {
		return false
	}
	eventKey, ok := ledgerEventKey(name)
	if !ok {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), gateLedgerTimeout)
	defer cancel()
	// An empty publisher makes the ledger resolve this instance's own row
	// (panel and EMCOM documents are written under the instance identity).
	stale, err := g.ledger.LifecycleSuperseded(ctx, "", eventKey, version)
	if err != nil {
		slog.Warn("broker gate: durable version lookup failed", "document", name, "error", err)
		return false
	}
	return stale
}

// ledgerEventKey extracts the lifecycle event key of one gate document
// name: "active:<source>:<event key>".
func ledgerEventKey(name string) (string, bool) {
	rest, ok := strings.CutPrefix(name, "active:")
	if !ok {
		return "", false
	}
	i := strings.IndexByte(rest, ':')
	if i < 0 || i == len(rest)-1 {
		return "", false
	}
	return rest[i+1:], true
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
	if version > 0 {
		if version < k.version {
			slog.Debug("broker publication superseded by a newer version",
				"document", name, "version", version, "published", k.version)
			return nil
		}
		if k.version == 0 && g.superseded(name, version) {
			// The bar is not in memory (this entry was evicted from a full
			// table, or the process restarted) but the durable ledger still
			// knows a newer version: publishing would put a superseded — a
			// possibly retired — document back on the broker.
			slog.Debug("broker publication superseded by a newer recorded version",
				"document", name, "version", version)
			return nil
		}
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

// lifecycleLedger returns the durable version ledger of the user store
// when it has one (nil = the gate orders from memory only).
func lifecycleLedger(store storage.DirectoryStore) gateLedger {
	if l, ok := store.(gateLedger); ok {
		return l
	}
	return nil
}
