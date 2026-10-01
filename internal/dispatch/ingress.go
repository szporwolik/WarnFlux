package dispatch

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// DefaultQueueSize is the default bounded ingress capacity.
const DefaultQueueSize = 1024

// DefaultInboxWriteTimeout bounds the durable inbox write inside Enqueue:
// the intake is non-blocking for the caller, and a database that stalls
// longer than this deadline degrades the event to emergency acceptance
// instead of hanging the MQTT callback forever.
const DefaultInboxWriteTimeout = 2 * time.Second

// LowDiskInboxRetention is the inbox retention cutoff used while the
// filesystem reports less free space than storage.min_free_mb: unevaluated
// rows are pruned much earlier so auxiliary data stops competing with
// primary storage on a full disk.
const LowDiskInboxRetention = 5 * time.Minute

// Inbox is an optional durable acceptance backend: every accepted event
// is persisted before it enters the live queue, so a full queue or a
// crash between acceptance and evaluation no longer loses the event.
// Implemented by the SQLite store.
type Inbox interface {
	AppendEvent(ctx context.Context, e Event) (int64, error)
}

// Acceptance is the explicit, auditable result of one Enqueue offer.
// The caller can tell durable acceptance from the emergency fallback —
// the operator must never mistake one for the other.
type Acceptance int

const (
	// Rejected means neither the durable inbox nor the live queue took
	// the event: it is lost and counted.
	Rejected Acceptance = iota
	// AcceptedDurable means the event is persisted in the inbox before
	// entering the live queue: a restart replays it via inbox recovery.
	AcceptedDurable
	// AcceptedEmergency means the event lives only in RAM (no inbox
	// attached, or the inbox write failed): it will be delivered now but
	// a restart loses it. EMCOM-relevant, never silent.
	AcceptedEmergency
)

// Ingress is the single bounded intake queue for canonical dispatch events.
//
// Semantics (documented, not stronger than reality):
//   - Enqueue never blocks the caller for more than the inbox write
//     deadline. Without an inbox, a full queue drops the event and
//     counts it. With a durable inbox attached, acceptance is persistent
//     FIRST — a full live queue defers the event to inbox recovery
//     instead of dropping it.
//   - Every offer returns an explicit Acceptance: durable, emergency
//     (RAM-only, visible and auditable) or rejected.
//   - After StopIntake, Enqueue always returns Rejected.
//   - Without an inbox there is no durable storage: events accepted now
//     are dropped during a crash or a shutdown drain deadline.
type Ingress struct {
	queue chan Event

	// inbox is the optional durable acceptance backend (see Inbox).
	inbox Inbox

	// writeTimeout bounds one inbox write (see DefaultInboxWriteTimeout).
	writeTimeout time.Duration

	// mu serializes enqueues against StopIntake's final drain-and-close:
	// a send can never land on a closed channel and the channel is never
	// closed while a send is in flight.
	mu     sync.Mutex
	closed bool

	received    atomic.Int64
	droppedFull atomic.Int64
	droppedLate atomic.Int64
	// inboxFailures counts append errors on an attached inbox (the event
	// falls back to emergency, live-only acceptance).
	inboxFailures atomic.Int64
	// durable counts events accepted through the inbox (delivered live
	// now or deferred to inbox recovery).
	durable atomic.Int64
	// emergency counts events accepted without durable storage: no inbox
	// attached, or the inbox write failed.
	emergency atomic.Int64
}

// NewIngress creates a bounded ingress queue. Sizes below 1 fall back to
// the default.
func NewIngress(size int) *Ingress {
	if size < 1 {
		size = DefaultQueueSize
	}
	return &Ingress{queue: make(chan Event, size), writeTimeout: DefaultInboxWriteTimeout}
}

// SetInbox attaches the durable acceptance backend. It must be called
// before the ingress starts accepting events.
func (g *Ingress) SetInbox(in Inbox) {
	g.mu.Lock()
	g.inbox = in
	g.mu.Unlock()
}

// SetInboxWriteTimeout bounds one durable inbox write (the "never
// blocks" guarantee is bounded, not infinite). Values ≤ 0 restore the
// default.
func (g *Ingress) SetInboxWriteTimeout(d time.Duration) {
	if d <= 0 {
		d = DefaultInboxWriteTimeout
	}
	g.mu.Lock()
	g.writeTimeout = d
	g.mu.Unlock()
}

// Enqueue offers a canonical event and reports exactly how it was
// accepted. With an inbox attached, acceptance is durable before the live
// queue is offered: the result is AcceptedDurable when the event was
// persisted (it will be delivered now or by inbox recovery),
// AcceptedEmergency when the inbox write failed but the live queue took
// it, and Rejected only when neither took it.
//
// An event that already carries an inbox row ID (e.InboxID != 0 — the
// journal transaction wrote the acceptance row) skips the append: the
// row already exists, so the live offer never duplicates it.
func (g *Ingress) Enqueue(e Event) Acceptance {
	g.mu.Lock()
	closed := g.closed
	inbox := g.inbox
	writeTimeout := g.writeTimeout
	g.mu.Unlock()
	if closed {
		g.droppedLate.Add(1)
		return Rejected
	}

	// Durable acceptance first, bounded by the write deadline: a stuck
	// database degrades to emergency acceptance instead of blocking the
	// receiver callback. Journal changes arrive with their inbox row
	// already committed (same transaction as the journal record), so the
	// append is skipped for them.
	if inbox != nil && e.InboxID == 0 {
		ctx, cancel := context.WithTimeout(context.Background(), writeTimeout)
		id, err := inbox.AppendEvent(ctx, e)
		cancel()
		if err == nil {
			e.InboxID = id
		} else {
			g.inboxFailures.Add(1)
		}
	}

	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		// Accepted durably right before shutdown: the row stays in the
		// inbox and re-enters after the restart.
		g.droppedLate.Add(1)
		if e.InboxID != 0 {
			g.durable.Add(1)
			return AcceptedDurable
		}
		return Rejected
	}
	select {
	case g.queue <- e:
		g.received.Add(1)
		if e.InboxID != 0 {
			g.durable.Add(1)
			return AcceptedDurable
		}
		g.emergency.Add(1)
		return AcceptedEmergency
	default:
		// Live queue full: with a durable inbox the event is only
		// deferred (recovery re-delivers it), not lost.
		g.droppedFull.Add(1)
		if e.InboxID != 0 {
			g.durable.Add(1)
			return AcceptedDurable
		}
		return Rejected
	}
}

// Events exposes the intake channel for a consumer (the future rule
// engine). The channel is closed by StopIntake, so a consumer reading
// until close sees every accepted event.
func (g *Ingress) Events() <-chan Event { return g.queue }

// StopIntake prevents new enqueues and closes the channel immediately.
// Already-accepted events stay queued: a consumer reading until close sees
// every accepted event. Callers that have no consumer drain the queue
// first (Drain) so accepted events are not silently discarded. Idempotent.
func (g *Ingress) StopIntake() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return
	}
	g.closed = true
	close(g.queue)
}

// Drain consumes and discards accepted events until the queue is empty or
// ctx expires. It is used at shutdown when no consumer is running, so
// accepted events are removed rather than left undelivered. Returns the
// number of drained events.
func (g *Ingress) Drain(ctx context.Context) int {
	n := 0
	for {
		select {
		case <-ctx.Done():
			return n
		case _, ok := <-g.queue:
			if !ok {
				return n
			}
			n++
		default:
			return n
		}
	}
}

// Stats returns intake counters.
func (g *Ingress) Stats() (received, droppedFull, droppedLate int64, queueDepth, queueCap int) {
	return g.received.Load(), g.droppedFull.Load(), g.droppedLate.Load(), len(g.queue), cap(g.queue)
}

// InboxFailures counts inbox append errors (the event fell back to
// emergency, live-only acceptance).
func (g *Ingress) InboxFailures() int64 {
	return g.inboxFailures.Load()
}

// DurableAccepted counts events accepted through the durable inbox
// (delivered live now or deferred to inbox recovery).
func (g *Ingress) DurableAccepted() int64 {
	return g.durable.Load()
}

// EmergencyAccepted counts events accepted without durable storage: no
// inbox attached, or the inbox write failed. They are visible to the
// operator (health page, metrics, logs) and lost on restart.
func (g *Ingress) EmergencyAccepted() int64 {
	return g.emergency.Load()
}
