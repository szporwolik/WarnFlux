package dispatch

import (
	"context"
	"sync"
	"sync/atomic"
)

// DefaultQueueSize is the default bounded ingress capacity.
const DefaultQueueSize = 1024

// Inbox is an optional durable acceptance backend: every accepted event
// is persisted before it enters the live queue, so a full queue or a
// crash between acceptance and evaluation no longer loses the event.
// Implemented by the SQLite store.
type Inbox interface {
	AppendEvent(ctx context.Context, e Event) (int64, error)
}

// Ingress is the single bounded intake queue for canonical dispatch events.
//
// Semantics (documented, not stronger than reality):
//   - Enqueue never blocks: without an inbox, a full queue drops the event
//     and counts it. With a durable inbox attached, acceptance is
//     persistent FIRST — a full live queue defers the event to inbox
//     recovery instead of dropping it.
//   - After StopIntake, Enqueue always returns false.
//   - Without an inbox there is no durable storage: events accepted now
//     are dropped during a crash or a shutdown drain deadline.
type Ingress struct {
	queue chan Event

	// inbox is the optional durable acceptance backend (see Inbox).
	inbox Inbox

	// mu serializes enqueues against StopIntake's final drain-and-close:
	// a send can never land on a closed channel and the channel is never
	// closed while a send is in flight.
	mu     sync.Mutex
	closed bool

	received    atomic.Int64
	droppedFull atomic.Int64
	droppedLate atomic.Int64
	// inboxFailures counts append errors on an attached inbox (the event
	// falls back to live-only acceptance).
	inboxFailures atomic.Int64
}

// NewIngress creates a bounded ingress queue. Sizes below 1 fall back to
// the default.
func NewIngress(size int) *Ingress {
	if size < 1 {
		size = DefaultQueueSize
	}
	return &Ingress{queue: make(chan Event, size)}
}

// SetInbox attaches the durable acceptance backend. It must be called
// before the ingress starts accepting events.
func (g *Ingress) SetInbox(in Inbox) {
	g.mu.Lock()
	g.inbox = in
	g.mu.Unlock()
}

// Enqueue offers a canonical event without ever blocking. With an inbox
// attached, acceptance is durable before the live queue is offered: the
// event returns true when it was persisted (it will be delivered now or
// by inbox recovery) and false only when neither the inbox nor the live
// queue accepted it.
func (g *Ingress) Enqueue(e Event) bool {
	g.mu.Lock()
	closed := g.closed
	inbox := g.inbox
	g.mu.Unlock()
	if closed {
		g.droppedLate.Add(1)
		return false
	}

	// Durable acceptance first.
	if inbox != nil {
		if id, err := inbox.AppendEvent(context.Background(), e); err == nil {
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
		return e.InboxID != 0
	}
	select {
	case g.queue <- e:
		g.received.Add(1)
		return true
	default:
		// Live queue full: with a durable inbox the event is only
		// deferred (recovery re-delivers it), not lost.
		g.droppedFull.Add(1)
		return e.InboxID != 0
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
// live-only acceptance).
func (g *Ingress) InboxFailures() int64 {
	return g.inboxFailures.Load()
}
