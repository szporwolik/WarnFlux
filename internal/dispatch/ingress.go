package dispatch

import (
	"context"
	"sync"
	"sync/atomic"
)

// DefaultQueueSize is the default bounded ingress capacity.
const DefaultQueueSize = 1024

// Ingress is the single bounded intake queue for canonical dispatch events.
//
// Semantics (documented, not stronger than reality):
//   - Enqueue never blocks: a full queue drops the event and counts it.
//   - After StopIntake, Enqueue always returns false.
//   - There is no durable storage: events accepted now are dropped during
//     a crash or a shutdown drain deadline. Durable rule/action semantics
//     come later.
type Ingress struct {
	queue chan Event

	// mu serializes enqueues against StopIntake's final drain-and-close:
	// a send can never land on a closed channel and the channel is never
	// closed while a send is in flight.
	mu     sync.Mutex
	closed bool

	received    atomic.Int64
	droppedFull atomic.Int64
	droppedLate atomic.Int64
}

// NewIngress creates a bounded ingress queue. Sizes below 1 fall back to
// the default.
func NewIngress(size int) *Ingress {
	if size < 1 {
		size = DefaultQueueSize
	}
	return &Ingress{queue: make(chan Event, size)}
}

// Enqueue offers a canonical event without ever blocking. It returns false
// (caller logs + counts the drop) when the queue is full or intake has
// been stopped.
func (g *Ingress) Enqueue(e Event) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		g.droppedLate.Add(1)
		return false
	}
	select {
	case g.queue <- e:
		g.received.Add(1)
		return true
	default:
		g.droppedFull.Add(1)
		return false
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
