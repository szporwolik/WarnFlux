package dispatch

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// blockingInbox records how many durable writes run at the same time and holds
// each one for a moment, so the test can observe the bound the ingress claims
// before it starts working.
type blockingInbox struct {
	hold time.Duration
	cur  atomic.Int64
	max  atomic.Int64
	done atomic.Int64
}

func (b *blockingInbox) AppendEvent(context.Context, Event) (int64, error) {
	n := b.cur.Add(1)
	for {
		observed := b.max.Load()
		if n <= observed || b.max.CompareAndSwap(observed, n) {
			break
		}
	}
	time.Sleep(b.hold)
	b.cur.Add(-1)
	return b.done.Add(1), nil
}

// TestIngressBoundsConcurrentInboxWrites is the regression test for the intake
// that was bounded only on paper: the durable write happens INSIDE Enqueue
// before the queue is offered, so with paho's per-frame goroutines (and any
// other concurrent producer) 200 writes ran at once against a queue of one —
// a slow SQLite then held 200 write contexts and their events in memory.
//
// The queue capacity must not be the only bound: the work is claimed before it
// starts.
func TestIngressBoundsConcurrentInboxWrites(t *testing.T) {
	g := NewIngress(1)
	g.SetMaxInFlight(2)
	inbox := &blockingInbox{hold: 5 * time.Millisecond}
	g.SetInbox(inbox)

	const producers = 60
	var wg sync.WaitGroup
	var accepted, rejected atomic.Int64
	for i := 0; i < producers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			switch g.Enqueue(Event{Kind: EventMQTTMessage}) {
			case Rejected:
				rejected.Add(1)
			default:
				accepted.Add(1)
			}
		}()
	}
	wg.Wait()

	if got := inbox.max.Load(); got > 2 {
		t.Fatalf("concurrent durable writes = %d, want at most the in-flight bound (2)", got)
	}
	if accepted.Load() == 0 {
		t.Error("every offer was rejected: the claim must still admit work")
	}
	if rejected.Load() == 0 {
		t.Error("nothing was rejected: the bound never engaged under 60 producers")
	}
	if got := g.Saturated(); got != rejected.Load() {
		t.Errorf("saturated counter = %d, want %d (every refusal came from the in-flight bound)", got, rejected.Load())
	}
	// Everything that was not accepted is still with the producer/broker.
	if accepted.Load()+rejected.Load() != producers {
		t.Errorf("accepted+rejected = %d, want %d", accepted.Load()+rejected.Load(), producers)
	}
}

// TestIngressDefaultInFlightBound pins the default: the claim exists even when
// nobody calls SetMaxInFlight.
func TestIngressDefaultInFlightBound(t *testing.T) {
	g := NewIngress(1)
	inbox := &blockingInbox{hold: 5 * time.Millisecond}
	g.SetInbox(inbox)

	var wg sync.WaitGroup
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			g.Enqueue(Event{Kind: EventMQTTMessage})
		}()
	}
	wg.Wait()
	if got := inbox.max.Load(); got > DefaultMaxInFlight {
		t.Errorf("concurrent durable writes = %d, want at most %d", got, DefaultMaxInFlight)
	}
}
