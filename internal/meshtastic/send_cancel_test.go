package meshtastic

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	pb "github.com/kabili207/meshtastic-go/core/proto"
	"github.com/kabili207/meshtastic-go/transport/client"
	"google.golang.org/protobuf/proto"
)

// recordingConn is a live-looking transport that only counts the frames it was
// asked to write: the review's proof that a cancelled request never reaches the
// radio.
type recordingConn struct {
	mu      sync.Mutex
	writes  []*pb.ToRadio
	sendErr error
}

func (c *recordingConn) Connect(context.Context) error                   { return nil }
func (c *recordingConn) IsConnected() bool                               { return true }
func (c *recordingConn) Stop() error                                     { return nil }
func (c *recordingConn) State() *client.DeviceState                      { return nil }
func (c *recordingConn) SetPacketHandler(func(*pb.MeshPacket))           {}
func (c *recordingConn) Handle(proto.Message, func(proto.Message) error) {}

func (c *recordingConn) SendToRadio(msg *pb.ToRadio) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.sendErr != nil {
		return c.sendErr
	}
	c.writes = append(c.writes, msg)
	return nil
}

func (c *recordingConn) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.writes)
}

// attachedHub is a hub with a live transport and no session goroutine.
func attachedHub(t *testing.T, conn transportConn) *Hub {
	t.Helper()
	h, err := NewHub(Config{Enabled: true, Device: "/dev/fake", EmcomChannel: 1}, nil)
	if err != nil {
		t.Fatalf("NewHub: %v", err)
	}
	h.mu.Lock()
	h.conn = conn
	h.connected = true
	h.mu.Unlock()
	return h
}

// TestCancelledSendDoesNotTransmit is WF-R5-01: the caller gave up before the
// API was even entered, so nothing may be written to the radio and no history
// row may suggest the message started.
func TestCancelledSendDoesNotTransmit(t *testing.T) {
	conn := &recordingConn{}
	h := attachedHub(t, conn)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := h.SendChannelText(ctx, 1, "nie wysylaj tego", "admin"); err == nil {
		t.Fatalf("a cancelled send reported success")
	} else if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	if got := conn.count(); got != 0 {
		t.Fatalf("radio writes = %d, want none", got)
	}

	// An already-expired deadline behaves the same way.
	expired, cancelExpired := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancelExpired()
	if err := h.SendContactMessage(expired, "12345678", "tez nie", "admin"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want context.DeadlineExceeded", err)
	}
	if got := conn.count(); got != 0 {
		t.Fatalf("radio writes = %d after an expired request, want none", got)
	}

	// A live request still goes out: the guard must not break sending.
	if err := h.SendChannelText(context.Background(), 1, "normalna", "admin"); err != nil {
		t.Fatalf("normal send = %v", err)
	}
	if got := conn.count(); got != 1 {
		t.Fatalf("radio writes = %d, want the one normal send", got)
	}
}

// TestSendCancelledWhileWaitingForTheRadio covers the second half: a request
// queued behind another transmission must leave the queue the moment it is
// cancelled, and must not be transmitted when the radio frees up.
func TestSendCancelledWhileWaitingForTheRadio(t *testing.T) {
	conn := &recordingConn{}
	h := attachedHub(t, conn)

	// Somebody else holds the transmission slot.
	if err := h.acquireSend(context.Background()); err != nil {
		t.Fatalf("acquireSend: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- h.SendChannelText(ctx, 1, "czeka", "admin") }()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("a cancelled send stayed queued behind the radio")
	}

	// The slot is released: the cancelled request must not go out now.
	h.releaseSend()
	time.Sleep(50 * time.Millisecond)
	if got := conn.count(); got != 0 {
		t.Fatalf("the cancelled request was transmitted later: %d writes", got)
	}
}
