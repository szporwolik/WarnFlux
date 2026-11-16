package meshtastic

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	pb "github.com/kabili207/meshtastic-go/core/proto"
	"github.com/kabili207/meshtastic-go/transport/client"
	"google.golang.org/protobuf/proto"
)

// answeringConn is a live-looking transport that answers every written frame
// the way the device does: a queueStatus carrying the next packet id, in the
// order the frames arrive. refuse maps a write index (0-based) onto the routing
// error the device reports for that frame, so a test can make the device
// refuse one specific send.
type answeringConn struct {
	hub    *Hub
	refuse map[int]pb.Routing_Error

	mu     sync.Mutex
	texts_ []string
	nextID uint32
}

func (c *answeringConn) Connect(context.Context) error                   { return nil }
func (c *answeringConn) IsConnected() bool                               { return true }
func (c *answeringConn) Stop() error                                     { return nil }
func (c *answeringConn) State() *client.DeviceState                      { return nil }
func (c *answeringConn) SetPacketHandler(func(*pb.MeshPacket))           {}
func (c *answeringConn) Handle(proto.Message, func(proto.Message) error) {}

func (c *answeringConn) SendToRadio(msg *pb.ToRadio) error {
	pkt := msg.GetPacket()
	text := string(pkt.GetDecoded().GetPayload())

	c.mu.Lock()
	idx := len(c.texts_)
	c.texts_ = append(c.texts_, text)
	c.nextID++
	id := c.nextID
	res := c.refuse[idx]
	c.mu.Unlock()

	// The device answers while the sender still holds the transmission slot:
	// the pairing path must not need that slot back.
	if c.hub != nil && pkt != nil {
		c.hub.pairQueueStatus(id, res)
	}
	return nil
}

func (c *answeringConn) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.texts_)
}

func (c *answeringConn) texts() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.texts_)
}

// blockingRecorder stalls the history write of one send so a test can run
// another transmission while that send holds the transmission slot. Every other
// recorder call passes straight through.
type blockingRecorder struct {
	captureRecorder
	entered chan struct{}
	release chan struct{}
}

func (b *blockingRecorder) RecordMeshtasticMessage(ctx context.Context, direction, sender, recipient, channel, text, operator string, hops int, at time.Time) error {
	select {
	case b.entered <- struct{}{}:
	default:
	}
	<-b.release
	return b.captureRecorder.RecordMeshtasticMessage(ctx, direction, sender, recipient, channel, text, operator, hops, at)
}

// TestRetryDoesNotOvertakeAnotherTransmission is WF-R6-01: a send holds the
// transmission slot while it writes the history row, and the maintenance loop
// retries an older message meanwhile. The retry must take the same slot — a
// frame written ahead of an older one while keeping the older one's queue
// position makes the device's queueStatus land on the wrong message.
func TestRetryDoesNotOvertakeAnotherTransmission(t *testing.T) {
	conn := &answeringConn{refuse: map[int]pb.Routing_Error{1: pb.Routing_TOO_LARGE}}
	h := attachedHub(t, conn)
	conn.hub = h
	h.lastRadio.Store(time.Now().UnixNano())

	rec := &blockingRecorder{entered: make(chan struct{}, 1), release: make(chan struct{})}
	h.SetRecorder(rec)

	// An older direct message the maintenance loop is about to retry.
	old := &pendingSend{to: 0x11111111, at: time.Now(), lastTry: time.Now().Add(-time.Minute), text: "stara", wantAck: true}
	h.sendMu.Lock()
	h.echoQueue = append(h.echoQueue, old)
	h.sendMu.Unlock()

	sent := make(chan error, 1)
	go func() { sent <- h.SendContactMessage(context.Background(), "22222222", "nowa", "admin") }()

	select {
	case <-rec.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the new send never reached the history write")
	}

	// The retry runs while the new send still holds the slot.
	retried := make(chan struct{})
	go func() {
		defer close(retried)
		h.retryPending(time.Now())
	}()

	time.Sleep(100 * time.Millisecond)
	if got := conn.texts(); len(got) != 0 {
		t.Fatalf("the retry wrote %v while another transmission held the slot", got)
	}

	close(rec.release)
	if err := <-sent; err != nil {
		t.Fatalf("the new send failed: %v", err)
	}
	select {
	case <-retried:
	case <-time.After(5 * time.Second):
		t.Fatal("the retry never left the maintenance pass")
	}

	if got, want := conn.texts(), []string{"nowa", "stara"}; !slices.Equal(got, want) {
		t.Fatalf("frames written = %v, want %v", got, want)
	}

	// The device answered in the order it received the frames, so the id of
	// each answer belongs to that frame's message.
	h.sendMu.Lock()
	first := h.pending[1]
	oldID := old.id
	h.sendMu.Unlock()
	if first == nil || first.text != "nowa" {
		t.Fatalf("packet id 1 paired with %v, want the new message", first)
	}
	if oldID != 2 {
		t.Fatalf("the retried message got packet id %d, want the second one (2)", oldID)
	}

	// The device refused the retried frame: the refusal settles THAT message
	// and leaves the other one alone.
	h.sendMu.Lock()
	newStatus := first.status
	oldStatus, oldReason := old.status, old.failReason
	h.sendMu.Unlock()
	if newStatus != TxSent {
		t.Fatalf("the new message settled %q, want %q", newStatus, TxSent)
	}
	if oldStatus != TxFailed {
		t.Fatalf("the retried message settled %q, want %q", oldStatus, TxFailed)
	}
	if oldReason != sendFailureReason(pb.Routing_TOO_LARGE) {
		t.Fatalf("the retried message carries reason %q, want %q", oldReason, sendFailureReason(pb.Routing_TOO_LARGE))
	}
}

// swappingConn loses the first frame (the socket the peer closed) and hands the
// session over to a live replacement, which is what the session goroutine does
// after a broken write.
type swappingConn struct {
	hub   *Hub
	fresh transportConn

	mu     sync.Mutex
	failed bool
}

func (c *swappingConn) Connect(context.Context) error                   { return nil }
func (c *swappingConn) IsConnected() bool                               { return true }
func (c *swappingConn) Stop() error                                     { return nil }
func (c *swappingConn) State() *client.DeviceState                      { return nil }
func (c *swappingConn) SetPacketHandler(func(*pb.MeshPacket))           {}
func (c *swappingConn) Handle(proto.Message, func(proto.Message) error) {}

func (c *swappingConn) SendToRadio(*pb.ToRadio) error {
	c.mu.Lock()
	if c.failed {
		c.mu.Unlock()
		return errors.New("write: broken pipe")
	}
	c.failed = true
	c.mu.Unlock()
	c.hub.mu.Lock()
	c.hub.conn = c.fresh
	c.hub.mu.Unlock()
	return errors.New("write: broken pipe")
}

// TestRetriedFrameKeepsItsQueuePosition pins the other half of WF-R6-01: a frame
// whose write died with the link is re-written on the replacement, and the
// device's answer must still pair with THAT message — the retry must not leave
// the queue entry behind and let the answer fall to the next frame.
func TestRetriedFrameKeepsItsQueuePosition(t *testing.T) {
	fresh := &answeringConn{}
	stale := &swappingConn{fresh: fresh}
	h := attachedHub(t, stale)
	stale.hub = h
	fresh.hub = h
	h.lastRadio.Store(time.Now().UnixNano())

	if err := h.SendContactMessage(context.Background(), "22222222", "przez nowy link", "admin"); err != nil {
		t.Fatalf("the send did not ride the rebuilt link: %v", err)
	}
	if got, want := fresh.texts(), []string{"przez nowy link"}; !slices.Equal(got, want) {
		t.Fatalf("frames on the rebuilt link = %v, want %v", got, want)
	}
	h.sendMu.Lock()
	paired, status := h.pending[1], ""
	if paired != nil {
		status = paired.status
	}
	h.sendMu.Unlock()
	if paired == nil || paired.text != "przez nowy link" {
		t.Fatalf("the device's answer paired with %v, want the retried frame", paired)
	}
	if status != TxSent {
		t.Fatalf("the retried frame settled %q, want %q", status, TxSent)
	}
}

// TestSendStampIsWrittenUnderTheSendLock is WF-R6-02: the reconnect retry
// refreshes the pairing stamp of an entry the maintenance loop is settling at
// the same time. Both touch it under the send lock — without it the runtime
// detector reports a data race on this test.
func TestSendStampIsWrittenUnderTheSendLock(t *testing.T) {
	conn := &recordingConn{}
	h := attachedHub(t, conn)
	h.lastRadio.Store(time.Now().UnixNano())

	ps := &pendingSend{to: 0x11111111, at: time.Now(), lastTry: time.Now().Add(-time.Minute), text: "stara", wantAck: true}
	h.sendMu.Lock()
	h.echoQueue = append(h.echoQueue, ps)
	h.sendMu.Unlock()

	msg := &pb.ToRadio{PayloadVariant: &pb.ToRadio_Packet{Packet: &pb.MeshPacket{
		To:      0x11111111,
		WantAck: true,
		PayloadVariant: &pb.MeshPacket_Decoded{Decoded: &pb.Data{
			Portnum: pb.PortNum_TEXT_MESSAGE_APP,
			Payload: []byte("stara"),
		}},
	}}}
	stale := &recordingConn{}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			h.lastRadio.Store(time.Now().UnixNano())
			h.retryPending(time.Now())
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			if err := h.retrySendOnAFreshLink(context.Background(), ps, msg, stale, errors.New("broken pipe")); err != nil {
				t.Errorf("retry on a fresh link = %v", err)
				return
			}
		}
	}()
	wg.Wait()
}

// TestBusyRetryDoesNotStealTheNextAck is WF-R7-01: the transmission slot stays
// taken past sendSlotWait, so the retry of an unconfirmed message gives up. It
// must NOT go back into the echo queue: that position claims the next answer the
// device reports, and the next answer belongs to the message the operator sent
// meanwhile. The deferred retry waits outside the pairing and gets its own id
// once it is really written.
func TestBusyRetryDoesNotStealTheNextAck(t *testing.T) {
	conn := &answeringConn{}
	h := attachedHub(t, conn)
	conn.hub = h
	h.lastRadio.Store(time.Now().UnixNano())

	// Somebody holds the slot (a history write, a slow broker).
	if err := h.acquireSend(context.Background()); err != nil {
		t.Fatalf("acquireSend: %v", err)
	}
	old := &pendingSend{
		to: 0x11111111, at: time.Now(),
		// Due for a retry, but still young enough to claim an answer: that is
		// exactly what makes the stale queue position dangerous.
		lastTry: time.Now().Add(-dmAckTimeout - time.Second),
		text:    "old retry", wantAck: true,
	}
	h.sendMu.Lock()
	h.echoQueue = append(h.echoQueue, old)
	h.sendMu.Unlock()

	done := make(chan struct{})
	go func() {
		defer close(done)
		h.retryPending(time.Now())
	}()
	select {
	case <-done:
	case <-time.After(sendSlotWait + 5*time.Second):
		t.Fatal("the maintenance pass never gave up on the busy slot")
	}
	h.releaseSend()

	// The deferred retry must spend no attempt: it never reached the radio.
	h.sendMu.Lock()
	retries := old.retries
	h.sendMu.Unlock()
	if retries != 0 {
		t.Fatalf("the deferred retry spent %d attempts, want none", retries)
	}

	if err := h.SendContactMessage(context.Background(), "22222222", "new message", "admin"); err != nil {
		t.Fatalf("the new send failed: %v", err)
	}
	if got, want := conn.texts(), []string{"new message"}; !slices.Equal(got, want) {
		t.Fatalf("frames written = %v, want only %v", got, want)
	}
	h.sendMu.Lock()
	paired, oldID := h.pending[1], old.id
	h.sendMu.Unlock()
	if paired == nil || paired.text != "new message" {
		t.Fatalf("the device's answer paired with %v, want the new message", paired)
	}
	if oldID != 0 {
		t.Fatalf("the deferred retry took packet id %d", oldID)
	}

	// The next free pass sends the old message, and only then does it get an id.
	h.retryPending(time.Now())
	if got, want := conn.texts(), []string{"new message", "old retry"}; !slices.Equal(got, want) {
		t.Fatalf("frames written = %v, want %v", got, want)
	}
	h.sendMu.Lock()
	oldID, retries = old.id, old.retries
	h.sendMu.Unlock()
	if oldID != 2 {
		t.Fatalf("the retried message got packet id %d, want 2", oldID)
	}
	if retries != 1 {
		t.Fatalf("the retried message spent %d attempts, want exactly one", retries)
	}
}

// TestCancelledTracerouteDoesNotTransmit is WF-R7-02: an already cancelled probe
// must not reach the radio, and a probe cancelled while it waits for the
// transmission slot must return promptly instead of transmitting afterwards.
func TestCancelledTracerouteDoesNotTransmit(t *testing.T) {
	conn := &answeringConn{}
	h := attachedHub(t, conn)
	conn.hub = h

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := h.Traceroute(cancelled, "11111111", time.Second); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled traceroute = %v, want context.Canceled", err)
	}
	if got := conn.count(); got != 0 {
		t.Fatalf("frames written by a cancelled probe = %d, want none", got)
	}

	// Cancelled while another transmission holds the slot.
	if err := h.acquireSend(context.Background()); err != nil {
		t.Fatalf("acquireSend: %v", err)
	}
	ctx, cancelLater := context.WithCancel(context.Background())
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancelLater()
	}()
	started := time.Now()
	_, err := h.Traceroute(ctx, "11111111", 30*time.Second)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("probe cancelled waiting for the slot = %v, want context.Canceled", err)
	}
	if waited := time.Since(started); waited >= sendSlotWait {
		t.Fatalf("the cancelled probe waited %v for the slot", waited.Round(time.Millisecond))
	}
	h.releaseSend()
	if got := conn.count(); got != 0 {
		t.Fatalf("frames written after the cancel = %d, want none", got)
	}
}

// TestUnicodeTextFitsOneRadioFrame is WF-R7-04: the rune limit alone let
// non-ASCII text exceed the payload one frame can carry, so the message the
// station "trimmed to the limit" could not be delivered at all. Both bounds have
// to hold, on a rune boundary, and an alert never loses its marker.
func TestUnicodeTextFitsOneRadioFrame(t *testing.T) {
	cases := []struct {
		name string
		text string
	}{
		{"ascii", strings.Repeat("x", 400)},
		{"polish", strings.Repeat("ą", 400)},
		{"cyrillic", strings.Repeat("д", 400)},
		{"emoji", strings.Repeat("🔔", 200)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			plain := PlainText(tc.text)
			if len(plain) > meshTextMaxBytes {
				t.Fatalf("PlainText = %d bytes, want at most %d", len(plain), meshTextMaxBytes)
			}
			if !utf8.ValidString(plain) {
				t.Fatalf("PlainText left invalid UTF-8: %q", plain)
			}
			if n := len([]rune(plain)); n > meshTextMaxRunes {
				t.Fatalf("PlainText = %d runes, want at most %d", n, meshTextMaxRunes)
			}

			alert := AlertText(tc.text)
			if len(alert) > meshTextMaxBytes {
				t.Fatalf("AlertText = %d bytes, want at most %d", len(alert), meshTextMaxBytes)
			}
			if !utf8.ValidString(alert) {
				t.Fatalf("AlertText left invalid UTF-8: %q", alert)
			}
			if !strings.HasSuffix(alert, AlertMarker) || !strings.HasPrefix(alert, AlertEmoji) {
				t.Fatalf("AlertText lost its framing: %q", alert)
			}
		})
	}

	// The alert framing must survive the send path's backstop too.
	conn := &recordingConn{}
	h := attachedHub(t, conn)
	long := AlertText(strings.Repeat("ż", meshTextMaxRunes+40))
	if err := h.SendChannelText(context.Background(), 1, long, "admin"); err != nil {
		t.Fatalf("SendChannelText: %v", err)
	}
	written := conn.writes
	if len(written) != 1 {
		t.Fatalf("frames written = %d, want one", len(written))
	}
	payload := written[0].GetPacket().GetDecoded().GetPayload()
	if len(payload) > meshTextMaxBytes {
		t.Fatalf("payload = %d bytes, want at most %d", len(payload), meshTextMaxBytes)
	}
	if !utf8.Valid(payload) || !strings.HasSuffix(string(payload), AlertMarker) {
		t.Fatalf("payload lost its UTF-8 or its alert marker: %q", payload)
	}

	// A caller that skipped the helpers cannot slip an oversized frame through.
	raw := strings.Repeat("э", 300)
	if err := h.SendContactMessage(context.Background(), "22222222", raw, "admin"); err != nil {
		t.Fatalf("SendContactMessage: %v", err)
	}
	if len(conn.writes) != 2 {
		t.Fatalf("frames written = %d, want two", len(conn.writes))
	}
	payload = conn.writes[1].GetPacket().GetDecoded().GetPayload()
	if len(payload) > meshTextMaxBytes || !utf8.Valid(payload) {
		t.Fatalf("unbounded payload reached the transport: %d bytes, valid=%v", len(payload), utf8.Valid(payload))
	}
}
