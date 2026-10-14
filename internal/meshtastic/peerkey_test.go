package meshtastic

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/kabili207/meshtastic-go/core/proto"
	"google.golang.org/protobuf/proto"
)

// peerNodeInfo builds the NodeInfo a peer announces over the air. The public
// key is what the radio needs to encrypt a direct message to that node; nil
// models a node that announces none — no direct message can ever reach it.
func peerNodeInfo(from uint32, longName, shortName string, key []byte) *pb.MeshPacket {
	payload, err := proto.Marshal(&pb.User{
		Id:        fmt.Sprintf("!%08x", from),
		LongName:  longName,
		ShortName: shortName,
		PublicKey: key,
	})
	if err != nil {
		panic(err)
	}
	return &pb.MeshPacket{
		From: from,
		To:   0xffffffff,
		PayloadVariant: &pb.MeshPacket_Decoded{Decoded: &pb.Data{
			Portnum: pb.PortNum_NODEINFO_APP,
			Payload: payload,
		}},
	}
}

// textFramesTo counts the transmissions of one text to one node: the frame on
// the wire is the only proof that a retry actually left the station.
func textFramesTo(r *testRadio, to uint32, text string) int {
	n := 0
	for _, p := range r.outbound() {
		d := p.GetDecoded()
		if d == nil || d.GetPortnum() != pb.PortNum_TEXT_MESSAGE_APP || p.GetTo() != to {
			continue
		}
		if string(d.GetPayload()) == text {
			n++
		}
	}
	return n
}

// refusedTextSend drives one direct message into the refusal the way the
// device reports it (a queueStatus carrying the routing error) and returns the
// recorder holding its history row.
func refusedTextSend(t *testing.T, res pb.Routing_Error, text string) (*testRadio, *captureRecorder) {
	t.Helper()
	radio := newTestRadio(t, Config{Enabled: true, Device: "/dev/fake", NodeTTL: time.Hour,
		AckSettleTimeout: time.Hour})
	radio.waitConnected(t)
	rec := &captureRecorder{}
	radio.hub.SetRecorder(rec)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := radio.hub.SendContactMessage(ctx, fmt.Sprintf("%08x", peerKeyNode), text, "admin"); err != nil {
		t.Fatalf("SendContactMessage: %v", err)
	}
	dispatchQueueStatus(t, radio, res, 4242)
	waitFor(t, "the refusal to settle the message", func() bool {
		msgs := rec.messages()
		return len(msgs) == 1 && msgs[0].Status == TxFailed
	})
	return radio, rec
}

// newBareHub builds a hub with its maps initialized and no session (nothing
// runs until a test calls a pass itself); the log goes to the given capture so
// a test can assert what the hub said.
func newBareHubWithLogs(t *testing.T, logs *logCapture) *Hub {
	t.Helper()
	h, err := NewHub(Config{NodeTTL: time.Hour}, slog.New(slog.NewTextHandler(logs, nil)))
	if err != nil {
		t.Fatalf("NewHub: %v", err)
	}
	return h
}

// keyWaitCount counts the messages waiting for a peer key: the wait is a queue
// PER PEER now, so the length of the map is the number of peers, not messages.
func keyWaitCount(h *Hub) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.keyWaitsLocked()
}

// logCapture collects the hub's log lines for assertions (the hub logs from
// its own goroutines, so the buffer needs the lock).
type logCapture struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (c *logCapture) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buf.Write(p)
}

func (c *logCapture) String() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buf.String()
}

// peerKeyNode is the node every test here addresses its direct message to.
const peerKeyNode = 0x6d2c8f6c

// TestHubPeerKeyArrivalRetriesTheDirectMessage pins the one refusal the
// operator can neither see through nor fix: the radio does not know the
// recipient's public key, so the frame never enters the air. The station asks
// the peer for its NodeInfo — and when that key lands, the text the operator
// already typed goes out again instead of staying failed for good.
func TestHubPeerKeyArrivalRetriesTheDirectMessage(t *testing.T) {
	radio, rec := refusedTextSend(t, pb.Routing_PKI_UNKNOWN_PUBKEY, "Test")
	if got := rec.reasonFor("Test"); got != "pki_unknown_key" {
		t.Fatalf("reason = %q, want pki_unknown_key", got)
	}

	// The request for the peer's NodeInfo is what the whole recovery rests on.
	waitFor(t, "the node info request", func() bool {
		for _, p := range radio.outbound() {
			if p.GetTo() == peerKeyNode && p.GetDecoded().GetPortnum() == pb.PortNum_NODEINFO_APP &&
				p.GetDecoded().GetWantResponse() {
				return true
			}
		}
		return false
	})
	if got := textFramesTo(radio, peerKeyNode, "Test"); got != 1 {
		t.Fatalf("transmissions before the key = %d, want only the refused one", got)
	}

	// The peer answers with its key: the message goes out again.
	dispatchPkt(t, radio, peerNodeInfo(peerKeyNode, "ZAY0", "ZAY0", bytes.Repeat([]byte{0x11}, 32)))
	waitFor(t, "the message to be re-sent", func() bool { return textFramesTo(radio, peerKeyNode, "Test") == 2 })

	// The retry keeps the original history row (same stamp, same text), so the
	// panel flips THE message instead of showing a duplicate nobody typed.
	// The device acks every client send in order: the NodeInfo request that
	// opened the recovery, then the retried text.
	dispatchQueueStatus(t, radio, pb.Routing_NONE, 5150)
	dispatchQueueStatus(t, radio, pb.Routing_NONE, 5151)
	waitFor(t, "the retried message to settle sent", func() bool {
		msgs := rec.messages()
		return len(msgs) == 1 && msgs[0].Status == TxSent
	})
	if waiting := keyWaitCount(radio.hub); waiting != 0 {
		t.Fatalf("key waits left = %d, want the wait consumed by the retry", waiting)
	}
}

// TestHubPeerKeyWithoutAKeyDoesNotRetry pins that only a NodeInfo carrying a
// key releases the wait: an announcement without one leaves the radio unable
// to encrypt, so a retry would only spend airtime to be refused again.
func TestHubPeerKeyWithoutAKeyDoesNotRetry(t *testing.T) {
	radio, _ := refusedTextSend(t, pb.Routing_PKI_UNKNOWN_PUBKEY, "Test")
	dispatchPkt(t, radio, peerNodeInfo(peerKeyNode, "ZAY0", "ZAY0", nil))
	time.Sleep(150 * time.Millisecond) // give a wrong retry every chance to happen
	if got := textFramesTo(radio, peerKeyNode, "Test"); got != 1 {
		t.Fatalf("transmissions = %d, want no retry without a peer key", got)
	}
	if waiting := keyWaitCount(radio.hub); waiting != 1 {
		t.Fatalf("key waits = %d, want the message still waiting for the key", waiting)
	}
}

// TestHubPeerKeyRetryHappensOnce pins the guard against a peer that keeps
// announcing a key the radio cannot use: the message is re-sent ONCE, and a
// second refusal leaves it failed (the panel shows why) instead of re-arming
// the wait forever.
func TestHubPeerKeyRetryHappensOnce(t *testing.T) {
	radio, _ := refusedTextSend(t, pb.Routing_PKI_UNKNOWN_PUBKEY, "Test")
	key := bytes.Repeat([]byte{0x22}, 32)
	dispatchPkt(t, radio, peerNodeInfo(peerKeyNode, "ZAY0", "ZAY0", key))
	waitFor(t, "the message to be re-sent", func() bool { return textFramesTo(radio, peerKeyNode, "Test") == 2 })

	// The device refuses the retry the same way (the key was not the problem).
	dispatchQueueStatus(t, radio, pb.Routing_PKI_UNKNOWN_PUBKEY, 6001)
	dispatchPkt(t, radio, peerNodeInfo(peerKeyNode, "ZAY0", "ZAY0", key))
	time.Sleep(150 * time.Millisecond)
	if got := textFramesTo(radio, peerKeyNode, "Test"); got != 2 {
		t.Fatalf("transmissions = %d, want exactly one retry", got)
	}
	if waiting := keyWaitCount(radio.hub); waiting != 0 {
		t.Fatalf("key waits = %d, want none after a refused retry", waiting)
	}
}

// TestHubPeerKeyWaitWindowIsBounded pins the deadline: a key that lands after
// the refusal has gone stale belongs to something else, and the wait is
// dropped (the sweep) instead of piling up in memory.
func TestHubPeerKeyWaitWindowIsBounded(t *testing.T) {
	hub := &Hub{}
	ps := &pendingSend{to: peerKeyNode, text: "Test", wantAck: true}
	hub.keyRetries = map[uint32][]*keyRetry{
		peerKeyNode: {{ps: ps, when: time.Now().Add(-keyWaitWindow - time.Minute)}},
	}
	if kept := hub.takeKeyRetry(peerKeyNode); len(kept) != 0 {
		t.Fatal("a key that arrived after the window still retried the message")
	}
	hub.keyRetries[peerKeyNode] = []*keyRetry{{ps: ps, when: time.Now().Add(-keyWaitWindow - time.Minute)}}
	hub.expireKeyRetries(time.Now())
	if _, ok := hub.keyRetries[peerKeyNode]; ok {
		t.Fatal("a stale wait was kept")
	}
}

// peerKeySet reads the accessor the panel uses, as a set for easy assertions.
func peerKeySet(h *Hub) map[string]bool {
	out := map[string]bool{}
	for _, id := range h.PeerKeyNodes() {
		out[id] = true
	}
	return out
}

// TestHubPeerKeyNodesFollowsTheEvidence pins the key state the panel shows:
// it comes from the device's own node table at the handshake and from every
// NodeInfo heard afterwards — and an announcement WITHOUT a key takes the mark
// back, because that is the state the radio will act on when a DM is sent.
func TestHubPeerKeyNodesFollowsTheEvidence(t *testing.T) {
	keyed := nodeInfo(0x6d2c8f6c, "SPM", "SPM")
	keyed.User.PublicKey = bytes.Repeat([]byte{0x33}, 32)
	keyless := nodeInfo(0xdeadbeef, "Bez klucza", "BK")
	radio := newTestRadio(t, Config{Enabled: true, Device: "/dev/fake", NodeTTL: time.Hour},
		testRadioOpts{nodes: []*pb.NodeInfo{keyed, keyless}})
	radio.waitConnected(t)
	waitFor(t, "the handshake to record the peer key", func() bool { return peerKeySet(radio.hub)["6d2c8f6c"] })
	if peerKeySet(radio.hub)["deadbeef"] {
		t.Fatal("a node with no key in the device table was reported as keyed")
	}

	// An announcement adds a key the table did not have (the fresh-announce
	// path: the radio learns it from the same packet).
	dispatchPkt(t, radio, peerNodeInfo(0x11223344, "Nowy", "NEW", bytes.Repeat([]byte{0x44}, 32)))
	waitFor(t, "the announced key", func() bool { return peerKeySet(radio.hub)["11223344"] })

	// A later NodeInfo without a key withdraws the mark: the panel must not
	// promise a direct message the radio will refuse.
	dispatchPkt(t, radio, peerNodeInfo(0x6d2c8f6c, "SPM", "SPM", nil))
	waitFor(t, "the withdrawn key", func() bool { return !peerKeySet(radio.hub)["6d2c8f6c"] })
}

// routingRefusedTextSend drives one direct message into the refusal the radio
// reports through the other path: the device ACCEPTS the frame first (a
// queueStatus that pairs the packet id) and the refusal then arrives as a
// ROUTING_APP frame carrying the routing error. Both paths must start the same
// recovery — that is the whole point of the test below.
func routingRefusedTextSend(t *testing.T, res pb.Routing_Error, text string) (*testRadio, *captureRecorder) {
	t.Helper()
	radio := newTestRadio(t, Config{Enabled: true, Device: "/dev/fake", NodeTTL: time.Hour,
		AckSettleTimeout: time.Hour})
	radio.waitConnected(t)
	rec := &captureRecorder{}
	radio.hub.SetRecorder(rec)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := radio.hub.SendContactMessage(ctx, fmt.Sprintf("%08x", peerKeyNode), text, "admin"); err != nil {
		t.Fatalf("SendContactMessage: %v", err)
	}
	// The device took the frame (no error), which is what pairs the id.
	dispatchQueueStatus(t, radio, pb.Routing_NONE, 4242)
	routing, err := proto.Marshal(&pb.Routing{Variant: &pb.Routing_ErrorReason{ErrorReason: res}})
	if err != nil {
		t.Fatal(err)
	}
	dispatchPkt(t, radio, &pb.MeshPacket{
		From: 0xabcd1234, To: 0xabcd1234, Id: 4242,
		PayloadVariant: &pb.MeshPacket_Decoded{
			Decoded: &pb.Data{Portnum: pb.PortNum_ROUTING_APP, RequestId: 4242, Payload: routing},
		},
	})
	waitFor(t, "the routing refusal to settle the message", func() bool {
		msgs := rec.messages()
		return len(msgs) == 1 && msgs[0].Status == TxFailed
	})
	return radio, rec
}

// TestHubRoutingRefusalRecoversTheSameWay pins the ticket case: a PKI refusal
// that arrives as a ROUTING_APP error must start the SAME recovery as the one
// the device reports in a queueStatus. Asking for the peer's NodeInfo without
// remembering the message means the key arrives and the operator's text stays
// failed — the recovery has to be one code path, not two.
func TestHubRoutingRefusalRecoversTheSameWay(t *testing.T) {
	radio, rec := routingRefusedTextSend(t, pb.Routing_PKI_UNKNOWN_PUBKEY, "Test")
	if got := rec.reasonFor("Test"); got != "pki_unknown_key" {
		t.Fatalf("reason = %q, want pki_unknown_key", got)
	}

	waitFor(t, "the node info request", func() bool {
		for _, p := range radio.outbound() {
			if p.GetTo() == peerKeyNode && p.GetDecoded().GetPortnum() == pb.PortNum_NODEINFO_APP &&
				p.GetDecoded().GetWantResponse() {
				return true
			}
		}
		return false
	})
	if got := textFramesTo(radio, peerKeyNode, "Test"); got != 1 {
		t.Fatalf("transmissions before the key = %d, want only the refused one", got)
	}

	// The peer answers with its key: the message goes out again, once.
	dispatchPkt(t, radio, peerNodeInfo(peerKeyNode, "ZAY0", "ZAY0", bytes.Repeat([]byte{0x11}, 32)))
	waitFor(t, "the message to be re-sent", func() bool { return textFramesTo(radio, peerKeyNode, "Test") == 2 })

	// Exactly one retry: another announcement of the same key arms nothing.
	dispatchPkt(t, radio, peerNodeInfo(peerKeyNode, "ZAY0", "ZAY0", bytes.Repeat([]byte{0x22}, 32)))
	time.Sleep(100 * time.Millisecond)
	if got := textFramesTo(radio, peerKeyNode, "Test"); got != 2 {
		t.Fatalf("transmissions = %d, want exactly one retry", got)
	}

	// The retry keeps the original history row, so the panel flips THE message
	// instead of showing a duplicate nobody typed. The device acks every client
	// send in order: the NodeInfo request, then the retried text.
	dispatchQueueStatus(t, radio, pb.Routing_NONE, 5150)
	dispatchQueueStatus(t, radio, pb.Routing_NONE, 5151)
	waitFor(t, "the retried message to settle sent", func() bool {
		msgs := rec.messages()
		return len(msgs) == 1 && msgs[0].Status == TxSent
	})
	if waiting := keyWaitCount(radio.hub); waiting != 0 {
		t.Fatalf("key waits left = %d, want the wait consumed by the retry", waiting)
	}
}

// TestHubPeerKeyRefusalKeepsEveryMessageForThePeer pins the ticket case: two
// messages to the SAME node refused for the missing key before it lands. The
// wait is a queue per peer, not a single slot — the second refusal must not
// push the first message out — so one key announcement re-sends BOTH, once
// each.
func TestHubPeerKeyRefusalKeepsEveryMessageForThePeer(t *testing.T) {
	radio := newTestRadio(t, Config{Enabled: true, Device: "/dev/fake", NodeTTL: time.Hour,
		AckSettleTimeout: time.Hour})
	radio.waitConnected(t)
	rec := &captureRecorder{}
	radio.hub.SetRecorder(rec)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for _, text := range []string{"One", "Two"} {
		if err := radio.hub.SendContactMessage(ctx, fmt.Sprintf("%08x", peerKeyNode), text, "admin"); err != nil {
			t.Fatalf("SendContactMessage(%s): %v", text, err)
		}
	}
	// The device refuses both, for the same missing key (the ids pair the
	// sends in order: the device answers every client send).
	dispatchQueueStatus(t, radio, pb.Routing_PKI_UNKNOWN_PUBKEY, 4242)
	dispatchQueueStatus(t, radio, pb.Routing_PKI_UNKNOWN_PUBKEY, 6001)
	waitFor(t, "both refusals to settle the messages", func() bool {
		msgs := rec.messages()
		if len(msgs) != 2 {
			return false
		}
		for _, m := range msgs {
			if m.Status != TxFailed {
				return false
			}
		}
		return true
	})
	for _, text := range []string{"One", "Two"} {
		if got := rec.reasonFor(text); got != "pki_unknown_key" {
			t.Fatalf("reason for %q = %q, want pki_unknown_key", text, got)
		}
		if got := textFramesTo(radio, peerKeyNode, text); got != 1 {
			t.Fatalf("transmissions of %q before the key = %d, want only the refused one", text, got)
		}
	}
	if waiting := keyWaitCount(radio.hub); waiting != 2 {
		t.Fatalf("key waits = %d, want one per refused message", waiting)
	}

	// ONE key announcement releases both messages, once each.
	dispatchPkt(t, radio, peerNodeInfo(peerKeyNode, "ZAY0", "ZAY0", bytes.Repeat([]byte{0x33}, 32)))
	waitFor(t, "both messages to be re-sent", func() bool {
		return textFramesTo(radio, peerKeyNode, "One") == 2 && textFramesTo(radio, peerKeyNode, "Two") == 2
	})
	if waiting := keyWaitCount(radio.hub); waiting != 0 {
		t.Fatalf("key waits = %d, want the queue consumed by the retries", waiting)
	}

	// A second announcement of the same key arms nothing: one retry per
	// message, not one per NodeInfo.
	dispatchPkt(t, radio, peerNodeInfo(peerKeyNode, "ZAY0", "ZAY0", bytes.Repeat([]byte{0x33}, 32)))
	time.Sleep(150 * time.Millisecond)
	for _, text := range []string{"One", "Two"} {
		if got := textFramesTo(radio, peerKeyNode, text); got != 2 {
			t.Fatalf("transmissions of %q = %d, want exactly one retry", text, got)
		}
	}
}

// TestHubPeerKeyRefusalQueueIsBounded pins the bound and its explicitness: past
// the per-peer limit the message is NOT remembered — it keeps its failed row
// and the hub says so in the log, instead of the queue growing silently and
// pretending the retry is still coming.
func TestHubPeerKeyRefusalQueueIsBounded(t *testing.T) {
	logs := &logCapture{}
	radio := newTestRadio(t, Config{Enabled: true, Device: "/dev/fake", NodeTTL: time.Hour,
		AckSettleTimeout: time.Hour}, testRadioOpts{
		setup: func(h *Hub) {
			h.logger = slog.New(slog.NewTextHandler(logs, nil))
			h.keyRetryPerPeerMax = 2
		},
	})
	radio.waitConnected(t)
	rec := &captureRecorder{}
	radio.hub.SetRecorder(rec)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	texts := []string{"One", "Two", "Three"}
	for _, text := range texts {
		if err := radio.hub.SendContactMessage(ctx, fmt.Sprintf("%08x", peerKeyNode), text, "admin"); err != nil {
			t.Fatalf("SendContactMessage(%s): %v", text, err)
		}
	}
	for i, id := range []uint32{4242, 6001, 6002} {
		dispatchQueueStatus(t, radio, pb.Routing_PKI_UNKNOWN_PUBKEY, id)
		_ = i
	}
	waitFor(t, "all three refusals to settle failed", func() bool {
		msgs := rec.messages()
		if len(msgs) != 3 {
			return false
		}
		for _, m := range msgs {
			if m.Status != TxFailed {
				return false
			}
		}
		return true
	})
	if waiting := keyWaitCount(radio.hub); waiting != 2 {
		t.Fatalf("key waits = %d, want the per-peer bound of 2", waiting)
	}
	if log := logs.String(); !strings.Contains(log, "too many messages wait for a peer key") {
		t.Fatalf("the overflow was silent; log lines: %s", log)
	}

	// The two remembered messages go out again; the refused third one stays
	// failed with its reason (the panel shows why).
	dispatchPkt(t, radio, peerNodeInfo(peerKeyNode, "ZAY0", "ZAY0", bytes.Repeat([]byte{0x44}, 32)))
	waitFor(t, "the remembered messages to be re-sent", func() bool {
		return textFramesTo(radio, peerKeyNode, "One") == 2 && textFramesTo(radio, peerKeyNode, "Two") == 2
	})
	time.Sleep(100 * time.Millisecond)
	if got := textFramesTo(radio, peerKeyNode, "Three"); got != 1 {
		t.Fatalf("transmissions of the overflowed message = %d, want it left failed", got)
	}
	if gone := keyWaitCount(radio.hub); gone != 0 {
		t.Fatalf("key waits = %d, want the queue drained", gone)
	}
}

// TestHubPeerKeyRefusalTotalQueueIsBounded pins the second bound: the whole
// hub keeps at most keyRetryTotal messages, so many peers with missing keys
// cannot grow the wait queue together.
func TestHubPeerKeyRefusalTotalQueueIsBounded(t *testing.T) {
	logs := &logCapture{}
	hub := &Hub{}
	hub.logger = slog.New(slog.NewTextHandler(logs, nil))
	hub.keyRetryTotalMax = 2
	for i := 0; i < 4; i++ {
		hub.awaitPeerKey(&pendingSend{to: peerKeyNode + uint32(i), text: "Test", wantAck: true})
	}
	if waiting := keyWaitCount(hub); waiting != 2 {
		t.Fatalf("key waits = %d, want the total bound of 2", waiting)
	}
	if log := logs.String(); !strings.Contains(log, "too many messages wait for a peer key") {
		t.Fatalf("the overflow was silent; log lines: %s", log)
	}
}

// TestHubPeerKeyRefusalLeavesDurableAlertsToTheWorker pins the double-retry
// ticket: an alert (a message with a durable action-progress identity) refused
// for a missing key is ALREADY re-armed by recording the failure, so the worker
// re-sends it. Remembering it for the key as well put the same text on the air
// three times: the refused copy, the worker's retry, and the earlier copy the
// key released. The key still has to be refreshed — that part is shared.
func TestHubPeerKeyRefusalLeavesDurableAlertsToTheWorker(t *testing.T) {
	radio := newTestRadio(t, Config{Enabled: true, Device: "/dev/fake", NodeTTL: time.Hour,
		AckSettleTimeout: time.Hour})
	radio.waitConnected(t)
	rec := &captureRecorder{}
	radio.hub.SetRecorder(rec)

	prog := ProgressRef{ActionID: "mesh-main", GroupID: 7, DedupKey: "c:1",
		Publisher: "p1", EventKey: "k", ChangeID: 1}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	send := func() {
		t.Helper()
		if err := radio.hub.SendContactMessageVersioned(ctx, fmt.Sprintf("%08x", peerKeyNode),
			"alarm", "admin", prog); err != nil {
			t.Fatalf("SendContactMessageVersioned: %v", err)
		}
	}
	send()
	// The device refuses the alert for the missing peer key.
	dispatchQueueStatus(t, radio, pb.Routing_PKI_UNKNOWN_PUBKEY, 4242)
	waitFor(t, "the refusal to settle the alert", func() bool {
		msgs := rec.messages()
		return len(msgs) == 1 && msgs[0].Status == TxFailed
	})
	if got := rec.rearmedCount(); got != 1 {
		t.Fatalf("re-armed jobs = %d, want 1: the durable queue owns this retry", got)
	}
	// The peer is still asked for its key (that is what unblocks later sends).
	waitFor(t, "the node info request", func() bool {
		for _, p := range radio.outbound() {
			if p.GetTo() == peerKeyNode && p.GetDecoded().GetPortnum() == pb.PortNum_NODEINFO_APP &&
				p.GetDecoded().GetWantResponse() {
				return true
			}
		}
		return false
	})

	// The worker retries the alert (the durable queue's own re-send)...
	send()
	waitFor(t, "the worker's retry", func() bool { return textFramesTo(radio, peerKeyNode, "alarm") == 2 })

	// ...and the key lands: nothing else may go out, the earlier copy is dead.
	dispatchPkt(t, radio, peerNodeInfo(peerKeyNode, "ZAY0", "ZAY0", bytes.Repeat([]byte{0x55}, 32)))
	time.Sleep(200 * time.Millisecond)
	if got := textFramesTo(radio, peerKeyNode, "alarm"); got != 2 {
		t.Fatalf("transmissions = %d, want 2 (the refused copy and the worker's retry): the key must not re-send the old copy", got)
	}
	if waiting := keyWaitCount(radio.hub); waiting != 0 {
		t.Fatalf("key waits = %d, want none", waiting)
	}
}

// TestHubExpiryForgetsPeerState pins the ticket case: the node directory is
// pruned by retention, and the two helper maps must follow it. Batches of new
// nodes that go quiet and expire must leave NOTHING behind — the reported
// reproduction was an empty directory with 100 entries in each map, and every
// further batch grew them again.
func TestHubExpiryForgetsPeerState(t *testing.T) {
	h := newBareHubWithLogs(t, &logCapture{})
	h.cfg.NodeTTL = time.Hour
	keys := func() int {
		h.mu.Lock()
		defer h.mu.Unlock()
		return len(h.peerKeys)
	}
	asked := func() int {
		h.mu.Lock()
		defer h.mu.Unlock()
		return len(h.nodeInfoAsked)
	}
	for batch := 0; batch < 3; batch++ {
		base := uint32(0x20000000 + batch*0x100)
		now := time.Now()
		for i := 0; i < 100; i++ {
			h.SeedNode(fmt.Sprintf("%08x", base+uint32(i)), "visitor", "VIS", 0, 0, now, nil)
		}
		// Every one of them announced a key and was asked for a refresh (the
		// refusal path does both).
		h.mu.Lock()
		for i := 0; i < 100; i++ {
			h.rememberPeerKeyLocked(base + uint32(i))
		}
		h.mu.Unlock()
		// The refresh request is the real writer of the second map (there is no
		// session here, so it only records the entry).
		for i := 0; i < 100; i++ {
			h.requestNodeInfo(base + uint32(i))
		}
		if got := keys(); got != 100 {
			t.Fatalf("batch %d: key marks = %d, want 100", batch, got)
		}
		if got := asked(); got != 100 {
			t.Fatalf("batch %d: refresh entries = %d, want 100", batch, got)
		}

		// They go quiet past the directory retention: everything about them
		// goes away, so the next batch starts from zero again.
		h.mu.Lock()
		for _, n := range h.nodes {
			n.LastSeen = time.Now().Add(-nodeDirRetention - time.Hour)
		}
		h.mu.Unlock()
		h.expireNodes()
		h.sweepPeerState(time.Now().Add(nodeInfoRefreshGap + time.Minute))
		if got := directorySize(h); got != 0 {
			t.Fatalf("batch %d: directory = %d, want the nodes dropped", batch, got)
		}
		if got := keys(); got != 0 {
			t.Fatalf("batch %d: key marks = %d, want none for a pruned directory", batch, got)
		}
		if got := asked(); got != 0 {
			t.Fatalf("batch %d: refresh entries = %d, want none for a pruned directory", batch, got)
		}
	}
}

// TestHubNodeInfoRefreshWaitExpires pins the deadline of the refresh rate
// limit: an entry older than the gap suppresses nothing (the next refusal may
// ask again), so keeping it would only be memory. The limit itself still works
// inside the window.
func TestHubNodeInfoRefreshWaitExpires(t *testing.T) {
	h := &Hub{}
	h.requestNodeInfo(peerKeyNode) // no session: the entry is what matters here
	h.mu.Lock()
	first, ok := h.nodeInfoAsked[peerKeyNode]
	h.mu.Unlock()
	if !ok {
		t.Fatal("the refresh request was not remembered")
	}
	h.requestNodeInfo(peerKeyNode) // inside the gap: nothing happens
	h.mu.Lock()
	second := h.nodeInfoAsked[peerKeyNode]
	h.mu.Unlock()
	if !second.Equal(first) {
		t.Fatal("a second request inside the gap moved the rate limit")
	}
	h.sweepPeerState(time.Now())
	if got := func() int { h.mu.Lock(); defer h.mu.Unlock(); return len(h.nodeInfoAsked) }(); got != 1 {
		t.Fatalf("refresh entries inside the gap = %d, want the entry kept", got)
	}
	h.sweepPeerState(time.Now().Add(nodeInfoRefreshGap + time.Minute))
	if got := func() int { h.mu.Lock(); defer h.mu.Unlock(); return len(h.nodeInfoAsked) }(); got != 0 {
		t.Fatalf("refresh entries after the gap = %d, want the entry expired", got)
	}
}

// TestHubPeerKeyMarksAreCapped pins the cap on the key marks: it drops what the
// panel cannot show first (marks of nodes the directory no longer holds), and
// past that it stops remembering new ones and counts them — reported once per
// sweep, never silently.
func TestHubPeerKeyMarksAreCapped(t *testing.T) {
	logs := &logCapture{}
	h := newBareHubWithLogs(t, logs)
	h.peerKeysMax = 2

	// Marks for nodes the directory does not hold: pressure clears them first,
	// so the panel never loses a mark it could display.
	h.mu.Lock()
	for i := 0; i < 4; i++ {
		h.rememberPeerKeyLocked(uint32(0x11111111 + i))
	}
	h.mu.Unlock()
	if got := len(h.peerKeys); got > 2 {
		t.Fatalf("key marks = %d, want the cap of 2 respected", got)
	}

	// Now every mark belongs to a node the directory holds: the surplus is
	// skipped and counted, and the sweep reports it.
	h.peerKeys = map[uint32]bool{}
	held := []string{}
	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("%08x", 0x30000000+i)
		held = append(held, id)
		h.SeedNode(id, "held", "HLD", 0, 0, time.Now(), nil)
	}
	h.mu.Lock()
	for i := 0; i < 3; i++ {
		h.rememberPeerKeyLocked(0x30000000 + uint32(i))
	}
	h.mu.Unlock()
	if got := len(h.peerKeys); got != 2 {
		t.Fatalf("key marks = %d, want the cap of 2", got)
	}
	h.mu.Lock()
	skipped := h.peerKeysSkipped
	h.mu.Unlock()
	if skipped != 1 {
		t.Fatalf("skipped marks = %d, want the one over the cap", skipped)
	}
	h.sweepPeerState(time.Now())
	if log := logs.String(); !strings.Contains(log, "peer key marks are at the cap") {
		t.Fatalf("the cap was silent; log lines: %s", log)
	}
	// The nodes stay in the directory: their own rows are untouched.
	if got := directorySize(h); got != len(held) {
		t.Fatalf("directory = %d, want the %d held nodes", got, len(held))
	}
}
