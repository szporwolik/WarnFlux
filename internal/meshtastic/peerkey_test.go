package meshtastic

import (
	"bytes"
	"context"
	"fmt"
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
	radio.hub.mu.Lock()
	waiting := len(radio.hub.keyRetries)
	radio.hub.mu.Unlock()
	if waiting != 0 {
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
	radio.hub.mu.Lock()
	waiting := len(radio.hub.keyRetries)
	radio.hub.mu.Unlock()
	if waiting != 1 {
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
	radio.hub.mu.Lock()
	waiting := len(radio.hub.keyRetries)
	radio.hub.mu.Unlock()
	if waiting != 0 {
		t.Fatalf("key waits = %d, want none after a refused retry", waiting)
	}
}

// TestHubPeerKeyWaitWindowIsBounded pins the deadline: a key that lands after
// the refusal has gone stale belongs to something else, and the wait is
// dropped (the sweep) instead of piling up in memory.
func TestHubPeerKeyWaitWindowIsBounded(t *testing.T) {
	hub := &Hub{}
	ps := &pendingSend{to: peerKeyNode, text: "Test", wantAck: true}
	hub.keyRetries = map[uint32]*keyRetry{
		peerKeyNode: {ps: ps, when: time.Now().Add(-keyWaitWindow - time.Minute)},
	}
	if kr := hub.takeKeyRetry(peerKeyNode); kr != nil {
		t.Fatal("a key that arrived after the window still retried the message")
	}
	hub.keyRetries[peerKeyNode] = &keyRetry{ps: ps, when: time.Now().Add(-keyWaitWindow - time.Minute)}
	hub.expireKeyRetries(time.Now())
	if _, ok := hub.keyRetries[peerKeyNode]; ok {
		t.Fatal("a stale wait was kept")
	}
}
