package meshtastic

import (
	"context"
	"testing"
	"time"

	pb "github.com/kabili207/meshtastic-go/core/proto"
)

// TestHubUnackedDirectMessageRecordsAReason pins the second silent failure an
// operator used to face: the device transmitted the frame, the recipient never
// acknowledged it, and the row settled "failed" with NO reason at all (prod,
// 2026-10-14 06:29 — a DM that looked refused but was merely unacknowledged).
func TestHubUnackedDirectMessageRecordsAReason(t *testing.T) {
	radio := newTestRadio(t, Config{Enabled: true, Device: "/dev/fake", NodeTTL: time.Hour,
		AckSettleTimeout: 40 * time.Millisecond})
	radio.waitConnected(t)
	rec := &captureRecorder{}
	radio.hub.SetRecorder(rec)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := radio.hub.SendContactMessage(ctx, "6d2c8f6c", "Cisza", "admin"); err != nil {
		t.Fatalf("SendContactMessage: %v", err)
	}
	dispatchQueueStatus(t, radio, pb.Routing_NONE, 7001)
	waitFor(t, "the send to be marked sent", func() bool {
		msgs := rec.messages()
		return len(msgs) == 1 && msgs[0].Status == TxSent
	})

	// The acknowledgment never arrives: the settle window closes and the
	// verdict has to name what actually happened.
	radio.hub.retryPending(time.Now().Add(60 * time.Millisecond))
	waitFor(t, "the unacknowledged message to settle failed", func() bool {
		msgs := rec.messages()
		return len(msgs) == 1 && msgs[0].Status == TxFailed
	})
	if got := rec.reasonFor("Cisza"); got != "no_response" {
		t.Fatalf("reason = %q, want no_response", got)
	}
	radio.hub.sendMu.Lock()
	_, lingering := radio.hub.pending[7001]
	radio.hub.sendMu.Unlock()
	if lingering {
		t.Fatal("the settled message stayed pending")
	}
}

// TestHubNeverConfirmedMessageRecordsAReason pins the other silent path: the
// device never assigned the frame an id even after the retries (it may
// never have left the radio), which is a different verdict from an
// unacknowledged delivery and carries its own reason.
func TestHubNeverConfirmedMessageRecordsAReason(t *testing.T) {
	hub := &Hub{cfg: Config{AckSettleTimeout: 40 * time.Millisecond}}
	hub.lastRadio.Store(time.Now().UnixNano())
	ps := &pendingSend{
		to: 0x6d2c8f6c, text: "Bez echa", wantAck: true,
		lastTry: time.Now().Add(-time.Minute), at: time.Now(), retries: dmMaxRetries,
	}
	hub.echoQueue = []*pendingSend{ps}

	hub.retryPending(time.Now())
	if ps.failReason != "not_confirmed" {
		t.Fatalf("reason = %q, want not_confirmed", ps.failReason)
	}
	if ps.status != TxFailed {
		t.Fatalf("status = %q, want %q", ps.status, TxFailed)
	}
	if len(hub.echoQueue) != 0 {
		t.Fatalf("echo queue = %d, want the abandoned send removed", len(hub.echoQueue))
	}
}
