package meshtastic

import (
	"sync"
	"testing"
	"time"

	pb "github.com/kabili207/meshtastic-go/core/proto"
	"google.golang.org/protobuf/proto"
)

// capturingConn records every frame written to the device. The test radio
// consumes ADMIN_APP frames itself (as the firmware does) and never hands
// them to OnOutboundPacket, so the favorite requests can only be observed on
// the way out.
type capturingConn struct {
	transportConn
	mu   sync.Mutex
	sent []*pb.ToRadio
}

func (c *capturingConn) SendToRadio(msg *pb.ToRadio) error {
	c.mu.Lock()
	c.sent = append(c.sent, msg)
	c.mu.Unlock()
	return c.transportConn.SendToRadio(msg)
}

// adminFavorites returns the node numbers of every set_favorite_node frame
// that went out, with the address each one used.
func (c *capturingConn) adminFavorites() map[uint32]uint32 {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := map[uint32]uint32{}
	for _, msg := range c.sent {
		pkt := msg.GetPacket()
		decoded := pkt.GetDecoded()
		if decoded == nil || decoded.GetPortnum() != pb.PortNum_ADMIN_APP {
			continue
		}
		admin := &pb.AdminMessage{}
		if err := proto.Unmarshal(decoded.GetPayload(), admin); err != nil {
			continue
		}
		if fav, ok := admin.GetPayloadVariant().(*pb.AdminMessage_SetFavoriteNode); ok {
			out[fav.SetFavoriteNode] = pkt.GetTo()
		}
	}
	return out
}

// TestPinUserNodesMarksFavorites pins the feature: the node IDs registered in
// the user directory are sent to the radio as favorites right after the
// handshake, so the firmware never evicts their public keys (a node whose key
// was evicted can only be messaged after it announces itself again).
func TestPinUserNodesMarksFavorites(t *testing.T) {
	cap := &capturingConn{}
	radio := newTestRadio(t, Config{Enabled: true, Device: "/dev/fake", PinUserNodes: true},
		testRadioOpts{
			wrap: func(t transportConn) transportConn {
				cap.transportConn = t
				return cap
			},
			setup: func(h *Hub) {
				h.SetUserNodes(func() []uint32 { return []uint32{0x6d2c8f6c, 0xdeadbeef} })
			},
		})
	radio.waitConnected(t)

	var favorites map[uint32]uint32
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if favorites = cap.adminFavorites(); len(favorites) == 2 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if len(favorites) != 2 {
		t.Fatalf("favorite frames = %v, want both registered nodes", favorites)
	}
	for node, to := range favorites {
		// The frame is addressed to OUR node: the firmware handles a packet
		// that came in over the local API itself, no channel involved.
		if to != 0xabcd1234 {
			t.Errorf("the favorite frame for %08x is addressed to %08x, want our own node 0xabcd1234", node, to)
		}
	}
	if _, ok := favorites[0x6d2c8f6c]; !ok {
		t.Error("the registered node 6d2c8f6c was not pinned")
	}
	if _, ok := favorites[0xdeadbeef]; !ok {
		t.Error("the registered node deadbeef was not pinned")
	}
}

// TestPinUserNodesDisabledAndEmpty: the pass is inert when the feature is off
// or when nothing is registered, so a station that never pinned anything
// sends no admin traffic at all.
func TestPinUserNodesDisabledAndEmpty(t *testing.T) {
	cases := []struct {
		name    string
		enabled bool
		nodes   []uint32
	}{
		{name: "disabled", enabled: false, nodes: []uint32{0x6d2c8f6c}},
		{name: "nothing registered", enabled: true, nodes: nil},
		{name: "no provider", enabled: true, nodes: nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cap := &capturingConn{}
			radio := newTestRadio(t, Config{Enabled: true, Device: "/dev/fake", PinUserNodes: c.enabled},
				testRadioOpts{
					wrap: func(t transportConn) transportConn {
						cap.transportConn = t
						return cap
					},
					setup: func(h *Hub) {
						if c.nodes != nil {
							h.SetUserNodes(func() []uint32 { return c.nodes })
						}
					},
				})
			radio.waitConnected(t)
			// Give the pass every chance to run (it runs inline after the
			// handshake), then look for admin frames.
			time.Sleep(150 * time.Millisecond)
			if got := cap.adminFavorites(); len(got) != 0 {
				t.Fatalf("favorite frames went out: %v", got)
			}
		})
	}
}

// TestPinUserNodesIntervalGatesThePass: the pass re-runs on its own cadence,
// not on every maintenance tick — the firmware needs no repeat every five
// seconds.
func TestPinUserNodesIntervalGatesThePass(t *testing.T) {
	h := &Hub{cfg: Config{PinUserNodes: true, PinInterval: time.Hour}}
	h.SetUserNodes(func() []uint32 { return []uint32{0x6d2c8f6c} })
	// No session: the pass must not even try to send, and must have armed
	// the next window.
	h.pinUserNodes()
	first := h.pinNext
	if first.IsZero() {
		t.Fatal("the pass did not arm its next window")
	}
	h.pinUserNodes()
	if !h.pinNext.Equal(first) {
		t.Errorf("a second pass inside the interval re-armed it: %v -> %v", first, h.pinNext)
	}
}
