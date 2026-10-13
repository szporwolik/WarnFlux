package meshtastic

import (
	"context"
	"errors"
	"sort"
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

// adminFavoriteRemovals returns the node numbers of every
// remove_favorite_node frame that went out.
func (c *capturingConn) adminFavoriteRemovals() []uint32 {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []uint32
	for _, msg := range c.sent {
		decoded := msg.GetPacket().GetDecoded()
		if decoded == nil || decoded.GetPortnum() != pb.PortNum_ADMIN_APP {
			continue
		}
		admin := &pb.AdminMessage{}
		if err := proto.Unmarshal(decoded.GetPayload(), admin); err != nil {
			continue
		}
		if fav, ok := admin.GetPayloadVariant().(*pb.AdminMessage_RemoveFavoriteNode); ok {
			out = append(out, fav.RemoveFavoriteNode)
		}
	}
	return out
}

// memFavoriteStore is the in-memory ledger of the stars this station set on
// the radio (the FavoriteStore the hub writes to).
type memFavoriteStore struct {
	mu      sync.Mutex
	ids     []uint32
	saves   int
	loadErr error
	// saveErr makes the store refuse writes, the way a locked database or a
	// full disk does; clearing it simulates the storage coming back.
	saveErr error
}

func (m *memFavoriteStore) LoadMeshtasticFavorites(context.Context) ([]uint32, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.loadErr != nil {
		return nil, m.loadErr
	}
	return append([]uint32(nil), m.ids...), nil
}

func (m *memFavoriteStore) SaveMeshtasticFavorites(_ context.Context, ids []uint32) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.saves++
	if m.saveErr != nil {
		return m.saveErr
	}
	m.ids = append([]uint32(nil), ids...)
	return nil
}

func (m *memFavoriteStore) failSaves(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.saveErr = err
}

func (m *memFavoriteStore) saveAttempts() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.saves
}

func (m *memFavoriteStore) snapshot() []uint32 {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := append([]uint32(nil), m.ids...)
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
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
				h.SetUserNodes(func() ([]uint32, error) {
					return []uint32{0x6d2c8f6c, 0xdeadbeef}, nil
				})
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

// TestPinUserNodesRemembersItsOwnStars: the pass writes down which stars it
// set, so a later pass (or a restarted process) can tell them from the ones an
// operator tapped on the device.
func TestPinUserNodesRemembersItsOwnStars(t *testing.T) {
	ledger := &memFavoriteStore{}
	cap := &capturingConn{}
	radio := newTestRadio(t, Config{Enabled: true, Device: "/dev/fake", PinUserNodes: true},
		testRadioOpts{
			wrap: func(t transportConn) transportConn {
				cap.transportConn = t
				return cap
			},
			setup: func(h *Hub) {
				h.SetFavoriteStore(ledger)
				h.SetUserNodes(func() ([]uint32, error) {
					return []uint32{0x6d2c8f6c, 0xdeadbeef}, nil
				})
			},
		})
	radio.waitConnected(t)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if len(ledger.snapshot()) == 2 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := ledger.snapshot(); len(got) != 2 || got[0] != 0x6d2c8f6c || got[1] != 0xdeadbeef {
		t.Fatalf("ledger = %08x, want the two pinned nodes", got)
	}
}

// TestPinUserNodesUnpinsNodesThatLeftTheDirectory pins the other half of the
// feature: favorites the station set for a node that is no longer registered
// are taken back, so the radio does not accumulate stars for people the
// station stopped serving. An EMPTY directory is a real answer and must
// unpin; a directory that cannot be read is not (covered below).
func TestPinUserNodesUnpinsNodesThatLeftTheDirectory(t *testing.T) {
	cases := []struct {
		name    string
		pinned  []uint32
		want    []uint32
		removed []uint32
	}{
		{
			name:    "one owner left",
			pinned:  []uint32{0x6d2c8f6c, 0xdeadbeef},
			want:    []uint32{0x6d2c8f6c},
			removed: []uint32{0xdeadbeef},
		},
		{
			name:    "directory emptied",
			pinned:  []uint32{0xdeadbeef, 0x11223344},
			want:    nil,
			removed: []uint32{0x11223344, 0xdeadbeef},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ledger := &memFavoriteStore{ids: c.pinned}
			cap := &capturingConn{}
			// The radio states that it still carries the star of the node
			// that stays in the directory: the pass has nothing to re-send.
			var deviceNodes []*pb.NodeInfo
			if len(c.want) > 0 {
				starred := nodeInfo(c.want[0], "SPM", "SPM")
				starred.IsFavorite = true
				deviceNodes = append(deviceNodes, starred)
			}
			radio := newTestRadio(t, Config{Enabled: true, Device: "/dev/fake", PinUserNodes: true},
				testRadioOpts{
					nodes: deviceNodes,
					wrap: func(t transportConn) transportConn {
						cap.transportConn = t
						return cap
					},
					setup: func(h *Hub) {
						h.SetFavoriteStore(ledger)
						h.SetUserNodes(func() ([]uint32, error) { return c.want, nil })
					},
				})
			radio.waitConnected(t)

			deadline := time.Now().Add(3 * time.Second)
			for time.Now().Before(deadline) {
				if len(cap.adminFavoriteRemovals()) == len(c.removed) {
					break
				}
				time.Sleep(5 * time.Millisecond)
			}
			got := cap.adminFavoriteRemovals()
			sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
			if len(got) != len(c.removed) {
				t.Fatalf("removed favorites = %08x, want %08x", got, c.removed)
			}
			for i, id := range c.removed {
				if got[i] != id {
					t.Fatalf("removed favorites = %08x, want %08x", got, c.removed)
				}
			}
			if ledger := ledger.snapshot(); len(ledger) != len(c.want) {
				t.Fatalf("ledger = %08x, want %08x", ledger, c.want)
			}
			// The node that is still registered keeps its star: no frame
			// about it at all.
			if favs := cap.adminFavorites(); len(favs) != 0 {
				t.Fatalf("set_favorite_node frames = %v, want none (the radio confirmed the remaining star)", favs)
			}
		})
	}
}

// TestPinUserNodesNeverUnpinsOnABadDirectory: a directory that cannot be read
// (or a ledger that cannot be read) must leave the radio's favorites exactly
// as they are — unpinning there would strip the keys of precisely the people
// the station has to keep messaging.
func TestPinUserNodesNeverUnpinsOnABadDirectory(t *testing.T) {
	readErr := errors.New("database is locked")
	cases := []struct {
		name     string
		ledger   *memFavoriteStore
		provider func() ([]uint32, error)
	}{
		{
			name:     "directory read failed",
			ledger:   &memFavoriteStore{ids: []uint32{0x6d2c8f6c, 0xdeadbeef}},
			provider: func() ([]uint32, error) { return nil, readErr },
		},
		{
			name:     "ledger read failed",
			ledger:   &memFavoriteStore{ids: []uint32{0x6d2c8f6c, 0xdeadbeef}, loadErr: readErr},
			provider: func() ([]uint32, error) { return []uint32{0x6d2c8f6c}, nil },
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cap := &capturingConn{}
			radio := newTestRadio(t, Config{Enabled: true, Device: "/dev/fake", PinUserNodes: true},
				testRadioOpts{
					wrap: func(t transportConn) transportConn {
						cap.transportConn = t
						return cap
					},
					setup: func(h *Hub) {
						h.SetFavoriteStore(c.ledger)
						h.SetUserNodes(c.provider)
					},
				})
			radio.waitConnected(t)
			time.Sleep(150 * time.Millisecond)
			if got := cap.adminFavoriteRemovals(); len(got) != 0 {
				t.Fatalf("favorites were removed after a failed read: %08x", got)
			}
			if got := capeSetFrames(cap); len(got) != 0 {
				t.Fatalf("favorites were set after a failed read: %v", got)
			}
			if got := c.ledger.snapshot(); len(got) != 2 {
				t.Fatalf("ledger was rewritten after a failed read: %08x", got)
			}
		})
	}
}

// TestPinUserNodesRetriesSoonAfterAFailedPass: a star the radio refused is not
// written down as ours (the station must never claim a pin it did not make),
// and the pass is re-armed in minutes rather than a whole interval later.
func TestPinUserNodesRetriesSoonAfterAFailedPass(t *testing.T) {
	ledger := &memFavoriteStore{}
	h := &Hub{cfg: Config{PinUserNodes: true, PinInterval: time.Hour}}
	h.SetFavoriteStore(ledger)
	h.SetUserNodes(func() ([]uint32, error) { return []uint32{0x6d2c8f6c}, nil })

	// No session: the admin frame cannot go out.
	h.pinUserNodes(false)
	if got := ledger.snapshot(); len(got) != 0 {
		t.Fatalf("ledger = %08x, want nothing (the pin never reached the radio)", got)
	}
	if h.pinned[0x6d2c8f6c] {
		t.Error("the failed pin is remembered as ours")
	}
	if got := time.Until(h.pinNext); got > pinRetryDelay+time.Second {
		t.Errorf("next pass in %s, want ~%s after a failed one", got, pinRetryDelay)
	}
}

// capeSetFrames is the set_favorite_node counterpart used where only the
// absence of traffic matters.
func capeSetFrames(c *capturingConn) map[uint32]uint32 { return c.adminFavorites() }

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
							h.SetUserNodes(func() ([]uint32, error) { return c.nodes, nil })
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
	h.SetUserNodes(func() ([]uint32, error) { return []uint32{0x6d2c8f6c}, nil })
	// No session: the pass must not even try to send, and must have armed
	// the next window.
	h.pinUserNodes(false)
	first := h.pinNext
	if first.IsZero() {
		t.Fatal("the pass did not arm its next window")
	}
	h.pinUserNodes(false)
	if !h.pinNext.Equal(first) {
		t.Errorf("a second pass inside the interval re-armed it: %v -> %v", first, h.pinNext)
	}
}

// TestPinnedNodesReportsItsOwnStars: the admin page reads the ledger through
// the hub, so a pin made by a PREVIOUS process still shows (the ledger is
// loaded on first use) and a pin the station took back no longer does.
func TestPinnedNodesReportsItsOwnStars(t *testing.T) {
	ledger := &memFavoriteStore{ids: []uint32{0xa0a85934, 0x6d2c8f6c}}
	h := &Hub{}
	h.SetFavoriteStore(ledger)
	got := h.PinnedNodes()
	if len(got) != 2 || got[0] != "6d2c8f6c" || got[1] != "a0a85934" {
		t.Fatalf("PinnedNodes = %v, want the persisted pins as sorted 8-hex ids", got)
	}
	h.rememberFavorite(0x6d2c8f6c, false)
	if got := h.PinnedNodes(); len(got) != 1 || got[0] != "a0a85934" {
		t.Fatalf("PinnedNodes after unpinning = %v, want only a0a85934", got)
	}
}

// TestPinUserNodesRepinsWhatTheRadioLost pins the ticket: the ledger says the
// station OWNS the star, which is not the same as the radio having it. A
// node-db reset (or a swapped device) leaves the ledger intact and the
// favorites gone — the pass must set them again at the next session instead of
// trusting its own bookkeeping.
func TestPinUserNodesRepinsWhatTheRadioLost(t *testing.T) {
	ledger := &memFavoriteStore{ids: []uint32{0x6d2c8f6c}}
	cap := &capturingConn{}
	radio := newTestRadio(t, Config{Enabled: true, Device: "/dev/fake", PinUserNodes: true},
		testRadioOpts{
			// A radio whose node table holds no favorites at all: exactly
			// what a node-db reset leaves behind.
			nodes: []*pb.NodeInfo{nodeInfo(0xdeadbeef, "PL-KR-MAKI", "MK")},
			wrap: func(t transportConn) transportConn {
				cap.transportConn = t
				return cap
			},
			setup: func(h *Hub) {
				h.SetFavoriteStore(ledger)
				h.SetUserNodes(func() ([]uint32, error) { return []uint32{0x6d2c8f6c}, nil })
			},
		})
	radio.waitConnected(t)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := cap.adminFavorites()[0x6d2c8f6c]; ok {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if _, ok := cap.adminFavorites()[0x6d2c8f6c]; !ok {
		t.Fatalf("the star the ledger owns was not re-asserted after the radio lost it: %v", cap.adminFavorites())
	}
	if got := ledger.snapshot(); len(got) != 1 || got[0] != 0x6d2c8f6c {
		t.Fatalf("ledger = %08x, want the single owned node", got)
	}
}

// TestPinUserNodesTrustsTheRadioThatConfirmsItsStars: the other side of the
// same rule — a radio that reports the star costs no admin traffic, so a
// reconnecting station does not re-send every favorite on every session.
func TestPinUserNodesTrustsTheRadioThatConfirmsItsStars(t *testing.T) {
	starred := nodeInfo(0x6d2c8f6c, "SPM", "SPM")
	starred.IsFavorite = true
	ledger := &memFavoriteStore{ids: []uint32{0x6d2c8f6c}}
	cap := &capturingConn{}
	radio := newTestRadio(t, Config{Enabled: true, Device: "/dev/fake", PinUserNodes: true},
		testRadioOpts{
			nodes: []*pb.NodeInfo{starred},
			wrap: func(t transportConn) transportConn {
				cap.transportConn = t
				return cap
			},
			setup: func(h *Hub) {
				h.SetFavoriteStore(ledger)
				h.SetUserNodes(func() ([]uint32, error) { return []uint32{0x6d2c8f6c}, nil })
			},
		})
	radio.waitConnected(t)
	time.Sleep(150 * time.Millisecond)
	if got := cap.adminFavorites(); len(got) != 0 {
		t.Fatalf("set_favorite_node frames = %v, want none (the radio still has the star)", got)
	}
	if got := cap.adminFavoriteRemovals(); len(got) != 0 {
		t.Fatalf("remove_favorite_node frames = %08x, want none", got)
	}
}

// TestPinUserNodesRetriesAFailedLedgerSave pins the ticket: the in-memory
// ledger changes before the store accepts it, so a failed write must stay
// pending — otherwise the next pass sees "nothing changed", never tries again,
// and a restart loses which stars are the station's own.
func TestPinUserNodesRetriesAFailedLedgerSave(t *testing.T) {
	ledger := &memFavoriteStore{}
	ledger.failSaves(errors.New("database is locked"))
	cap := &capturingConn{}
	radio := newTestRadio(t, Config{Enabled: true, Device: "/dev/fake", PinUserNodes: true},
		testRadioOpts{
			nodes: []*pb.NodeInfo{nodeInfo(0xdeadbeef, "PL-KR-MAKI", "MK")},
			wrap: func(t transportConn) transportConn {
				cap.transportConn = t
				return cap
			},
			setup: func(h *Hub) {
				h.SetFavoriteStore(ledger)
				h.SetUserNodes(func() ([]uint32, error) { return []uint32{0x6d2c8f6c}, nil })
			},
		})
	radio.waitConnected(t)

	// The star goes out and is remembered in RAM, but the store refuses it.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if ledger.saveAttempts() > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if ledger.saveAttempts() == 0 {
		t.Fatal("the pass never tried to save the ledger")
	}
	if got := ledger.snapshot(); len(got) != 0 {
		t.Fatalf("store = %08x, want nothing (the write failed)", got)
	}
	if !radio.hub.dirtyFavorites() {
		t.Fatal("the unsaved ledger change was forgotten")
	}
	if got := time.Until(radio.hub.pinNext); got > pinRetryDelay+time.Second {
		t.Errorf("next pass in %s, want ~%s after a failed save", got, pinRetryDelay)
	}

	// The store comes back: the next pass writes the ledger WITHOUT any new
	// change from the directory, and without re-sending the star the radio
	// already carries.
	before := ledger.saveAttempts()
	ledger.failSaves(nil)
	radio.hub.pinUserNodes(true)
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if got := ledger.snapshot(); len(got) == 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := ledger.snapshot(); len(got) != 1 || got[0] != 0x6d2c8f6c {
		t.Fatalf("store = %08x, want the retried ledger", got)
	}
	if ledger.saveAttempts() <= before {
		t.Error("the pending ledger was not written again")
	}
	if radio.hub.dirtyFavorites() {
		t.Error("the flag survived a successful write")
	}
	if got := len(cap.adminFavorites()); got != 1 {
		t.Errorf("set_favorite_node frames = %d, want 1 (the retry must not re-pin)", got)
	}
}

// TestFlushFavoritesKeepsThePendingFlag pins the flag contract itself: a failed
// write leaves the change pending, a later write clears it, and a hub with no
// store at all (RAM-only mode) is not permanently dirty.
func TestFlushFavoritesKeepsThePendingFlag(t *testing.T) {
	ledger := &memFavoriteStore{}
	ledger.failSaves(errors.New("disk full"))
	h := &Hub{}
	h.SetFavoriteStore(ledger)
	h.rememberFavorite(0x6d2c8f6c, true)

	if h.flushFavorites() {
		t.Error("a failed write reported success")
	}
	if !h.dirtyFavorites() {
		t.Error("the pending change was dropped after a failed write")
	}
	if got := ledger.snapshot(); len(got) != 0 {
		t.Fatalf("store = %08x, want nothing", got)
	}
	ledger.failSaves(nil)
	if !h.flushFavorites() {
		t.Error("the recovered write reported failure")
	}
	if h.dirtyFavorites() {
		t.Error("the flag survived a successful write")
	}
	if got := ledger.snapshot(); len(got) != 1 || got[0] != 0x6d2c8f6c {
		t.Fatalf("store = %08x, want the pinned node", got)
	}
	if h.flushFavorites() != true || ledger.saveAttempts() != 2 {
		t.Errorf("a clean ledger was written again (%d attempts)", ledger.saveAttempts())
	}

	ram := &Hub{}
	ram.rememberFavorite(0xa0a85934, true)
	if !ram.flushFavorites() || ram.dirtyFavorites() {
		t.Error("a hub without a store stayed dirty")
	}
}
