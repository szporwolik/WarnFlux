package meshtastic

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kabili207/meshtastic-go/core"
	pb "github.com/kabili207/meshtastic-go/core/proto"
	"github.com/kabili207/meshtastic-go/device/clientapi"
	"github.com/kabili207/meshtastic-go/transport/client"
	"github.com/kabili207/meshtastic-go/transport/stream"
	"google.golang.org/protobuf/proto"

	"github.com/szporwolik/WarnFlux/internal/radiocli"
	"github.com/szporwolik/WarnFlux/internal/storage"
)

// testNodeTable serves the handshake node directory.
type testNodeTable struct {
	self *pb.NodeInfo
	all  []*pb.NodeInfo
}

func (t *testNodeTable) SelfInfo() *pb.NodeInfo { return t.self }
func (t *testNodeTable) All() []*pb.NodeInfo {
	return append([]*pb.NodeInfo(nil), t.all...)
}

func nodeInfo(num uint32, long, short string) *pb.NodeInfo {
	return &pb.NodeInfo{
		Num: num,
		User: &pb.User{
			LongName:  long,
			ShortName: short,
		},
	}
}

// nodeInfoPos adds a device node-DB position (1e-7 scaled) and last_heard.
func nodeInfoPos(num uint32, long, short string, latI, lonI int32, lastHeard uint32) *pb.NodeInfo {
	ni := nodeInfo(num, long, short)
	ni.Position = &pb.Position{LatitudeI: &latI, LongitudeI: &lonI}
	ni.LastHeard = lastHeard
	return ni
}

// testRadio wires a hub onto the library's in-memory client API server:
// real protocol over net.Pipe, no serial hardware.
type testRadio struct {
	srv    *clientapi.Server
	cancel context.CancelFunc
	hub    *Hub
	runErr chan error
	mu     sync.Mutex
	rx     []*pb.MeshPacket // outbound packets captured via OnOutboundPacket
	dials  atomic.Int32     // transport dials through the seam
}

// newTestTable is the handshake node directory used by the test radio.
func newTestTable() *testNodeTable {
	lat := int32(500200000) // 50.02
	lon := int32(200000000) // 20.00
	return &testNodeTable{
		self: nodeInfo(0xabcd1234, "RKSR-OWN", "OWN"),
		all: []*pb.NodeInfo{
			nodeInfoPos(0xef010203, "RKSR-TN-R3", "R3", lat, lon, 1790900000),
			nodeInfo(0xdeadbeef, "PL-KR-MAKI", "MK"),
		},
	}
}

func newTestRadio(t *testing.T, cfg Config) *testRadio {
	t.Helper()
	table := newTestTable()
	radio := &testRadio{}
	ctx, cancel := context.WithCancel(context.Background())
	radio.cancel = cancel
	radio.srv = clientapi.New(clientapi.Config{
		NodeID:       core.NodeID(0xabcd1234),
		LongName:     "RKSR-OWN",
		ShortName:    "OWN",
		Nodes:        table,
		NextPacketID: func() uint32 { return 1 },
		OnOutboundPacket: func(_ context.Context, pkt *pb.MeshPacket) {
			radio.mu.Lock()
			radio.rx = append(radio.rx, pkt)
			radio.mu.Unlock()
		},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	go radio.srv.Start(ctx)

	// Override the dial seam with the in-memory client transport.
	oldDial := Dial
	Dial = func(_ context.Context, _ Config) (transportConn, error) {
		radio.dials.Add(1)
		conn := radio.srv.Conn(ctx)
		sc, err := stream.NewClientConn(conn)
		if err != nil {
			return nil, err
		}
		return &clientAdapter{t: client.NewTransport(sc, client.TransportConfig{
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		})}, nil
	}
	t.Cleanup(func() {
		Dial = oldDial
		cancel()
	})

	hub, err := NewHub(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("NewHub: %v", err)
	}
	radio.hub = hub
	radio.runErr = make(chan error, 1)
	runCtx, stopRun := context.WithCancel(context.Background())
	t.Cleanup(stopRun)
	go func() { radio.runErr <- hub.Run(runCtx) }()
	return radio
}

// waitConnected blocks until the hub reports a live session.
func (r *testRadio) waitConnected(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if r.hub.Connected() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("hub did not connect in time")
}

// outbound returns the captured outbound packets.
func (r *testRadio) outbound() []*pb.MeshPacket {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]*pb.MeshPacket(nil), r.rx...)
}

// waitOutbound blocks until the server captured n outbound packets.
func (r *testRadio) waitOutbound(t *testing.T, n int) []*pb.MeshPacket {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if out := r.outbound(); len(out) >= n {
			return out
		}
		time.Sleep(5 * time.Millisecond)
	}
	return r.outbound()
}

func textPacket(from, to uint32, text string) *pb.MeshPacket {
	return &pb.MeshPacket{
		From: from,
		To:   to,
		PayloadVariant: &pb.MeshPacket_Decoded{
			Decoded: &pb.Data{
				Portnum: pb.PortNum_TEXT_MESSAGE_APP,
				Payload: []byte(text),
			},
		},
	}
}

func dispatch(t *testing.T, r *testRadio, pkt *pb.MeshPacket) {
	t.Helper()
	r.srv.DispatchToClients(&pb.FromRadio{
		PayloadVariant: &pb.FromRadio_Packet{Packet: pkt},
	})
}

// TestHubConnect pins the handshake: the hub learns its identity, the
// device metadata and the node directory from the config sync.
func TestHubConnect(t *testing.T) {
	radio := newTestRadio(t, Config{Enabled: true, Device: "/dev/fake", NodeTTL: time.Hour})
	radio.waitConnected(t)

	snap := radio.hub.Snapshot()
	if !snap.Connected {
		t.Fatal("snapshot.Connected = false, want true")
	}
	if snap.Self.ID != "abcd1234" {
		t.Fatalf("self id = %q, want abcd1234", snap.Self.ID)
	}
	if snap.Self.LongName != "RKSR-OWN" {
		t.Fatalf("self name = %q, want RKSR-OWN", snap.Self.LongName)
	}
	if snap.Self.Firmware == "" {
		t.Fatal("self firmware empty, want the device firmware string")
	}
	if len(snap.Nodes) != 2 {
		t.Fatalf("nodes = %v, want 2 neighbours", snap.Nodes)
	}
	want := map[string]string{"ef010203": "RKSR-TN-R3", "deadbeef": "PL-KR-MAKI"}
	for _, n := range snap.Nodes {
		if want[n.ID] != n.Name {
			t.Fatalf("node %s name = %q, want %q", n.ID, n.Name, want[n.ID])
		}
		if n.ID == "ef010203" {
			if n.Short != "R3" {
				t.Fatalf("node ef010203 short = %q, want R3", n.Short)
			}
			if n.Lat != 50.02 || n.Lon != 20.0 {
				t.Fatalf("node ef010203 pos = (%v, %v), want (50.02, 20)", n.Lat, n.Lon)
			}
			if n.LastSeen.IsZero() {
				t.Fatal("node ef010203 last seen empty, want the device last_heard")
			}
		}
	}
}

// TestHubPreconnectedTransport pins the production dial path: the
// transport arrives ALREADY handshaken (serial.Connect performs the
// handshake before returning), so the hub must adopt it without calling
// Connect again — a second Connect on a complete state blocks forever
// (the config-complete signal never fires twice).
func TestHubPreconnectedTransport(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	srv := clientapi.New(clientapi.Config{
		NodeID:    core.NodeID(0xabcd1234),
		LongName:  "RKSR-OWN",
		ShortName: "OWN",
		Nodes:     newTestTable(),
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	go srv.Start(ctx)

	oldDial := Dial
	Dial = func(dctx context.Context, _ Config) (transportConn, error) {
		sc, err := stream.NewClientConn(srv.Conn(dctx))
		if err != nil {
			return nil, err
		}
		c := client.NewTransport(sc, client.TransportConfig{
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		})
		// serial.Connect semantics: handshake completes before return.
		if err := c.Connect(dctx); err != nil {
			return nil, err
		}
		return &clientAdapter{t: c}, nil
	}
	t.Cleanup(func() { Dial = oldDial })

	hub, err := NewHub(Config{Enabled: true, Device: "/dev/fake", NodeTTL: time.Hour},
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	runCtx, stopRun := context.WithCancel(context.Background())
	defer stopRun()
	go func() { _ = hub.Run(runCtx) }()

	// The old code called Connect again and hung forever — this wait
	// bounds the regression to a hard failure.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if hub.Connected() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !hub.Connected() {
		t.Fatal("hub did not adopt the pre-connected transport in time")
	}
	snap := hub.Snapshot()
	if snap.Self.ID != "abcd1234" || snap.Self.LongName != "RKSR-OWN" {
		t.Fatalf("self = %+v, want abcd1234 RKSR-OWN", snap.Self)
	}
	if len(snap.Nodes) != 2 {
		t.Fatalf("nodes = %d, want 2 from the handshake directory", len(snap.Nodes))
	}
}

// TestHubSendChannelText pins a broadcast on a specific device channel:
// the packet carries the channel index and the history row carries the
// channel label.
func TestHubSendChannelText(t *testing.T) {
	radio := newTestRadio(t, Config{Enabled: true, Device: "/dev/fake", NodeTTL: time.Hour})
	radio.waitConnected(t)
	rec := &captureRecorder{}
	radio.hub.SetRecorder(rec)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := radio.hub.SendChannelText(ctx, 2, "channel two", "admin"); err != nil {
		t.Fatalf("SendChannelText: %v", err)
	}
	out := radio.waitOutbound(t, 1)
	if len(out) != 1 || out[0].GetChannel() != 2 {
		t.Fatalf("outbound = %v, want one packet on channel 2", out)
	}
	if out[0].GetTo() != core.BroadcastNodeID.Uint32() || out[0].GetWantAck() {
		t.Fatalf("packet = %v, want broadcast without ack", out[0])
	}
	msgs := rec.messages()
	if len(msgs) != 1 || msgs[0].Direction != "tx" || msgs[0].Channel != "ch2" {
		t.Fatalf("recorded tx = %v, want a ch2 row", msgs)
	}

	if err := radio.hub.SendChannelText(ctx, 8, "x", "admin"); err == nil {
		t.Fatal("channel 8 accepted, want 0-7 validation")
	}
	if err := radio.hub.SendChannelText(ctx, -1, "x", "admin"); err == nil {
		t.Fatal("channel -1 accepted, want 0-7 validation")
	}
}

// TestHubAckWithoutEcho pins the 2.7.x firmware behavior: the device does
// not echo direct messages, but the recipient's ROUTING_APP alone must
// still settle the tx (paired by recipient, FIFO).
func TestHubAckWithoutEcho(t *testing.T) {
	radio := newTestRadio(t, Config{Enabled: true, Device: "/dev/fake", NodeTTL: time.Hour})
	radio.waitConnected(t)
	rec := &captureRecorder{}
	radio.hub.SetRecorder(rec)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := radio.hub.SendContactMessage(ctx, "ef010203", "no echo", "admin"); err != nil {
		t.Fatalf("SendContactMessage: %v", err)
	}

	routing, err := proto.Marshal(&pb.Routing{Variant: &pb.Routing_ErrorReason{ErrorReason: pb.Routing_NONE}})
	if err != nil {
		t.Fatal(err)
	}
	dispatch(t, radio, &pb.MeshPacket{
		From: 0xef010203, To: 0xabcd1234,
		PayloadVariant: &pb.MeshPacket_Decoded{
			Decoded: &pb.Data{Portnum: pb.PortNum_ROUTING_APP, RequestId: 999, Payload: routing},
		},
	})

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		msgs := rec.messages()
		if len(msgs) == 1 && msgs[0].Status == TxDelivered {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	msgs := rec.messages()
	if len(msgs) != 1 {
		t.Fatalf("recorded = %v, want one tx row", msgs)
	}
	if msgs[0].Status != TxDelivered {
		t.Fatalf("tx status = %q, want delivered", msgs[0].Status)
	}
}

// TestHubSendAckFlow pins the delivery tracking: the device echo marks
// the tx "sent", and the recipient's ROUTING_APP acknowledgment marks it
// "delivered".
func TestHubSendAckFlow(t *testing.T) {
	radio := newTestRadio(t, Config{Enabled: true, Device: "/dev/fake", NodeTTL: time.Hour})
	radio.waitConnected(t)
	rec := &captureRecorder{}
	radio.hub.SetRecorder(rec)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := radio.hub.SendContactMessage(ctx, "ef010203", "ack me", "admin"); err != nil {
		t.Fatalf("SendContactMessage: %v", err)
	}

	// The device echoes our frame back with the assigned packet id.
	dispatch(t, radio, &pb.MeshPacket{
		From: 0xabcd1234, Id: 4242, To: 0xef010203,
		PayloadVariant: &pb.MeshPacket_Decoded{
			Decoded: &pb.Data{Portnum: pb.PortNum_TEXT_MESSAGE_APP, Payload: []byte("ack me")},
		},
	})
	// The recipient acknowledges with a routing frame.
	routing, err := proto.Marshal(&pb.Routing{Variant: &pb.Routing_ErrorReason{ErrorReason: pb.Routing_NONE}})
	if err != nil {
		t.Fatal(err)
	}
	dispatch(t, radio, &pb.MeshPacket{
		From: 0xef010203, To: 0xabcd1234,
		PayloadVariant: &pb.MeshPacket_Decoded{
			Decoded: &pb.Data{Portnum: pb.PortNum_ROUTING_APP, RequestId: 4242, Payload: routing},
		},
	})

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		msgs := rec.messages()
		if len(msgs) == 1 && msgs[0].Status == TxDelivered {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	msgs := rec.messages()
	if len(msgs) != 1 {
		t.Fatalf("recorded = %v, want one tx row", msgs)
	}
	if msgs[0].Status != TxDelivered {
		t.Fatalf("tx status = %q, want delivered", msgs[0].Status)
	}
}

// TestHubSilentDeviceReconnects pins the radio-silence watchdog: a
// session with no FromRadio traffic for longer than the silence timeout
// is torn down and redialed instead of hanging forever.
func TestHubSilentDeviceReconnects(t *testing.T) {
	radio := newTestRadio(t, Config{Enabled: true, Device: "/dev/fake", NodeTTL: time.Hour,
		SilenceTimeout: 150 * time.Millisecond})
	radio.waitConnected(t)

	// The watchdog must tear the silent session down and dial a fresh
	// one (the replacement is silent too, so the loop keeps cycling).
	deadline := time.Now().Add(12 * time.Second)
	for time.Now().Before(deadline) && radio.dials.Load() < 2 {
		time.Sleep(10 * time.Millisecond)
	}
	if radio.dials.Load() < 2 {
		t.Fatalf("dial calls = %d, want the silent session to be reconnected", radio.dials.Load())
	}
}

// TestHubNoResendOnceDeviceConfirms pins the duplicate-message fix: the
// firmware retransmits want_ack frames itself, so a direct message whose
// id the device already confirmed must never be client-side resent —
// it only waits for the recipient's acknowledgment and then settles.
func TestHubNoResendOnceDeviceConfirms(t *testing.T) {
	radio := newTestRadio(t, Config{Enabled: true, Device: "/dev/fake", NodeTTL: time.Hour,
		AckSettleTimeout: 300 * time.Millisecond})
	radio.waitConnected(t)
	rec := &captureRecorder{}
	radio.hub.SetRecorder(rec)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := radio.hub.SendContactMessage(ctx, "ef010203", "only once", "admin"); err != nil {
		t.Fatalf("SendContactMessage: %v", err)
	}

	// The device confirms the send (echo with the assigned packet id).
	dispatch(t, radio, &pb.MeshPacket{
		From: 0xabcd1234, Id: 77, To: 0xef010203,
		PayloadVariant: &pb.MeshPacket_Decoded{
			Decoded: &pb.Data{Portnum: pb.PortNum_TEXT_MESSAGE_APP, Payload: []byte("only once")},
		},
	})

	// Wait past the settle window (and one maintenance tick): the send
	// must settle failed WITHOUT ever being re-transmitted.
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		msgs := rec.messages()
		if len(msgs) == 1 && msgs[0].Status == TxFailed {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	msgs := rec.messages()
	if len(msgs) != 1 || msgs[0].Status != TxFailed {
		t.Fatalf("tx = %+v, want one failed row", msgs)
	}
	if out := radio.outbound(); len(out) != 1 {
		t.Fatalf("outbound transmissions = %d, want exactly 1 (no client-side resend)", len(out))
	}
}

// memNodeStore is an in-memory NodeStore for directory persistence tests.
type memNodeStore struct {
	mu    sync.Mutex
	nodes []storage.MeshtasticNode
}

func (m *memNodeStore) LoadMeshtasticNodes(_ context.Context) ([]storage.MeshtasticNode, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]storage.MeshtasticNode(nil), m.nodes...), nil
}

func (m *memNodeStore) SaveMeshtasticNodes(_ context.Context, nodes []storage.MeshtasticNode) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nodes = append([]storage.MeshtasticNode(nil), nodes...)
	return nil
}

// TestHubNodeDirectoryPersists pins the persistent directory: a node
// learned by one hub survives into a fresh hub through the store.
func TestHubNodeDirectoryPersists(t *testing.T) {
	store := &memNodeStore{}
	radio := newTestRadio(t, Config{Enabled: true, Device: "/dev/fake", NodeTTL: time.Hour})
	radio.waitConnected(t)
	radio.hub.SetNodeStore(store)

	// Learn a node from a live packet, then force a directory save.
	dispatch(t, radio, &pb.MeshPacket{
		From: 0xef010203,
		PayloadVariant: &pb.MeshPacket_Decoded{
			Decoded: &pb.Data{Portnum: pb.PortNum_TELEMETRY_APP},
		},
	})
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		radio.hub.mu.Lock()
		has := radio.hub.nodes["ef010203"] != nil
		radio.hub.mu.Unlock()
		if has {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	radio.hub.mu.Lock()
	radio.hub.lastPersist = time.Time{} // defeat the 30s throttle
	radio.hub.mu.Unlock()
	radio.hub.persistNodes()
	// The save runs on a background goroutine; wait for the store.
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if got, _ := store.LoadMeshtasticNodes(context.Background()); len(got) > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	// A fresh hub restores the directory from the same store.
	hub2, err := NewHub(Config{Enabled: true, Device: "/dev/fake", NodeTTL: time.Hour},
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	hub2.SetNodeStore(store)
	hub2.restoreNodes(context.Background())
	hub2.mu.Lock()
	n := hub2.nodes["ef010203"]
	hub2.mu.Unlock()
	if n == nil || n.ID != "ef010203" {
		t.Fatalf("restored node = %+v, want ef010203", n)
	}
}

// TestHubEmcomBeacon pins the periodic presence beacon: identity,
// uptime and node count broadcast on the configured emcom channel — and
// never on the PRIMARY channel.
func TestHubEmcomBeacon(t *testing.T) {
	radio := newTestRadio(t, Config{Enabled: true, Device: "/dev/fake", NodeTTL: time.Hour,
		EmcomChannel: 1, EmcomInterval: time.Hour,
		EmcomIdentity: "WarnFlux v1.0 - SOSNA - sosna.sp9moa.pl"})
	radio.waitConnected(t)

	// The startup beacon fires on the first maintenance tick (no manual
	// scheduling). Poll with a generous deadline: the ticker interval is
	// 5s and -race runs are slow.
	deadline := time.Now().Add(10 * time.Second)
	var out []*pb.MeshPacket
	for time.Now().Before(deadline) {
		if out = radio.outbound(); len(out) >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(out) != 1 {
		t.Fatalf("outbound = %v, want one beacon", out)
	}
	if out[0].GetChannel() != 1 {
		t.Fatalf("beacon channel = %d, want 1 (never the primary)", out[0].GetChannel())
	}
	if out[0].GetTo() != core.BroadcastNodeID.Uint32() {
		t.Fatalf("beacon to = %v, want a broadcast", out[0].GetTo())
	}
	text := string(out[0].GetDecoded().GetPayload())
	for _, want := range []string{"WarnFlux v1.0 - SOSNA - sosna.sp9moa.pl", "up ", "nodes "} {
		if !strings.Contains(text, want) {
			t.Fatalf("beacon text = %q, missing %q", text, want)
		}
	}
}

// TestHubEmcomBeaconAtStartup pins the restart requirement: the first
// session schedules the presence beacon for now, so it fires on the
// first maintenance tick — no one-minute warm-up.
func TestHubEmcomBeaconAtStartup(t *testing.T) {
	radio := newTestRadio(t, Config{Enabled: true, Device: "/dev/fake", NodeTTL: time.Hour,
		EmcomChannel: 1, EmcomInterval: time.Hour, EmcomIdentity: "WarnFlux v1.0"})
	radio.waitConnected(t)

	var next time.Time
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		radio.hub.mu.Lock()
		next = radio.hub.emcomNext
		radio.hub.mu.Unlock()
		if !next.IsZero() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if next.IsZero() {
		t.Fatal("startup beacon not scheduled")
	}
	if next.After(time.Now()) {
		t.Fatalf("startup beacon scheduled in %s, want it due immediately", time.Until(next))
	}
}

// TestHubEmcomDefaults pins the default beacon spacing (4 hours).
func TestHubEmcomDefaults(t *testing.T) {
	hub, err := NewHub(Config{Enabled: true, Device: "/dev/fake"},
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if hub.cfg.EmcomInterval != 4*time.Hour {
		t.Fatalf("default EmcomInterval = %s, want 4h", hub.cfg.EmcomInterval)
	}
}

// TestHubChannelZeroBlocked pins the policy guard: broadcasts on the
// default PRIMARY channel are refused.
func TestHubChannelZeroBlocked(t *testing.T) {
	radio := newTestRadio(t, Config{Enabled: true, Device: "/dev/fake", NodeTTL: time.Hour})
	radio.waitConnected(t)
	ctx := context.Background()
	if err := radio.hub.SendChannelText(ctx, 0, "x", "admin"); !errors.Is(err, ErrPrimaryChannelBlocked) {
		t.Fatalf("SendChannelText(0) = %v, want ErrPrimaryChannelBlocked", err)
	}
	if err := radio.hub.SendChannelMessage(ctx, "x", "admin"); !errors.Is(err, ErrPrimaryChannelBlocked) {
		t.Fatalf("SendChannelMessage = %v, want ErrPrimaryChannelBlocked", err)
	}
	if out := radio.outbound(); len(out) != 0 {
		t.Fatalf("outbound after blocked sends = %v, want none", out)
	}
}

// TestHubNodeSignals pins the observed packet kinds: telemetry and text
// packets mark the sender's node, surfaced as the admin/map badges.
func TestHubNodeSignals(t *testing.T) {
	radio := newTestRadio(t, Config{Enabled: true, Device: "/dev/fake", NodeTTL: time.Hour})
	radio.waitConnected(t)

	dispatch(t, radio, &pb.MeshPacket{
		From: 0xdeadbeef,
		To:   core.BroadcastNodeID.Uint32(),
		PayloadVariant: &pb.MeshPacket_Decoded{
			Decoded: &pb.Data{Portnum: pb.PortNum_TELEMETRY_APP, Payload: []byte{1}},
		},
	})
	waitFor := func(id string) []string {
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			for _, n := range radio.hub.Snapshot().Nodes {
				if n.ID == id && len(n.Sends) > 0 {
					return n.Sends
				}
			}
			time.Sleep(5 * time.Millisecond)
		}
		return nil
	}
	sends := waitFor("deadbeef")
	if len(sends) != 1 || sends[0] != SignalTelemetry {
		t.Fatalf("deadbeef sends = %v, want [telemetry]", sends)
	}

	dispatch(t, radio, textPacket(0x12345678, core.BroadcastNodeID.Uint32(), "hi"))
	sends = waitFor("12345678")
	if len(sends) != 1 || sends[0] != SignalText {
		t.Fatalf("12345678 sends = %v, want [text]", sends)
	}
}

// TestHubTelemetryStored pins the full telemetry capture: device,
// environment and air-quality sections accumulate on the node and ride
// along in the retained station document.
func TestHubTelemetryStored(t *testing.T) {
	radio := newTestRadio(t, Config{Enabled: true, Device: "/dev/fake", NodeTTL: time.Hour})
	radio.waitConnected(t)

	docs := make(chan []byte, 8)
	radio.hub.SetStationSink(func(_ context.Context, topic string, _ bool, payload []byte) error {
		if topic == "meshtastic/stations/deadbeef" {
			docs <- append([]byte(nil), payload...)
		}
		return nil
	})

	telePacket := func(tele *pb.Telemetry) *pb.MeshPacket {
		payload, err := proto.Marshal(tele)
		if err != nil {
			t.Fatal(err)
		}
		return &pb.MeshPacket{
			From: 0xdeadbeef,
			To:   core.BroadcastNodeID.Uint32(),
			PayloadVariant: &pb.MeshPacket_Decoded{
				Decoded: &pb.Data{Portnum: pb.PortNum_TELEMETRY_APP, Payload: payload},
			},
		}
	}

	dispatch(t, radio, telePacket(&pb.Telemetry{
		Variant: &pb.Telemetry_DeviceMetrics{DeviceMetrics: &pb.DeviceMetrics{
			BatteryLevel: proto.Uint32(87), Voltage: proto.Float32(4.1),
			ChannelUtilization: proto.Float32(12.5), AirUtilTx: proto.Float32(1.5),
			UptimeSeconds: proto.Uint32(12345),
		}},
	}))
	dispatch(t, radio, telePacket(&pb.Telemetry{
		Variant: &pb.Telemetry_EnvironmentMetrics{EnvironmentMetrics: &pb.EnvironmentMetrics{
			Temperature: proto.Float32(21.5), RelativeHumidity: proto.Float32(55),
			BarometricPressure: proto.Float32(1013), Iaq: proto.Uint32(42),
		}},
	}))
	dispatch(t, radio, telePacket(&pb.Telemetry{
		Variant: &pb.Telemetry_AirQualityMetrics{AirQualityMetrics: &pb.AirQualityMetrics{
			Pm25Standard: proto.Uint32(13), Pm10Standard: proto.Uint32(20), Co2: proto.Uint32(510),
		}},
	}))

	var n *Node
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, sn := range radio.hub.Snapshot().Nodes {
			if sn.ID == "deadbeef" && sn.Telemetry != nil && sn.Telemetry.CO2 > 0 {
				n = &sn
				break
			}
		}
		if n != nil {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if n == nil {
		t.Fatal("deadbeef telemetry never accumulated")
	}
	tel := n.Telemetry
	if tel.BatteryLevel != 87 || tel.Voltage != 4.1 || tel.ChannelUtil != 12.5 || tel.UptimeSecs != 12345 {
		t.Fatalf("device metrics = %+v, want battery 87 / 4.1V / 12.5%% / 12345s", tel)
	}
	if tel.Temperature != 21.5 || tel.Humidity != 55 || tel.Pressure != 1013 || tel.IAQ != 42 {
		t.Fatalf("environment metrics = %+v, want 21.5°C / 55%% / 1013 hPa / iaq 42", tel)
	}
	if tel.PM25Std != 13 || tel.PM10Std != 20 || tel.CO2 != 510 {
		t.Fatalf("air quality metrics = %+v, want pm2.5 13 / pm10 20 / co2 510", tel)
	}
	if !tel.At.After(time.Now().Add(-time.Minute)) {
		t.Fatalf("telemetry At = %v, want a fresh receipt time", tel.At)
	}

	// The sink receives one document per telemetry section; keep the
	// LAST one (it carries the full accumulated set).
	deadline = time.Now().Add(5 * time.Second)
	var wire struct {
		Telemetry struct {
			BatteryLevel uint32  `json:"battery_level"`
			Temperature  float32 `json:"temperature"`
			Pm25Standard uint32  `json:"pm25_standard"`
			Co2          uint32  `json:"co2"`
		} `json:"telemetry"`
	}
	for {
		select {
		case doc := <-docs:
			if err := json.Unmarshal(doc, &wire); err != nil {
				t.Fatalf("station doc: %v", err)
			}
			if wire.Telemetry.Co2 > 0 {
				if wire.Telemetry.BatteryLevel != 87 || wire.Telemetry.Temperature != 21.5 ||
					wire.Telemetry.Pm25Standard != 13 || wire.Telemetry.Co2 != 510 {
					t.Fatalf("station doc telemetry = %+v, want the full accumulated set", wire.Telemetry)
				}
				return
			}
		case <-time.After(time.Until(deadline)):
			t.Fatal("station document with the full telemetry never published")
		}
	}
}

// TestHubSendBroadcast pins a broadcast transmission: To is the broadcast
// id, the port is TEXT_MESSAGE_APP and the tx lands in the history.
type captureRecorder struct {
	mu  sync.Mutex
	got []Message
}

func (c *captureRecorder) RecordMeshtasticMessage(_ context.Context, direction, sender, channel, text, operator string, hops int, at time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.got = append(c.got, Message{Direction: direction, Sender: sender, Channel: channel, Text: text, Operator: operator, Hops: hops, At: at})
	return nil
}

func (c *captureRecorder) UpdateMeshtasticMessageStatus(_ context.Context, status string, at time.Time, text string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := range c.got {
		if c.got[i].Direction == "tx" && c.got[i].At.Equal(at) && c.got[i].Text == text {
			c.got[i].Status = status
		}
	}
	return nil
}

func (c *captureRecorder) messages() []Message {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]Message(nil), c.got...)
}

func TestHubSendBroadcast(t *testing.T) {
	radio := newTestRadio(t, Config{Enabled: true, Device: "/dev/fake", NodeTTL: time.Hour})
	radio.waitConnected(t)
	rec := &captureRecorder{}
	radio.hub.SetRecorder(rec)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	// Broadcasts happen on a configured secondary channel; the PRIMARY
	// channel (0) is blocked by policy.
	if err := radio.hub.SendChannelText(ctx, 2, "test broadcast", "admin"); err != nil {
		t.Fatalf("SendChannelText: %v", err)
	}
	out := radio.waitOutbound(t, 1)
	if len(out) != 1 {
		t.Fatalf("outbound packets = %d, want 1", len(out))
	}
	pkt := out[0]
	if pkt.GetTo() != core.BroadcastNodeID.Uint32() {
		t.Fatalf("To = %08x, want broadcast", pkt.GetTo())
	}
	decoded := pkt.GetDecoded()
	if decoded == nil || decoded.GetPortnum() != pb.PortNum_TEXT_MESSAGE_APP {
		t.Fatalf("decoded = %v, want TEXT_MESSAGE_APP", decoded)
	}
	if string(decoded.GetPayload()) != "test broadcast" {
		t.Fatalf("payload = %q, want test broadcast", decoded.GetPayload())
	}
	if pkt.GetWantAck() {
		t.Fatal("broadcast wants ack, want false")
	}
	msgs := rec.messages()
	if len(msgs) != 1 || msgs[0].Direction != "tx" || msgs[0].Operator != "admin" || msgs[0].Text != "test broadcast" {
		t.Fatalf("recorded tx = %v, want the broadcast row", msgs)
	}
}

// TestHubSendDirect pins a direct message to an 8-hex node id, including
// the '!' form and bad-id rejection.
func TestHubSendDirect(t *testing.T) {
	radio := newTestRadio(t, Config{Enabled: true, Device: "/dev/fake", NodeTTL: time.Hour})
	radio.waitConnected(t)
	rec := &captureRecorder{}
	radio.hub.SetRecorder(rec)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := radio.hub.SendContactMessage(ctx, "!ef010203", "direct hi", "admin"); err != nil {
		t.Fatalf("SendContactMessage: %v", err)
	}
	out := radio.waitOutbound(t, 1)
	if len(out) != 1 || out[0].GetTo() != 0xef010203 {
		t.Fatalf("outbound = %v, want direct to ef010203", out)
	}
	if !out[0].GetWantAck() {
		t.Fatal("direct message wants ack, want true")
	}
	msgs := rec.messages()
	if len(msgs) != 1 || msgs[0].Direction != "tx" || msgs[0].Channel != "dm" {
		t.Fatalf("recorded tx = %v, want the dm row", msgs)
	}

	if err := radio.hub.SendContactMessage(ctx, "zzzz", "x", "admin"); err == nil {
		t.Fatal("4-char id accepted, want error")
	}
	if err := radio.hub.SendContactMessage(ctx, "ghijklmn", "x", "admin"); err == nil {
		t.Fatal("non-hex id accepted, want error")
	}
}

// TestHubReceive pins the rx path: an incoming text packet is recorded
// and a station/message sink pair receives the feeds.
func TestHubReceive(t *testing.T) {
	radio := newTestRadio(t, Config{Enabled: true, Device: "/dev/fake", NodeTTL: time.Hour})
	radio.waitConnected(t)
	rec := &captureRecorder{}
	radio.hub.SetRecorder(rec)

	type doc struct {
		Topic     string
		Retained  bool
		Payload   []byte
		Published bool
	}
	var (
		mu       sync.Mutex
		msgs     []doc
		stations []doc
	)
	radio.hub.SetMessageSink(func(_ context.Context, topic string, retained bool, payload []byte) error {
		mu.Lock()
		msgs = append(msgs, doc{Topic: topic, Retained: retained, Payload: payload, Published: true})
		mu.Unlock()
		return nil
	})
	radio.hub.SetStationSink(func(_ context.Context, topic string, retained bool, payload []byte) error {
		mu.Lock()
		stations = append(stations, doc{Topic: topic, Retained: retained, Payload: payload, Published: true})
		mu.Unlock()
		return nil
	})

	dispatch(t, radio, textPacket(0xdeadbeef, core.BroadcastNodeID.Uint32(), "hello mesh"))

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(msgs)
		mu.Unlock()
		if n > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	mu.Lock()
	nMsgs, msgDocs := len(msgs), append([]doc(nil), msgs...)
	mu.Unlock()
	if nMsgs != 1 {
		t.Fatalf("message feed docs = %d, want 1", nMsgs)
	}
	m := msgDocs[0]
	if m.Topic != "meshtastic/messages" || m.Retained {
		t.Fatalf("message doc = %+v, want non-retained meshtastic/messages", m)
	}
	var body map[string]any
	if err := json.Unmarshal(m.Payload, &body); err != nil {
		t.Fatalf("message payload = %s: %v", m.Payload, err)
	}
	if body["sender"] != "deadbeef" || body["text"] != "hello mesh" || body["direction"] != "rx" {
		t.Fatalf("message body = %v, want rx from deadbeef", body)
	}
	rows := rec.messages()
	if len(rows) != 1 || rows[0].Direction != "rx" || rows[0].Sender != "deadbeef" {
		t.Fatalf("recorded rows = %v, want the rx row", rows)
	}

	// A position update from an unknown node creates a station document.
	lat := int32(500200000)
	lon := int32(200000000)
	dispatch(t, radio, &pb.MeshPacket{
		From: 0x12345678,
		To:   core.BroadcastNodeID.Uint32(),
		PayloadVariant: &pb.MeshPacket_Decoded{
			Decoded: &pb.Data{
				Portnum: pb.PortNum_POSITION_APP,
				Payload: mustMarshal(t, &pb.Position{LatitudeI: &lat, LongitudeI: &lon}),
			},
		},
	})
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		found := false
		for _, s := range stations {
			if s.Topic == "meshtastic/stations/12345678" {
				found = true
				break
			}
		}
		mu.Unlock()
		if found {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	mu.Lock()
	stationDocs := append([]doc(nil), stations...)
	mu.Unlock()
	found := false
	for _, s := range stationDocs {
		if s.Topic == "meshtastic/stations/12345678" && s.Retained {
			found = true
		}
	}
	if !found {
		t.Fatalf("station docs = %+v, want retained meshtastic/stations/12345678", stationDocs)
	}
}

// TestHubEventBridge pins the command-only alarm policy: a /debug DM to
// our node from a gate-approved sender becomes a /events document, while
// plain DMs, broadcasts and unapproved senders stay off the events
// stream.
func TestHubEventBridge(t *testing.T) {
	radio := newTestRadio(t, Config{Enabled: true, Device: "/dev/fake", NodeTTL: time.Hour})
	radio.waitConnected(t)
	var (
		mu     sync.Mutex
		events []string
	)
	radio.hub.SetSenderGate(func(id string) string {
		if id == "deadbeef" {
			return "sp9kow"
		}
		return ""
	})
	radio.hub.SetEventSink(func(_ context.Context, topic string, _ bool, payload []byte) error {
		mu.Lock()
		events = append(events, string(payload))
		mu.Unlock()
		return nil
	})
	radio.hub.SetCLI(radiocli.New("WarnFlux v1.0 - SOSNA - sosna.sp9moa.pl"))

	// /debug DM to us from the approved node.
	dispatch(t, radio, textPacket(0xdeadbeef, 0xabcd1234, "/debug"))
	// Plain DM from the approved node: stays off /events.
	dispatch(t, radio, textPacket(0xdeadbeef, 0xabcd1234, "powodz w krakowie"))
	// Broadcast from the approved node: stays off /events.
	dispatch(t, radio, textPacket(0xdeadbeef, core.BroadcastNodeID.Uint32(), "broadcast"))
	// DM from an unapproved node.
	dispatch(t, radio, textPacket(0x12345678, 0xabcd1234, "spam"))

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(events)
		mu.Unlock()
		if n >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond) // settle: no further events expected
	mu.Lock()
	defer mu.Unlock()
	if len(events) != 1 {
		t.Fatalf("events = %d, want exactly 1 (the /debug command only)", len(events))
	}
	var we MessageEventWire
	if err := json.Unmarshal([]byte(events[0]), &we); err != nil {
		t.Fatalf("event payload: %v", err)
	}
	if we.Event.Source != "meshtastic" || we.Event.SourceID != "deadbeef" {
		t.Fatalf("event = %+v, want source meshtastic from deadbeef", we.Event)
	}
	if !strings.Contains(we.Event.Headline, "sp9kow") {
		t.Fatalf("headline = %q, want the directory owner sp9kow", we.Event.Headline)
	}
	if !strings.Contains(we.Event.Headline, "PL-KR-MAKI") {
		t.Fatalf("headline = %q, want the node name", we.Event.Headline)
	}
	if !strings.Contains(we.Event.Headline, "/debug") {
		t.Fatalf("headline = %q, want the command text", we.Event.Headline)
	}
	if !strings.Contains(we.Event.Description, "sp9kow") {
		t.Fatalf("description = %q, want the sender", we.Event.Description)
	}
}

// TestHubRadioCLI pins the radio-command interpreter: /help is answered
// in-band and stays off the alarm pipeline, /debug fires the alarm like
// a plain message plus a confirmation, and plain messages still route.
func TestHubRadioCLI(t *testing.T) {
	radio := newTestRadio(t, Config{Enabled: true, Device: "/dev/fake", NodeTTL: time.Hour})
	radio.waitConnected(t)
	var (
		mu     sync.Mutex
		events []string
	)
	radio.hub.SetSenderGate(func(id string) string {
		if id == "deadbeef" {
			return "sp9kow"
		}
		return ""
	})
	radio.hub.SetEventSink(func(_ context.Context, topic string, _ bool, payload []byte) error {
		mu.Lock()
		events = append(events, string(payload))
		mu.Unlock()
		return nil
	})
	radio.hub.SetCLI(radiocli.New("WarnFlux v1.0 - SOSNA - sosna.sp9moa.pl"))

	// /help: answered, no alarm.
	dispatch(t, radio, textPacket(0xdeadbeef, 0xabcd1234, "/help"))
	reply := radio.waitOutbound(t, 1)
	if reply[0].GetTo() != 0xdeadbeef {
		t.Fatalf("help reply to = %08x, want deadbeef", reply[0].GetTo())
	}
	if got := string(reply[0].GetDecoded().GetPayload()); !strings.Contains(got, "Commands") {
		t.Fatalf("help reply = %q, want the command list", got)
	}
	mu.Lock()
	n := len(events)
	mu.Unlock()
	if n != 0 {
		t.Fatalf("events after /help = %d, want 0", n)
	}

	// /debug: alarm + confirmation.
	before := len(radio.outbound())
	dispatch(t, radio, textPacket(0xdeadbeef, 0xabcd1234, "/debug"))
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n = len(events)
		mu.Unlock()
		if n >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	mu.Lock()
	n = len(events)
	mu.Unlock()
	if n != 1 {
		t.Fatalf("events after /debug = %d, want 1", n)
	}
	if !strings.Contains(events[0], "/debug") {
		t.Fatalf("debug event = %q, want the command text", events[0])
	}
	radio.waitOutbound(t, before+1)
	out := radio.outbound()
	if got := string(out[len(out)-1].GetDecoded().GetPayload()); !strings.Contains(got, "debug alarm") {
		t.Fatalf("debug reply = %q, want the confirmation", got)
	}

	// Unauthorized senders: commands never run and never alarm — a
	// slash attempt only gets the public installation banner.
	before = len(radio.outbound())
	dispatch(t, radio, textPacket(0xcafebabe, 0xabcd1234, "/debug"))
	radio.waitOutbound(t, before+1)
	out = radio.outbound()
	if got := string(out[len(out)-1].GetDecoded().GetPayload()); !strings.Contains(got, "WarnFlux v1.0 - SOSNA") {
		t.Fatalf("unauthorized slash reply = %q, want the identity banner", got)
	}
	if got := string(out[len(out)-1].GetDecoded().GetPayload()); !strings.Contains(got, "type /help for help") {
		t.Fatalf("unauthorized slash reply = %q, missing the /help hint", got)
	}
	if out[len(out)-1].GetTo() != 0xcafebabe {
		t.Fatalf("unauthorized reply to = %08x, want cafebabe", out[len(out)-1].GetTo())
	}
	mu.Lock()
	n = len(events)
	mu.Unlock()
	if n != 1 {
		t.Fatalf("events after unauthorized /debug = %d, want still 1", n)
	}

	// /help is public: an unregistered sender still gets the command
	// list — but only of the commands they may run (no /debug).
	before = len(radio.outbound())
	dispatch(t, radio, textPacket(0xcafebabe, 0xabcd1234, "/help"))
	radio.waitOutbound(t, before+1)
	out = radio.outbound()
	got := string(out[len(out)-1].GetDecoded().GetPayload())
	if !strings.Contains(got, "Commands:") || !strings.Contains(got, "/help") {
		t.Fatalf("unauthorized /help reply = %q, want the public command list", got)
	}
	if strings.Contains(got, "/debug") {
		t.Fatalf("unauthorized /help reply = %q, must not list restricted commands", got)
	}
	if out[len(out)-1].GetTo() != 0xcafebabe {
		t.Fatalf("public /help reply to = %08x, want cafebabe", out[len(out)-1].GetTo())
	}
	mu.Lock()
	n = len(events)
	mu.Unlock()
	if n != 1 {
		t.Fatalf("events after public /help = %d, want still 1", n)
	}

	// A plain direct message never becomes an alarm: the standard
	// installation banner (with the /help hint) answers instead.
	before = len(radio.outbound())
	dispatch(t, radio, textPacket(0xdeadbeef, 0xabcd1234, "plain hello"))
	radio.waitOutbound(t, before+1)
	out = radio.outbound()
	if got := string(out[len(out)-1].GetDecoded().GetPayload()); !strings.Contains(got, "WarnFlux v1.0 - SOSNA") {
		t.Fatalf("plain reply = %q, want the standard banner", got)
	}
	if got := string(out[len(out)-1].GetDecoded().GetPayload()); !strings.Contains(got, "type /help for help") {
		t.Fatalf("plain reply = %q, missing the /help hint", got)
	}
	mu.Lock()
	n = len(events)
	mu.Unlock()
	if n != 1 {
		t.Fatalf("events after plain hello = %d, want still 1", n)
	}
}

// TestHubSendWithoutDevice pins the disconnected send failure: an
// unconnected hub refuses transmissions instead of panicking.
func TestHubSendWithoutDevice(t *testing.T) {
	hub, err := NewHub(Config{Enabled: true, Device: "/dev/fake", NodeTTL: time.Hour},
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := hub.SendChannelMessage(ctx, "x", "admin"); err == nil {
		t.Fatal("send without device succeeded, want error")
	}
}

// mustMarshal marshals a proto or fails the test.
func mustMarshal(t *testing.T, msg proto.Message) []byte {
	t.Helper()
	b, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}
