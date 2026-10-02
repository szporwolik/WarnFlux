package meshtastic

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kabili207/meshtastic-go/core"
	pb "github.com/kabili207/meshtastic-go/core/proto"
	"github.com/kabili207/meshtastic-go/device/clientapi"
	"github.com/kabili207/meshtastic-go/transport/client"
	"github.com/kabili207/meshtastic-go/transport/stream"
	"google.golang.org/protobuf/proto"
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

// testRadio wires a hub onto the library's in-memory client API server:
// real protocol over net.Pipe, no serial hardware.
type testRadio struct {
	srv    *clientapi.Server
	cancel context.CancelFunc
	hub    *Hub
	runErr chan error
	mu     sync.Mutex
	rx     []*pb.MeshPacket // outbound packets captured via OnOutboundPacket
}

// newTestTable is the handshake node directory used by the test radio.
func newTestTable() *testNodeTable {
	return &testNodeTable{
		self: nodeInfo(0xabcd1234, "RKSR-OWN", "OWN"),
		all: []*pb.NodeInfo{
			nodeInfo(0xef010203, "RKSR-TN-R3", "R3"),
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
	if err := radio.hub.SendChannelMessage(ctx, "test broadcast", "admin"); err != nil {
		t.Fatalf("SendChannelMessage: %v", err)
	}
	out := radio.outbound()
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
	out := radio.outbound()
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

// TestHubEventBridge pins the direct-message routing: a DM to our node
// from a gate-approved sender becomes a /events document; unapproved or
// non-direct packets stay off the events stream.
func TestHubEventBridge(t *testing.T) {
	radio := newTestRadio(t, Config{Enabled: true, Device: "/dev/fake", NodeTTL: time.Hour})
	radio.waitConnected(t)
	var (
		mu     sync.Mutex
		events []string
	)
	radio.hub.SetSenderGate(func(id string) bool { return id == "deadbeef" })
	radio.hub.SetEventSink(func(_ context.Context, topic string, _ bool, payload []byte) error {
		mu.Lock()
		events = append(events, string(payload))
		mu.Unlock()
		return nil
	})

	// DM to us from the approved node.
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
		t.Fatalf("events = %d, want exactly 1", len(events))
	}
	var we MessageEventWire
	if err := json.Unmarshal([]byte(events[0]), &we); err != nil {
		t.Fatalf("event payload: %v", err)
	}
	if we.Event.Source != "meshtastic" || we.Event.SourceID != "deadbeef" {
		t.Fatalf("event = %+v, want source meshtastic from deadbeef", we.Event)
	}
	if !strings.Contains(we.Event.Headline, "powodz w krakowie") {
		t.Fatalf("headline = %q, want the message text", we.Event.Headline)
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
