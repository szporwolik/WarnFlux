package meshtastic

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	"github.com/kabili207/meshtastic-go/transport/serial"
	"github.com/kabili207/meshtastic-go/transport/stream"
	"google.golang.org/protobuf/proto"

	"github.com/szporwolik/WarnFlux/internal/dispatch"
	"github.com/szporwolik/WarnFlux/internal/radiocli"
	"github.com/szporwolik/WarnFlux/internal/severity"
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
	srv     *clientapi.Server
	cancel  context.CancelFunc
	stopRun context.CancelFunc
	hub     *Hub
	runErr  chan error
	mu      sync.Mutex
	rx      []*pb.MeshPacket // outbound packets captured via OnOutboundPacket
	dials   atomic.Int32     // transport dials through the seam
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

// testRadioOpts tunes the harness: wrap decorates every dialled transport
// (to make the link misbehave) and setup runs on the hub BEFORE its
// session goroutine starts, so a test can still write its fields.
type testRadioOpts struct {
	wrap  func(transportConn) transportConn
	setup func(*Hub)
	// nodes overrides the device node table served at the handshake (nil =
	// the default table). The radio states its own favorites through it.
	nodes []*pb.NodeInfo
}

func newTestRadio(t *testing.T, cfg Config, opts ...testRadioOpts) *testRadio {
	t.Helper()
	opt := testRadioOpts{}
	if len(opts) > 0 {
		opt = opts[0]
	}
	table := newTestTable()
	if opt.nodes != nil {
		table.all = opt.nodes
	}
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
		var out transportConn = &clientAdapter{t: client.NewTransport(sc, client.TransportConfig{
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		})}
		// wrap lets a test make the link misbehave: the decorator runs
		// for EVERY dial, so the replacement session is wrapped too and
		// the test can tell a rebuilt link from the first.
		if opt.wrap != nil {
			out = opt.wrap(out)
		}
		return out, nil
	}
	t.Cleanup(func() {
		Dial = oldDial
		cancel()
	})

	hub, err := NewHub(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("NewHub: %v", err)
	}
	if opt.setup != nil {
		opt.setup(hub)
	}
	radio.hub = hub
	radio.runErr = make(chan error, 1)
	runCtx, stopRun := context.WithCancel(context.Background())
	radio.stopRun = stopRun
	t.Cleanup(stopRun)
	go func() { radio.runErr <- hub.Run(runCtx) }()
	return radio
}

// stopHub cancels the hub's Run context and waits for it to return, so a test
// can assert that shutting the station down does not wait for the broker.
func (r *testRadio) stopHub(t *testing.T) {
	t.Helper()
	if r.stopRun != nil {
		r.stopRun()
	}
	select {
	case <-r.runErr:
	case <-time.After(5 * time.Second):
		t.Fatal("hub.Run did not return after its context was cancelled")
	}
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

func dispatchPkt(t *testing.T, r *testRadio, pkt *pb.MeshPacket) {
	t.Helper()
	r.srv.DispatchToClients(&pb.FromRadio{
		PayloadVariant: &pb.FromRadio_Packet{Packet: pkt},
	})
}

// dispatchQueueStatus feeds one device queueStatus answer (the reply to a
// client send) into the session.
func dispatchQueueStatus(t *testing.T, r *testRadio, res pb.Routing_Error, id uint32) {
	t.Helper()
	r.srv.DispatchToClients(&pb.FromRadio{
		PayloadVariant: &pb.FromRadio_QueueStatus{
			QueueStatus: &pb.QueueStatus{Res: int32(res), MeshPacketId: id},
		},
	})
}

// fakeTransportConn is a minimal transportConn stand-in for the dial
// candidate tests: nothing about the session is exercised, only WHICH
// serial path the dial ended up opening.
type fakeTransportConn struct {
	port    string
	stopped int32
}

func (f *fakeTransportConn) Connect(context.Context) error { return nil }
func (f *fakeTransportConn) IsConnected() bool             { return true }
func (f *fakeTransportConn) Stop() error {
	atomic.AddInt32(&f.stopped, 1)
	return nil
}
func (f *fakeTransportConn) State() *client.DeviceState { return nil }
func (f *fakeTransportConn) SetPacketHandler(func(*pb.MeshPacket)) {
}
func (f *fakeTransportConn) Handle(proto.Message, func(proto.Message) error) {}
func (f *fakeTransportConn) SendToRadio(*pb.ToRadio) error                   { return nil }

// TestDeviceCandidates pins the parsing of the comma-separated device
// field: entries are trimmed and empty ones dropped.
func TestDeviceCandidates(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want []string
	}{
		{"/dev/ttyACM0", []string{"/dev/ttyACM0"}},
		{"/dev/ttyACM0, /dev/ttyACM1", []string{"/dev/ttyACM0", "/dev/ttyACM1"}},
		{"/dev/ttyACM1,/dev/ttyACM0", []string{"/dev/ttyACM1", "/dev/ttyACM0"}},
		{", /dev/ttyACM0,", []string{"/dev/ttyACM0"}},
		{" , ", nil},
		{"", nil},
	} {
		got := deviceCandidates(tc.in)
		if fmt.Sprint(got) != fmt.Sprint(tc.want) {
			t.Errorf("deviceCandidates(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

// TestDialTriesDeviceCandidates pins the reconnect self-healing: the hub
// dials the configured paths IN ORDER and connects to the first one that
// opens, so a node that re-enumerated onto another port number is found
// without a config edit (reported dev pain: ttyACM0 <-> ttyACM1 flips).
func TestDialTriesDeviceCandidates(t *testing.T) {
	old := serialDial
	t.Cleanup(func() { serialDial = old })

	var tried []string
	serialDial = func(_ context.Context, cfg serial.Config) (transportConn, error) {
		tried = append(tried, cfg.Port)
		if cfg.Port == "/dev/ttyACM1" {
			return &fakeTransportConn{port: cfg.Port}, nil
		}
		return nil, errors.New("no such file or directory")
	}

	conn, err := Dial(context.Background(), Config{Device: "/dev/ttyACM0, /dev/ttyACM1", Baud: 115200})
	if err != nil {
		t.Fatalf("Dial = %v, want the second candidate", err)
	}
	if fc, ok := conn.(*fakeTransportConn); !ok || fc.port != "/dev/ttyACM1" {
		t.Fatalf("connected via %v, want the /dev/ttyACM1 candidate", conn)
	}
	if len(tried) != 2 || tried[0] != "/dev/ttyACM0" || tried[1] != "/dev/ttyACM1" {
		t.Fatalf("tried %v, want [/dev/ttyACM0 /dev/ttyACM1] in order", tried)
	}

	// Every candidate failing: the error names all of them.
	tried = nil
	serialDial = func(_ context.Context, cfg serial.Config) (transportConn, error) {
		tried = append(tried, cfg.Port)
		return nil, errors.New("no such file or directory")
	}
	if _, err := Dial(context.Background(), Config{Device: "/dev/ttyACM0,/dev/ttyACM1"}); err == nil {
		t.Fatal("Dial with all candidates failing = nil, want error")
	} else if !strings.Contains(err.Error(), "/dev/ttyACM0") || !strings.Contains(err.Error(), "/dev/ttyACM1") {
		t.Fatalf("error %q, want both candidate paths named", err)
	}
	if len(tried) != 2 {
		t.Fatalf("tried %v, want both candidates attempted", tried)
	}
}

// TestDialTCPTransport pins the tcp link: a bare host gets the default
// API port appended, an explicit port passes through, the dial goes
// through the tcpDial seam, and misconfiguration is rejected without a
// dial attempt.
func TestDialTCPTransport(t *testing.T) {
	old := tcpDial
	t.Cleanup(func() { tcpDial = old })

	var gotAddr string
	tcpDial = func(_ context.Context, address string) (transportConn, error) {
		gotAddr = address
		return &fakeTransportConn{port: address}, nil
	}

	conn, err := Dial(context.Background(), Config{Transport: "tcp", Host: "pirx-node"})
	if err != nil {
		t.Fatalf("Dial(tcp) = %v", err)
	}
	if fc, ok := conn.(*fakeTransportConn); !ok || fc.port != "pirx-node:4403" {
		t.Fatalf("connected via %v, want pirx-node:4403", conn)
	}
	if gotAddr != "pirx-node:4403" {
		t.Fatalf("tcpDial address = %q, want pirx-node:4403", gotAddr)
	}

	// An explicit port passes through untouched.
	tcpDial = func(_ context.Context, address string) (transportConn, error) {
		gotAddr = address
		return &fakeTransportConn{port: address}, nil
	}
	if _, err := Dial(context.Background(), Config{Transport: "tcp", Host: "10.0.0.40:4403"}); err != nil {
		t.Fatalf("Dial(tcp host:port) = %v", err)
	}
	if gotAddr != "10.0.0.40:4403" {
		t.Fatalf("explicit port dialed as %q, want it unchanged", gotAddr)
	}

	// Missing host and unknown transports are errors, never dials.
	dials := 0
	tcpDial = func(_ context.Context, _ string) (transportConn, error) {
		dials++
		return &fakeTransportConn{}, nil
	}
	if _, err := Dial(context.Background(), Config{Transport: "tcp"}); err == nil {
		t.Fatal("tcp without host = nil, want error")
	}
	if _, err := Dial(context.Background(), Config{Transport: "udp", Host: "x"}); err == nil {
		t.Fatal("unknown transport = nil, want error")
	}
	if dials != 0 {
		t.Fatalf("misconfiguration still dialed %d times", dials)
	}

	// A failing TCP dial surfaces the endpoint in the error.
	tcpDial = func(_ context.Context, _ string) (transportConn, error) {
		return nil, errors.New("connection refused")
	}
	if _, err := Dial(context.Background(), Config{Transport: "tcp", Host: "10.0.0.99"}); err == nil {
		t.Fatal("failing tcp dial = nil, want error")
	} else if !strings.Contains(err.Error(), "10.0.0.99:4403") {
		t.Fatalf("error %q, want the dialed endpoint named", err)
	}
}

// TestDialTimeoutOnBlockedOpen pins the reconnect-loop self-healing: a
// serial open that hangs forever (the library ignoring the context on
// the blocking syscall path — the prod incident: "device radio silent"
// followed by a stuck reconnect) must not freeze Dial. Every attempt is
// bounded by dialAttemptTimeout, so the Run backoff loop keeps turning.
func TestDialTimeoutOnBlockedOpen(t *testing.T) {
	oldDial := serialDial
	oldTimeout := dialAttemptTimeout
	t.Cleanup(func() {
		serialDial = oldDial
		dialAttemptTimeout = oldTimeout
	})

	dialAttemptTimeout = 50 * time.Millisecond
	serialDial = func(ctx context.Context, _ serial.Config) (transportConn, error) {
		<-ctx.Done() // a hung open: it only unblocks when cancelled
		return nil, ctx.Err()
	}

	start := time.Now()
	_, err := Dial(context.Background(), Config{Device: "/dev/ttyACM0", Baud: 115200})
	if err == nil {
		t.Fatal("Dial with a hung open = nil, want the attempt timeout error")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("Dial took %v, want the attempt window to bound it", elapsed)
	}
}

// TestDialReapsLateConnect pins the leak guard: a serial open that
// completes JUST after the attempt deadline must be stopped, never left
// as a live orphan transport.
func TestDialReapsLateConnect(t *testing.T) {
	oldDial := serialDial
	oldTimeout := dialAttemptTimeout
	t.Cleanup(func() {
		serialDial = oldDial
		dialAttemptTimeout = oldTimeout
	})

	dialAttemptTimeout = 20 * time.Millisecond
	fc := &fakeTransportConn{port: "/dev/ttyACM0"}
	serialDial = func(_ context.Context, cfg serial.Config) (transportConn, error) {
		time.Sleep(80 * time.Millisecond) // finishes after the deadline
		return fc, nil
	}

	if _, err := Dial(context.Background(), Config{Device: "/dev/ttyACM0"}); err == nil {
		t.Fatal("Dial = nil, want the attempt timeout error")
	}
	// The reap goroutine stops the late transport shortly after it lands.
	deadline := time.Now().Add(time.Second)
	for atomic.LoadInt32(&fc.stopped) == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if atomic.LoadInt32(&fc.stopped) != 1 {
		t.Fatalf("late transport stopped %d times, want 1 (orphan leak)", atomic.LoadInt32(&fc.stopped))
	}
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

// TestHubNodeDirectoryRetention pins the 24/7/365 bound: nodes unheard
// for longer than nodeDirRetention are dropped from the directory, so
// memory and the persisted table never grow without bound.
func TestHubNodeDirectoryRetention(t *testing.T) {
	radio := newTestRadio(t, Config{Enabled: true, Device: "/dev/fake", NodeTTL: time.Hour})
	radio.waitConnected(t)
	radio.hub.SeedNode("11111111", "old visitor", "old", 0, 0, time.Now().Add(-31*24*time.Hour), nil)
	radio.hub.SeedNode("22222222", "active member", "act", 0, 0, time.Now(), nil)

	radio.hub.expireNodes()

	snap := radio.hub.Snapshot()
	ids := make(map[string]bool, len(snap.Nodes))
	for _, n := range snap.Nodes {
		ids[n.ID] = true
	}
	if ids["11111111"] {
		t.Fatal("node unheard for 31 days still in the directory")
	}
	if !ids["22222222"] {
		t.Fatal("recent node dropped from the directory")
	}
}

// TestNodeHopsFromPacket pins the hop count learned from the packet
// header: hops travelled = hop_start - hop_limit; a direct packet
// counts 0. The recorded message row must carry the same travelled
// value (hop_start alone overstates direct packets).
func TestNodeHopsFromPacket(t *testing.T) {
	radio := newTestRadio(t, Config{Enabled: true, Device: "/dev/fake", NodeTTL: time.Hour})
	radio.waitConnected(t)
	rec := &captureRecorder{}
	radio.hub.SetRecorder(rec)

	pkt := textPacket(0xef010203, 0xabcd1234, "hello")
	pkt.HopStart, pkt.HopLimit = 3, 1
	dispatchPkt(t, radio, pkt)

	deadline := time.Now().Add(5 * time.Second)
	var hops int
	for time.Now().Before(deadline) {
		for _, n := range radio.hub.Snapshot().Nodes {
			if n.ID == "ef010203" {
				hops = n.Hops
			}
		}
		if hops == 2 {
			for _, m := range rec.messages() {
				if m.Direction == "rx" && m.Hops != 2 {
					t.Fatalf("recorded message hops = %d, want 2", m.Hops)
				}
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("node hops = %d, want 2", hops)
}

// TestTraceroute pins the route probe: the hub sends a TRACEROUTE_APP
// frame with the destination node number in the payload, and the
// destination's route_reply resolves the probe with the intermediate
// hops and SNRs (scaled 4x in the protocol).
func TestTraceroute(t *testing.T) {
	radio := newTestRadio(t, Config{Enabled: true, Device: "/dev/fake", NodeTTL: time.Hour})
	radio.waitConnected(t)

	type out struct {
		res TracerouteResult
		err error
	}
	done := make(chan out, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		res, err := radio.hub.Traceroute(ctx, "ef010203", 5*time.Second)
		done <- out{res, err}
	}()

	pkt := radio.waitOutbound(t, 1)
	if len(pkt) != 1 || pkt[0].GetTo() != 0xef010203 {
		t.Fatalf("outbound = %v, want one probe to ef010203", pkt)
	}
	if got := pkt[0].GetDecoded().GetPortnum(); got != pb.PortNum_TRACEROUTE_APP {
		t.Fatalf("portnum = %v, want TRACEROUTE_APP", got)
	}
	// The firmware only answers traceroute frames flagged want_ack +
	// want_response.
	if !pkt[0].GetWantAck() || !pkt[0].GetDecoded().GetWantResponse() {
		t.Fatalf("probe flags = ack:%v resp:%v, want both true", pkt[0].GetWantAck(), pkt[0].GetDecoded().GetWantResponse())
	}
	// The payload is an empty RouteDiscovery protobuf (the firmware 2.x
	// format: relays extend it along the way).
	var rd pb.RouteDiscovery
	if err := proto.Unmarshal(pkt[0].GetDecoded().GetPayload(), &rd); err != nil {
		t.Fatalf("payload is not a RouteDiscovery: %v", err)
	}
	if len(rd.GetRoute()) != 0 || len(rd.GetSnrTowards()) != 0 {
		t.Fatalf("probe payload must start empty, got route=%v snr=%v", rd.GetRoute(), rd.GetSnrTowards())
	}

	// The destination answers with the route: one intermediate hop at
	// 24/4 = 6 dB.
	rr, err := proto.Marshal(&pb.Routing{Variant: &pb.Routing_RouteReply{RouteReply: &pb.RouteDiscovery{
		Route:   []uint32{0xaaaa0001},
		SnrBack: []int32{24},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	dispatchPkt(t, radio, &pb.MeshPacket{
		From: 0xef010203, To: 0xabcd1234,
		PayloadVariant: &pb.MeshPacket_Decoded{
			Decoded: &pb.Data{Portnum: pb.PortNum_ROUTING_APP, Payload: rr},
		},
	})

	select {
	case r := <-done:
		if r.err != nil {
			t.Fatalf("Traceroute: %v", r.err)
		}
		if !r.res.Reached || len(r.res.Hops) != 2 {
			t.Fatalf("result = %+v, want reached with intermediate + destination", r.res)
		}
		if r.res.Hops[0].ID != "aaaa0001" || r.res.Hops[0].SNR != 6.0 {
			t.Fatalf("hop = %+v, want aaaa0001 @ 6 dB", r.res.Hops[0])
		}
		if r.res.Hops[1].ID != "ef010203" {
			t.Fatalf("last hop = %+v, want the destination", r.res.Hops[1])
		}
	case <-time.After(6 * time.Second):
		t.Fatal("traceroute did not settle in time")
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
	dispatchPkt(t, radio, &pb.MeshPacket{
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
	dispatchPkt(t, radio, &pb.MeshPacket{
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
	dispatchPkt(t, radio, &pb.MeshPacket{
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

// TestHubAckForeignNodeIgnored pins the ack binding: a routing frame
// from a node that is NOT the recipient (same request id) never settles
// the exchange — the wait stays pending and the real recipient's ack
// still delivers it.
func TestHubAckForeignNodeIgnored(t *testing.T) {
	radio := newTestRadio(t, Config{Enabled: true, Device: "/dev/fake", NodeTTL: time.Hour})
	radio.waitConnected(t)
	rec := &captureRecorder{}
	radio.hub.SetRecorder(rec)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := radio.hub.SendContactMessage(ctx, "ef010203", "ack me", "admin"); err != nil {
		t.Fatalf("SendContactMessage: %v", err)
	}

	// The device echo pairs the packet id (modem acceptance: TxSent).
	dispatchPkt(t, radio, &pb.MeshPacket{
		From: 0xabcd1234, Id: 4242, To: 0xef010203,
		PayloadVariant: &pb.MeshPacket_Decoded{
			Decoded: &pb.Data{Portnum: pb.PortNum_TEXT_MESSAGE_APP, Payload: []byte("ack me")},
		},
	})
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if msgs := rec.messages(); len(msgs) == 1 && msgs[0].Status == TxSent {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	routing, err := proto.Marshal(&pb.Routing{Variant: &pb.Routing_ErrorReason{ErrorReason: pb.Routing_NONE}})
	if err != nil {
		t.Fatal(err)
	}
	// A FOREIGN node's routing frame with the right id: ignored.
	dispatchPkt(t, radio, &pb.MeshPacket{
		From: 0xcafebabe, To: 0xabcd1234,
		PayloadVariant: &pb.MeshPacket_Decoded{
			Decoded: &pb.Data{Portnum: pb.PortNum_ROUTING_APP, RequestId: 4242, Payload: routing},
		},
	})
	time.Sleep(150 * time.Millisecond)
	if msgs := rec.messages(); len(msgs) != 1 || msgs[0].Status != TxSent {
		t.Fatalf("after foreign ack = %v, want still TxSent", msgs)
	}

	// The real recipient's ack settles the exchange.
	dispatchPkt(t, radio, &pb.MeshPacket{
		From: 0xef010203, To: 0xabcd1234,
		PayloadVariant: &pb.MeshPacket_Decoded{
			Decoded: &pb.Data{Portnum: pb.PortNum_ROUTING_APP, RequestId: 4242, Payload: routing},
		},
	})
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if msgs := rec.messages(); len(msgs) == 1 && msgs[0].Status == TxDelivered {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	msgs := rec.messages()
	if len(msgs) != 1 || msgs[0].Status != TxDelivered {
		t.Fatalf("after recipient ack = %v, want TxDelivered", msgs)
	}
}

// TestHubModemRejectsDelivery pins the modem/delivery distinction: a
// self-addressed routing frame WITH an error reason means the modem
// accepted the message but could not deliver it — the tx fails instead
// of staying "sent".
func TestHubModemRejectsDelivery(t *testing.T) {
	radio := newTestRadio(t, Config{Enabled: true, Device: "/dev/fake", NodeTTL: time.Hour})
	radio.waitConnected(t)
	rec := &captureRecorder{}
	radio.hub.SetRecorder(rec)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := radio.hub.SendContactMessage(ctx, "ef010203", "ack me", "admin"); err != nil {
		t.Fatalf("SendContactMessage: %v", err)
	}
	dispatchPkt(t, radio, &pb.MeshPacket{
		From: 0xabcd1234, Id: 4242, To: 0xef010203,
		PayloadVariant: &pb.MeshPacket_Decoded{
			Decoded: &pb.Data{Portnum: pb.PortNum_TEXT_MESSAGE_APP, Payload: []byte("ack me")},
		},
	})
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if msgs := rec.messages(); len(msgs) == 1 && msgs[0].Status == TxSent {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// The modem reports it could not deliver (self-addressed, error set).
	fail, err := proto.Marshal(&pb.Routing{Variant: &pb.Routing_ErrorReason{ErrorReason: pb.Routing_MAX_RETRANSMIT}})
	if err != nil {
		t.Fatal(err)
	}
	dispatchPkt(t, radio, &pb.MeshPacket{
		From: 0xabcd1234, To: 0xabcd1234, Id: 4242,
		PayloadVariant: &pb.MeshPacket_Decoded{
			Decoded: &pb.Data{Portnum: pb.PortNum_ROUTING_APP, RequestId: 4242, Payload: fail},
		},
	})
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if msgs := rec.messages(); len(msgs) == 1 && msgs[0].Status == TxFailed {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	msgs := rec.messages()
	if len(msgs) != 1 || msgs[0].Status != TxFailed {
		t.Fatalf("after modem rejection = %v, want TxFailed", msgs)
	}
}

// TestHubModemAckKeepsRecipientWait pins the conditional P2 fix: a
// self-addressed routing frame WITHOUT an error is the modem's
// acceptance of a direct message, NOT its final result. The wait must
// stay registered so a later recipient ack still lifts the status to
// delivered (the synthetic "own ack, then recipient ack" sequence).
func TestHubModemAckKeepsRecipientWait(t *testing.T) {
	radio := newTestRadio(t, Config{Enabled: true, Device: "/dev/fake", NodeTTL: time.Hour,
		AckSettleTimeout: 10 * time.Second})
	radio.waitConnected(t)
	rec := &captureRecorder{}
	radio.hub.SetRecorder(rec)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := radio.hub.SendContactMessage(ctx, "ef010203", "synthetic seq", "admin"); err != nil {
		t.Fatalf("SendContactMessage: %v", err)
	}

	// The device echo pairs the packet id (modem acceptance: TxSent).
	dispatchPkt(t, radio, &pb.MeshPacket{
		From: 0xabcd1234, Id: 4242, To: 0xef010203,
		PayloadVariant: &pb.MeshPacket_Decoded{
			Decoded: &pb.Data{Portnum: pb.PortNum_TEXT_MESSAGE_APP, Payload: []byte("synthetic seq")},
		},
	})
	waitSent := func() {
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if msgs := rec.messages(); len(msgs) == 1 && msgs[0].Status == TxSent {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatalf("tx never reached sent: %v", rec.messages())
	}
	waitSent()

	// The modem's own routing frame (no error) confirms the acceptance
	// but must NOT consume the wait for the recipient's ack.
	routing, err := proto.Marshal(&pb.Routing{Variant: &pb.Routing_ErrorReason{ErrorReason: pb.Routing_NONE}})
	if err != nil {
		t.Fatal(err)
	}
	dispatchPkt(t, radio, &pb.MeshPacket{
		From: 0xabcd1234, To: 0xabcd1234, Id: 4242,
		PayloadVariant: &pb.MeshPacket_Decoded{
			Decoded: &pb.Data{Portnum: pb.PortNum_ROUTING_APP, RequestId: 4242, Payload: routing},
		},
	})
	time.Sleep(150 * time.Millisecond)
	if msgs := rec.messages(); len(msgs) != 1 || msgs[0].Status != TxSent {
		t.Fatalf("after own ack = %v, want still TxSent (the wait must survive)", msgs)
	}
	radio.hub.sendMu.Lock()
	kept := radio.hub.pending[4242] != nil
	radio.hub.sendMu.Unlock()
	if !kept {
		t.Fatal("own ack consumed the pending wait; the recipient's ack could never deliver")
	}

	// The recipient's ack is the FINAL result: delivered.
	dispatchPkt(t, radio, &pb.MeshPacket{
		From: 0xef010203, To: 0xabcd1234,
		PayloadVariant: &pb.MeshPacket_Decoded{
			Decoded: &pb.Data{Portnum: pb.PortNum_ROUTING_APP, RequestId: 4242, Payload: routing},
		},
	})
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if msgs := rec.messages(); len(msgs) == 1 && msgs[0].Status == TxDelivered {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	msgs := rec.messages()
	if len(msgs) != 1 || msgs[0].Status != TxDelivered {
		t.Fatalf("after own ack + recipient ack = %v, want TxDelivered", msgs)
	}
}

// TestHubModemAckStillSettlesUnanswered pins the kept wait's final
// result: a modem-confirmed direct message whose recipient never acks
// settles failed through the settle window — the wait does not linger
// forever.
func TestHubModemAckStillSettlesUnanswered(t *testing.T) {
	radio := newTestRadio(t, Config{Enabled: true, Device: "/dev/fake", NodeTTL: time.Hour,
		AckSettleTimeout: 250 * time.Millisecond})
	radio.waitConnected(t)
	rec := &captureRecorder{}
	radio.hub.SetRecorder(rec)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := radio.hub.SendContactMessage(ctx, "ef010203", "never acked", "admin"); err != nil {
		t.Fatalf("SendContactMessage: %v", err)
	}
	dispatchPkt(t, radio, &pb.MeshPacket{
		From: 0xabcd1234, Id: 4242, To: 0xef010203,
		PayloadVariant: &pb.MeshPacket_Decoded{
			Decoded: &pb.Data{Portnum: pb.PortNum_TEXT_MESSAGE_APP, Payload: []byte("never acked")},
		},
	})
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if msgs := rec.messages(); len(msgs) == 1 && msgs[0].Status == TxSent {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	routing, err := proto.Marshal(&pb.Routing{Variant: &pb.Routing_ErrorReason{ErrorReason: pb.Routing_NONE}})
	if err != nil {
		t.Fatal(err)
	}
	dispatchPkt(t, radio, &pb.MeshPacket{
		From: 0xabcd1234, To: 0xabcd1234, Id: 4242,
		PayloadVariant: &pb.MeshPacket_Decoded{
			Decoded: &pb.Data{Portnum: pb.PortNum_ROUTING_APP, RequestId: 4242, Payload: routing},
		},
	})

	// The settle window (retryPending tick) resolves the unanswered wait
	// to failed — never an eternal "sent".
	deadline = time.Now().Add(12 * time.Second)
	for time.Now().Before(deadline) {
		if msgs := rec.messages(); len(msgs) == 1 && msgs[0].Status == TxFailed {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	msgs := rec.messages()
	if len(msgs) != 1 || msgs[0].Status != TxFailed {
		t.Fatalf("unanswered modem-confirmed dm = %v, want the settle to failed", msgs)
	}
}

// TestHubDeviceRefusalFailsSendImmediately pins the diagnosability fix:
// a device refusal (queueStatus.res) means the frame never reached the
// air, so the send settles failed AT ONCE and with the refusal reason
// recorded — instead of being stamped "sent" and left waiting out the
// acknowledgment window for a message that was never transmitted.
func TestHubDeviceRefusalFailsSendImmediately(t *testing.T) {
	radio := newTestRadio(t, Config{Enabled: true, Device: "/dev/fake", NodeTTL: time.Hour,
		AckSettleTimeout: time.Hour})
	radio.waitConnected(t)
	rec := &captureRecorder{}
	radio.hub.SetRecorder(rec)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := radio.hub.SendContactMessage(ctx, "6d2c8f6c", "bez klucza", "admin"); err != nil {
		t.Fatalf("SendContactMessage: %v", err)
	}

	dispatchQueueStatus(t, radio, pb.Routing_PKI_SEND_FAIL_PUBLIC_KEY, 4242)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if msgs := rec.messages(); len(msgs) == 1 && msgs[0].Status == TxFailed {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if msgs := rec.messages(); len(msgs) != 1 || msgs[0].Status != TxFailed {
		t.Fatalf("refused send = %v, want an immediate TxFailed", msgs)
	}
	if got := rec.reasonFor("bez klucza"); got != "pki_no_key" {
		t.Fatalf("reason = %q, want pki_no_key", got)
	}
	// The refusal is final: nothing may stay pending, or the same row
	// would settle a second time minutes later.
	radio.hub.sendMu.Lock()
	_, lingering := radio.hub.pending[4242]
	radio.hub.sendMu.Unlock()
	if lingering {
		t.Fatal("a refused send stayed pending for an acknowledgment")
	}

	// A missing recipient key additionally asks the peer to re-announce
	// its NodeInfo: the radio caches peer keys from NodeInfo (and never
	// falls back to channel encryption), so the refresh is what unblocks
	// a later attempt.
	out := radio.waitOutbound(t, 2)
	last := out[len(out)-1]
	if last.GetTo() != 0x6d2c8f6c || last.GetDecoded().GetPortnum() != pb.PortNum_NODEINFO_APP ||
		!last.GetDecoded().GetWantResponse() {
		t.Fatalf("node info refresh = %+v, want a want_response NODEINFO_APP to 6d2c8f6c", last)
	}
}

// TestHubDeviceRefusalWithoutKeyReason pins that only a PEER KEY refusal
// spends the extra airtime of a NodeInfo refresh; every other refusal
// keeps its reason and sends nothing more.
func TestHubDeviceRefusalWithoutKeyReason(t *testing.T) {
	radio := newTestRadio(t, Config{Enabled: true, Device: "/dev/fake", NodeTTL: time.Hour,
		AckSettleTimeout: time.Hour})
	radio.waitConnected(t)
	rec := &captureRecorder{}
	radio.hub.SetRecorder(rec)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := radio.hub.SendContactMessage(ctx, "deadbeef", "nie ma trasy", "admin"); err != nil {
		t.Fatalf("SendContactMessage: %v", err)
	}

	dispatchQueueStatus(t, radio, pb.Routing_NO_ROUTE, 4243)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if msgs := rec.messages(); len(msgs) == 1 && msgs[0].Status == TxFailed {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if msgs := rec.messages(); len(msgs) != 1 || msgs[0].Status != TxFailed {
		t.Fatalf("refused send = %v, want TxFailed", msgs)
	}
	if got := rec.reasonFor("nie ma trasy"); got != "no_route" {
		t.Fatalf("reason = %q, want no_route", got)
	}
	time.Sleep(150 * time.Millisecond)
	if out := radio.outbound(); len(out) != 1 {
		t.Fatalf("outbound = %d packets, want only the refused message", len(out))
	}
}

// TestSendFailureReasonKeys pins the reason keys the log line, the stored
// history row and the UI translation share. An unknown code keeps its
// numeric form so a newer firmware enum is never swallowed silently.
func TestSendFailureReasonKeys(t *testing.T) {
	cases := map[pb.Routing_Error]string{
		pb.Routing_NO_ROUTE:                      "no_route",
		pb.Routing_GOT_NAK:                       "got_nak",
		pb.Routing_TIMEOUT:                       "timeout",
		pb.Routing_NO_INTERFACE:                  "no_interface",
		pb.Routing_MAX_RETRANSMIT:                "max_retransmit",
		pb.Routing_NO_CHANNEL:                    "no_channel",
		pb.Routing_TOO_LARGE:                     "too_large",
		pb.Routing_NO_RESPONSE:                   "no_response",
		pb.Routing_DUTY_CYCLE_LIMIT:              "duty_cycle",
		pb.Routing_BAD_REQUEST:                   "bad_request",
		pb.Routing_NOT_AUTHORIZED:                "not_authorized",
		pb.Routing_PKI_FAILED:                    "pki_failed",
		pb.Routing_PKI_UNKNOWN_PUBKEY:            "pki_unknown_key",
		pb.Routing_ADMIN_BAD_SESSION_KEY:         "admin_bad_session_key",
		pb.Routing_ADMIN_PUBLIC_KEY_UNAUTHORIZED: "admin_unauthorized",
		pb.Routing_RATE_LIMIT_EXCEEDED:           "rate_limit",
		pb.Routing_PKI_SEND_FAIL_PUBLIC_KEY:      "pki_no_key",
		pb.Routing_Error(99):                     "res_99",
	}
	for res, want := range cases {
		if got := sendFailureReason(res); got != want {
			t.Errorf("sendFailureReason(%d) = %q, want %q", int32(res), got, want)
		}
	}
	for res, want := range map[pb.Routing_Error]bool{
		pb.Routing_PKI_FAILED:               true,
		pb.Routing_PKI_UNKNOWN_PUBKEY:       true,
		pb.Routing_PKI_SEND_FAIL_PUBLIC_KEY: true,
		pb.Routing_NO_ROUTE:                 false,
		pb.Routing_RATE_LIMIT_EXCEEDED:      false,
	} {
		if got := needsNodeInfoRefresh(res); got != want {
			t.Errorf("needsNodeInfoRefresh(%d) = %v, want %v", int32(res), got, want)
		}
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
	dispatchPkt(t, radio, &pb.MeshPacket{
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
	dispatchPkt(t, radio, &pb.MeshPacket{
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

// TestTabChannel pins the extra message-history tab channel: the
// read-only watch channel wins when configured, otherwise the tab
// follows the emcom channel.
func TestTabChannel(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	hub, err := NewHub(Config{Enabled: false, EmcomChannel: 1}, logger)
	if err != nil {
		t.Fatalf("NewHub: %v", err)
	}
	if got := hub.TabChannel(); got != 1 {
		t.Fatalf("TabChannel with emcom only = %d, want 1", got)
	}
	hub2, err := NewHub(Config{Enabled: false, EmcomChannel: 1, WatchChannel: 2}, logger)
	if err != nil {
		t.Fatalf("NewHub: %v", err)
	}
	if got := hub2.TabChannel(); got != 2 {
		t.Fatalf("TabChannel with watch = %d, want 2", got)
	}
}

// TestNodeHopsAndLastSeen pins the traceroute planning accessors: hops
// and last-seen of a heard node (unknown nodes report -1 / zero time).
func TestNodeHopsAndLastSeen(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	hub, err := NewHub(Config{Enabled: false}, logger)
	if err != nil {
		t.Fatalf("NewHub: %v", err)
	}
	ts := time.Now().Add(-2 * time.Hour)
	hub.mu.Lock()
	hub.nodes["aa010203"] = &Node{ID: "aa010203", Hops: 4, LastSeen: ts}
	hub.mu.Unlock()

	if got := hub.NodeHops("aa010203"); got != 4 {
		t.Fatalf("NodeHops = %d, want 4", got)
	}
	if got := hub.NodeHops("AA010203"); got != 4 {
		t.Fatalf("NodeHops (uppercase id) = %d, want 4", got)
	}
	if got := hub.NodeHops("ffffffff"); got != -1 {
		t.Fatalf("NodeHops (unknown) = %d, want -1", got)
	}
	if got := hub.NodeLastSeen("aa010203"); !got.Equal(ts) {
		t.Fatalf("NodeLastSeen = %v, want %v", got, ts)
	}
	if got := hub.NodeLastSeen("ffffffff"); !got.IsZero() {
		t.Fatalf("NodeLastSeen (unknown) = %v, want zero time", got)
	}
}

// TestResolveNode pins the query resolution for contact sends: an
// 8-hex id, a name, a short name and a name prefix all map onto the
// canonical id; unknown queries stay empty.
func TestResolveNode(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	hub, err := NewHub(Config{Enabled: false}, logger)
	if err != nil {
		t.Fatalf("NewHub: %v", err)
	}
	hub.mu.Lock()
	hub.nodes["b2a73ef8"] = &Node{ID: "b2a73ef8", Name: "format HQ", Short: "foip"}
	hub.nodes["aa010203"] = &Node{ID: "aa010203", Name: "Some Other", Short: "OTR"}
	hub.nodes["cc040506"] = &Node{ID: "cc040506", Name: "Server Node", Short: "EDG"}
	hub.mu.Unlock()

	for _, q := range []string{"b2a73ef8", "B2A73EF8", "!b2a73ef8", "format hq", "FORMAT HQ", "foip", "format", "hq"} {
		if got := hub.ResolveNode(q); got != "b2a73ef8" {
			t.Fatalf("ResolveNode(%q) = %q, want b2a73ef8", q, got)
		}
	}
	if got := hub.ResolveNode("nope"); got != "" {
		t.Fatalf("ResolveNode(nope) = %q, want empty", got)
	}
	// An ambiguous substring query must stay unresolved: "er" appears
	// in both "Some Other" and "Server Node".
	if got := hub.ResolveNode("er"); got != "" {
		t.Fatalf("ResolveNode(er) = %q, want empty (ambiguous)", got)
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
	// The hub invents nothing: the configuration layer names either a daily
	// anchor or a spacing, and neither one means the beacon is off.
	if hub.cfg.EmcomInterval != 0 || hub.cfg.EmcomAt != "" {
		t.Fatalf("hub invented a beacon cadence: interval=%s at=%q", hub.cfg.EmcomInterval, hub.cfg.EmcomAt)
	}

	// Both at once is a configuration error, not a silent override.
	if _, err := NewHub(Config{Enabled: true, Device: "/dev/fake", EmcomAt: "06:00", EmcomInterval: time.Hour},
		slog.New(slog.NewTextHandler(io.Discard, nil))); err == nil {
		t.Fatal("anchor plus spacing was accepted")
	}
	if _, err := NewHub(Config{Enabled: true, Device: "/dev/fake", EmcomAt: "6 rano"},
		slog.New(slog.NewTextHandler(io.Discard, nil))); err == nil {
		t.Fatal("a bogus anchor time was accepted")
	}
}

// TestHubBeaconDailyAnchor pins the daily presence beacon: it is armed for
// the next clock time (never at startup, so a restart cannot beacon ahead
// of schedule), fires once when that time comes, and re-arms for tomorrow.
func TestHubBeaconDailyAnchor(t *testing.T) {
	radio := newTestRadio(t, Config{Enabled: true, Device: "/dev/fake", NodeTTL: time.Hour,
		EmcomChannel: 1, EmcomIdentity: "WarnFlux v0.7.6 - SOSNA", EmcomAt: "06:00"})
	// The station's clock sits at noon, so the first beacon is due at 06:00
	// the next morning.
	noon := time.Date(2026, 10, 12, 12, 0, 0, 0, time.Local)
	now := noon
	radio.hub.now = func() time.Time { return now }
	radio.hub.startedAt = noon
	radio.waitConnected(t)

	radio.hub.mu.Lock()
	armed := radio.hub.emcomNext
	radio.hub.mu.Unlock()
	if want := time.Date(2026, 10, 13, 6, 0, 0, 0, time.Local); !armed.Equal(want) {
		t.Fatalf("armed for %s, want %s", armed, want)
	}

	// Nothing is sent before the anchor.
	radio.hub.beaconTick()
	if n := len(radio.outbound()); n != 0 {
		t.Fatalf("beacon fired before its anchor (%d packets)", n)
	}

	// Exactly at the anchor it goes out and re-arms for tomorrow. The send
	// reaches the radio asynchronously, so wait for the packet instead of
	// racing a loaded test machine.
	now = time.Date(2026, 10, 13, 6, 0, 0, 0, time.Local)
	radio.hub.beaconTick()
	if n := len(radio.waitOutbound(t, 1)); n != 1 {
		t.Fatalf("beacon packets at the anchor = %d, want 1", n)
	}
	radio.hub.mu.Lock()
	next := radio.hub.emcomNext
	radio.hub.mu.Unlock()
	if want := time.Date(2026, 10, 14, 6, 0, 0, 0, time.Local); !next.Equal(want) {
		t.Fatalf("re-armed for %s, want %s", next, want)
	}

	// The same day does not produce a second beacon.
	now = time.Date(2026, 10, 13, 12, 0, 0, 0, time.Local)
	radio.hub.beaconTick()
	if n := len(radio.outbound()); n != 1 {
		t.Fatalf("beacon repeated within the day (%d packets)", n)
	}
}

// TestHubHazardDigest pins the periodic channel status line: ONE message
// with the hazard counters and the public site — never the hazard list,
// which would repeat on the air what already went out as alerts — on the
// emcom channel, fired on the first maintenance tick after the first
// session.
func TestHubHazardDigest(t *testing.T) {
	radio := newTestRadio(t, Config{Enabled: true, Device: "/dev/fake", NodeTTL: time.Hour,
		EmcomChannel: 1, EmcomHazardsInterval: time.Hour, EmcomSite: "https://sosna.example.pl"})
	radio.hub.SetHazardSummarySource(func() HazardSummary {
		return HazardSummary{Severe: 2, Other: 12}
	})
	radio.waitConnected(t)

	deadline := time.Now().Add(10 * time.Second)
	var out []*pb.MeshPacket
	for time.Now().Before(deadline) {
		if out = radio.outbound(); len(out) >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(out) != 1 {
		t.Fatalf("outbound = %d packets, want a single status line", len(out))
	}
	p := out[0]
	if p.GetChannel() != 1 {
		t.Fatalf("digest channel = %d, want 1", p.GetChannel())
	}
	if p.GetTo() != core.BroadcastNodeID.Uint32() {
		t.Fatalf("digest to = %v, want a broadcast", p.GetTo())
	}
	if got, want := string(p.GetDecoded().GetPayload()),
		"WarnFlux: 2 severe or more, 12 other active — https://sosna.example.pl · DM me /hazard for the list"; got != want {
		t.Fatalf("digest line = %q, want %q", got, want)
	}
	if n := len([]rune(string(p.GetDecoded().GetPayload()))); n > meshTextMaxRunes {
		t.Fatalf("digest line = %d runes, over the channel limit", n)
	}

	// Without a configured site the counters still go out, bare.
	if got, want := (&Hub{cfg: Config{}}).hazardSummaryLine(HazardSummary{Other: 3}),
		"WarnFlux: 0 severe or more, 3 other active · DM me /hazard for the list"; got != want {
		t.Fatalf("site-less digest line = %q, want %q", got, want)
	}

	// A site long enough to fill the message drops the hint instead of
	// cutting it in half.
	long := &Hub{cfg: Config{EmcomSite: "https://" + strings.Repeat("x", meshTextMaxRunes) + ".example.pl"}}
	got := long.hazardSummaryLine(HazardSummary{Severe: 1, Other: 2})
	if strings.Contains(got, "/hazard") {
		t.Fatalf("hint must be dropped when it does not fit: %q", got)
	}
	if !strings.HasPrefix(got, "WarnFlux: 1 severe or more, 2 other active — https://") {
		t.Fatalf("counters must survive: %q", got)
	}
}

// TestHubHazardDigestSilent pins the severe-or-higher rule: a quiet
// channel is not worth a line, and neither is one carrying only hazards
// below the alert bar ("0 severe or more, 12 other active" every hour
// would train the readers to ignore the line that matters).
func TestHubHazardDigestSilent(t *testing.T) {
	radio := newTestRadio(t, Config{Enabled: true, Device: "/dev/fake", NodeTTL: time.Hour,
		EmcomChannel: 1, EmcomHazardsInterval: time.Hour})
	var mu sync.Mutex
	summary := HazardSummary{}
	radio.hub.SetHazardSummarySource(func() HazardSummary {
		mu.Lock()
		defer mu.Unlock()
		return summary
	})
	radio.waitConnected(t)

	// The maintenance ticker runs every dmRetryInterval; wait past one
	// full tick and confirm the radio stayed silent.
	deadline := time.Now().Add(dmRetryInterval + 2*time.Second)
	for time.Now().Before(deadline) {
		if n := len(radio.outbound()); n > 0 {
			t.Fatalf("digest sent %d packets with no active hazards", n)
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Only hazards BELOW the bar: still nothing on the air.
	mu.Lock()
	summary = HazardSummary{Other: 12}
	mu.Unlock()
	deadline = time.Now().Add(dmRetryInterval + 2*time.Second)
	for time.Now().Before(deadline) {
		if n := len(radio.outbound()); n > 0 {
			t.Fatalf("digest sent %d packets for below-severe hazards only", n)
		}
		time.Sleep(20 * time.Millisecond)
	}

	// A severe hazard IS worth the line (counters plus the /hazard hint);
	// that path is pinned by TestHubHazardDigest, and the skipped window
	// above consumed this cadence, so nothing is expected here yet.
}

// TestHubHazardDigestDisabled pins the interval-0 switch: no source is
// consulted and nothing is broadcast.
func TestHubHazardDigestDisabled(t *testing.T) {
	radio := newTestRadio(t, Config{Enabled: true, Device: "/dev/fake", NodeTTL: time.Hour,
		EmcomChannel: 1, EmcomHazardsInterval: 0})
	called := false
	radio.hub.SetHazardSummarySource(func() HazardSummary {
		called = true
		return HazardSummary{Severe: 1}
	})
	radio.waitConnected(t)
	deadline := time.Now().Add(dmRetryInterval + 2*time.Second)
	for time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if called {
		t.Fatal("digest source consulted while disabled")
	}
	if n := len(radio.outbound()); n > 0 {
		t.Fatalf("disabled digest sent %d packets", n)
	}
}

// TestHazardDigestLineTruncates pins the channel limit: every line fits
// the Meshtastic text limit, trimming the description tail first.
func TestHazardDigestLineTruncates(t *testing.T) {
	ts := time.Date(2026, 10, 5, 10, 0, 0, 0, time.UTC)
	got := hazardDigestLine(ActiveHazard{
		Headline:    "Długi nagłówek",
		Description: strings.Repeat("opis", 200),
		EffectiveAt: &ts,
		ExpiresAt:   &ts,
	})
	if n := len([]rune(got)); n != meshTextMaxRunes {
		t.Fatalf("line runes = %d, want %d", n, meshTextMaxRunes)
	}
	if !strings.HasPrefix(got, "2026-10-05 10:00Z-2026-10-05 10:00Z Długi nagłówek — ") {
		t.Fatalf("line lost its prefix: %q", got)
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

	dispatchPkt(t, radio, &pb.MeshPacket{
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

	dispatchPkt(t, radio, textPacket(0x12345678, core.BroadcastNodeID.Uint32(), "hi"))
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

	dispatchPkt(t, radio, telePacket(&pb.Telemetry{
		Variant: &pb.Telemetry_DeviceMetrics{DeviceMetrics: &pb.DeviceMetrics{
			BatteryLevel: proto.Uint32(87), Voltage: proto.Float32(4.1),
			ChannelUtilization: proto.Float32(12.5), AirUtilTx: proto.Float32(1.5),
			UptimeSeconds: proto.Uint32(12345),
		}},
	}))
	dispatchPkt(t, radio, telePacket(&pb.Telemetry{
		Variant: &pb.Telemetry_EnvironmentMetrics{EnvironmentMetrics: &pb.EnvironmentMetrics{
			Temperature: proto.Float32(21.5), RelativeHumidity: proto.Float32(55),
			BarometricPressure: proto.Float32(1013), Iaq: proto.Uint32(42),
		}},
	}))
	dispatchPkt(t, radio, telePacket(&pb.Telemetry{
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
	mu       sync.Mutex
	got      []Message
	reasons  map[string]string
	progress map[string]bool
	failures map[string]bool
	rearmed  int
}

func (c *captureRecorder) RecordMeshtasticMessage(_ context.Context, direction, sender, recipient, channel, text, operator string, hops int, at time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.got = append(c.got, Message{Direction: direction, Sender: sender, Recipient: recipient, Channel: channel, Text: text, Operator: operator, Hops: hops, At: at})
	return nil
}

func (c *captureRecorder) UpdateMeshtasticMessageStatus(_ context.Context, status, reason string, at time.Time, text string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := range c.got {
		if c.got[i].Direction == "tx" && c.got[i].At.Equal(at) && c.got[i].Text == text {
			c.got[i].Status = status
		}
	}
	if reason != "" {
		if c.reasons == nil {
			c.reasons = map[string]string{}
		}
		c.reasons[text] = reason
	}
	return nil
}

// reasonFor reports the refusal reason recorded for one sent text.
func (c *captureRecorder) reasonFor(text string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.reasons[text]
}

// The captureRecorder also keeps the versioned action progress ledger.
func (c *captureRecorder) progressKey(publisher, eventKey string, changeID int64, recipient string, channel int) string {
	return fmt.Sprintf("%s|%s|%d|%s|%d", publisher, eventKey, changeID, recipient, channel)
}

func (c *captureRecorder) RecordMeshActionProgress(_ context.Context, publisher, eventKey string, changeID int64, recipient string, channel int, _ time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.progress == nil {
		c.progress = map[string]bool{}
	}
	c.progress[c.progressKey(publisher, eventKey, changeID, recipient, channel)] = true
	return nil
}

func (c *captureRecorder) MeshActionProgressDone(_ context.Context, publisher, eventKey string, changeID int64, recipient string, channel int) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.progress[c.progressKey(publisher, eventKey, changeID, recipient, channel)], nil
}

func (c *captureRecorder) DeleteMeshActionProgress(_ context.Context, publisher, eventKey string, changeID int64, recipient string, channel int) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.progress, c.progressKey(publisher, eventKey, changeID, recipient, channel))
	return nil
}

func (c *captureRecorder) failureKey(actionID string, groupID int64, dedupKey, publisher, eventKey string, changeID int64, recipient string, channel int) string {
	return fmt.Sprintf("%s|%d|%s|%s|%s|%d|%s|%d", actionID, groupID, dedupKey, publisher, eventKey, changeID, recipient, channel)
}

func (c *captureRecorder) SetMeshActionFailed(_ context.Context, actionID string, groupID int64, dedupKey, publisher, eventKey string, changeID int64, recipient string, channel int, failed bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failures == nil {
		c.failures = map[string]bool{}
	}
	k := c.failureKey(actionID, groupID, dedupKey, publisher, eventKey, changeID, recipient, channel)
	if failed {
		c.failures[k] = true
	} else {
		delete(c.failures, k)
	}
	return nil
}

func (c *captureRecorder) MeshActionFailed(_ context.Context, actionID string, groupID int64, dedupKey, publisher, eventKey string, changeID int64) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	prefix := fmt.Sprintf("%s|%d|%s|%s|%s|%d|", actionID, groupID, dedupKey, publisher, eventKey, changeID)
	for k := range c.failures {
		if strings.HasPrefix(k, prefix) {
			return true, nil
		}
	}
	return false, nil
}

// RecordMeshFailureAndRequeue mirrors the atomic store call: progress
// revoked, marker recorded, re-armed jobs counted — all under one lock.
func (c *captureRecorder) RecordMeshFailureAndRequeue(_ context.Context, actionID string, groupID int64, dedupKey, publisher, eventKey string, changeID int64, recipient string, channel int, _ time.Time) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.progress, c.progressKey(publisher, eventKey, changeID, recipient, channel))
	if c.failures == nil {
		c.failures = map[string]bool{}
	}
	c.failures[c.failureKey(actionID, groupID, dedupKey, publisher, eventKey, changeID, recipient, channel)] = true
	c.rearmed++
	return 1, nil
}

// rearmedCount reads the durable re-arm counter under the lock (the hub
// records from its own goroutines).
func (c *captureRecorder) rearmedCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.rearmed
}

func (c *captureRecorder) messages() []Message {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]Message(nil), c.got...)
}

// TestHubMeshActionProgress pins the durable versioned progress ledger:
// a transmission counts as done only for the exact (publisher, event
// key, change id, recipient, channel) identity — a different version,
// recipient or channel never matches, so a historical delivered message
// with the same text can never suppress a new alert. A hub without a
// recorder reports no progress (fail-open).
func TestHubMeshActionProgress(t *testing.T) {
	rec := &captureRecorder{}
	h := &Hub{recorder: rec}
	ctx := context.Background()

	if done, err := h.MeshActionProgressDone(ctx, "p1", "k", 1, "a0a85934", 0); err != nil || done {
		t.Fatalf("empty ledger = (%v, %v), want false", done, err)
	}
	if err := h.RecordMeshActionProgress(ctx, "p1", "k", 1, "a0a85934", 0); err != nil {
		t.Fatal(err)
	}
	// The id spelling variants resolve to the same entry.
	if done, err := h.MeshActionProgressDone(ctx, "p1", "k", 1, "!A0A85934", 0); err != nil || !done {
		t.Fatalf("recorded DM = (%v, %v), want true", done, err)
	}
	// A different version of the SAME text identity must not match.
	if done, err := h.MeshActionProgressDone(ctx, "p1", "k", 2, "a0a85934", 0); err != nil || done {
		t.Fatalf("new version = (%v, %v), want false (the version is part of the identity)", done, err)
	}
	// Different recipient / channel / publisher / event key: no match.
	probes := []struct {
		publisher, eventKey, recipient string
		changeID                       int64
		channel                        int
	}{
		{"p1", "k", "b0b85934", 1, 0},
		{"p1", "k", "a0a85934", 1, 1},
		{"p2", "k", "a0a85934", 1, 0},
		{"p1", "other", "a0a85934", 1, 0},
	}
	for _, probe := range probes {
		if done, err := h.MeshActionProgressDone(ctx, probe.publisher, probe.eventKey, probe.changeID, probe.recipient, probe.channel); err != nil || done {
			t.Fatalf("other identity %+v = (%v, %v), want false", probe, done, err)
		}
	}
	// No recorder: fail-open, never claim progress.
	empty := &Hub{}
	if done, err := empty.MeshActionProgressDone(ctx, "p1", "k", 1, "a0a85934", 0); err != nil || done {
		t.Fatalf("no recorder = (%v, %v), want false", done, err)
	}
	if err := empty.RecordMeshActionProgress(ctx, "p1", "k", 1, "a0a85934", 0); err != nil {
		t.Fatalf("record without recorder: %v", err)
	}
}

// TestHubProgressFollowsTransmissionOutcome pins the P1: the progress
// entry follows the TRANSMISSION RESULT. The radio transmitting the
// frame (sent) or the recipient's ack (delivered) records the entry; a
// later TxFailed REVOKES it, so the next group attempt sends the
// recipient again instead of skipping a failed transmission.
func TestHubProgressFollowsTransmissionOutcome(t *testing.T) {
	rec := &captureRecorder{}
	h := &Hub{recorder: rec}
	ctx := context.Background()
	ps := &pendingSend{
		at:   time.Now().Truncate(time.Millisecond),
		text: "alarm",
		prog: ProgressRef{ActionID: "mesh-main", GroupID: 7, DedupKey: "c:1", Publisher: "p1", EventKey: "k", ChangeID: 1, recipient: "a0a85934", channel: 0},
	}

	// Before the radio result nothing is recorded.
	if done, _ := rec.MeshActionProgressDone(ctx, "p1", "k", 1, "a0a85934", 0); done {
		t.Fatal("progress exists before the transmission result")
	}

	// The radio transmitted the frame: progress recorded.
	h.markStatus(ps, TxSent)
	if done, _ := rec.MeshActionProgressDone(ctx, "p1", "k", 1, "a0a85934", 0); !done {
		t.Fatal("TxSent did not record the progress entry")
	}

	// The delivery ultimately failed: the progress entry is revoked, the
	// durable failure marker is set and the settled jobs are re-armed —
	// all in ONE atomic recorder call.
	h.markStatus(ps, TxFailed)
	if done, _ := rec.MeshActionProgressDone(ctx, "p1", "k", 1, "a0a85934", 0); done {
		t.Fatal("TxFailed did not revoke the progress entry")
	}
	if failed, _ := rec.MeshActionFailed(ctx, "mesh-main", 7, "c:1", "p1", "k", 1); !failed {
		t.Fatal("TxFailed did not set the failure marker")
	}
	if failed, _ := rec.MeshActionFailed(ctx, "other-action", 7, "c:1", "p1", "k", 1); failed {
		t.Fatal("the failure marker leaked into another action")
	}
	// P2: the marker is job-scoped — another GROUP's job of the same
	// action+version stays clean.
	if failed, _ := rec.MeshActionFailed(ctx, "mesh-main", 8, "c:2", "p1", "k", 1); failed {
		t.Fatal("the failure marker leaked into another group's job of the same action")
	}
	if rec.rearmed != 1 {
		t.Fatalf("re-armed calls = %d, want 1 (the atomic failure record)", rec.rearmed)
	}

	// A retransmit that gets the ack records again (delivered) and
	// clears the failure marker.
	h.markStatus(ps, TxDelivered)
	if done, _ := rec.MeshActionProgressDone(ctx, "p1", "k", 1, "a0a85934", 0); !done {
		t.Fatal("TxDelivered did not record the progress entry")
	}
	if failed, _ := rec.MeshActionFailed(ctx, "mesh-main", 7, "c:1", "p1", "k", 1); failed {
		t.Fatal("TxDelivered did not clear the failure marker")
	}
	if rec.rearmed != 1 {
		t.Fatalf("re-armed calls = %d, want 1 (only the failure re-arms)", rec.rearmed)
	}

	// A send without a progress identity never touches the ledger.
	plain := &pendingSend{at: time.Now(), text: "other"}
	h.markStatus(plain, TxSent)
	h.markStatus(plain, TxFailed)
	if n := len(rec.progress); n != 1 {
		t.Fatalf("progress entries = %d, want 1 (the plain send must not touch the ledger)", n)
	}
	if n := len(rec.failures); n != 0 {
		t.Fatalf("failure markers = %d, want 0 (the plain send must not touch the ledger)", n)
	}
}

// TestHubVersionedSendProgressFollowsEcho pins the end-to-end seam: a
// versioned send attaches the progress identity to the pending send, the
// device echo (modem acceptance) stamps the entry, and the modem's
// failure reason revokes it — the action retry sees the recipient as
// unfinished again.
func TestHubVersionedSendProgressFollowsEcho(t *testing.T) {
	radio := newTestRadio(t, Config{Enabled: true, Device: "/dev/fake", NodeTTL: time.Hour})
	radio.waitConnected(t)
	rec := &captureRecorder{}
	radio.hub.SetRecorder(rec)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := radio.hub.SendContactMessageVersioned(ctx, "ef010203", "ack me", "system",
		ProgressRef{ActionID: "mesh-main", GroupID: 7, DedupKey: "c:1", Publisher: "p1", EventKey: "k", ChangeID: 1}); err != nil {
		t.Fatalf("versioned send: %v", err)
	}

	// The device echo (modem acceptance) stamps the progress.
	dispatchPkt(t, radio, &pb.MeshPacket{
		From: 0xabcd1234, Id: 4242, To: 0xef010203,
		PayloadVariant: &pb.MeshPacket_Decoded{
			Decoded: &pb.Data{Portnum: pb.PortNum_TEXT_MESSAGE_APP, Payload: []byte("ack me")},
		},
	})
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if done, _ := rec.MeshActionProgressDone(ctx, "p1", "k", 1, "ef010203", 0); done {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if done, _ := rec.MeshActionProgressDone(ctx, "p1", "k", 1, "ef010203", 0); !done {
		t.Fatal("echo did not stamp the progress entry")
	}

	// The modem reports the delivery failed: the entry is revoked.
	fail, err := proto.Marshal(&pb.Routing{Variant: &pb.Routing_ErrorReason{ErrorReason: pb.Routing_MAX_RETRANSMIT}})
	if err != nil {
		t.Fatal(err)
	}
	dispatchPkt(t, radio, &pb.MeshPacket{
		From: 0xabcd1234, To: 0xabcd1234,
		PayloadVariant: &pb.MeshPacket_Decoded{
			Decoded: &pb.Data{Portnum: pb.PortNum_ROUTING_APP, RequestId: 4242, Payload: fail},
		},
	})
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if done, _ := rec.MeshActionProgressDone(ctx, "p1", "k", 1, "ef010203", 0); !done {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if done, _ := rec.MeshActionProgressDone(ctx, "p1", "k", 1, "ef010203", 0); done {
		t.Fatal("modem failure did not revoke the progress entry")
	}
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

	dispatchPkt(t, radio, textPacket(0xdeadbeef, core.BroadcastNodeID.Uint32(), "hello mesh"))

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
	dispatchPkt(t, radio, &pb.MeshPacket{
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

// TestHubEventBridge pins the command-only alarm policy: an /alert DM to
// our node from a gate-approved sender becomes a /events document, while
// plain DMs, broadcasts and unapproved senders stay off the events
// stream.
// testRadioCLI builds the shared radio interpreter with the /alert
// command the daemon registers on it: the hub tests drive the routed
// command path through this instance, and the authorized /help list
// shows exactly the restricted commands this station really has.
func testRadioCLI(t *testing.T) *radiocli.Bot {
	t.Helper()
	cli := radiocli.New("WarnFlux v1.0 - SOSNA - sosna.sp9moa.pl")
	cli.RegisterRestricted("alert", "alert", func(args string, _ radiocli.Sender) radiocli.Result {
		return radiocli.Result{Handled: true, Alert: &radiocli.AlertSpec{Headline: strings.TrimSpace(args), TTL: 4 * time.Hour}, Reply: "OK: alert raised"}
	})
	return cli
}

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
	radio.hub.SetCLI(testRadioCLI(t))

	// /alert DM to us from the approved node.
	dispatchPkt(t, radio, textPacket(0xdeadbeef, 0xabcd1234, "/alert alarm test"))
	// Plain DM from the approved node: stays off /events.
	dispatchPkt(t, radio, textPacket(0xdeadbeef, 0xabcd1234, "powodz w krakowie"))
	// Broadcast from the approved node: stays off /events.
	dispatchPkt(t, radio, textPacket(0xdeadbeef, core.BroadcastNodeID.Uint32(), "broadcast"))
	// DM from an unapproved node.
	dispatchPkt(t, radio, textPacket(0x12345678, 0xabcd1234, "spam"))

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
		t.Fatalf("events = %d, want exactly 1 (the /alert command only)", len(events))
	}
	var we MessageEventWire
	if err := json.Unmarshal([]byte(events[0]), &we); err != nil {
		t.Fatalf("event payload: %v", err)
	}
	if we.Event.Source != "meshtastic" || !strings.HasPrefix(we.Event.SourceID, "deadbeef:alert:") {
		t.Fatalf("event = %+v, want source meshtastic from deadbeef", we.Event)
	}
	if we.Event.Headline != "alarm test" {
		t.Fatalf("headline = %q, want the alert text", we.Event.Headline)
	}
	if !strings.Contains(we.Event.Description, "sp9kow") || !strings.Contains(we.Event.Description, "PL-KR-MAKI") {
		t.Fatalf("description = %q, want the sender identity (owner + node name)", we.Event.Description)
	}
}

// TestHubRadioCLI pins the radio-command interpreter: /help is answered
// in-band and stays off the alarm pipeline, /alert raises the hazard plus
// a confirmation, and plain messages still route.
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
	radio.hub.SetCLI(testRadioCLI(t))

	// /help: answered, no alarm.
	dispatchPkt(t, radio, textPacket(0xdeadbeef, 0xabcd1234, "/help"))
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

	// /alert: hazard + confirmation.
	before := len(radio.outbound())
	dispatchPkt(t, radio, textPacket(0xdeadbeef, 0xabcd1234, "/alert alarm test"))
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
		t.Fatalf("events after /alert = %d, want 1", n)
	}
	if !strings.Contains(events[0], "alarm test") {
		t.Fatalf("alert event = %q, want the alert text", events[0])
	}
	radio.waitOutbound(t, before+1)
	out := radio.outbound()
	if got := string(out[len(out)-1].GetDecoded().GetPayload()); !strings.Contains(got, "OK: alert raised") {
		t.Fatalf("debug reply = %q, want the confirmation", got)
	}

	// Unauthorized senders: commands never run and never alarm — a
	// restricted command gets an explicit denial.
	before = len(radio.outbound())
	dispatchPkt(t, radio, textPacket(0xcafebabe, 0xabcd1234, "/alert alarm test"))
	radio.waitOutbound(t, before+1)
	out = radio.outbound()
	if got := string(out[len(out)-1].GetDecoded().GetPayload()); !strings.Contains(got, "WarnFlux v1.0 - SOSNA") || !strings.Contains(got, "You are not authorized") {
		t.Fatalf("unauthorized /alert reply = %q, want the banner with the denial", got)
	}
	if out[len(out)-1].GetTo() != 0xcafebabe {
		t.Fatalf("unauthorized reply to = %08x, want cafebabe", out[len(out)-1].GetTo())
	}
	mu.Lock()
	n = len(events)
	mu.Unlock()
	if n != 1 {
		t.Fatalf("events after unauthorized /alert = %d, want still 1", n)
	}

	// /help is public: an unregistered sender still gets the command
	// list — but only of the commands they may run (no /alert).
	before = len(radio.outbound())
	dispatchPkt(t, radio, textPacket(0xcafebabe, 0xabcd1234, "/help"))
	radio.waitOutbound(t, before+1)
	out = radio.outbound()
	got := string(out[len(out)-1].GetDecoded().GetPayload())
	if !strings.Contains(got, "Commands:") || !strings.Contains(got, "/help") {
		t.Fatalf("unauthorized /help reply = %q, want the public command list", got)
	}
	if strings.Contains(got, "/alert alarm test") {
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
	dispatchPkt(t, radio, textPacket(0xdeadbeef, 0xabcd1234, "plain hello"))
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

// TestHubCLIMultilineReply pins the /hazard-style list replies on the
// mesh side: one multi-line command answer becomes one direct message
// per line, each fitted to the mesh text limit.
func TestHubCLIMultilineReply(t *testing.T) {
	radio := newTestRadio(t, Config{Enabled: true, Device: "/dev/fake", NodeTTL: time.Hour})
	radio.waitConnected(t)
	// The command path needs a sender gate and an event sink wired (the
	// address + owner checks run before the interpreter).
	radio.hub.SetSenderGate(func(id string) string {
		if id == "deadbeef" {
			return "sp9kow"
		}
		return ""
	})
	radio.hub.SetEventSink(func(_ context.Context, _ string, _ bool, _ []byte) error { return nil })
	cli := radiocli.New("WarnFlux v1.0 - SOSNA - sosna.sp9moa.pl")
	cli.Register("list", "test list", func(string, radiocli.Sender) radiocli.Result {
		return radiocli.Result{Handled: true, Reply: "header\nline one\nline two"}
	})
	radio.hub.SetCLI(cli)

	dispatchPkt(t, radio, textPacket(0xdeadbeef, 0xabcd1234, "/list"))
	reply := radio.waitOutbound(t, 3)
	for i, want := range []string{"header", "line one", "line two"} {
		if reply[i].GetTo() != 0xdeadbeef {
			t.Fatalf("line %d to = %08x, want deadbeef", i, reply[i].GetTo())
		}
		if got := string(reply[i].GetDecoded().GetPayload()); got != want {
			t.Fatalf("line %d = %q, want %q", i, got, want)
		}
	}
}
func TestHubCommandsRequireExactAddressee(t *testing.T) {
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
	radio.hub.SetCLI(testRadioCLI(t))

	// To=0 (unset) and broadcasts: never executed, never answered.
	dispatchPkt(t, radio, textPacket(0xdeadbeef, 0, "/alert alarm test"))
	dispatchPkt(t, radio, textPacket(0xdeadbeef, core.BroadcastNodeID.Uint32(), "/alert alarm test"))
	time.Sleep(150 * time.Millisecond)
	mu.Lock()
	n := len(events)
	mu.Unlock()
	if n != 0 {
		t.Fatalf("events after non-addressed commands = %d, want 0", n)
	}
	if out := radio.outbound(); len(out) != 0 {
		t.Fatalf("outbound after non-addressed commands = %d, want 0", len(out))
	}

	// An exact DM executes.
	dispatchPkt(t, radio, textPacket(0xdeadbeef, 0xabcd1234, "/alert alarm test"))
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
		t.Fatalf("events after exact DM /alert = %d, want 1", n)
	}
}

// TestRadioEventContentDeterministic pins the restart-proof dedup: the
// event content (lifecycle anchor from the durable registry resolver,
// content-derived ChangeID) is byte-identical for two deliveries of the
// same packet, so the store classifies a post-restart replay as a
// duplicate and never issues a second delivery.
func TestRadioEventContentDeterministic(t *testing.T) {
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
	radio.hub.SetCLI(testRadioCLI(t))
	// The resolver is the durable-registry half: it hands back the
	// stored lifecycle anchor (here a fixed one), so a re-issued command
	// maps to byte-identical content.
	fixed := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	radio.hub.SetEventTimesResolver(func(ctx context.Context, key string) (time.Time, time.Time, string, bool) {
		return fixed, fixed.Add(time.Hour), "", true
	})

	pkt := textPacket(0xdeadbeef, 0xabcd1234, "/alert alarm test")
	pkt.Id = 4242
	dispatchPkt(t, radio, pkt)
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

	// Simulated restart: the RAM cache is gone, the packet is delivered
	// again — the published content must be byte-identical.
	radio.hub.mu.Lock()
	radio.hub.cmds = make(map[string]*cmdRecord)
	radio.hub.mu.Unlock()
	dispatchPkt(t, radio, pkt)
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(events)
		mu.Unlock()
		if n >= 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(events) != 2 {
		t.Fatalf("events = %d, want 2", len(events))
	}
	if events[0] != events[1] {
		t.Fatalf("replayed /alert content differs:\n%q\n%q", events[0], events[1])
	}
}

// TestHubCommandRegistryResultReplay pins the durable command registry:
// a command whose acceptance recorded its result (transactionally with
// the inbox row) is NEVER re-executed after a restart — the
// redelivery replays the stored result. The ChangeID dedup alone could
// not prove this: it only suppresses the second DELIVERY, while the job
// itself still ran. Here the job does not run twice.
func TestHubCommandRegistryResultReplay(t *testing.T) {
	radio := newTestRadio(t, Config{Enabled: true, Device: "/dev/fake", NodeTTL: time.Hour})
	radio.waitConnected(t)
	radio.hub.SetSenderGate(func(id string) string {
		if id == "deadbeef" {
			return "sp9kow"
		}
		return ""
	})
	cli := radiocli.New("WarnFlux v1.0 - SOSNA - sosna.sp9moa.pl")
	cli.RegisterRestricted("alert", "alert", func(args string, _ radiocli.Sender) radiocli.Result {
		return radiocli.Result{Handled: true, Alert: &radiocli.AlertSpec{Headline: strings.TrimSpace(args), TTL: 4 * time.Hour}, Reply: "OK: alert raised"}
	})
	radio.hub.SetCLI(cli)
	// The command path gates on a non-nil event sink; the acceptor is
	// the recording half of the registry.
	radio.hub.SetEventSink(func(_ context.Context, _ string, _ bool, _ []byte) error { return nil })

	// A miniature durable registry: the acceptor records the result per
	// event key exactly like the transactional inbox anchor; the
	// resolver reads it back.
	var (
		mu       sync.Mutex
		registry = make(map[string]string)
		calls    int
	)
	radio.hub.SetEventAcceptor(func(payload []byte) dispatch.Acceptance {
		mu.Lock()
		defer mu.Unlock()
		calls++
		var we MessageEventWire
		if err := json.Unmarshal(payload, &we); err == nil {
			registry[we.EventKey] = we.CommandResult
		}
		return dispatch.AcceptedDurable
	})
	fixed := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	radio.hub.SetEventTimesResolver(func(ctx context.Context, key string) (time.Time, time.Time, string, bool) {
		mu.Lock()
		res, ok := registry[key]
		mu.Unlock()
		if !ok {
			return time.Time{}, time.Time{}, "", false
		}
		return fixed, fixed.Add(4 * time.Hour), res, true
	})

	// First execution: registry miss, the job runs once.
	pkt := textPacket(0xdeadbeef, 0xabcd1234, "/alert pozar lasu")
	pkt.Id = 4242
	dispatchPkt(t, radio, pkt)
	radio.waitOutbound(t, 1)
	out := radio.outbound()
	if got := string(out[len(out)-1].GetDecoded().GetPayload()); got != "OK: alert raised" {
		t.Fatalf("first confirmation = %q, want the handler reply", got)
	}

	// Simulated restart: the RAM command cache is gone.
	radio.hub.mu.Lock()
	radio.hub.cmds = make(map[string]*cmdRecord)
	radio.hub.mu.Unlock()

	// The redelivery finds the stored result: NO re-execution, the
	// stored confirmation is replayed.
	dispatchPkt(t, radio, pkt)
	radio.waitOutbound(t, 2)
	out = radio.outbound()
	if got := string(out[len(out)-1].GetDecoded().GetPayload()); got != "OK: alert raised" {
		t.Fatalf("replayed confirmation = %q, want the stored result", got)
	}
	mu.Lock()
	defer mu.Unlock()
	if calls != 1 {
		t.Fatalf("acceptor calls = %d, want 1 (the stored result must stop re-execution)", calls)
	}
}

// TestHubCommandDedup pins the retransmission guard: the same /alert
// packet delivered twice raises ONE alarm, and the redelivery gets the
// previous reply instead of executing the command again. The event
// identity is stable (sender + packet id), so the storage deduplication
// collapses late redeliveries too.
func TestHubCommandDedup(t *testing.T) {
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
	radio.hub.SetCLI(testRadioCLI(t))

	// The same packet (same device-assigned id) is delivered twice.
	pkt := textPacket(0xdeadbeef, 0xabcd1234, "/alert alarm test")
	pkt.Id = 4242
	dispatchPkt(t, radio, pkt)
	dispatchPkt(t, radio, pkt)

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(events)
		mu.Unlock()
		if n >= 1 && len(radio.outbound()) >= 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond) // settle: no second event may appear
	mu.Lock()
	if len(events) != 1 {
		mu.Unlock()
		t.Fatalf("events after redelivered /alert = %d, want 1", len(events))
	}
	var we MessageEventWire
	if err := json.Unmarshal([]byte(events[0]), &we); err != nil {
		mu.Unlock()
		t.Fatalf("event payload: %v", err)
	}
	mu.Unlock()
	want := "deadbeef:alert:4242:h" + radiocli.ContentID("deadbeef", "/alert alarm test")
	if we.Event.SourceID != want {
		t.Fatalf("event source id = %q, want %q (packet id + content fingerprint)", we.Event.SourceID, want)
	}
	out := radio.outbound()
	if len(out) < 2 {
		t.Fatalf("replies = %d, want 2 (initial + previous result)", len(out))
	}
	for _, o := range out[len(out)-2:] {
		if got := string(o.GetDecoded().GetPayload()); !strings.Contains(got, "OK: alert raised") {
			t.Fatalf("reply = %q, want the previous result on both deliveries", got)
		}
	}

	// The digest fallback: two id-less packets with the same content are
	// one command too.
	plain := textPacket(0xdeadbeef, 0xabcd1234, "/alert alarm test now")
	dispatchPkt(t, radio, plain)
	dispatchPkt(t, radio, plain)
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(events)
		mu.Unlock()
		if n >= 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	mu.Lock()
	n := len(events)
	mu.Unlock()
	if n != 2 {
		t.Fatalf("events after id-less redelivery = %d, want 2 total", n)
	}

	// A distinct packet executes again: its own event.
	pkt2 := textPacket(0xdeadbeef, 0xabcd1234, "/alert alarm test")
	pkt2.Id = 4243
	dispatchPkt(t, radio, pkt2)
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n = len(events)
		mu.Unlock()
		if n >= 3 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	mu.Lock()
	n = len(events)
	mu.Unlock()
	if n != 3 {
		t.Fatalf("events after a distinct packet = %d, want 3 total", n)
	}
}

// TestHubAlertConfirmationTracksAcceptance pins the honest /alert
// confirmation: the reply follows the LOCAL acceptance — durable keeps
// the handler confirmation, the emergency fallback is reported as such,
// and a rejection never claims success. A rejection is TRANSIENT: the
// failure is replayed within the retry cooldown, and after it a
// redelivery executes the command again (controlled retry). The
// acceptor replaces the event sink here, so its calls double as the
// event capture.
func TestHubAlertConfirmationTracksAcceptance(t *testing.T) {
	radio := newTestRadio(t, Config{Enabled: true, Device: "/dev/fake", NodeTTL: time.Hour,
		CmdRetryCooldown: 500 * time.Millisecond})
	radio.waitConnected(t)
	radio.hub.SetSenderGate(func(id string) string {
		if id == "deadbeef" {
			return "sp9kow"
		}
		return ""
	})
	// The event sink gates the whole command path; the acceptor replaces
	// it for /alert payloads, so a no-op sink is enough here.
	radio.hub.SetEventSink(func(_ context.Context, topic string, _ bool, payload []byte) error {
		return nil
	})
	cli := radiocli.New("WarnFlux v1.0 - SOSNA - sosna.sp9moa.pl")
	cli.RegisterRestricted("alert", "alert", func(args string, _ radiocli.Sender) radiocli.Result {
		return radiocli.Result{Handled: true, Alert: &radiocli.AlertSpec{Headline: strings.TrimSpace(args), TTL: 4 * time.Hour}, Reply: "OK: alert raised"}
	})
	radio.hub.SetCLI(cli)

	var (
		mu       sync.Mutex
		accepted []dispatch.Acceptance
		accValue = dispatch.AcceptedDurable
	)
	radio.hub.SetEventAcceptor(func([]byte) dispatch.Acceptance {
		mu.Lock()
		defer mu.Unlock()
		accepted = append(accepted, accValue)
		return accValue
	})

	// Durable acceptance: the plain handler confirmation.
	dispatchPkt(t, radio, textPacket(0xdeadbeef, 0xabcd1234, "/alert pozar lasu"))
	radio.waitOutbound(t, 1)
	if got := string(radio.outbound()[0].GetDecoded().GetPayload()); got != "OK: alert raised" {
		t.Fatalf("durable confirmation = %q, want the plain handler reply", got)
	}

	// Emergency acceptance: the fallback is reported explicitly.
	accValue = dispatch.AcceptedEmergency
	dispatchPkt(t, radio, textPacket(0xdeadbeef, 0xabcd1234, "/alert pozar lasu 2"))
	radio.waitOutbound(t, 2)
	if got := string(radio.outbound()[1].GetDecoded().GetPayload()); !strings.Contains(got, "OK: alert raised") || !strings.Contains(got, "failover") {
		t.Fatalf("emergency confirmation = %q, want the explicit failover note", got)
	}

	// Rejection: never claim success.
	accValue = dispatch.Rejected
	rejPkt := textPacket(0xdeadbeef, 0xabcd1234, "/alert pozar lasu 3")
	dispatchPkt(t, radio, rejPkt)
	radio.waitOutbound(t, 3)
	if got := string(radio.outbound()[2].GetDecoded().GetPayload()); got != "FAILED: alert rejected" {
		t.Fatalf("rejected confirmation = %q, want the explicit failure", got)
	}

	// A redelivery within the cooldown replays the failure WITHOUT
	// re-executing (no new acceptor call).
	dispatchPkt(t, radio, rejPkt)
	radio.waitOutbound(t, 4)
	if got := string(radio.outbound()[3].GetDecoded().GetPayload()); got != "FAILED: alert rejected" {
		t.Fatalf("in-cooldown redelivery = %q, want the replayed failure", got)
	}
	mu.Lock()
	n := len(accepted)
	mu.Unlock()
	if n != 3 {
		t.Fatalf("acceptor called %d times, want 3 (no re-execution within the cooldown)", n)
	}

	// After the cooldown the same packet executes again: the recovered
	// pipeline accepts it durably.
	time.Sleep(600 * time.Millisecond)
	accValue = dispatch.AcceptedDurable
	dispatchPkt(t, radio, rejPkt)
	radio.waitOutbound(t, 5)
	if got := string(radio.outbound()[4].GetDecoded().GetPayload()); got != "OK: alert raised" {
		t.Fatalf("post-recovery retry = %q, want the success confirmation", got)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(accepted) != 4 {
		t.Fatalf("acceptor called %d times, want 4 (the post-cooldown retry)", len(accepted))
	}
}

// TestHubReplyBurstLimit pins the shared output limit: a flooding sender
// (ten distinct /unknown commands) gets at most ReplyBurst automatic
// replies per window — slash commands can no longer bypass the banner
// limits.
func TestHubReplyBurstLimit(t *testing.T) {
	radio := newTestRadio(t, Config{Enabled: true, Device: "/dev/fake", NodeTTL: time.Hour,
		ReplyBurst: 3})
	radio.waitConnected(t)
	radio.hub.SetSenderGate(func(id string) string {
		if id == "deadbeef" {
			return "sp9kow"
		}
		return ""
	})
	radio.hub.SetEventSink(func(_ context.Context, topic string, _ bool, payload []byte) error {
		return nil
	})
	radio.hub.SetCLI(testRadioCLI(t))

	for i := 0; i < 10; i++ {
		dispatchPkt(t, radio, textPacket(0xdeadbeef, 0xabcd1234, fmt.Sprintf("/unknown %d", i)))
	}
	radio.waitOutbound(t, 3)
	time.Sleep(150 * time.Millisecond) // settle: no further replies
	if got := len(radio.outbound()); got != 3 {
		t.Fatalf("automatic replies after a 10-command flood = %d, want exactly ReplyBurst (3)", got)
	}
}

// TestHubBannerRateLimits pins the automatic-answer guards: a sender
// gets at most one banner per window, the whole banner path is globally
// paced, and queued banners yield to direct alarm replies.
func TestHubBannerRateLimits(t *testing.T) {
	radio := newTestRadio(t, Config{
		Enabled: true, Device: "/dev/fake", NodeTTL: time.Hour,
		BannerMinInterval: time.Second, BannerGlobalInterval: 500 * time.Millisecond,
	})
	radio.waitConnected(t)
	radio.hub.SetSenderGate(func(id string) string {
		if id == "deadbeef" {
			return "sp9kow"
		}
		return ""
	})
	radio.hub.SetEventSink(func(_ context.Context, topic string, _ bool, payload []byte) error {
		return nil
	})
	radio.hub.SetCLI(testRadioCLI(t))

	// First plain DM: one banner.
	dispatchPkt(t, radio, textPacket(0xdeadbeef, 0xabcd1234, "hello one"))
	radio.waitOutbound(t, 1)

	// Second plain DM from the SAME sender within the window: silence.
	dispatchPkt(t, radio, textPacket(0xdeadbeef, 0xabcd1234, "hello two"))
	time.Sleep(300 * time.Millisecond)
	if n := len(radio.outbound()); n != 1 {
		t.Fatalf("second banner within the per-sender window: outbound = %d, want 1", n)
	}

	// A different sender gets a banner once the global pacing allows it.
	dispatchPkt(t, radio, textPacket(0xcafebabe, 0xabcd1234, "hello three"))
	radio.waitOutbound(t, 2)

	// A banner queued while the global window is closed must yield to
	// direct replies: /alert answers immediately, the queued banner
	// follows only on later traffic.
	dispatchPkt(t, radio, textPacket(0xdead1234, 0xabcd1234, "hello four"))
	dispatchPkt(t, radio, textPacket(0xdeadbeef, 0xabcd1234, "/alert alarm test"))
	radio.waitOutbound(t, 3)
	out := radio.outbound()
	if got := string(out[2].GetDecoded().GetPayload()); !strings.Contains(got, "OK: alert raised") {
		t.Fatalf("alarm reply = %q, want the direct /alert confirmation BEFORE any banner", got)
	}

	// After the global window, the next inbound packet drains the queued
	// banner.
	time.Sleep(600 * time.Millisecond)
	dispatchPkt(t, radio, textPacket(0xbeef0000, core.BroadcastNodeID.Uint32(), "unrelated broadcast"))
	radio.waitOutbound(t, 4)
	out = radio.outbound()
	if out[3].GetTo() != 0xdead1234 {
		t.Fatalf("drained banner to = %08x, want dead1234", out[3].GetTo())
	}
	if got := string(out[3].GetDecoded().GetPayload()); !strings.Contains(got, "WarnFlux v1.0 - SOSNA") {
		t.Fatalf("drained banner = %q, want the standard banner", got)
	}

	// After the per-sender window the same sender is answered again.
	time.Sleep(1100 * time.Millisecond)
	dispatchPkt(t, radio, textPacket(0xdeadbeef, 0xabcd1234, "hello five"))
	radio.waitOutbound(t, 5)
	out = radio.outbound()
	if got := string(out[4].GetDecoded().GetPayload()); !strings.Contains(got, "WarnFlux v1.0 - SOSNA") {
		t.Fatalf("post-window banner = %q, want the standard banner", got)
	}
}

// TestHubAlertCommand pins the operator /alert command: an authorized
// sender raises a severe hazard with a ~4-hour expiry through the event
// bridge; a missing parameter answers with the usage; an unauthorized
// sender gets the denial.
func TestHubAlertCommand(t *testing.T) {
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
	cli := radiocli.New("WarnFlux v1.0 - SOSNA - sosna.sp9moa.pl")
	cli.RegisterRestricted("alert", "alert", func(args string, _ radiocli.Sender) radiocli.Result {
		headline := strings.TrimSpace(args)
		if headline == "" {
			return radiocli.Result{Handled: true, Reply: "Missing parameter: /alert <text>"}
		}
		return radiocli.Result{Handled: true, Alert: &radiocli.AlertSpec{Headline: headline, TTL: 4 * time.Hour}, Reply: "OK: alert raised"}
	})
	radio.hub.SetCLI(cli)

	// /alert with text: severe event + confirmation reply.
	dispatchPkt(t, radio, textPacket(0xdeadbeef, 0xabcd1234, "/alert pozar lasu"))
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
	mu.Lock()
	defer mu.Unlock()
	if len(events) != 1 {
		t.Fatalf("events after /alert = %d, want 1", len(events))
	}
	var we MessageEventWire
	if err := json.Unmarshal([]byte(events[0]), &we); err != nil {
		t.Fatalf("alert payload: %v", err)
	}
	if we.Event.Source != "meshtastic" || we.Event.Severity != "severe" {
		t.Fatalf("alert event = %+v, want severe from meshtastic", we.Event)
	}
	if we.Event.Headline != "pozar lasu" {
		t.Fatalf("headline = %q, want the operator text", we.Event.Headline)
	}
	if we.Event.ExpiresAt == nil {
		t.Fatal("alert event has no expiry")
	}
	exp, err := time.Parse(time.RFC3339, *we.Event.ExpiresAt)
	if err != nil {
		t.Fatalf("expiry = %q: %v", *we.Event.ExpiresAt, err)
	}
	if d := time.Until(exp); d < 3*time.Hour+45*time.Minute || d > 4*time.Hour+15*time.Minute {
		t.Fatalf("expiry in %s, want ~4h", d)
	}
	radio.waitOutbound(t, 1)
	out := radio.outbound()
	if got := string(out[len(out)-1].GetDecoded().GetPayload()); !strings.Contains(got, "OK: alert raised") {
		t.Fatalf("alert reply = %q, want the confirmation", got)
	}

	// /alert without text: usage reply, no new event.
	before := len(radio.outbound())
	dispatchPkt(t, radio, textPacket(0xdeadbeef, 0xabcd1234, "/alert"))
	radio.waitOutbound(t, before+1)
	out = radio.outbound()
	if got := string(out[len(out)-1].GetDecoded().GetPayload()); !strings.Contains(got, "Missing parameter") {
		t.Fatalf("no-arg /alert reply = %q, want the usage", got)
	}
	if len(events) != 1 {
		t.Fatalf("events after no-arg /alert = %d, want still 1", len(events))
	}

	// Unauthorized sender: denial, no event.
	before = len(radio.outbound())
	dispatchPkt(t, radio, textPacket(0xcafebabe, 0xabcd1234, "/alert x"))
	radio.waitOutbound(t, before+1)
	out = radio.outbound()
	if got := string(out[len(out)-1].GetDecoded().GetPayload()); !strings.Contains(got, "You are not authorized") {
		t.Fatalf("unauthorized /alert reply = %q, want the denial", got)
	}
	if len(events) != 1 {
		t.Fatalf("events after unauthorized /alert = %d, want still 1", len(events))
	}
}

// TestHubReplyFitsChannelLimit pins the shared fitting at the gateway:
// replies within the mesh limit pass unchanged, and a very long
// installation identity shortens progressively instead of overflowing.
func TestHubReplyFitsChannelLimit(t *testing.T) {
	radio := newTestRadio(t, Config{Enabled: true, Device: "/dev/fake", NodeTTL: time.Hour,
		// Tiny banner windows: this test pins the fitting, not the
		// rate limiting (covered by TestHubBannerRateLimits).
		BannerMinInterval: time.Millisecond, BannerGlobalInterval: time.Millisecond})
	radio.waitConnected(t)
	radio.hub.SetSenderGate(func(id string) string { return "sp9kow" })
	radio.hub.SetEventSink(func(_ context.Context, _ string, _ bool, _ []byte) error { return nil })
	radio.hub.SetCLI(radiocli.New("WarnFlux v1.0 - SOSNA - a.very.long.domain.example.org"))

	dispatchPkt(t, radio, textPacket(0xdeadbeef, 0xabcd1234, "hello"))
	radio.waitOutbound(t, 1)
	out := radio.outbound()
	if got := string(out[0].GetDecoded().GetPayload()); got != "WarnFlux v1.0 - SOSNA - a.very.long.domain.example.org | type /help for help" {
		t.Fatalf("reply = %q, want the full banner (it fits the mesh limit)", got)
	}

	// An identity that cannot fit shortens progressively: the domain
	// and the installation name drop, the payload survives.
	radio.hub.SetCLI(radiocli.New("WarnFlux v1.0 - " + strings.Repeat("x", 110)))
	before := len(radio.outbound())
	dispatchPkt(t, radio, textPacket(0xdeadbeef, 0xabcd1234, "hello"))
	radio.waitOutbound(t, before+1)
	out = radio.outbound()
	got := string(out[len(out)-1].GetDecoded().GetPayload())
	if len([]rune(got)) > meshTextMaxRunes {
		t.Fatalf("reply = %q, %d runes — over the mesh limit", got, len([]rune(got)))
	}
	if got != "WarnFlux v1.0 | type /help for help" {
		t.Fatalf("reply = %q, want the instance dropped", got)
	}
}

// TestAlertText pins the shared alert marking: the 🔔 label in front
// (what the official clients show; a label only), the in-band alert
// character at the end (the byte the firmware scans for and reacts to)
// and the hard bound to the channel limit that never loses the marker.
func TestAlertText(t *testing.T) {
	if got, want := AlertText("  test  "), "🔔 test "+AlertMarker; got != want {
		t.Fatalf("AlertText = %q, want %q", got, want)
	}
	long := strings.Repeat("x", meshTextMaxRunes+50)
	got := AlertText(long)
	if !strings.HasPrefix(got, AlertEmoji+" ") {
		t.Fatalf("alert label lost: %q", got[:8])
	}
	if !strings.HasSuffix(got, " "+AlertMarker) {
		t.Fatalf("alert marker lost: %q", got[len(got)-8:])
	}
	if n := len([]rune(got)); n != meshTextMaxRunes {
		t.Fatalf("length = %d runes, want the hard cut at %d", n, meshTextMaxRunes)
	}
	if !strings.Contains(got, "xxx") {
		t.Fatalf("text mangled: %q", got)
	}
}

// TestAlertsAt pins the alarm threshold: only severe and above ring the
// recipients' devices, and a value outside the canonical scale never
// does (a malformed severity must not wake anybody).
func TestAlertsAt(t *testing.T) {
	for _, tc := range []struct {
		sev  string
		want bool
	}{
		{severity.Extreme, true},
		{severity.Severe, true},
		{severity.Moderate, false},
		{severity.Minor, false},
		{severity.Unknown, false},
		{"", false},
		{"SEVERE", false}, // providers normalize upstream
		{"catastrophic", false},
	} {
		if got := AlertsAt(tc.sev); got != tc.want {
			t.Errorf("AlertsAt(%q) = %v, want %v", tc.sev, got, tc.want)
		}
	}
}

// TestPlainText pins the quiet path: trimmed and bounded to the channel
// limit, with no alert framing at all.
func TestPlainText(t *testing.T) {
	if got := PlainText("  test  "); got != "test" {
		t.Fatalf("PlainText = %q, want the trimmed text", got)
	}
	got := PlainText(strings.Repeat("y", meshTextMaxRunes+20))
	if n := len([]rune(got)); n != meshTextMaxRunes {
		t.Fatalf("length = %d runes, want the cut at %d", n, meshTextMaxRunes)
	}
	if strings.Contains(got, AlertMarker) || strings.Contains(got, AlertEmoji) {
		t.Fatalf("quiet text carries the alert framing: %q", got)
	}
	if n := AlertFramingRunes(); n != len([]rune(AlertEmoji))+3 {
		t.Fatalf("AlertFramingRunes() = %d, want %d", n, len([]rune(AlertEmoji))+3)
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

// TestTracerouteProgress pins the panel's window into a probe in flight:
// the hub reports the elapsed time, the frames sent and the hops heard so
// far, then forgets the probe once it settled.
func TestTracerouteProgress(t *testing.T) {
	radio := newTestRadio(t, Config{Enabled: true, Device: "/dev/fake", NodeTTL: time.Hour})
	radio.waitConnected(t)

	if _, ok := radio.hub.TracerouteProgress("ef010203"); ok {
		t.Fatal("a probe was reported before one started")
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = radio.hub.Traceroute(ctx, "ef010203", 5*time.Second)
	}()

	// The first frame is on the air: the panel can already say so.
	radio.waitOutbound(t, 1)
	waitForHub(t, func() bool {
		p, ok := radio.hub.TracerouteProgress("ef010203")
		return ok && p.Probes == 1 && p.MaxProbes == 3 && p.Timeout == 5*time.Second
	}, "the probe was never reported as sent")
	p, _ := radio.hub.TracerouteProgress("ef010203")
	if p.Destination != "ef010203" || p.Elapsed() <= 0 || len(p.Hops) != 0 || p.Reached {
		t.Fatalf("fresh probe = %+v", p)
	}
	// A probe to another node is not this one.
	if _, ok := radio.hub.TracerouteProgress("abcd1234"); ok {
		t.Fatal("the progress is not keyed by destination")
	}

	// The destination answers: the route lands in the progress and the
	// record disappears once the call returns.
	reply, err := proto.Marshal(&pb.Routing{Variant: &pb.Routing_RouteReply{RouteReply: &pb.RouteDiscovery{
		Route:   []uint32{0xaaaa0001},
		SnrBack: []int32{24},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	dispatchPkt(t, radio, &pb.MeshPacket{
		From: 0xef010203, To: 0xabcd1234,
		PayloadVariant: &pb.MeshPacket_Decoded{
			Decoded: &pb.Data{Portnum: pb.PortNum_ROUTING_APP, Payload: reply},
		},
	})
	select {
	case <-done:
	case <-time.After(6 * time.Second):
		t.Fatal("the probe never settled")
	}
	if p, ok := radio.hub.TracerouteProgress("ef010203"); ok {
		t.Fatalf("a settled probe is still reported: %+v", p)
	}
}

// waitForHub polls cond for up to two seconds.
func waitForHub(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal(what)
}

// breakableLink decorates every dialled transport so a test can break the
// write of ONE named text frame the way a socket the peer closed does
// ("broken pipe"). The break is armed explicitly and matched by payload,
// so a frame the hub sends on its own (a NodeInfo request, a beacon, a
// CLI reply) can never consume it.
type breakableLink struct {
	mu     sync.Mutex
	text   string
	failed bool
}

func (b *breakableLink) decorate(c transportConn) transportConn {
	return &breakableConn{transportConn: c, link: b}
}

// breakSendOfText arms the failure for the next frame carrying text.
func (b *breakableLink) breakSendOfText(text string) {
	b.mu.Lock()
	b.text, b.failed = text, false
	b.mu.Unlock()
}

type breakableConn struct {
	transportConn
	link *breakableLink
}

func (c *breakableConn) SendToRadio(msg *pb.ToRadio) error {
	if pkt := msg.GetPacket(); pkt != nil {
		c.link.mu.Lock()
		fail := c.link.text != "" && !c.link.failed &&
			string(pkt.GetDecoded().GetPayload()) == c.link.text
		if fail {
			c.link.failed = true
		}
		c.link.mu.Unlock()
		if fail {
			return errors.New("write tcp 172.30.0.2:51242->10.0.0.121:4403: write: broken pipe")
		}
	}
	return c.transportConn.SendToRadio(msg)
}

// TestHubSendRebuildsTheLinkAfterABrokenFrame pins the reported prod
// failure: sending on the group channel answered "write: broken pipe"
// and the message was lost, because a send that met a link the peer had
// already closed gave up instead of rebuilding it. The frame must now
// ride a fresh session and reach the device.
func TestHubSendRebuildsTheLinkAfterABrokenFrame(t *testing.T) {
	link := &breakableLink{}
	radio := newTestRadio(t, Config{Enabled: true, Device: "/dev/fake", NodeTTL: time.Hour},
		testRadioOpts{wrap: link.decorate})
	radio.waitConnected(t)
	rec := &captureRecorder{}
	radio.hub.SetRecorder(rec)

	link.breakSendOfText("SOSNA test")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := radio.hub.SendChannelText(ctx, 1, "SOSNA test", "admin"); err != nil {
		t.Fatalf("send on a closed link must ride a rebuilt session, got %v", err)
	}
	if got := radio.dials.Load(); got != 2 {
		t.Errorf("dials = %d, want 2 (the broken link plus its replacement)", got)
	}
	out := radio.waitOutbound(t, 1)
	if len(out) != 1 || string(out[0].GetDecoded().GetPayload()) != "SOSNA test" {
		t.Fatalf("outbound = %v, want the frame delivered on the rebuilt link", out)
	}
	if msgs := rec.messages(); len(msgs) != 1 {
		t.Fatalf("history rows = %d, want exactly one: the retry must not duplicate the row", len(msgs))
	}
}

// TestHubSendWaitsOutAReconnect pins the other half of the reported
// failure: a send that landed while the link was being rebuilt answered
// "device not connected" at once. It must wait for the replacement
// session and then go out.
func TestHubSendWaitsOutAReconnect(t *testing.T) {
	radio := newTestRadio(t, Config{Enabled: true, Device: "/dev/fake", NodeTTL: time.Hour})
	radio.waitConnected(t)

	// Ask for a rebuild the way a dead link does, and wait until the hub
	// reports the link down — the send below lands inside that window.
	dialsBefore := radio.dials.Load()
	radio.hub.requestReconnect()
	waitForHub(t, func() bool { return !radio.hub.Connected() }, "the session never went down")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := radio.hub.SendChannelText(ctx, 1, "link wait", "admin"); err != nil {
		t.Fatalf("send during a reconnect must wait for the link, got %v", err)
	}
	if got := radio.dials.Load(); got <= dialsBefore {
		t.Errorf("dials = %d, want the session rebuilt past %d", got, dialsBefore)
	}
	out := radio.waitOutbound(t, 1)
	if len(out) != 1 || string(out[0].GetDecoded().GetPayload()) != "link wait" {
		t.Fatalf("outbound = %v, want the frame delivered after the wait", out)
	}
}

// heartbeatBreaker fails the heartbeat write on the FIRST dialled
// transport only, so the replacement session is healthy.
type heartbeatBreaker struct {
	mu    sync.Mutex
	dials int
	seen  atomic.Int32
}

func (b *heartbeatBreaker) decorate(c transportConn) transportConn {
	b.mu.Lock()
	b.dials++
	first := b.dials == 1
	b.mu.Unlock()
	return &heartbeatConn{transportConn: c, breakIt: first, seen: &b.seen}
}

type heartbeatConn struct {
	transportConn
	breakIt bool
	seen    *atomic.Int32
}

func (c *heartbeatConn) SendToRadio(msg *pb.ToRadio) error {
	if msg.GetHeartbeat() != nil {
		c.seen.Add(1)
		if c.breakIt {
			return errors.New("write tcp 172.30.0.2:51242->10.0.0.121:4403: write: broken pipe")
		}
	}
	return c.transportConn.SendToRadio(msg)
}

// TestHubHeartbeatRebuildsADeadLink pins the keepalive: the client pings
// the device with the protocol heartbeat, so a socket the peer closed
// surfaces as a failed write and the session is rebuilt at once, instead
// of sitting dead until the silence watchdog fires two minutes later.
func TestHubHeartbeatRebuildsADeadLink(t *testing.T) {
	hb := &heartbeatBreaker{}
	// A silence timeout far beyond the test keeps the watchdog out of the
	// picture: only the heartbeat can explain the rebuild.
	radio := newTestRadio(t, Config{Enabled: true, Device: "/dev/fake", NodeTTL: time.Hour,
		SilenceTimeout: time.Minute}, testRadioOpts{
		wrap:  hb.decorate,
		setup: func(h *Hub) { h.heartbeatEvery = 30 * time.Millisecond },
	})
	radio.waitConnected(t)

	waitForHubTimeout(t, 15*time.Second, func() bool { return radio.dials.Load() >= 2 },
		"a failed heartbeat never rebuilt the session")
	if got := hb.seen.Load(); got == 0 {
		t.Fatal("no heartbeat was ever written to the device")
	}
}

// waitForHubTimeout is waitForHub with an explicit budget.
func waitForHubTimeout(t *testing.T, budget time.Duration, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal(what)
}
