package meshcore

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeRecorder captures recorded messages.
type fakeRecorder struct {
	mu  sync.Mutex
	got []Message
}

func (f *fakeRecorder) RecordMeshMessage(_ context.Context, direction, sender, channel, text, operator string, hops int, at time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.got = append(f.got, Message{Direction: direction, Sender: sender, Channel: channel, Hops: hops, Operator: operator, Text: text, At: at})
	return nil
}

func (f *fakeRecorder) messages() []Message {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]Message(nil), f.got...)
}

// encodeDeviceFrame frames a device→host frame (0x3E header).
func encodeDeviceFrame(payload []byte) []byte {
	out := []byte{frameIn, 0, 0}
	binary.LittleEndian.PutUint16(out[1:3], uint16(len(payload)))
	return append(out, payload...)
}

// readHostFrame reads one host→device frame (0x3C header) payload from the
// fake device end of the pipe.
func readHostFrame(host net.Conn) ([]byte, error) {
	buf := make([]byte, 256)
	host.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, err := host.Read(buf)
	if err != nil {
		return nil, err
	}
	if n < 3 || buf[0] != frameOut {
		return buf[:n], nil // let the caller diagnose
	}
	plen := int(binary.LittleEndian.Uint16(buf[1:3]))
	if n < 3+plen {
		return buf[:n], nil
	}
	return buf[3 : 3+plen], nil
}

func TestHubSession(t *testing.T) {
	dev, host := net.Pipe()
	defer dev.Close()
	defer host.Close()

	origDial := Dial
	Dial = func(Config) (conn, error) { return dev, nil }
	defer func() { Dial = origDial }()

	hub, err := NewHub(Config{Enabled: true, Device: "/dev/fake", ChannelIdx: 2, ChannelName: "#sp9moa", NodeTTL: time.Hour}, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	rec := &fakeRecorder{}
	hub.SetRecorder(rec)
	msgCapture := &stationCapture{}
	hub.SetMessageSink(msgCapture.publish)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- hub.Run(ctx) }()

	// The device side: read the handshake, then push self info + advert +
	// a channel message.
	devDone := make(chan struct{})
	go func() {
		defer close(devDone)
		buf := make([]byte, 256)
		gotBytes := 0
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) && gotBytes == 0 {
			host.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
			n, err := host.Read(buf)
			if n > 0 {
				gotBytes = n // the handshake is host→device (0x3C framing)
			}
			if err != nil && !isTimeout(err) {
				break
			}
		}
		if gotBytes == 0 {
			t.Log("DEVICE: no handshake bytes read")
			return
		}

		// The hub's syncChannel sends GET_CHANNEL next; answer with a
		// stale name so it must issue SET_CHANNEL (read it back before
		// writing anything else — the pipe is synchronous).
		get, err := readHostFrame(host)
		if err != nil {
			t.Logf("DEVICE: get-channel read: %v", err)
			return
		}
		if len(get) != 2 || get[0] != cmdGetChannel || get[1] != 2 {
			t.Logf("DEVICE: unexpected get-channel frame %x", get)
			return
		}
		ci := []byte{respChannelInfo, 2}
		name32 := make([]byte, 32)
		copy(name32, "#stary")
		ci = append(ci, name32...)
		ci = append(ci, make([]byte, 16)...) // unencrypted slot
		host.Write(encodeDeviceFrame(ci))

		set, err := readHostFrame(host)
		if err != nil {
			t.Logf("DEVICE: set-channel read: %v", err)
			return
		}
		if len(set) != 2+32+16 || set[0] != cmdSetChannel || set[1] != 2 {
			t.Logf("DEVICE: unexpected set-channel frame %x", set)
			return
		}
		if got := strings.TrimRight(string(set[2:34]), "\x00"); got != "#sp9moa" {
			t.Logf("DEVICE: set-channel name = %q, want #sp9moa", got)
			return
		}

		// The startup drain sends SYNC_NEXT before the hub reads anything
		// else; answer with NO_MORE so the session moves to the pump.
		sync, err := readHostFrame(host)
		if err != nil || len(sync) != 1 || sync[0] != cmdSyncNextMessage {
			t.Logf("DEVICE: unexpected drain frame %x (%v)", sync, err)
			return
		}
		host.Write(encodeDeviceFrame([]byte{respNoMoreMessages}))
		host.Write(encodeDeviceFrame([]byte{respOK})) // the SET_CHANNEL ack

		// Self info reply: type,tx,maxTx + pubkey32 + lat/lon + 3 reserved
		// + manual + freq + bw + sf + cr + name.
		raw := []byte{respSelfInfo, 0x01, 0x14, 0x16}
		raw = append(raw, make([]byte, 32)...)
		var b [4]byte
		binary.LittleEndian.PutUint32(b[:], 50_000_000)
		raw = append(raw, b[:]...)
		binary.LittleEndian.PutUint32(b[:], 20_000_000)
		raw = append(raw, b[:]...)
		raw = append(raw, 0, 0, 0, 0)                // reserved3 + manualAddContacts
		binary.LittleEndian.PutUint32(b[:], 869_618) // kHz → 869.618 MHz
		raw = append(raw, b[:]...)
		binary.LittleEndian.PutUint32(b[:], 62_500)
		raw = append(raw, b[:]...)
		raw = append(raw, 8, 5)
		raw = append(raw, []byte("MOA SOSNA")...)
		host.Write(encodeDeviceFrame(raw))

		// New advert push for one neighbour.
		adv := []byte{pushNewAdvert}
		adv = append(adv, make([]byte, 32)...)
		adv = append(adv, 0x02, 0x00, 0x00) // type, flags, pathLen
		adv = append(adv, make([]byte, 64)...)
		name := make([]byte, 32)
		copy(name, "RKSR-TN-R3")
		adv = append(adv, name...)
		adv = append(adv, 0, 0, 0, 0)                   // lastAdvert
		binary.LittleEndian.PutUint32(b[:], 50_020_000) // ~2.2 km N of us
		adv = append(adv, b[:]...)
		binary.LittleEndian.PutUint32(b[:], 20_000_000) // same longitude
		adv = append(adv, b[:]...)
		adv = append(adv, 0, 0, 0, 0) // lastMod
		host.Write(encodeDeviceFrame(adv))

		// Channel message (V3): snr, reserved2, chIdx, pathLen, txtType, ts.
		msg := []byte{respChannelMsgV3, 0x0C, 0x00, 0x00, 0x00, 0x03, 0x00}
		msg = binary.LittleEndian.AppendUint32(msg, 1234567890)
		msg = append(msg, []byte("Test SOSNA")...)
		host.Write(encodeDeviceFrame(msg))
		t.Log("DEVICE: channel sync done")
	}()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		snap := hub.Snapshot()
		if snap.Name == "MOA SOSNA" && len(snap.Nodes) == 1 && len(rec.messages()) == 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	snap := hub.Snapshot()
	if snap.Name != "MOA SOSNA" {
		t.Fatalf("self name = %q, want MOA SOSNA", snap.Name)
	}
	if snap.ChannelIdx != 2 || snap.ChannelName != "#sp9moa" {
		t.Fatalf("channel = %d %q, want 2 #sp9moa", snap.ChannelIdx, snap.ChannelName)
	}
	if len(snap.Nodes) != 1 || snap.Nodes[0].Name != "RKSR-TN-R3" {
		t.Fatalf("nodes = %+v", snap.Nodes)
	}
	// The advert sits 0.02° north of the self position: ~2.2 km, bearing ~0.
	if d := snap.Nodes[0].DistKM; d < 2.0 || d > 2.5 {
		t.Fatalf("node distance = %.2f km, want ~2.2", d)
	}
	if b := snap.Nodes[0].BearingDeg; b > 1.5 {
		t.Fatalf("node bearing = %.2f, want ~0", b)
	}
	got := rec.messages()
	if len(got) != 1 || got[0].Text != "Test SOSNA" || got[0].Direction != "rx" || got[0].Hops != 3 {
		t.Fatalf("recorded = %+v", got)
	}

	// The device side must have completed the channel-name exchange before
	// the outbound send, or it would eat that frame.
	select {
	case <-devDone:
	case <-time.After(3 * time.Second):
		t.Fatal("device goroutine did not finish the channel sync")
	}

	// Outbound: send a channel message; the device reads it back and
	// acknowledges, so the hub records the tx. The frame uses host→device
	// framing (0x3C header).
	sendErr := make(chan error, 1)
	go func() { sendErr <- hub.SendChannelMessage("HELLO MESH", "admin") }()
	buf := make([]byte, 128)
	host.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, err := host.Read(buf)
	if err != nil {
		t.Fatalf("device read: %v", err)
	}
	if n < 10 || buf[0] != frameOut {
		t.Fatalf("device received %x", buf[:n])
	}
	plen := int(binary.LittleEndian.Uint16(buf[1:3]))
	payload := buf[3 : 3+plen]
	if payload[0] != cmdSendChannelTxtMsg || string(payload[7:]) != "HELLO MESH" {
		t.Fatalf("device payload %x", payload)
	}
	host.Write(encodeDeviceFrame([]byte{respOK}))
	select {
	case err := <-sendErr:
		if err != nil {
			t.Fatalf("SendChannelMessage = %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("SendChannelMessage did not finish after the device ack")
	}
	got = rec.messages()
	if len(got) != 2 || got[1].Text != "HELLO MESH" || got[1].Direction != "tx" || got[1].Operator != "admin" {
		t.Fatalf("recorded after tx = %+v", got)
	}

	// Both messages must also go out on the MQTT message feed.
	pubDeadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(pubDeadline) && msgCapture.count() < 2 {
		time.Sleep(20 * time.Millisecond)
	}
	msgCapture.mu.Lock()
	pubs := append([]stationPub(nil), msgCapture.got...)
	msgCapture.mu.Unlock()
	if len(pubs) != 2 {
		t.Fatalf("message publishes = %d, want 2: %+v", len(pubs), pubs)
	}
	if pubs[0].topic != "meshcore/messages" || pubs[0].retained || !bytes.Contains(pubs[0].payload, []byte("Test SOSNA")) {
		t.Fatalf("rx publish = %+v", pubs[0])
	}
	if pubs[1].topic != "meshcore/messages" || pubs[1].retained || !bytes.Contains(pubs[1].payload, []byte("HELLO MESH")) || !bytes.Contains(pubs[1].payload, []byte(`"direction":"tx"`)) {
		t.Fatalf("tx publish = %+v", pubs[1])
	}

	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("hub did not stop on cancel")
	}
}

func TestDistanceAndBearing(t *testing.T) {
	// One degree of latitude is about 111.2 km.
	d := DistanceKM(0, 0, 1, 0)
	if d < 110.5 || d > 111.5 {
		t.Fatalf("distance(0,0 -> 1,0) = %.2f km, want ~111.2", d)
	}
	if b := BearingDeg(0, 0, 1, 0); b > 1 {
		t.Fatalf("bearing north = %.2f, want ~0", b)
	}
	if b := BearingDeg(0, 0, 0, 1); b < 89 || b > 91 {
		t.Fatalf("bearing east = %.2f, want ~90", b)
	}
	// Warsaw (52.2297, 21.0122) to Krakow (50.0647, 19.9450): ~252 km SSW.
	d = DistanceKM(52.2297, 21.0122, 50.0647, 19.9450)
	if d < 247 || d > 257 {
		t.Fatalf("Warsaw->Krakow distance = %.1f km, want ~252", d)
	}
	b := BearingDeg(52.2297, 21.0122, 50.0647, 19.9450)
	if b < 190 || b > 200 {
		t.Fatalf("Warsaw->Krakow bearing = %.1f, want ~195", b)
	}
}

// TestPublicChannelBlocked pins the safety rule: the hub never transmits
// on the Public channel (index 0), even with a live connection.
func TestPublicChannelBlocked(t *testing.T) {
	hub, err := NewHub(Config{Enabled: true, Device: "/dev/fake", ChannelIdx: 0, NodeTTL: time.Hour}, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	if err := hub.SendChannelMessage("must not go out", "admin"); err == nil {
		t.Fatal("SendChannelMessage on public channel 0 succeeded, want refusal")
	}
	if hub.Snapshot().ChannelIdx != 0 {
		t.Fatal("snapshot should report the configured channel 0")
	}
}

// TestHubContactLookup pins the name enrichment: a known contact only
// pushes its pubkey (0x80), the hub asks CMD_GET_CONTACT_BY_KEY and fills
// the node from the full contact record.
func TestHubContactLookup(t *testing.T) {
	dev, host := net.Pipe()
	defer dev.Close()
	defer host.Close()

	origDial := Dial
	Dial = func(Config) (conn, error) { return dev, nil }
	defer func() { Dial = origDial }()

	hub, err := NewHub(Config{Enabled: true, Device: "/dev/fake", ChannelIdx: 2, NodeTTL: time.Hour}, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hub.Run(ctx)

	go func() {
		buf := make([]byte, 256)
		gotBytes := 0
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) && gotBytes == 0 {
			host.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
			n, err := host.Read(buf)
			if n > 0 {
				gotBytes = n // handshake
			}
			if err != nil && !isTimeout(err) {
				return
			}
		}
		if gotBytes == 0 {
			return
		}
		// Startup drain: answer NO_MORE so the pump starts.
		sync, err := readHostFrame(host)
		if err != nil || len(sync) != 1 || sync[0] != cmdSyncNextMessage {
			t.Logf("DEVICE: unexpected drain frame %x (%v)", sync, err)
			return
		}
		host.Write(encodeDeviceFrame([]byte{respNoMoreMessages}))

		// A known contact announces via the bare pubkey push.
		key := bytes.Repeat([]byte{0xAB}, 32)
		host.Write(encodeDeviceFrame(append([]byte{pushAdvert}, key...)))

		// The hub must ask for the full record.
		req, err := readHostFrame(host)
		if err != nil || len(req) != 33 || req[0] != cmdGetContactByKey || !bytes.Equal(req[1:], key) {
			t.Logf("DEVICE: unexpected contact query %x (%v)", req, err)
			return
		}

		// Full contact record: [0x03, pubkey32, type, flags, pathLen,
		// path64, name32, lastAdvert4, lat4, lon4, lastMod4].
		rec := []byte{respContact}
		rec = append(rec, key...)
		rec = append(rec, 0x02, 0x00, 0x03) // type repeater, flags 0, 3 hops
		rec = append(rec, make([]byte, 64)...)
		name := make([]byte, 32)
		copy(name, "RKSR-TN-R3")
		rec = append(rec, name...)
		rec = append(rec, 0, 0, 0, 0) // lastAdvert
		var b [4]byte
		binary.LittleEndian.PutUint32(b[:], 50_020_000)
		rec = append(rec, b[:]...)
		binary.LittleEndian.PutUint32(b[:], 20_000_000)
		rec = append(rec, b[:]...)
		rec = append(rec, 0, 0, 0, 0) // lastMod
		host.Write(encodeDeviceFrame(rec))
	}()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		snap := hub.Snapshot()
		if len(snap.Nodes) == 1 && snap.Nodes[0].Name == "RKSR-TN-R3" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	snap := hub.Snapshot()
	if len(snap.Nodes) != 1 {
		t.Fatalf("nodes = %+v", snap.Nodes)
	}
	n := snap.Nodes[0]
	if n.Name != "RKSR-TN-R3" || n.Type != 2 || n.Hops != 3 || n.Lat != 50.02 {
		t.Fatalf("node = %+v", n)
	}
}

// TestHubSendRejected pins the device-error surfacing: when the device
// rejects a direct send (unknown contact), the caller gets the error and
// no tx is recorded.
func TestHubSendRejected(t *testing.T) {
	dev, host := net.Pipe()
	defer dev.Close()
	defer host.Close()

	origDial := Dial
	Dial = func(Config) (conn, error) { return dev, nil }
	defer func() { Dial = origDial }()

	hub, err := NewHub(Config{Enabled: true, Device: "/dev/fake", ChannelIdx: 2, NodeTTL: time.Hour}, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	rec := &fakeRecorder{}
	hub.SetRecorder(rec)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hub.Run(ctx)

	// Wait until the session is up before sending.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && !hub.Connected() {
		time.Sleep(20 * time.Millisecond)
	}

	go func() {
		buf := make([]byte, 256)
		gotBytes := 0
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) && gotBytes == 0 {
			host.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
			n, err := host.Read(buf)
			if n > 0 {
				gotBytes = n
			}
			if err != nil && !isTimeout(err) {
				return
			}
		}
		if gotBytes == 0 {
			return
		}
		// Startup drain: answer NO_MORE so the pump starts.
		sync, err := readHostFrame(host)
		if err != nil || len(sync) != 1 || sync[0] != cmdSyncNextMessage {
			t.Logf("DEVICE: unexpected drain frame %x (%v)", sync, err)
			return
		}
		host.Write(encodeDeviceFrame([]byte{respNoMoreMessages}))
		req, err := readHostFrame(host)
		if err != nil || len(req) < 7 || req[0] != cmdSendTxtMsg {
			t.Logf("DEVICE: unexpected direct send %x (%v)", req, err)
			return
		}
		// Unknown contact on the device: PACKET_ERR, code NOT_FOUND.
		host.Write(encodeDeviceFrame([]byte{respErr, 2}))
	}()

	err = hub.SendContactMessage("abcd1234abcd", "hello", "admin")
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("SendContactMessage = %v, want device not-found error", err)
	}
	if got := rec.messages(); len(got) != 0 {
		t.Fatalf("recorded = %+v, want no tx for a rejected send", got)
	}
}

// TestHubSendAutoAdd pins the retry path: with the full public key, a
// not-found rejection adds the contact on the device and retries the
// send once.
func TestHubSendAutoAdd(t *testing.T) {
	dev, host := net.Pipe()
	defer dev.Close()
	defer host.Close()

	origDial := Dial
	Dial = func(Config) (conn, error) { return dev, nil }
	defer func() { Dial = origDial }()

	hub, err := NewHub(Config{Enabled: true, Device: "/dev/fake", ChannelIdx: 2, NodeTTL: time.Hour}, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	rec := &fakeRecorder{}
	hub.SetRecorder(rec)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hub.Run(ctx)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && !hub.Connected() {
		time.Sleep(20 * time.Millisecond)
	}

	fullKey := bytes.Repeat([]byte{0xAB}, 32)
	go func() {
		buf := make([]byte, 256)
		gotBytes := 0
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) && gotBytes == 0 {
			host.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
			n, err := host.Read(buf)
			if n > 0 {
				gotBytes = n
			}
			if err != nil && !isTimeout(err) {
				return
			}
		}
		if gotBytes == 0 {
			return
		}
		// Startup drain: answer NO_MORE so the pump starts.
		sync, err := readHostFrame(host)
		if err != nil || len(sync) != 1 || sync[0] != cmdSyncNextMessage {
			t.Logf("DEVICE: unexpected drain frame %x (%v)", sync, err)
			return
		}
		host.Write(encodeDeviceFrame([]byte{respNoMoreMessages}))
		// First direct send: unknown contact.
		req, err := readHostFrame(host)
		if err != nil || len(req) < 7 || req[0] != cmdSendTxtMsg {
			t.Logf("DEVICE: unexpected direct send %x (%v)", req, err)
			return
		}
		host.Write(encodeDeviceFrame([]byte{respErr, 2}))

		// The hub must add the contact now.
		add, err := readHostFrame(host)
		if err != nil || len(add) != 1+32+1+1+1+64+32+4+4+4+4 || add[0] != cmdAddUpdateContact || !bytes.Equal(add[1:33], fullKey) {
			t.Logf("DEVICE: unexpected add-contact frame %x (%v)", add, err)
			return
		}
		host.Write(encodeDeviceFrame([]byte{respOK}))

		// Retried send: accepted.
		req, err = readHostFrame(host)
		if err != nil || len(req) < 7 || req[0] != cmdSendTxtMsg {
			t.Logf("DEVICE: unexpected retry send %x (%v)", req, err)
			return
		}
		host.Write(encodeDeviceFrame([]byte{respSent}))
	}()

	if err := hub.SendContactMessage(hex.EncodeToString(fullKey), "hello", "admin"); err != nil {
		t.Fatalf("SendContactMessage = %v, want success after auto-add", err)
	}
	got := rec.messages()
	if len(got) != 1 || got[0].Text != "hello" || got[0].Direction != "tx" || got[0].Channel != "direct" {
		t.Fatalf("recorded = %+v", got)
	}
}

// TestHubAutoaddConfig pins the auto-add setup: with AutoAddContacts the
// hub issues SET_AUTOADD_CONFIG right after the channel sync.
func TestHubAutoaddConfig(t *testing.T) {
	dev, host := net.Pipe()
	defer dev.Close()
	defer host.Close()

	origDial := Dial
	Dial = func(Config) (conn, error) { return dev, nil }
	defer func() { Dial = origDial }()

	hub, err := NewHub(Config{Enabled: true, Device: "/dev/fake", ChannelIdx: 2, AutoAddContacts: true, NodeTTL: time.Hour}, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hub.Run(ctx)

	go func() {
		buf := make([]byte, 256)
		gotBytes := 0
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) && gotBytes == 0 {
			host.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
			n, err := host.Read(buf)
			if n > 0 {
				gotBytes = n
			}
			if err != nil && !isTimeout(err) {
				return
			}
		}
		if gotBytes == 0 {
			return
		}
		req, err := readHostFrame(host)
		if err != nil || len(req) != 3 || req[0] != cmdSetAutoaddConfig {
			t.Logf("DEVICE: unexpected autoadd frame %x (%v)", req, err)
			return
		}
		if req[1] != 0x1F || req[2] != 8 {
			t.Logf("DEVICE: autoadd mask/hops = %x, want 1f/8", req[1:])
			return
		}
		host.Write(encodeDeviceFrame([]byte{respOK}))
		// Startup drain: answer NO_MORE.
		sync, err := readHostFrame(host)
		if err != nil || len(sync) != 1 || sync[0] != cmdSyncNextMessage {
			t.Logf("DEVICE: unexpected drain frame %x (%v)", sync, err)
			return
		}
		host.Write(encodeDeviceFrame([]byte{respNoMoreMessages}))
	}()

	// The hub considers autoadd non-fatal but should complete it; give it
	// a moment and confirm the session stays connected.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && !hub.Connected() {
		time.Sleep(20 * time.Millisecond)
	}
	if !hub.Connected() {
		t.Fatal("hub session did not come up")
	}
}

type stationPub struct {
	topic    string
	retained bool
	payload  []byte
}

type stationCapture struct {
	mu  sync.Mutex
	got []stationPub
}

func (c *stationCapture) publish(_ context.Context, topic string, retained bool, payload []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.got = append(c.got, stationPub{topic: topic, retained: retained, payload: append([]byte(nil), payload...)})
	return nil
}

func (c *stationCapture) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.got)
}

// newAdvertFrame builds a NEW_ADVERT push with the given key, name and type.
func newAdvertFrame(key []byte, name string, typ byte) []byte {
	adv := []byte{pushNewAdvert}
	adv = append(adv, key[:32]...)
	adv = append(adv, typ, 0x00, 0x03) // type, flags, 3 hops
	adv = append(adv, make([]byte, 64)...)
	n32 := make([]byte, 32)
	copy(n32, name)
	adv = append(adv, n32...)
	adv = append(adv, 0, 0, 0, 0) // lastAdvert
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], 50_020_000)
	adv = append(adv, b[:]...)
	binary.LittleEndian.PutUint32(b[:], 20_000_000)
	adv = append(adv, b[:]...)
	adv = append(adv, 0, 0, 0, 0) // lastMod
	return adv
}

// newDirectMsgFrame builds a CONTACT_MSG (V3) push from a 6-byte key
// prefix.
func newDirectMsgFrame(prefix []byte, text string) []byte {
	msg := []byte{respContactMsgV3, 0x0C, 0x00, 0x00} // snr, reserved2
	msg = append(msg, prefix[:6]...)
	msg = append(msg, 0x00, 0x01) // pathLen, txtType
	msg = binary.LittleEndian.AppendUint32(msg, 1234567890)
	return append(msg, []byte(text)...)
}

// startEventTestHub starts a hub on a fake serial pipe and returns the
// device end plus the /events capture. Cleanup cancels the hub, closes
// the pipe and waits for Run to return before restoring Dial, so a hub
// from one test can never reconnect into another test's pipe.
func startEventTestHub(t *testing.T, cfg Config) (*Hub, net.Conn, *stationCapture) {
	dev, host := net.Pipe()
	t.Cleanup(func() { dev.Close(); host.Close() })

	origDial := Dial
	Dial = func(Config) (conn, error) { return dev, nil }

	hub, err := NewHub(cfg, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	capture := &stationCapture{}
	hub.SetEventSink(capture.publish)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { hub.Run(ctx); close(done) }()
	t.Cleanup(func() {
		cancel()
		dev.Close()
		host.Close()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
		}
		Dial = origDial
	})
	return hub, host, capture
}

// pumpDevice waits for the hub's handshake bytes, then delivers the given
// device frames.
func pumpDevice(host net.Conn, frames [][]byte) {
	buf := make([]byte, 256)
	gotBytes := 0
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && gotBytes == 0 {
		host.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		n, err := host.Read(buf)
		if n > 0 {
			gotBytes = n
		}
		if err != nil && !isTimeout(err) {
			return
		}
	}
	if gotBytes == 0 {
		return
	}
	// Startup drain: answer NO_MORE so the pump starts before the frames.
	sync, err := readHostFrame(host)
	if err == nil && len(sync) == 1 && sync[0] == cmdSyncNextMessage {
		host.Write(encodeDeviceFrame([]byte{respNoMoreMessages}))
	}
	for _, f := range frames {
		host.Write(encodeDeviceFrame(f))
	}
}

// TestMeshDirectMessageEvent pins the meshcore-message → /events bridge:
// a direct message from a directory-known sender is re-published on the
// events stream with the "meshcore" source, the "Message from:" headline
// prefix (node name when known) and severe severity.
func TestMeshDirectMessageEvent(t *testing.T) {
	hub, host, capture := startEventTestHub(t, Config{
		Enabled: true, Device: "/dev/fake", ChannelIdx: 2, NodeTTL: time.Hour,
		RouteMessages: true,
	})
	hub.SetSenderGate(func(key string) bool { return key == "aabbccddeeff" })

	key := append([]byte{0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF}, make([]byte, 26)...)
	go pumpDevice(host, [][]byte{
		newAdvertFrame(key, "RKSR-TN-R3", 0x02),
		newDirectMsgFrame(key[:6], "flood on the Raba river"),
	})

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && capture.count() == 0 {
		time.Sleep(20 * time.Millisecond)
	}
	capture.mu.Lock()
	got := append([]stationPub(nil), capture.got...)
	capture.mu.Unlock()
	if len(got) != 1 {
		t.Fatalf("events = %d, want 1", len(got))
	}
	p := got[0]
	if p.topic != "events" || p.retained {
		t.Fatalf("publish = %+v", p)
	}
	var ev MessageEventWire
	if err := json.Unmarshal(p.payload, &ev); err != nil {
		t.Fatalf("event payload: %v", err)
	}
	if ev.SchemaVersion != meshMessageEventSchemaVersion || ev.ChangeType != "new" || ev.ChangeID == 0 {
		t.Fatalf("envelope = %+v", ev)
	}
	if !strings.HasPrefix(ev.EventKey, "meshcore:aabbccddeeff:") {
		t.Fatalf("event key = %q", ev.EventKey)
	}
	h := ev.Event
	if h.Source != "meshcore" || h.SourceID != "aabbccddeeff" || h.Event != "MeshCore message" {
		t.Fatalf("hazard identity = %+v", h)
	}
	if h.Severity != "severe" {
		t.Fatalf("severity = %q, want severe", h.Severity)
	}
	if h.Headline != "Message from: RKSR-TN-R3: flood on the Raba river" {
		t.Fatalf("headline = %q, want node name + text", h.Headline)
	}
	if !strings.Contains(h.Description, "MeshCore") {
		t.Fatalf("description = %q", h.Description)
	}
	if h.ExpiresAt == nil || h.EffectiveAt == nil || h.Status != "active" {
		t.Fatalf("lifecycle = %+v", h)
	}
}

// TestMeshDirectMessageEventExclusions pins the anti-noise rules:
// unregistered senders, ack/rej frames and empty texts never route, and
// a valid message from an approved sender routes exactly once.
func TestMeshDirectMessageEventExclusions(t *testing.T) {
	hub, host, capture := startEventTestHub(t, Config{
		Enabled: true, Device: "/dev/fake", ChannelIdx: 2, NodeTTL: time.Hour,
		RouteMessages: true,
	})
	hub.SetSenderGate(func(key string) bool { return key == "aabbccddeeff" })

	approved := []byte{0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF}
	unknown := []byte{0x11, 0x22, 0x33, 0x44, 0x55, 0x66}
	go pumpDevice(host, [][]byte{
		newDirectMsgFrame(unknown, "unknown sender"),
		newDirectMsgFrame(approved, "ack00001"),
		newDirectMsgFrame(approved, "rej00002"),
		newDirectMsgFrame(approved, "   "),
		newDirectMsgFrame(approved, "real alert"),
	})

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && capture.count() == 0 {
		time.Sleep(20 * time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond)
	capture.mu.Lock()
	got := append([]stationPub(nil), capture.got...)
	capture.mu.Unlock()
	if len(got) != 1 {
		t.Fatalf("events = %d, want exactly 1: %s", len(got), func() string {
			out := ""
			for _, p := range got {
				out += string(p.payload) + "\n"
			}
			return out
		}())
	}
	if !strings.Contains(string(got[0].payload), "real alert") {
		t.Fatalf("payload = %s", got[0].payload)
	}
}

// TestMeshDirectMessageEventNoGate pins the fail-closed behavior: without
// a sender gate no direct message ever becomes a hazard event.
func TestMeshDirectMessageEventNoGate(t *testing.T) {
	_, host, capture := startEventTestHub(t, Config{
		Enabled: true, Device: "/dev/fake", ChannelIdx: 2, NodeTTL: time.Hour,
		RouteMessages: true,
	})
	approved := []byte{0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF}
	go pumpDevice(host, [][]byte{newDirectMsgFrame(approved, "hello ops")})
	time.Sleep(300 * time.Millisecond)
	if n := capture.count(); n != 0 {
		t.Fatalf("no-gate message produced %d events", n)
	}
}

// TestMeshDirectMessageEventDisabled pins that routing stays off unless
// meshcore.route_messages is enabled.
func TestMeshDirectMessageEventDisabled(t *testing.T) {
	hub, host, capture := startEventTestHub(t, Config{
		Enabled: true, Device: "/dev/fake", ChannelIdx: 2, NodeTTL: time.Hour,
	})
	hub.SetSenderGate(func(key string) bool { return true })
	approved := []byte{0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF}
	go pumpDevice(host, [][]byte{newDirectMsgFrame(approved, "hello ops")})
	time.Sleep(300 * time.Millisecond)
	if n := capture.count(); n != 0 {
		t.Fatalf("disabled routing produced %d events", n)
	}
}

// TestHubSeedNode pins the restart restore path: seeding merges a
// retained document into the registry without publishing, and documents
// older than NodeTTL are tombstoned instead of restored.
func TestHubSeedNode(t *testing.T) {
	hub, err := NewHub(Config{Enabled: true, Device: "/dev/fake", ChannelIdx: 2, NodeTTL: time.Hour}, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	key := "43188f3a7e2d4fcdb5d5d29a0b25c645466d621a5f1750eb5f654fb8fed25c25"
	seen := time.Now().Add(-10 * time.Minute)

	// No sink installed: seeding must not panic or publish anything.
	hub.SeedNode(key, "PL-KR-MAKI-RPT", 2, 50.012734, 19.886038, 1, seen)

	snap := hub.Snapshot()
	if len(snap.Nodes) != 1 {
		t.Fatalf("seeded nodes = %d, want 1", len(snap.Nodes))
	}
	n := snap.Nodes[0]
	if n.PubKey != key || n.Name != "PL-KR-MAKI-RPT" || n.Type != 2 || n.Lat == 0 || n.Hops != 1 {
		t.Fatalf("seeded node = %+v", n)
	}

	// A stale document (older than NodeTTL) is tombstoned, not restored.
	capture := &stationCapture{}
	hub.SetStationSink(capture.publish)
	stale := "9f986ab53ada7f53b7fb5ae09a813582fcb9895c26c2bf8d5fe163ba80a88819"
	hub.SeedNode(stale, "OLD-NODE", 2, 50, 20, 0, time.Now().Add(-2*time.Hour))

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && capture.count() == 0 {
		time.Sleep(20 * time.Millisecond)
	}
	capture.mu.Lock()
	got := append([]stationPub(nil), capture.got...)
	capture.mu.Unlock()
	if len(got) != 1 || got[0].topic != "meshcore/stations/9f986ab53ada" || !got[0].retained || len(got[0].payload) != 0 {
		t.Fatalf("tombstone publish = %+v", got)
	}
}

// TestHubStationPublish pins the MQTT station feed: one retained document
// per node, throttled, with the node's details.
func TestHubStationPublish(t *testing.T) {
	dev, host := net.Pipe()
	defer dev.Close()
	defer host.Close()

	origDial := Dial
	Dial = func(Config) (conn, error) { return dev, nil }
	defer func() { Dial = origDial }()

	hub, err := NewHub(Config{Enabled: true, Device: "/dev/fake", ChannelIdx: 2, NodeTTL: time.Hour}, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	capture := &stationCapture{}
	hub.SetStationSink(capture.publish)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hub.Run(ctx)

	key := bytes.Repeat([]byte{0xAB}, 32)
	go func() {
		buf := make([]byte, 256)
		gotBytes := 0
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) && gotBytes == 0 {
			host.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
			n, err := host.Read(buf)
			if n > 0 {
				gotBytes = n
			}
			if err != nil && !isTimeout(err) {
				return
			}
		}
		if gotBytes == 0 {
			return
		}
		// Startup drain: answer NO_MORE before the adverts.
		sync, err := readHostFrame(host)
		if err != nil || len(sync) != 1 || sync[0] != cmdSyncNextMessage {
			t.Logf("DEVICE: unexpected drain frame %x (%v)", sync, err)
			return
		}
		host.Write(encodeDeviceFrame([]byte{respNoMoreMessages}))
		host.Write(encodeDeviceFrame(newAdvertFrame(key, "RKSR-TN-R3", 0x02)))
		// Second advert of the same node within the throttle window.
		host.Write(encodeDeviceFrame(newAdvertFrame(key, "RKSR-TN-R3", 0x02)))
	}()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && capture.count() == 0 {
		time.Sleep(20 * time.Millisecond)
	}
	capture.mu.Lock()
	got := append([]stationPub(nil), capture.got...)
	capture.mu.Unlock()
	if len(got) == 0 {
		t.Fatal("no station publish")
	}
	p := got[0]
	if p.topic != "meshcore/stations/abababababab" || !p.retained || !bytes.Contains(p.payload, []byte("RKSR-TN-R3")) {
		t.Fatalf("publish = %+v payload=%s", p, p.payload)
	}
	time.Sleep(100 * time.Millisecond)
	if n := capture.count(); n != 1 {
		t.Fatalf("publish count = %d, want 1 (throttled)", n)
	}
}

// TestHubStationTombstone pins the expiry path: a pruned node publishes an
// empty retained payload (the topic delete).
func TestHubStationTombstone(t *testing.T) {
	dev, host := net.Pipe()
	defer dev.Close()
	defer host.Close()

	origDial := Dial
	Dial = func(Config) (conn, error) { return dev, nil }
	defer func() { Dial = origDial }()

	hub, err := NewHub(Config{Enabled: true, Device: "/dev/fake", ChannelIdx: 2, NodeTTL: 100 * time.Millisecond}, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	capture := &stationCapture{}
	hub.SetStationSink(capture.publish)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hub.Run(ctx)

	keyA := bytes.Repeat([]byte{0xAB}, 32)
	keyB := bytes.Repeat([]byte{0xCD}, 32)
	go func() {
		buf := make([]byte, 256)
		gotBytes := 0
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) && gotBytes == 0 {
			host.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
			n, err := host.Read(buf)
			if n > 0 {
				gotBytes = n
			}
			if err != nil && !isTimeout(err) {
				return
			}
		}
		if gotBytes == 0 {
			return
		}
		// Startup drain: answer NO_MORE before the adverts.
		sync, err := readHostFrame(host)
		if err != nil || len(sync) != 1 || sync[0] != cmdSyncNextMessage {
			t.Logf("DEVICE: unexpected drain frame %x (%v)", sync, err)
			return
		}
		host.Write(encodeDeviceFrame([]byte{respNoMoreMessages}))
		host.Write(encodeDeviceFrame(newAdvertFrame(keyA, "NODE-A", 0x02)))
		time.Sleep(200 * time.Millisecond) // A expires
		host.Write(encodeDeviceFrame(newAdvertFrame(keyB, "NODE-B", 0x02)))
	}()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && capture.count() < 3 {
		time.Sleep(20 * time.Millisecond)
	}
	capture.mu.Lock()
	got := append([]stationPub(nil), capture.got...)
	capture.mu.Unlock()
	var tombstone *stationPub
	for i := range got {
		if got[i].topic == "meshcore/stations/abababababab" && got[i].retained && got[i].payload == nil {
			tombstone = &got[i]
		}
	}
	if tombstone == nil {
		t.Fatalf("no tombstone publish, got %+v", got)
	}
}

// TestHubDrainQueuedMessages pins the startup drain: messages the device
// queued while the app was away are pulled immediately on (re)connect
// instead of waiting for the next tickle.
func TestHubDrainQueuedMessages(t *testing.T) {
	dev, host := net.Pipe()
	defer dev.Close()
	defer host.Close()

	origDial := Dial
	Dial = func(Config) (conn, error) { return dev, nil }
	defer func() { Dial = origDial }()

	hub, err := NewHub(Config{Enabled: true, Device: "/dev/fake", ChannelIdx: 2, NodeTTL: time.Hour}, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	rec := &fakeRecorder{}
	hub.SetRecorder(rec)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hub.Run(ctx)

	go func() {
		buf := make([]byte, 256)
		gotBytes := 0
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) && gotBytes == 0 {
			host.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
			n, err := host.Read(buf)
			if n > 0 {
				gotBytes = n
			}
			if err != nil && !isTimeout(err) {
				return
			}
		}
		if gotBytes == 0 {
			return
		}
		// First SYNC_NEXT from the drain: answer with one queued direct
		// message.
		req, err := readHostFrame(host)
		if err != nil || len(req) != 1 || req[0] != cmdSyncNextMessage {
			t.Logf("DEVICE: unexpected drain frame %x (%v)", req, err)
			return
		}
		prefix := []byte{0x61, 0xc8, 0x49, 0x15, 0x2b, 0x91}
		host.Write(encodeDeviceFrame(newDirectMsgFrame(prefix, "ret")))
		// Next SYNC_NEXT: nothing left.
		req, err = readHostFrame(host)
		if err != nil || len(req) != 1 || req[0] != cmdSyncNextMessage {
			t.Logf("DEVICE: unexpected second drain frame %x (%v)", req, err)
			return
		}
		host.Write(encodeDeviceFrame([]byte{respNoMoreMessages}))
	}()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		got := rec.messages()
		if len(got) == 1 && got[0].Direction == "rx" && got[0].Sender == "61c849152b91" && got[0].Text == "ret" {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("drained messages = %+v, want the queued ret", rec.messages())
}

// TestHubContactProtect pins the directory protection: when the device
// overwrites a contact whose key the protector approves, the hub re-adds
// it immediately.
func TestHubContactProtect(t *testing.T) {
	dev, host := net.Pipe()
	defer dev.Close()
	defer host.Close()

	origDial := Dial
	Dial = func(Config) (conn, error) { return dev, nil }
	defer func() { Dial = origDial }()

	hub, err := NewHub(Config{Enabled: true, Device: "/dev/fake", ChannelIdx: 2, NodeTTL: time.Hour}, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	key := []byte{0x61, 0xc8, 0x49, 0x15, 0x2b, 0x91, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	hub.SetContactProtector(func(k string) bool {
		return k == pubKeyHex(key)
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hub.Run(ctx)

	done := make(chan struct{})
	go func() {
		defer close(done)
		buf := make([]byte, 256)
		gotBytes := 0
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) && gotBytes == 0 {
			host.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
			n, err := host.Read(buf)
			if n > 0 {
				gotBytes = n
			}
			if err != nil && !isTimeout(err) {
				return
			}
		}
		if gotBytes == 0 {
			return
		}
		// Answer the startup drain with NO_MORE so the pump can start.
		req, err := readHostFrame(host)
		if err != nil || len(req) != 1 || req[0] != cmdSyncNextMessage {
			t.Logf("DEVICE: unexpected drain frame %x (%v)", req, err)
			return
		}
		host.Write(encodeDeviceFrame([]byte{respNoMoreMessages}))

		// Push the overwrite event for the protected key.
		host.Write(encodeDeviceFrame(append([]byte{pushContactDeleted}, key...)))

		// The hub must answer with ADD_UPDATE_CONTACT for that key.
		host.SetReadDeadline(time.Now().Add(2 * time.Second))
		req, err = readHostFrame(host)
		if err != nil {
			t.Logf("DEVICE: re-add read: %v", err)
			return
		}
		if len(req) < 33 || req[0] != cmdAddUpdateContact || !bytes.Equal(req[1:33], key) {
			t.Logf("DEVICE: re-add frame %x, want ADD_UPDATE_CONTACT with the key", req)
			return
		}
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("device side did not finish")
	}
}

// TestHubChannelLabels pins the friendly channel-name resolution: the
// configured map first, then the pinned TX slot name, then "ch<N>".
func TestHubChannelLabels(t *testing.T) {
	hub, err := NewHub(Config{
		Enabled:     true,
		Device:      "/dev/fake",
		ChannelIdx:  2,
		ChannelName: "#sp9moa",
		ChannelNames: map[int]string{
			0: "Public",
			5: "Klub",
		},
	}, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		idx  int
		want string
	}{
		{0, "Public"},      // mapped
		{2, "#sp9moa"},     // pinned TX slot fallback
		{5, "Klub"},        // mapped
		{7, "ch7"},         // unmapped fallback
		{2 + 100, "ch102"}, // numeric fallback
	}
	for _, c := range cases {
		if got := hub.ChannelLabel(c.idx); got != c.want {
			t.Errorf("ChannelLabel(%d) = %q, want %q", c.idx, got, c.want)
		}
	}
}

// TestDirectMessageHopSentinel pins the 0xFF path normalization: the
// device sends 0xFF as "no path info" on direct frames and the hub must
// record 0 hops, never 255.
func TestDirectMessageHopSentinel(t *testing.T) {
	hub, host, _ := startEventTestHub(t, Config{
		Enabled: true, Device: "/dev/fake", ChannelIdx: 2, NodeTTL: time.Hour,
	})
	rec := &fakeRecorder{}
	hub.SetRecorder(rec)

	key := append([]byte{0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF}, make([]byte, 26)...)
	frame := []byte{respContactMsgV3, 0x0C, 0x00, 0x00} // snr, reserved2
	frame = append(frame, key[:6]...)
	frame = append(frame, 0xFF, 0x01) // pathLen sentinel, txtType
	frame = binary.LittleEndian.AppendUint32(frame, 1234567890)
	frame = append(frame, []byte("tak")...)
	go pumpDevice(host, [][]byte{frame})

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && len(rec.messages()) == 0 {
		time.Sleep(20 * time.Millisecond)
	}
	got := rec.messages()
	if len(got) != 1 || got[0].Hops != 0 {
		t.Fatalf("recorded = %+v, want hops normalized to 0", got)
	}
}

// TestHubQueryAckCannotConfirmSend pins the ack-isolation rework: the OK
// reply to a contact lookup must be consumed by the query's own waiter,
// never by a concurrent send — otherwise a rejected send would still be
// recorded as TX.
func TestHubQueryAckCannotConfirmSend(t *testing.T) {
	hub, host, _ := startEventTestHub(t, Config{
		Enabled: true, Device: "/dev/fake", ChannelIdx: 2, NodeTTL: time.Hour,
	})
	rec := &fakeRecorder{}
	hub.SetRecorder(rec)

	// Stage one: handshake → drain NO_MORE, so the pump is running
	// before the test commands hit the wire. No self info is sent — the
	// hub proceeds without it (and the test finishes before the 10s
	// handshake retry). Ordering matters: the device must answer the
	// drain before writing anything itself (net.Pipe writes block).
	devReady := make(chan struct{})
	go func() {
		defer close(devReady)
		buf := make([]byte, 256)
		host.SetReadDeadline(time.Now().Add(3 * time.Second))
		if _, err := host.Read(buf); err != nil {
			t.Logf("DEVICE: handshake read: %v", err)
			return
		}
		sync, err := readHostFrame(host)
		if err != nil || len(sync) != 1 || sync[0] != cmdSyncNextMessage {
			t.Logf("DEVICE: drain frame = %x (%v)", sync, err)
			return
		}
		host.Write(encodeDeviceFrame([]byte{respNoMoreMessages}))
	}()

	select {
	case <-devReady:
	case <-time.After(3 * time.Second):
		hub.mu.Lock()
		lastErr := hub.lastErr
		hub.mu.Unlock()
		t.Fatalf("device did not complete the startup choreography, hub err=%v", lastErr)
	}

	key := append([]byte{0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF}, make([]byte, 26)...)
	hub.touchNode(key, "", 1, 0, 0, 0, time.Now())

	// Stage two: the query gets OK, the send gets ERR — the send must
	// fail and nothing may be recorded as TX.
	go func() {
		q, err := readHostFrame(host)
		if err != nil || len(q) < 1 || q[0] != cmdGetContactByKey {
			t.Logf("DEVICE: query frame = %x (%v)", q, err)
			return
		}
		host.Write(encodeDeviceFrame([]byte{respOK}))
		s, err := readHostFrame(host)
		if err != nil || len(s) < 1 || s[0] != cmdSendChannelTxtMsg {
			t.Logf("DEVICE: send frame = %x (%v)", s, err)
			return
		}
		host.Write(encodeDeviceFrame([]byte{respErr, 2}))
	}()

	hub.maybeQueryContact(key)
	if err := hub.SendChannelMessage("hello", "admin"); err == nil {
		t.Fatal("send must fail when the device rejects it")
	}
	time.Sleep(150 * time.Millisecond)
	if got := rec.messages(); len(got) != 0 {
		t.Fatalf("rejected send recorded TX: %+v", got)
	}
}
