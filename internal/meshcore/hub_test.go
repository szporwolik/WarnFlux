package meshcore

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
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
	return readHostFrameWithin(host, 2*time.Second)
}

// readHostFrameWithin is readHostFrame with an explicit read deadline:
// reads that wait for a command whose budget is spent elsewhere (e.g. a
// send arriving only after a query timeout) need a longer window.
func readHostFrameWithin(host net.Conn, d time.Duration) ([]byte, error) {
	buf := make([]byte, 256)
	host.SetReadDeadline(time.Now().Add(d))
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
	go func() { sendErr <- hub.SendChannelMessage(context.Background(), "HELLO MESH", "admin") }()
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
	if err := hub.SendChannelMessage(context.Background(), "must not go out", "admin"); err == nil {
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

	err = hub.SendContactMessage(context.Background(), "abcd1234abcd", "hello", "admin")
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

	if err := hub.SendContactMessage(context.Background(), hex.EncodeToString(fullKey), "hello", "admin"); err != nil {
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
		// The mismatched OK does not satisfy the query: the send is
		// written only after the query's own budget (2s) expires, so
		// this read needs a longer window than the default.
		s, err := readHostFrameWithin(host, 8*time.Second)
		if err != nil || len(s) < 1 || s[0] != cmdSendChannelTxtMsg {
			t.Logf("DEVICE: send frame = %x (%v)", s, err)
			return
		}
		host.Write(encodeDeviceFrame([]byte{respErr, 2}))
	}()

	hub.maybeQueryContact(key)
	if err := hub.SendChannelMessage(context.Background(), "hello", "admin"); err == nil {
		t.Fatal("send must fail when the device rejects it")
	}
	time.Sleep(150 * time.Millisecond)
	if got := rec.messages(); len(got) != 0 {
		t.Fatalf("rejected send recorded TX: %+v", got)
	}
}

// TestSendHonorsCallerCancellationWhileUnready pins the reported P1 half
// one: a send whose caller context expires while the session is not yet
// ready returns promptly with the context error — the old code ignored
// the cancellation and blocked in waitReady for up to 12 s, which the
// action wrapper (10 s default timeout) misread as a hung plugin and
// permanently disabled the instance.
func TestSendHonorsCallerCancellationWhileUnready(t *testing.T) {
	hub, _, _ := startEventTestHub(t, Config{
		Enabled: true, Device: "/dev/fake", ChannelIdx: 2, NodeTTL: time.Hour,
	})
	// No device choreography: the session never becomes ready.
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := hub.SendChannelMessage(ctx, "hello", "admin")
	if err == nil {
		t.Fatal("send succeeded without a ready session")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("send error = %v, want the caller's context deadline", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("send took %v, want prompt cancellation (the old code waited up to 12 s)", elapsed)
	}
}

// TestSendHonorsCallerCancellationWhileAwaitingAck pins the second half:
// the ACK wait in the command worker also aborts on the caller's
// context, so a silent device yields a transient error at the caller's
// deadline — not at the worker's 5 s command timeout.
func TestSendHonorsCallerCancellationWhileAwaitingAck(t *testing.T) {
	hub, host, _ := startEventTestHub(t, Config{
		Enabled: true, Device: "/dev/fake", ChannelIdx: 2, NodeTTL: time.Hour,
	})

	// Startup choreography: handshake → drain NO_MORE, so the pump is
	// running and the session is ready before the send.
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
		t.Fatal("device did not complete the startup choreography")
	}

	// The device reads the send frame and deliberately stays silent.
	silent := make(chan struct{})
	go func() {
		defer close(silent)
		s, err := readHostFrameWithin(host, 8*time.Second)
		if err == nil && (len(s) < 1 || s[0] != cmdSendChannelTxtMsg) {
			t.Logf("DEVICE: unexpected frame %x", s)
		}
		// No ACK — the caller's context must end the wait.
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := hub.SendChannelMessage(ctx, "hello", "admin")
	if err == nil {
		t.Fatal("send succeeded without an ACK")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("send error = %v, want the caller's context deadline", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("send took %v, want prompt cancellation (the old code waited for the 5 s command timeout)", elapsed)
	}
	select {
	case <-silent:
	case <-time.After(3 * time.Second):
		t.Fatal("device never saw the send frame")
	}
}

// countingConn records every Write without ever delivering it — the
// deterministic "did this reach the transport" probe.
type countingConn struct{ writes atomic.Int32 }

func (c *countingConn) Read([]byte) (int, error) { return 0, errors.New("unexpected read") }
func (c *countingConn) Write(p []byte) (int, error) {
	c.writes.Add(1)
	return len(p), nil
}
func (c *countingConn) Close() error                     { return nil }
func (c *countingConn) SetReadDeadline(time.Time) error  { return nil }
func (c *countingConn) SetWriteDeadline(time.Time) error { return nil }

// shortWriter accepts at most ONE byte per Write call — the reported P2
// reproduction ("a transport accepting one byte is enough to reproduce
// the error"). It accumulates everything it received.
type shortWriter struct {
	mu  sync.Mutex
	got []byte
}

func (s *shortWriter) Read([]byte) (int, error) { return 0, errors.New("unexpected read") }
func (s *shortWriter) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(p) == 0 {
		return 0, nil
	}
	s.got = append(s.got, p[0])
	return 1, nil
}
func (s *shortWriter) Close() error                     { return nil }
func (s *shortWriter) SetReadDeadline(time.Time) error  { return nil }
func (s *shortWriter) SetWriteDeadline(time.Time) error { return nil }
func (s *shortWriter) received() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]byte(nil), s.got...)
}

// failingWriter rejects every write and records that it was closed (the
// session-recreation trigger).
type failingWriter struct{ closed atomic.Bool }

func (f *failingWriter) Read([]byte) (int, error) { return 0, errors.New("unexpected read") }
func (f *failingWriter) Write([]byte) (int, error) {
	return 0, errors.New("line down")
}
func (f *failingWriter) Close() error                     { f.closed.Store(true); return nil }
func (f *failingWriter) SetReadDeadline(time.Time) error  { return nil }
func (f *failingWriter) SetWriteDeadline(time.Time) error { return nil }

// TestCancelledCommandNeverReachesTransport pins the reported P1 half
// one: a command whose caller context is ALREADY cancelled must be
// rejected before any I/O — the old code started the write first, so a
// cancelled command still leaked its frame onto the wire (6 bytes).
func TestCancelledCommandNeverReachesTransport(t *testing.T) {
	hub, err := NewHub(Config{
		Enabled: true, Device: "/dev/fake", ChannelIdx: 2, NodeTTL: time.Hour,
	}, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	c := &countingConn{}
	reqCtx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled BEFORE the worker starts the command
	reply := make(chan error, 1)
	hub.runCommand(context.Background(), c, cmdReq{
		payload: buildSendChannelTxtMsg(2, "hello"),
		expect:  expectAck,
		reply:   reply,
		ctx:     reqCtx,
	})
	if err := <-reply; !errors.Is(err, context.Canceled) {
		t.Fatalf("reply = %v, want context.Canceled", err)
	}
	// Give a stray write goroutine (the old behavior) time to surface.
	time.Sleep(150 * time.Millisecond)
	if got := c.writes.Load(); got != 0 {
		t.Fatalf("already-cancelled command wrote %d frames to the transport, want 0", got)
	}
}

// TestShortWriteIsCompleted pins the reported P2: a transport that
// accepts only ONE byte per Write must never be mistaken for success —
// the hub writes the remainder until the whole frame is on the wire.
func TestShortWriteIsCompleted(t *testing.T) {
	hub, err := NewHub(Config{
		Enabled: true, Device: "/dev/fake", ChannelIdx: 2, NodeTTL: time.Hour,
	}, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	sw := &shortWriter{}
	payload := buildSendChannelTxtMsg(2, "hello")
	frame := encodeFrame(payload)
	if err := hub.writeFrameOn(context.Background(), sw, payload); err != nil {
		t.Fatalf("writeFrameOn = %v, want the whole frame written", err)
	}
	if got := sw.received(); !bytes.Equal(got, frame) {
		t.Fatalf("transport received %x (%d bytes), want the complete frame %x", got, len(got), frame)
	}
}

// TestWriteAllHonorsCancellation pins the cancellation handling of the
// remainder loop: a cancelled context aborts before any chunk is
// written.
func TestWriteAllHonorsCancellation(t *testing.T) {
	sw := &shortWriter{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := writeAll(ctx, sw, []byte{1, 2, 3}, frameWriteTimeout)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("writeAll = %v, want context.Canceled", err)
	}
	if got := len(sw.received()); got != 0 {
		t.Fatalf("cancelled write reached the transport: %x", sw.received())
	}
}

// TestWriteDeadlineAborts pins the deadline handling: a stalled line
// buffer aborts the frame instead of blocking forever.
func TestWriteDeadlineAborts(t *testing.T) {
	// net.Pipe with no reader: Write blocks until the deadline that
	// writeAll sets through SetWriteDeadline.
	_, host := net.Pipe()
	defer host.Close()
	start := time.Now()
	err := writeAll(context.Background(), host, []byte{1, 2, 3, 4}, 100*time.Millisecond)
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("writeAll = %v, want the transport write deadline", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("deadline abort took %v, want the bounded write", elapsed)
	}
}

// TestCommandWriteErrorRecreatesSession pins the partial-frame rule at
// the worker level: a failed frame write must never leave the stream
// usable — the connection is closed, so the next command runs on a
// freshly dialed session instead of following a partial frame.
func TestCommandWriteErrorRecreatesSession(t *testing.T) {
	hub, err := NewHub(Config{
		Enabled: true, Device: "/dev/fake", ChannelIdx: 2, NodeTTL: time.Hour,
	}, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	c := &failingWriter{}
	reply := make(chan error, 1)
	hub.runCommand(context.Background(), c, cmdReq{
		payload: buildSendChannelTxtMsg(2, "hello"),
		expect:  expectAck,
		reply:   reply,
	})
	if err := <-reply; err == nil {
		t.Fatal("a failed frame write reported success")
	}
	if !c.closed.Load() {
		t.Fatal("the connection was not closed after a failed frame write; the next command could follow a partial frame")
	}
}

// TestCancellationMidWriteRecreatesSession pins the second half: a write
// that is in flight when the caller cancels is never abandoned
// mid-frame on the live stream — the worker waits a bounded grace and
// then recreates the session, so the next command can never interleave
// with the abandoned frame.
func TestCancellationMidWriteRecreatesSession(t *testing.T) {
	hub, host, _ := startEventTestHub(t, Config{
		Enabled: true, Device: "/dev/fake", ChannelIdx: 2, NodeTTL: time.Hour,
	})

	// Startup choreography; afterwards the device STOPS reading, so the
	// next write blocks on the pipe.
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
		t.Fatal("device did not complete the startup choreography")
	}

	// The device never reads the send frame: the write blocks until the
	// session recreation closes the connection.
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := hub.SendChannelMessage(ctx, "hello", "admin")
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("send error = %v, want the caller's context deadline", err)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("send took %v, want prompt cancellation (grace + close)", elapsed)
	}

	// The session must end: the abandoned frame is gone with it and the
	// next command runs on a freshly dialed connection.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && hub.Connected() {
		time.Sleep(20 * time.Millisecond)
	}
	if hub.Connected() {
		t.Fatal("session survived a write abandoned mid-frame; a next command would interleave")
	}
}

// TestCancellationMidWriteSettlesCleanly pins the grace path: when the
// abandoned write completes within the grace (a healthy transport), the
// session survives and the next command works on the same stream.
func TestCancellationMidWriteSettlesCleanly(t *testing.T) {
	hub, host, _ := startEventTestHub(t, Config{
		Enabled: true, Device: "/dev/fake", ChannelIdx: 2, NodeTTL: time.Hour,
	})
	rec := &fakeRecorder{}
	hub.SetRecorder(rec)

	// Startup choreography.
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
		t.Fatal("device did not complete the startup choreography")
	}

	// The device reads the abandoned send frame 200 ms late — within the
	// 500 ms grace — then answers and handles the follow-up send.
	go func() {
		time.Sleep(200 * time.Millisecond)
		f1, err := readHostFrameWithin(host, 5*time.Second)
		if err != nil || len(f1) < 1 || f1[0] != cmdSendChannelTxtMsg {
			t.Logf("DEVICE: first send frame = %x (%v)", f1, err)
			return
		}
		host.Write(encodeDeviceFrame([]byte{respOK}))
		f2, err := readHostFrameWithin(host, 5*time.Second)
		if err != nil || len(f2) < 1 || f2[0] != cmdSendChannelTxtMsg {
			t.Logf("DEVICE: second send frame = %x (%v)", f2, err)
			return
		}
		host.Write(encodeDeviceFrame([]byte{respOK}))
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	err := hub.SendChannelMessage(ctx, "hello", "admin")
	cancel()
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("cancelled send error = %v, want the caller's deadline", err)
	}

	// The write settled cleanly: the session survives and the follow-up
	// send is transmitted and recorded.
	if !hub.Connected() {
		t.Fatal("session was recreated even though the write settled within the grace")
	}
	if err := hub.SendChannelMessage(context.Background(), "again", "admin"); err != nil {
		t.Fatalf("follow-up send on the surviving session failed: %v", err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && len(rec.messages()) == 0 {
		time.Sleep(20 * time.Millisecond)
	}
	if got := rec.messages(); len(got) != 1 {
		t.Fatalf("recorded TX = %d, want exactly the follow-up send", len(got))
	}
}

// contactRecord builds a respContact frame for the given key and name.
func contactRecord(key []byte, name string) []byte {
	rec := []byte{respContact}
	rec = append(rec, key...)
	rec = append(rec, 0x02, 0x00, 0x03) // type repeater, flags 0, 3 hops
	rec = append(rec, make([]byte, 64)...)
	name32 := make([]byte, 32)
	copy(name32, name)
	rec = append(rec, name32...)
	rec = append(rec, 0, 0, 0, 0) // lastAdvert
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], 50_020_000)
	rec = append(rec, b[:]...)
	binary.LittleEndian.PutUint32(b[:], 20_000_000)
	rec = append(rec, b[:]...)
	rec = append(rec, 0, 0, 0, 0) // lastMod
	return rec
}

// TestContactQueryDoesNotStealSendAck pins the P1 scenario: a contact
// query issued from the frame pump (bare-key advert) answers with a
// CONTACT RECORD, not an ack. The later respOK for the send must satisfy
// the SEND, never the query's stale waiter — the send succeeds and is
// recorded as TX.
func TestContactQueryDoesNotStealSendAck(t *testing.T) {
	hub, host, _ := startEventTestHub(t, Config{
		Enabled: true, Device: "/dev/fake", ChannelIdx: 2, NodeTTL: time.Hour,
	})
	rec := &fakeRecorder{}
	hub.SetRecorder(rec)

	// Stage one: startup choreography only (handshake → drain NO_MORE),
	// like TestHubQueryAckCannotConfirmSend. The pipe is synchronous, so
	// the device must answer the drain before writing anything itself.
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
		t.Fatal("device did not complete the startup choreography")
	}

	// Stage two (separate goroutine — never gated on the send): advert →
	// query → contact record → send → OK.
	key := bytes.Repeat([]byte{0xAB}, 32)
	devDone := make(chan struct{})
	queryRead := make(chan struct{})
	go func() {
		defer close(devDone)
		// A bare-key advert: the hub must ask for the full record.
		host.Write(encodeDeviceFrame(append([]byte{pushAdvert}, key...)))

		// The query arrives first and is answered with the contact
		// RECORD (never an ack frame). Reading the query proves it is
		// already in flight: a send submitted now queues BEHIND it.
		q, err := readHostFrame(host)
		if err != nil || len(q) != 33 || q[0] != cmdGetContactByKey || !bytes.Equal(q[1:], key) {
			t.Logf("DEVICE: query frame = %x (%v)", q, err)
			return
		}
		close(queryRead)
		host.Write(encodeDeviceFrame(contactRecord(key, "RKSR-TN-R3")))

		// Only then the send: its ack must land on the send's own
		// waiter.
		s, err := readHostFrame(host)
		if err != nil || len(s) < 1 || s[0] != cmdSendChannelTxtMsg {
			t.Logf("DEVICE: send frame = %x (%v)", s, err)
			return
		}
		host.Write(encodeDeviceFrame([]byte{respOK}))
	}()

	select {
	case <-queryRead:
	case <-time.After(5 * time.Second):
		t.Fatal("device never received the contact query")
	}

	if err := hub.SendChannelMessage(context.Background(), "hello", "admin"); err != nil {
		t.Fatalf("SendChannelMessage = %v, want success (query must not steal the ack)", err)
	}
	select {
	case <-devDone:
	case <-time.After(8 * time.Second):
		t.Fatal("device choreography did not complete")
	}
	if got := rec.messages(); len(got) != 1 || got[0].Direction != "tx" || got[0].Text != "hello" {
		t.Fatalf("recorded = %+v, want exactly one TX", got)
	}
	waitForMesh(t, func() bool {
		snap := hub.Snapshot()
		return len(snap.Nodes) == 1 && snap.Nodes[0].Name == "RKSR-TN-R3"
	}, "contact record applied")
}

// TestQueryReplyTypeIsolation pins the explicit-expected-reply design: an
// ack frame that arrives while a contact query is in flight does NOT
// satisfy the query (wrong type). The query times out on its own budget
// and the send that follows gets its own ack — delayed replies never
// cross commands.
func TestQueryReplyTypeIsolation(t *testing.T) {
	hub, host, _ := startEventTestHub(t, Config{
		Enabled: true, Device: "/dev/fake", ChannelIdx: 2, NodeTTL: time.Hour,
	})
	rec := &fakeRecorder{}
	hub.SetRecorder(rec)

	// Stage one: startup choreography only (handshake → drain NO_MORE).
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
		t.Fatal("device did not complete the startup choreography")
	}

	// Stage two: advert → query → MISMATCHED reply → send → OK.
	key := bytes.Repeat([]byte{0xCD}, 32)
	devDone := make(chan struct{})
	queryRead := make(chan struct{})
	go func() {
		defer close(devDone)
		host.Write(encodeDeviceFrame(append([]byte{pushAdvert}, key...)))
		q, err := readHostFrame(host)
		if err != nil || len(q) != 33 || q[0] != cmdGetContactByKey {
			t.Logf("DEVICE: query frame = %x (%v)", q, err)
			return
		}
		close(queryRead)
		// A MISMATCHED reply: the query expects a contact record, but
		// the device sends a bare OK. It must not satisfy the query.
		host.Write(encodeDeviceFrame([]byte{respOK}))

		// The send follows only after the query's own budget (2s)
		// expires, so this read needs a longer window than the default.
		s, err := readHostFrameWithin(host, 8*time.Second)
		if err != nil || len(s) < 1 || s[0] != cmdSendChannelTxtMsg {
			t.Logf("DEVICE: send frame = %x (%v)", s, err)
			return
		}
		host.Write(encodeDeviceFrame([]byte{respOK}))
	}()

	select {
	case <-queryRead:
	case <-time.After(5 * time.Second):
		t.Fatal("device never received the contact query")
	}

	start := time.Now()
	if err := hub.SendChannelMessage(context.Background(), "hello", "admin"); err != nil {
		t.Fatalf("SendChannelMessage = %v, want success despite the mismatched query reply", err)
	}
	if elapsed := time.Since(start); elapsed > 4*time.Second {
		t.Errorf("send took %v, want ≤ query budget + margin", elapsed)
	}
	select {
	case <-devDone:
	case <-time.After(8 * time.Second):
		t.Fatal("device choreography did not complete")
	}
	if got := rec.messages(); len(got) != 1 || got[0].Direction != "tx" {
		t.Fatalf("recorded = %+v, want exactly one TX", got)
	}
}

// TestSendFailsFastAfterSessionEnd pins the teardown cleanup: once the
// device link dies, a send must fail fast (not hang on a dead session),
// and the command worker must not outlive its session.
func TestSendFailsFastAfterSessionEnd(t *testing.T) {
	hub, host, _ := startEventTestHub(t, Config{
		Enabled: true, Device: "/dev/fake", ChannelIdx: 2, NodeTTL: time.Hour,
	})

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
		t.Fatal("device did not complete the startup choreography")
	}

	// Kill the link; wait until the hub notices (session ended).
	host.Close()
	waitForMesh(t, func() bool {
		hub.mu.Lock()
		conn := hub.client
		hub.mu.Unlock()
		return conn == nil
	}, "session ended after link close")

	start := time.Now()
	err := hub.SendChannelMessage(context.Background(), "hello", "admin")
	if err == nil {
		t.Fatal("send must fail after the session ended")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("failed send took %v, want a fast error", elapsed)
	}
}

// waitForMesh polls a condition with a short deadline (test-local).
func waitForMesh(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}
