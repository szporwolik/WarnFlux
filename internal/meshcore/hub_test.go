package meshcore

import (
	"bytes"
	"context"
	"encoding/binary"
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

func (f *fakeRecorder) RecordMeshMessage(_ context.Context, direction, sender, channel, text string, at time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.got = append(f.got, Message{Direction: direction, Sender: sender, Channel: channel, Text: text, At: at})
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
		host.Write(encodeDeviceFrame([]byte{respOK}))

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
		msg := []byte{respChannelMsgV3, 0x0C, 0x00, 0x00, 0x00, 0xFF, 0x00}
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
	if len(got) != 1 || got[0].Text != "Test SOSNA" || got[0].Direction != "rx" {
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
	go func() { sendErr <- hub.SendChannelMessage("HELLO MESH") }()
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
	if len(got) != 2 || got[1].Text != "HELLO MESH" || got[1].Direction != "tx" {
		t.Fatalf("recorded after tx = %+v", got)
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
	if err := hub.SendChannelMessage("must not go out"); err == nil {
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
		req, err := readHostFrame(host)
		if err != nil || len(req) < 7 || req[0] != cmdSendTxtMsg {
			t.Logf("DEVICE: unexpected direct send %x (%v)", req, err)
			return
		}
		// Unknown contact on the device: PACKET_ERR, code NOT_FOUND.
		host.Write(encodeDeviceFrame([]byte{respErr, 2}))
	}()

	err = hub.SendContactMessage("abcd1234abcd", "hello")
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("SendContactMessage = %v, want device not-found error", err)
	}
	if got := rec.messages(); len(got) != 0 {
		t.Fatalf("recorded = %+v, want no tx for a rejected send", got)
	}
}
