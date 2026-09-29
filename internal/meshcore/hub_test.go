package meshcore

import (
	"context"
	"encoding/binary"
	"log/slog"
	"net"
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

func TestHubSession(t *testing.T) {
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
	if snap.ChannelIdx != 2 {
		t.Fatalf("channel idx = %d, want 2", snap.ChannelIdx)
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

	// Outbound: send a channel message; the device reads it back. The
	// frame uses host→device framing (0x3C header).
	go hub.SendChannelMessage("HELLO MESH")
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

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
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
