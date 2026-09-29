package meshcore

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"
)

// encodeInbound frames a device→host frame (0x3E header).
func encodeInbound(payload []byte) []byte {
	out := []byte{frameIn, 0, 0}
	binary.LittleEndian.PutUint16(out[1:3], uint16(len(payload)))
	return append(out, payload...)
}

func TestFrameRoundTrip(t *testing.T) {
	payload := buildAppStart("warnflux")
	frame := encodeInbound(payload)
	if frame[0] != frameIn || int(binary.LittleEndian.Uint16(frame[1:3])) != len(payload) {
		t.Fatalf("bad frame header: %x", frame[:3])
	}

	dec := &decoder{}
	// Feed in odd chunks to exercise incremental reassembly.
	var got [][]byte
	for i := 0; i < len(frame); i += 7 {
		end := i + 7
		if end > len(frame) {
			end = len(frame)
		}
		got = append(got, dec.feed(frame[i:end])...)
	}
	if len(got) != 1 || !bytes.Equal(got[0], payload) {
		t.Fatalf("decoded = %d frames, want 1 with payload %x", len(got), payload)
	}
}

func TestDecoderResyncs(t *testing.T) {
	payload := []byte{0x05, 0x01}
	frame := encodeInbound(payload)
	// Prepend garbage and an embedded false 0x3E inside a longer frame.
	stream := append([]byte{0x00, 0xFF}, frame...)
	stream = append(stream, 0x3E, 0x01, 0x00, 0x05, 0x06) // valid tiny frame
	dec := &decoder{}
	got := dec.feed(stream)
	if len(got) != 2 || !bytes.Equal(got[0], payload) || !bytes.Equal(got[1], []byte{0x05}) {
		t.Fatalf("resync failed: %x", got)
	}
}

func TestParseSelfInfo(t *testing.T) {
	raw := []byte{0x05}
	raw = append(raw, 0x01, 0x14, 0x16)                  // type, tx, maxTx
	raw = append(raw, bytes.Repeat([]byte{0xAB}, 32)...) // pubkey
	lat := int32(50_000_000)
	lon := int32(20_000_000)
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], uint32(lat))
	raw = append(raw, b[:]...)
	binary.LittleEndian.PutUint32(b[:], uint32(lon))
	raw = append(raw, b[:]...)
	raw = append(raw, 0, 0, 0, 0) // reserved 3 + manual
	freq := uint32(869_618)       // kHz → 869.618 MHz
	bw := uint32(62_500)
	binary.LittleEndian.PutUint32(b[:], freq)
	raw = append(raw, b[:]...)
	binary.LittleEndian.PutUint32(b[:], bw)
	raw = append(raw, b[:]...)
	raw = append(raw, 8, 5)
	raw = append(raw, []byte("MOA SOSNA")...)

	s, err := parseSelfInfo(raw[1:])
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if s.Name != "MOA SOSNA" || s.Lat() != 50.0 || s.Lon() != 20.0 {
		t.Fatalf("self info = %+v", s)
	}
	if s.RadioFreqKHz != freq || s.RadioSF != 8 || s.RadioCR != 5 {
		t.Fatalf("radio = %+v", s)
	}
}

func TestParseNewAdvertAndChannelMsg(t *testing.T) {
	// NEW_ADVERT (0x8A): pubkey32, type, flags, pathLen, path64, name32, lastAdv, lat, lon, lastMod
	raw := []byte{0x8A}
	raw = append(raw, bytes.Repeat([]byte{0xCD}, 32)...)
	raw = append(raw, 0x02, 0x00, 0x00)
	raw = append(raw, make([]byte, 64)...)
	name := make([]byte, 32)
	copy(name, "RKSR-TN-R3")
	raw = append(raw, name...)
	raw = append(raw, 0, 0, 0, 0) // lastAdvert
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], 50_000_000)
	raw = append(raw, b[:]...)
	binary.LittleEndian.PutUint32(b[:], 20_000_000)
	raw = append(raw, b[:]...)
	raw = append(raw, 0, 0, 0, 0) // lastMod

	a, err := parseNewAdvert(raw[1:])
	if err != nil {
		t.Fatalf("advert: %v", err)
	}
	if a.AdvName != "RKSR-TN-R3" || a.Type != 2 || a.Lat() != 50.0 {
		t.Fatalf("advert = %+v", a)
	}

	// ChannelMsgRecvV3 (0x11): snr, 2 reserved, chIdx, pathLen, txtType, ts, text
	msg := []byte{0x11, 0x0C, 0x00, 0x00, 0x00, 0xFF, 0x00}
	msg = binary.LittleEndian.AppendUint32(msg, 1234567890)
	msg = append(msg, []byte("Test SOSNA")...)
	m, err := parseChannelMsg(msg[1:], true)
	if err != nil {
		t.Fatalf("channel msg: %v", err)
	}
	if m.Text != "Test SOSNA" || m.SNR == nil || *m.SNR != 3.0 {
		t.Fatalf("channel msg = %+v snr=%v", m, m.SNR)
	}
}

func TestBuildChannelSend(t *testing.T) {
	p := buildSendChannelTxtMsg(0, "HELLO")
	if p[0] != cmdSendChannelTxtMsg || p[1] != 0 || p[2] != 0 {
		t.Fatalf("send header = %x", p[:3])
	}
	if string(p[7:]) != "HELLO" {
		t.Fatalf("send text = %q", p[7:])
	}
}

func TestEncodeFrameHeader(t *testing.T) {
	frame := encodeFrame([]byte{0x01, 0x02, 0x03})
	if frame[0] != frameOut || binary.LittleEndian.Uint16(frame[1:3]) != 3 {
		t.Fatalf("bad outbound header: %x", frame[:3])
	}
	if !bytes.Equal(frame[3:], []byte{0x01, 0x02, 0x03}) {
		t.Fatalf("bad outbound payload: %x", frame[3:])
	}
}

func TestChannelFrames(t *testing.T) {
	if got := buildGetChannel(2); len(got) != 2 || got[0] != cmdGetChannel || got[1] != 2 {
		t.Fatalf("get channel = %x", got)
	}
	set := buildSetChannel(2, "#sp9moa", make([]byte, 16))
	if len(set) != 2+channelNameLen+16 || set[0] != cmdSetChannel || set[1] != 2 {
		t.Fatalf("set channel frame = %x", set)
	}
	if name := strings.TrimRight(string(set[2:2+channelNameLen]), "\x00"); name != "#sp9moa" {
		t.Fatalf("set channel name = %q", name)
	}
	if buildSetChannel(2, "#sp9moa", make([]byte, 32)) != nil {
		t.Fatal("32-byte secret must be rejected (device supports 128-bit only)")
	}

	info := []byte{0x12, 2}
	name32 := make([]byte, 32)
	copy(name32, "#stary")
	info = append(info, name32...)
	info = append(info, make([]byte, 16)...)
	idx, name, secret, err := parseChannelInfo(info[1:])
	if err != nil || idx != 2 || name != "#stary" || len(secret) != 16 {
		t.Fatalf("channel info = %d %q %x %v", idx, name, secret, err)
	}
	if _, _, _, err := parseChannelInfo([]byte{1, 2, 3}); err == nil {
		t.Fatal("short channel info accepted")
	}
}
