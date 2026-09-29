// Package meshcore implements the MeshCore Companion protocol client:
// the serial frame codec, packet parsing and the shared hub that the
// source plugin, the outbound action and the web admin page all use.
//
// Wire format (mirrors meshcore.js): every serial frame is
// [frameType 1B][length uint16 LE][payload]; 0x3C is host→device,
// 0x3E is device→host.
package meshcore

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"io"
	"time"
)

// Serial frame types.
const (
	frameIn  = 0x3E
	frameOut = 0x3C
)

// Command codes (host → device).
const (
	cmdAppStart          = 1
	cmdSendTxtMsg        = 2
	cmdSendChannelTxtMsg = 3
	cmdSendSelfAdvert    = 7
	cmdSyncNextMessage   = 10
	cmdGetBattery        = 20
	cmdDeviceQuery       = 22
	cmdGetChannel        = 31
	cmdSetChannel        = 32
	cmdGetStats          = 56
	cmdSendChannelData   = 62
)

// Response / push codes (device → host).
const (
	respOK              = 0
	respErr             = 1
	respSelfInfo        = 5
	respSent            = 6
	respContactMsg      = 7
	respChannelMsg      = 8
	respNoMoreMessages  = 10
	respBattery         = 12
	respDeviceInfo      = 13
	respContactMsgV3    = 16
	respChannelMsgV3    = 17
	respChannelInfo     = 18
	respStats           = 24
	respChannelDataRecv = 27

	pushAdvert        = 0x80
	pushSendConfirmed = 0x82
	pushMsgWaiting    = 0x83
	pushLogRxData     = 0x88
	pushNewAdvert     = 0x8A
)

// Advert types (SendSelfAdvert argument).
const (
	AdvertZeroHop = 0
	AdvertFlood   = 1
)

// maxSerialFrame bounds one accepted serial frame (devices send adverts
// and log data of a few hundred bytes at most).
const maxSerialFrame = 4096

// encodeFrame builds one outgoing serial frame.
func encodeFrame(payload []byte) []byte {
	out := make([]byte, 0, len(payload)+3)
	out = append(out, frameOut)
	out = binary.LittleEndian.AppendUint16(out, uint16(len(payload)))
	return append(out, payload...)
}

// decoder is the incremental stream decoder: it feeds raw bytes and
// returns every complete incoming frame payload.
type decoder struct {
	buf []byte
}

func (d *decoder) feed(chunk []byte) [][]byte {
	d.buf = append(d.buf, chunk...)
	var out [][]byte
	for {
		if len(d.buf) < 3 {
			return out
		}
		if d.buf[0] != frameIn {
			// Out-of-sync byte (e.g. device boot noise): drop and resync.
			d.buf = d.buf[1:]
			continue
		}
		n := int(binary.LittleEndian.Uint16(d.buf[1:3]))
		if n == 0 || n > maxSerialFrame {
			d.buf = d.buf[1:]
			continue
		}
		if len(d.buf) < 3+n {
			return out
		}
		out = append(out, append([]byte(nil), d.buf[3:3+n]...))
		d.buf = d.buf[3+n:]
	}
}

// ---- packet readers ----

type reader struct {
	b   []byte
	off int
}

func newReader(b []byte) *reader { return &reader{b: b} }

func (r *reader) remaining() int { return len(r.b) - r.off }

func (r *reader) byte() (byte, error) {
	if r.remaining() < 1 {
		return 0, io.ErrUnexpectedEOF
	}
	v := r.b[r.off]
	r.off++
	return v, nil
}

func (r *reader) bytes(n int) ([]byte, error) {
	if r.remaining() < n {
		return nil, io.ErrUnexpectedEOF
	}
	v := r.b[r.off : r.off+n]
	r.off += n
	return v, nil
}

func (r *reader) u16() (uint16, error) {
	b, err := r.bytes(2)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint16(b), nil
}

func (r *reader) u32() (uint32, error) {
	b, err := r.bytes(4)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(b), nil
}

func (r *reader) i32() (int32, error) {
	v, err := r.u32()
	return int32(v), err
}

func (r *reader) cstr(max int) (string, error) {
	b, err := r.bytes(max)
	if err != nil {
		return "", err
	}
	if i := bytes.IndexByte(b, 0); i >= 0 {
		b = b[:i]
	}
	return string(b), nil
}

func (r *reader) rest() ([]byte, error) {
	return r.bytes(r.remaining())
}

// ---- parsed types ----

// SelfInfo is the device's answer to APP_START.
type SelfInfo struct {
	AdvType      byte
	TxPower      byte
	MaxTxPower   byte
	PublicKey    []byte // 32 bytes
	AdvLatRaw    int32  // degrees * 1e6
	AdvLonRaw    int32
	RadioFreqKHz uint32 // carrier frequency in kHz (869618 → 869.618 MHz)
	RadioBwHz    uint32
	RadioSF      byte
	RadioCR      byte
	Name         string
}

// Lat returns the advert latitude in degrees (0 when unset).
func (s SelfInfo) Lat() float64 { return float64(s.AdvLatRaw) / 1e6 }

// Lon returns the advert longitude in degrees (0 when unset).
func (s SelfInfo) Lon() float64 { return float64(s.AdvLonRaw) / 1e6 }

// DeviceInfo is the device's answer to DEVICE_QUERY.
type DeviceInfo struct {
	FirmwareVer byte
	Build       string
	Model       string
}

// Advert is one parsed neighbour advert (push 0x8A in manual mode).
type Advert struct {
	PublicKey  []byte // 32 bytes
	Type       byte
	OutPathLen int8
	AdvName    string
	LastAdvert uint32
	AdvLatRaw  int32
	AdvLonRaw  int32
}

// Lat/Lon in degrees (0 when unset).
func (a Advert) Lat() float64 { return float64(a.AdvLatRaw) / 1e6 }
func (a Advert) Lon() float64 { return float64(a.AdvLonRaw) / 1e6 }

// ChannelMessage is one received channel text message.
type ChannelMessage struct {
	ChannelIdx byte
	PathLen    byte
	TxtType    byte
	Timestamp  uint32
	Text       string
	SNR        *float64 // V3 only
}

// ContactMessage is one received direct (contact) text message.
type ContactMessage struct {
	PubKeyPrefix []byte // 6 bytes
	PathLen      byte
	TxtType      byte
	Timestamp    uint32
	Text         string
	SNR          *float64 // V3 only
}

// parseSelfInfo decodes a PACKET_SELF_INFO payload (after the 0x05 byte).
func parseSelfInfo(b []byte) (SelfInfo, error) {
	r := newReader(b)
	var s SelfInfo
	var err error
	if s.AdvType, err = r.byte(); err != nil {
		return s, err
	}
	if s.TxPower, err = r.byte(); err != nil {
		return s, err
	}
	if s.MaxTxPower, err = r.byte(); err != nil {
		return s, err
	}
	if s.PublicKey, err = r.bytes(32); err != nil {
		return s, err
	}
	if s.AdvLatRaw, err = r.i32(); err != nil {
		return s, err
	}
	if s.AdvLonRaw, err = r.i32(); err != nil {
		return s, err
	}
	if _, err = r.bytes(3); err != nil { // reserved
		return s, err
	}
	if _, err = r.byte(); err != nil { // manualAddContacts
		return s, err
	}
	if s.RadioFreqKHz, err = r.u32(); err != nil {
		return s, err
	}
	if s.RadioBwHz, err = r.u32(); err != nil {
		return s, err
	}
	if s.RadioSF, err = r.byte(); err != nil {
		return s, err
	}
	if s.RadioCR, err = r.byte(); err != nil {
		return s, err
	}
	name, err := r.rest()
	if err != nil {
		return s, err
	}
	s.Name = string(name)
	return s, nil
}

// parseDeviceInfo decodes a PACKET_DEVICE_INFO payload (after the 0x0D).
func parseDeviceInfo(b []byte) (DeviceInfo, error) {
	r := newReader(b)
	var d DeviceInfo
	var err error
	if d.FirmwareVer, err = r.byte(); err != nil {
		return d, err
	}
	if _, err = r.bytes(6); err != nil { // reserved
		return d, err
	}
	if d.Build, err = r.cstr(12); err != nil {
		return d, err
	}
	rest, err := r.rest()
	if err != nil {
		return d, err
	}
	d.Model = string(rest)
	return d, nil
}

// parseNewAdvert decodes a PACKET_NEW_ADVERT push (after the 0x8A).
func parseNewAdvert(b []byte) (Advert, error) {
	r := newReader(b)
	var a Advert
	var err error
	if a.PublicKey, err = r.bytes(32); err != nil {
		return a, err
	}
	if a.Type, err = r.byte(); err != nil {
		return a, err
	}
	if _, err = r.byte(); err != nil { // flags
		return a, err
	}
	pathLen, err := r.byte()
	if err != nil {
		return a, err
	}
	a.OutPathLen = int8(pathLen)
	if _, err = r.bytes(64); err != nil { // out path
		return a, err
	}
	if a.AdvName, err = r.cstr(32); err != nil {
		return a, err
	}
	if a.LastAdvert, err = r.u32(); err != nil {
		return a, err
	}
	if a.AdvLatRaw, err = r.i32(); err != nil {
		return a, err
	}
	if a.AdvLonRaw, err = r.i32(); err != nil {
		return a, err
	}
	if _, err = r.u32(); err != nil { // lastMod
		return a, err
	}
	return a, nil
}

// parseChannelMsg decodes a channel message, V3 when snr is true.
func parseChannelMsg(b []byte, v3 bool) (ChannelMessage, error) {
	r := newReader(b)
	var m ChannelMessage
	var err error
	var snr byte
	if v3 {
		if snr, err = r.byte(); err != nil {
			return m, err
		}
		if _, err = r.bytes(2); err != nil { // reserved
			return m, err
		}
	}
	if m.ChannelIdx, err = r.byte(); err != nil {
		return m, err
	}
	if m.PathLen, err = r.byte(); err != nil {
		return m, err
	}
	if m.TxtType, err = r.byte(); err != nil {
		return m, err
	}
	if m.Timestamp, err = r.u32(); err != nil {
		return m, err
	}
	rest, err := r.rest()
	if err != nil {
		return m, err
	}
	m.Text = string(rest)
	if v3 {
		v := float64(int8(snr)) / 4
		m.SNR = &v
	}
	return m, nil
}

// parseContactMsg decodes a contact message, V3 when snr is true.
func parseContactMsg(b []byte, v3 bool) (ContactMessage, error) {
	r := newReader(b)
	var m ContactMessage
	var err error
	var snr byte
	if v3 {
		if snr, err = r.byte(); err != nil {
			return m, err
		}
		if _, err = r.bytes(2); err != nil { // reserved
			return m, err
		}
	}
	if m.PubKeyPrefix, err = r.bytes(6); err != nil {
		return m, err
	}
	if m.PathLen, err = r.byte(); err != nil {
		return m, err
	}
	if m.TxtType, err = r.byte(); err != nil {
		return m, err
	}
	if m.Timestamp, err = r.u32(); err != nil {
		return m, err
	}
	rest, err := r.rest()
	if err != nil {
		return m, err
	}
	m.Text = string(rest)
	if v3 {
		v := float64(int8(snr)) / 4
		m.SNR = &v
	}
	return m, nil
}

// parseChannelData decodes a channel data datagram.
func parseChannelData(b []byte) (snr float64, channelIdx byte, dataType uint16, data []byte, err error) {
	r := newReader(b)
	var raw byte
	if raw, err = r.byte(); err != nil {
		return
	}
	snr = float64(int8(raw)) / 4
	if _, err = r.bytes(2); err != nil { // reserved
		return
	}
	if channelIdx, err = r.byte(); err != nil {
		return
	}
	if _, err = r.byte(); err != nil { // pathLen
		return
	}
	if dataType, err = r.u16(); err != nil {
		return
	}
	var n byte
	if n, err = r.byte(); err != nil {
		return
	}
	data, err = r.bytes(int(n))
	return
}

// ---- builders ----

func buildAppStart(name string) []byte {
	out := []byte{cmdAppStart, 1, 0, 0, 0, 0, 0, 0}
	return append(out, []byte(name)...)
}

func buildDeviceQuery(targetVer byte) []byte {
	return []byte{cmdDeviceQuery, targetVer}
}

func buildSendChannelTxtMsg(channelIdx byte, text string) []byte {
	out := []byte{cmdSendChannelTxtMsg, 0, channelIdx}
	out = binary.LittleEndian.AppendUint32(out, uint32(nowUnix()))
	return append(out, []byte(text)...)
}

func buildSendSelfAdvert(kind byte) []byte {
	return []byte{cmdSendSelfAdvert, kind}
}

func buildSyncNextMessage() []byte {
	return []byte{cmdSyncNextMessage}
}

func buildGetBattery() []byte {
	return []byte{cmdGetBattery}
}

// pubKeyHex renders a public key (or prefix) as hex for map keys.
func pubKeyHex(b []byte) string { return hex.EncodeToString(b) }

// nowUnix returns the current unix time (injectable seam for tests).
var nowUnix = func() int64 { return time.Now().Unix() }
