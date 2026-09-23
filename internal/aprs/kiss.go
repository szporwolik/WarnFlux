package aprs

import (
	"fmt"
	"strings"
)

// KISS framing constants (SLIP-style).
const (
	kissFEND  = 0xC0
	kissFESC  = 0xDB
	kissTFEND = 0xDC
	kissTFESC = 0xDD
	kissData  = 0x00 // KISS data frame command byte
)

// AX.25 UI frame constants.
const (
	ax25ControlUI = 0x03
	ax25PIDNoL3   = 0xF0
)

// EncodeKISS wraps one payload in a KISS data frame.
func EncodeKISS(payload []byte) []byte {
	out := make([]byte, 0, len(payload)+4)
	out = append(out, kissFEND, kissData)
	for _, b := range payload {
		switch b {
		case kissFEND:
			out = append(out, kissFESC, kissTFEND)
		case kissFESC:
			out = append(out, kissFESC, kissTFESC)
		default:
			out = append(out, b)
		}
	}
	return append(out, kissFEND)
}

// KISSDecoder is an incremental KISS stream decoder. Feed pushes raw bytes
// from the wire and returns every complete data frame found. Non-data
// frames (e.g. TNC command frames) are skipped.
type KISSDecoder struct {
	buf     []byte
	esc     bool
	inFrame bool
	wantCmd bool
}

// Feed processes raw bytes and returns complete KISS data frames.
func (d *KISSDecoder) Feed(chunk []byte) [][]byte {
	var out [][]byte
	for _, b := range chunk {
		switch {
		case b == kissFEND:
			if d.inFrame && !d.wantCmd && len(d.buf) > 0 {
				out = append(out, append([]byte(nil), d.buf...))
				d.buf = d.buf[:0]
			}
			d.inFrame = true
			d.wantCmd = true
			d.esc = false
		case d.esc:
			switch b {
			case kissTFEND:
				d.buf = append(d.buf, kissFEND)
			case kissTFESC:
				d.buf = append(d.buf, kissFESC)
			}
			d.esc = false
		case b == kissFESC:
			d.esc = true
		case d.wantCmd:
			// The frame command byte: only data frames carry payload.
			if b == kissData {
				d.wantCmd = false
			} else {
				d.inFrame = false
			}
		case d.inFrame:
			d.buf = append(d.buf, b)
		}
	}
	return out
}

// ax25Address is one decoded AX.25 address field.
type ax25Address struct {
	Callsign string
	SSID     int
	Repeated bool
}

func splitCallsign(callsign string) (string, int) {
	callsign = NormalizeCallsign(callsign)
	if i := strings.IndexByte(callsign, '-'); i >= 0 && len(callsign[i+1:]) > 0 {
		ssid := 0
		for _, c := range callsign[i+1:] {
			if c < '0' || c > '9' {
				return callsign[:i], 0
			}
			ssid = ssid*10 + int(c-'0')
		}
		return callsign[:i], ssid
	}
	return callsign, 0
}

// encodeAddress renders one 7-byte AX.25 address field. last sets the
// "more addresses follow" bit to zero.
func encodeAddress(callsign string, ssid int, last bool) ([]byte, error) {
	base, csID := splitCallsign(callsign)
	if base == "" {
		return nil, fmt.Errorf("empty callsign")
	}
	if ssid == 0 {
		ssid = csID
	}
	if ssid < 0 || ssid > 15 {
		return nil, fmt.Errorf("ssid out of range for %q", callsign)
	}
	field := make([]byte, 7)
	for i := 0; i < 6; i++ {
		c := byte(' ')
		if i < len(base) {
			c = base[i]
		}
		field[i] = c << 1
	}
	field[6] = byte(ssid<<1) | 0x60
	if last {
		field[6] |= 0x01
	}
	return field, nil
}

// BuildUIFrame assembles one AX.25 UI frame (no layer 3) carrying info:
// dst <- src via digis (unproto path), control 0x03, PID 0xF0.
func BuildUIFrame(src, dst string, digis []string, info []byte) ([]byte, error) {
	frame := make([]byte, 0, 7*3+2+len(info))

	addrs := append([]string{dst, src}, digis...)
	for i, a := range addrs {
		field, err := encodeAddress(a, 0, i == len(addrs)-1)
		if err != nil {
			return nil, err
		}
		frame = append(frame, field...)
	}
	frame = append(frame, ax25ControlUI, ax25PIDNoL3)
	frame = append(frame, info...)
	return frame, nil
}

// DecodeUIFrame parses one AX.25 UI frame into its parts. It returns ok
// false for non-UI frames or frames without a layer-3 PID.
func DecodeUIFrame(frame []byte) (src, dst string, digis []string, info []byte, ok bool) {
	pos := 0
	readAddr := func() (ax25Address, bool, bool) {
		if pos+7 > len(frame) {
			return ax25Address{}, false, false
		}
		var callsign strings.Builder
		for i := 0; i < 6; i++ {
			if c := frame[pos+i] >> 1; c >= 0x20 && c < 0x7f {
				callsign.WriteByte(c)
			}
		}
		flags := frame[pos+6]
		a := ax25Address{
			Callsign: strings.TrimRight(callsign.String(), " "),
			SSID:     int(flags>>1) & 0x0F,
			Repeated: flags&0x80 != 0,
		}
		pos += 7
		return a, flags&0x01 != 0, true
	}

	var addrs []ax25Address
	for {
		a, last, valid := readAddr()
		if !valid {
			return "", "", nil, nil, false
		}
		addrs = append(addrs, a)
		if last {
			break
		}
	}
	if len(addrs) < 2 || pos+1 >= len(frame) || frame[pos] != ax25ControlUI || frame[pos+1] != ax25PIDNoL3 {
		return "", "", nil, nil, false
	}
	pos += 2
	info = append([]byte(nil), frame[pos:]...)

	format := func(a ax25Address) string {
		if a.SSID == 0 {
			return a.Callsign
		}
		return fmt.Sprintf("%s-%d", a.Callsign, a.SSID)
	}
	dst = format(addrs[0])
	src = format(addrs[1])
	for _, a := range addrs[2:] {
		name := format(a)
		if a.Repeated {
			name += "*"
		}
		digis = append(digis, name)
	}
	return src, dst, digis, info, true
}

// FrameToFeedLine renders a decoded UI frame as a TNC2 feed line for
// ParseFeedLine: SRC>DST,PATH:INFO.
func FrameToFeedLine(src, dst string, digis []string, info []byte) string {
	var b strings.Builder
	b.WriteString(src)
	b.WriteByte('>')
	b.WriteString(dst)
	if len(digis) > 0 {
		b.WriteByte(',')
		b.WriteString(strings.Join(digis, ","))
	}
	b.WriteByte(':')
	b.Write(info)
	return b.String()
}
