package gsm

// PDU mode primitives (3GPP TS 23.040 / 23.038): text-mode AT commands
// cannot express the user-data header, so concatenated messages and
// non-GSM-7 alphabets travel as raw PDUs. Both directions are handled
// here — EncodePDU builds SMS-SUBMIT TP-DUs for sending, ParseDeliverPDU
// decodes SMS-DELIVER TP-DUs for receiving.

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"unicode/utf16"
)

// ErrTooLong is returned when the message exceeds the segment limit.
var ErrTooLong = errors.New("gsm: message exceeds the segment limit")

// MaxSegments caps one outgoing PDU message (a 3-segment concatenation).
// The PDU encoder is kept for modems that support PDU-mode sending; the
// Huawei E173 does NOT (CMS ERROR 500), so its send path uses text mode.
const MaxSegments = 3

// gsm7Translit maps characters outside the GSM-7 alphabet to an
// acceptable equivalent (Polish diacritics first).
var gsm7Translit = map[rune]string{
	'ą': "a", 'ć': "c", 'ę': "e", 'ł': "l", 'ń': "n", 'ó': "o", 'ś': "s", 'ź': "z", 'ż': "z",
	'Ą': "A", 'Ć': "C", 'Ę': "E", 'Ł': "L", 'Ń': "N", 'Ó': "O", 'Ś': "S", 'Ź': "Z", 'Ż': "Z",
	'…': "...", '–': "-", '—': "-", '„': "\"", '”': "\"", '’': "'", '\u00a0': " ",
}

// TransliterateGSM7 renders text sendable through text mode: GSM-7
// characters pass through, known characters get an ASCII equivalent and
// anything else becomes '?'.
func TransliterateGSM7(text string) string {
	var sb strings.Builder
	for _, r := range text {
		if _, ok := gsm7ToSeptet[r]; ok {
			sb.WriteRune(r)
			continue
		}
		if _, ok := gsm7ToExt[r]; ok {
			sb.WriteRune(r)
			continue
		}
		if rep, ok := gsm7Translit[r]; ok {
			sb.WriteString(rep)
		} else {
			sb.WriteByte('?')
		}
	}
	return sb.String()
}

// GSM 7-bit default alphabet (TS 23.038 §6.2.1), index 0..127.
var gsm7Basic = [128]rune{
	0x00: '@', 0x01: '£', 0x02: '$', 0x03: '¥', 0x04: 'è', 0x05: 'é',
	0x06: 'ù', 0x07: 'ì', 0x08: 'ò', 0x09: 'Ç', 0x0A: '\n', 0x0B: 'Ø',
	0x0C: 'ø', 0x0D: '\r', 0x0E: 'Å', 0x0F: 'å',
	0x10: 'Δ', 0x11: '_', 0x12: 'Φ', 0x13: 'Γ', 0x14: 'Λ', 0x15: 'Ω',
	0x16: 'Π', 0x17: 'Ψ', 0x18: 'Σ', 0x19: 'Θ', 0x1A: 'Ξ', 0x1C: 'Æ',
	0x1D: 'æ', 0x1E: 'ß', 0x1F: 'É',
	0x20: ' ', 0x21: '!', 0x22: '"', 0x23: '#', 0x24: '¤', 0x25: '%',
	0x26: '&', 0x27: '\'', 0x28: '(', 0x29: ')', 0x2A: '*', 0x2B: '+',
	0x2C: ',', 0x2D: '-', 0x2E: '.', 0x2F: '/',
	0x30: '0', 0x31: '1', 0x32: '2', 0x33: '3', 0x34: '4', 0x35: '5',
	0x36: '6', 0x37: '7', 0x38: '8', 0x39: '9',
	0x3A: ':', 0x3B: ';', 0x3C: '<', 0x3D: '=', 0x3E: '>', 0x3F: '?',
	0x40: '¡', 0x41: 'A', 0x42: 'B', 0x43: 'C', 0x44: 'D', 0x45: 'E',
	0x46: 'F', 0x47: 'G', 0x48: 'H', 0x49: 'I', 0x4A: 'J', 0x4B: 'K',
	0x4C: 'L', 0x4D: 'M', 0x4E: 'N', 0x4F: 'O', 0x50: 'P', 0x51: 'Q',
	0x52: 'R', 0x53: 'S', 0x54: 'T', 0x55: 'U', 0x56: 'V', 0x57: 'W',
	0x58: 'X', 0x59: 'Y', 0x5A: 'Z', 0x5B: 'Ä', 0x5C: 'Ö', 0x5D: 'Ñ',
	0x5E: 'Ü', 0x5F: '§',
	0x60: '¿', 0x61: 'a', 0x62: 'b', 0x63: 'c', 0x64: 'd', 0x65: 'e',
	0x66: 'f', 0x67: 'g', 0x68: 'h', 0x69: 'i', 0x6A: 'j', 0x6B: 'k',
	0x6C: 'l', 0x6D: 'm', 0x6E: 'n', 0x6F: 'o', 0x70: 'p', 0x71: 'q',
	0x72: 'r', 0x73: 's', 0x74: 't', 0x75: 'u', 0x76: 'v', 0x77: 'w',
	0x78: 'x', 0x79: 'y', 0x7A: 'z', 0x7B: 'ä', 0x7C: 'ö', 0x7D: 'ñ',
	0x7E: 'ü', 0x7F: 'à',
}

// gsm7Ext is the extension table (0x1B prefix).
var gsm7Ext = map[byte]rune{
	0x0A: '\f', 0x14: '^', 0x28: '{', 0x29: '}', 0x2F: '\\', 0x3C: '[',
	0x3D: '~', 0x3E: ']', 0x40: '|', 0x65: '€',
}

var (
	gsm7ToSeptet = map[rune]byte{}
	gsm7ToExt    = map[rune]byte{}
)

func init() {
	for i, r := range gsm7Basic {
		gsm7ToSeptet[r] = byte(i)
	}
	for i, r := range gsm7Ext {
		gsm7ToExt[r] = i
	}
}

// gsm7Septets returns the septet count of text (each extension-table
// character costs two septets) or -1 when any character is outside the
// GSM-7 alphabet.
func gsm7Septets(text string) int {
	n := 0
	for _, r := range text {
		if _, ok := gsm7ToSeptet[r]; ok {
			n++
			continue
		}
		if _, ok := gsm7ToExt[r]; ok {
			n += 2
			continue
		}
		return -1
	}
	return n
}

// utf16Units counts the UTF-16 code units of text.
func utf16Units(text string) int {
	n := 0
	for _, r := range text {
		if r > 0xFFFF {
			n += 2
		} else {
			n++
		}
	}
	return n
}

// SMSegments reports how many SMS segments the text occupies and
// whether it must travel as UCS-2 (any non-GSM-7 character forces it).
func SMSegments(text string) (segs int, ucs2 bool) {
	if n := gsm7Septets(text); n >= 0 {
		if n <= 160 {
			return 1, false
		}
		return (n + 152) / 153, false
	}
	n := utf16Units(text)
	if n <= 70 {
		return 1, true
	}
	return (n + 66) / 67, true
}

// pduSegment is one ready-to-send SMS-SUBMIT.
type pduSegment struct {
	// Hex is the full PDU (SMSC field first, empty = modem's own).
	Hex string
	// TPDULen is the AT+CMGS length parameter (octets after the SMSC).
	TPDULen int
}

// encodeAddress renders a phone number as an address field
// (length octets = type + semi-octet digits).
func encodeAddress(number string) []byte {
	international := strings.HasPrefix(number, "+")
	digits := strings.TrimPrefix(number, "+")
	tp := byte(0x81)
	if international {
		tp = 0x91
	}
	var out []byte
	out = append(out, 0, tp) // length patched below
	for i := 0; i < len(digits); i += 2 {
		hi := digits[i] - '0'
		var lo byte = 0xF
		if i+1 < len(digits) {
			lo = digits[i+1] - '0'
		}
		out = append(out, lo<<4|hi)
	}
	out[0] = byte(len(out) - 1)
	return out
}

// packSeptets packs a septet stream into octets, flushing the partial
// byte at every segment boundary (each non-final segment holds exactly
// 153 septets = 134 octets, one fill bit).
func packSeptets(septets []byte, perSegment int) []byte {
	var out []byte
	var buf uint16
	bits := 0
	for i, s := range septets {
		buf |= uint16(s) << bits
		bits += 7
		for bits >= 8 {
			out = append(out, byte(buf&0xff))
			buf >>= 8
			bits -= 8
		}
		if perSegment > 0 && (i+1)%perSegment == 0 && bits > 0 {
			out = append(out, byte(buf&0xff))
			buf, bits = 0, 0
		}
	}
	if bits > 0 {
		out = append(out, byte(buf&0xff))
	}
	return out
}

// encodePDU builds the SMS-SUBMIT segments for one message. ref is the
// concatenation reference (stable for tests).
func encodePDU(number, text string, ref byte) ([]pduSegment, error) {
	segs, ucs2 := SMSegments(text)
	if segs > MaxSegments {
		return nil, ErrTooLong
	}
	da := encodeAddress(number)
	var userData [][]byte // per segment, UDH included
	switch {
	case !ucs2:
		septets := make([]byte, 0, utf16Units(text))
		for _, r := range text {
			if b, ok := gsm7ToSeptet[r]; ok {
				septets = append(septets, b)
				continue
			}
			if b, ok := gsm7ToExt[r]; ok {
				septets = append(septets, 0x1B, b)
			}
		}
		per := 153
		for seg := 0; seg < segs; seg++ {
			lo, hi := seg*per, (seg+1)*per
			if hi > len(septets) {
				hi = len(septets)
			}
			var ud []byte
			if segs > 1 {
				ud = []byte{0x05, 0x00, 0x03, ref, byte(segs), byte(seg + 1)}
			}
			ud = append(ud, packSeptets(septets[lo:hi], per)...)
			userData = append(userData, ud)
		}
	default:
		units := utf16.Encode([]rune(text))
		per := 67
		for seg := 0; seg < segs; seg++ {
			lo, hi := seg*per, (seg+1)*per
			if hi > len(units) {
				hi = len(units)
			}
			var ud []byte
			if segs > 1 {
				ud = []byte{0x05, 0x00, 0x03, ref, byte(segs), byte(seg + 1)}
			}
			for _, u := range units[lo:hi] {
				ud = append(ud, byte(u>>8), byte(u))
			}
			userData = append(userData, ud)
		}
	}
	var out []pduSegment
	for _, ud := range userData {
		dcs := byte(0x00)
		if ucs2 {
			dcs = 0x08
		}
		first := byte(0x11) // SMS-SUBMIT, relative validity
		if len(userData) > 1 {
			first |= 0x40 // TP-UDHI
		}
		tpdu := []byte{first, 0x00} // TP-MR left to the network
		tpdu = append(tpdu, da...)
		tpdu = append(tpdu, 0x00, dcs, 0xAA, byte(len(ud)))
		tpdu = append(tpdu, ud...)
		out = append(out, pduSegment{
			Hex:     "00" + strings.ToUpper(hex.EncodeToString(tpdu)),
			TPDULen: len(tpdu),
		})
	}
	return out, nil
}

// EncodePDU builds the sendable segments for one message with a random
// concatenation reference.
func EncodePDU(number, text string) ([]pduSegment, error) {
	var b [1]byte
	ref := byte(0)
	if _, err := rand.Read(b[:]); err == nil {
		ref = b[0]
	}
	return encodePDU(number, text, ref)
}

// DeliverPDU is one decoded SMS-DELIVER.
type DeliverPDU struct {
	From, Text string
	// ConcatRef/ConcatTotal/ConcatSeq identify one part of a
	// concatenated message (zero values = ordinary message).
	ConcatRef   uint16
	ConcatTotal int
	ConcatSeq   int
}

// unpackSeptets decodes a packed 7-bit stream (TS 23.038 §6.1.2.1).
func unpackSeptets(octets []byte) []byte {
	septets := make([]byte, 0, len(octets)*8/7+1)
	var buf uint16
	bits := 0
	for _, b := range octets {
		buf |= uint16(b) << bits
		bits += 8
		for bits >= 7 {
			septets = append(septets, byte(buf&0x7f))
			buf >>= 7
			bits -= 7
		}
	}
	// When the true septet count is 7 mod 8, the whole final octet is
	// fill bits and the walk produces one bogus trailing 0x00 septet
	// (the classic "trailing @"): drop it. A message genuinely ending
	// in '@' with such a count is a documented corner case.
	if n := len(septets); n > 0 && septets[n-1] == 0x00 && n*7 == len(octets)*8 {
		septets = septets[:n-1]
	}
	return septets
}

// decodeGSM7 maps septets (with the 0x1B escape) back to runes.
func decodeGSM7(septets []byte) string {
	var sb strings.Builder
	for i := 0; i < len(septets); i++ {
		s := septets[i]
		if s == 0x1B && i+1 < len(septets) {
			if r, ok := gsm7Ext[septets[i+1]]; ok {
				sb.WriteRune(r)
				i++
				continue
			}
		}
		if s < 128 {
			sb.WriteRune(gsm7Basic[s])
		}
	}
	return sb.String()
}

// parseAddress decodes an address field (length, type, digits).
func parseAddress(b []byte, pos int) (addr string, next int, ok bool) {
	if pos >= len(b) {
		return "", pos, false
	}
	l := int(b[pos])
	pos++
	if pos >= len(b) || pos+l > len(b) {
		return "", pos, false
	}
	tp := b[pos]
	digits := b[pos+1 : pos+l]
	next = pos + l
	alnum := tp&0x70 == 0x50
	international := tp&0x70 == 0x10
	if alnum {
		return decodeGSM7(unpackSeptets(digits)), next, true
	}
	var sb strings.Builder
	if international {
		sb.WriteByte('+')
	}
	for _, d := range digits {
		sb.WriteByte('0' + (d & 0x0f))
		if hi := d >> 4; hi < 0x0f {
			sb.WriteByte('0' + hi)
		}
	}
	return sb.String(), next, true
}

// decodeUD decodes the user data per the data coding scheme.
func decodeUD(dcs byte, ud []byte) string {
	switch dcs & 0x0C {
	case 0x08: // UCS-2
		units := make([]uint16, 0, len(ud)/2)
		for i := 0; i+1 < len(ud); i += 2 {
			units = append(units, binary.BigEndian.Uint16(ud[i:i+2]))
		}
		return string(utf16.Decode(units))
	case 0x04: // 8-bit data
		var sb strings.Builder
		for _, b := range ud {
			sb.WriteRune(rune(b))
		}
		return sb.String()
	default: // 7-bit (including 0x10/0x18 group variants)
		return decodeGSM7(unpackSeptets(ud))
	}
}

// ParseDeliverPDU decodes one SMS-DELIVER PDU (hex, SMSC field first).
func ParseDeliverPDU(hexPDU string) (DeliverPDU, error) {
	raw, err := hex.DecodeString(strings.TrimSpace(hexPDU))
	if err != nil {
		return DeliverPDU{}, fmt.Errorf("gsm: pdu hex: %w", err)
	}
	if len(raw) < 2 {
		return DeliverPDU{}, errors.New("gsm: pdu too short")
	}
	pos := 1 + int(raw[0]) // SMSC
	if pos >= len(raw) {
		return DeliverPDU{}, errors.New("gsm: pdu SMSC field overflows")
	}
	first := raw[pos]
	if first&0x03 != 0 { // SMS-DELIVER
		return DeliverPDU{}, fmt.Errorf("gsm: pdu not a deliver (0x%02x)", first)
	}
	pos++
	from, pos, ok := parseAddress(raw, pos)
	if !ok {
		return DeliverPDU{}, errors.New("gsm: pdu originator field overflows")
	}
	// PID, DCS, SCTS (7 octets), UDL.
	if pos+1+1+7+1 > len(raw) {
		return DeliverPDU{}, errors.New("gsm: pdu header overflows")
	}
	pos++ // TP-PID
	dcs := raw[pos]
	pos++
	pos += 7 // TP-SCTS (stamp unused — history uses receipt time)
	udl := int(raw[pos])
	pos++
	if pos+udl > len(raw) {
		return DeliverPDU{}, errors.New("gsm: pdu user data overflows")
	}
	ud := raw[pos : pos+udl]
	d := DeliverPDU{From: from}
	if first&0x40 != 0 && len(ud) > 0 { // TP-UDHI
		udhl := int(ud[0])
		if udhl+1 <= len(ud) {
			udh := ud[1 : 1+udhl]
			body := ud[1+udhl:]
			for i := 0; i+1 < len(udh); {
				iei, iel := udh[i], int(udh[i+1])
				i += 2
				if i+iel > len(udh) {
					break
				}
				data := udh[i : i+iel]
				i += iel
				switch {
				case iei == 0x00 && iel == 3:
					d.ConcatRef = uint16(data[0])
					d.ConcatTotal = int(data[1])
					d.ConcatSeq = int(data[2])
				case iei == 0x08 && iel == 4:
					d.ConcatRef = binary.BigEndian.Uint16(data[0:2])
					d.ConcatTotal = int(data[2])
					d.ConcatSeq = int(data[3])
				}
			}
			d.Text = decodeUD(dcs, body)
		} else {
			d.Text = decodeUD(dcs, ud)
		}
	} else {
		d.Text = decodeUD(dcs, ud)
	}
	return d, nil
}
