package gsm

import (
	"encoding/hex"
	"strings"
	"testing"
)

// deliverPDU builds one SMS-DELIVER PDU hex (empty SMSC, 7-bit body).
// concat (ref, total, seq) adds the concatenation UDH.
func deliverPDU(t *testing.T, from, text string, concat ...int) string {
	t.Helper()
	var da []byte
	digits := strings.TrimPrefix(from, "+")
	tp := byte(0x81)
	if strings.HasPrefix(from, "+") {
		tp = 0x91
	}
	da = append(da, 0, tp) // length patched below
	for i := 0; i < len(digits); i += 2 {
		hi := digits[i] - '0'
		var lo byte = 0xF
		if i+1 < len(digits) {
			lo = digits[i+1] - '0'
		}
		da = append(da, lo<<4|hi)
	}
	da[0] = byte(len(da) - 1)
	first := byte(0x04) // MTI=SMS-DELIVER
	var ud []byte
	if len(concat) == 3 {
		first |= 0x40
		ud = append(ud, 0x05, 0x00, 0x03, byte(concat[0]), byte(concat[1]), byte(concat[2]))
	}
	septets := make([]byte, 0, len(text))
	for _, r := range text {
		septets = append(septets, gsm7ToSeptet[r])
	}
	ud = append(ud, packSeptets(septets, 0)...)
	tpdu := []byte{first}
	tpdu = append(tpdu, da...)
	tpdu = append(tpdu, 0x00, 0x00)          // PID, DCS (7-bit)
	tpdu = append(tpdu, 0, 0, 0, 0, 0, 0, 0) // SCTS
	tpdu = append(tpdu, byte(len(ud)))
	tpdu = append(tpdu, ud...)
	return "00" + strings.ToUpper(hex.EncodeToString(tpdu))
}

func TestEncodePDUSingle7Bit(t *testing.T) {
	pdus, err := encodePDU("+48600111222", "hellohello", 42)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if len(pdus) != 1 {
		t.Fatalf("segments = %d, want 1", len(pdus))
	}
	p := pdus[0]
	if p.TPDULen != len(p.Hex)/2-1 {
		t.Fatalf("tpdu len = %d, hex len = %d", p.TPDULen, len(p.Hex)/2-1)
	}
	// The canonical 7-bit packing of "hellohello" (TS 23.038).
	if !strings.HasSuffix(p.Hex, "E8329BFD4697D9EC37") {
		t.Fatalf("hex = %s, want the canonical hellohello tail", p.Hex)
	}
	// SMS-SUBMIT, no UDH, +48 number as 07 91 84 06 10 11 22 F2.
	if !strings.HasPrefix(p.Hex, "00110007918406101122F2") {
		t.Fatalf("hex = %s, want the canonical submit header", p.Hex)
	}
}

func TestEncodePDUConcat(t *testing.T) {
	text := strings.Repeat("A", 200)
	pdus, err := encodePDU("+48600111222", text, 42)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if len(pdus) != 2 {
		t.Fatalf("segments = %d, want 2", len(pdus))
	}
	if !strings.Contains(pdus[0].Hex, "0500032A0201") {
		t.Fatalf("segment 1 UDH missing: %s", pdus[0].Hex)
	}
	if !strings.Contains(pdus[1].Hex, "0500032A0202") {
		t.Fatalf("segment 2 UDH missing: %s", pdus[1].Hex)
	}
	if !strings.HasPrefix(pdus[0].Hex, "0051") || !strings.HasPrefix(pdus[1].Hex, "0051") {
		t.Fatalf("UDHI not set: %s / %s", pdus[0].Hex, pdus[1].Hex)
	}
}

func TestEncodePDUUCS2(t *testing.T) {
	pdus, err := encodePDU("+48600111222", "Zażółć 🚀", 42)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if len(pdus) != 1 {
		t.Fatalf("segments = %d, want 1", len(pdus))
	}
	// DCS 0x08 = UCS-2, body 005A 0061 ...
	if !strings.Contains(pdus[0].Hex, "0008AA") {
		t.Fatalf("hex = %s, want DCS 0x08", pdus[0].Hex)
	}
}

func TestEncodePDUTooLong(t *testing.T) {
	if _, err := encodePDU("+48600111222", strings.Repeat("A", 460), 42); err == nil {
		t.Fatalf("encode of 460 chars succeeded, want ErrTooLong")
	}
	if segs, _ := SMSegments(strings.Repeat("A", 460)); segs != 4 {
		t.Fatalf("segments(460 A) = %d, want 4", segs)
	}
}

func TestUnpackSeptetsCanonical(t *testing.T) {
	octets := []byte{0xE8, 0x32, 0x9B, 0xFD, 0x46, 0x97, 0xD9, 0xEC, 0x37}
	if got := decodeGSM7(unpackSeptets(octets)); got != "hellohello" {
		t.Fatalf("decode = %q, want hellohello", got)
	}
}

func TestParseDeliverPDU(t *testing.T) {
	d, err := ParseDeliverPDU(deliverPDU(t, "+48600111222", "Wiadomosc testowa"))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if d.From != "+48600111222" || d.Text != "Wiadomosc testowa" {
		t.Fatalf("decode = %+v", d)
	}
	if d.ConcatTotal != 0 {
		t.Fatalf("unexpected concat: %+v", d)
	}
}

func TestParseDeliverPDUConcat(t *testing.T) {
	d1, err := ParseDeliverPDU(deliverPDU(t, "+48600111222", "Czesc pierwsza ", 0x2A, 2, 1))
	if err != nil {
		t.Fatalf("parse 1: %v", err)
	}
	d2, err := ParseDeliverPDU(deliverPDU(t, "+48600111222", "czesc druga", 0x2A, 2, 2))
	if err != nil {
		t.Fatalf("parse 2: %v", err)
	}
	if d1.ConcatRef != 0x2A || d1.ConcatTotal != 2 || d1.ConcatSeq != 1 {
		t.Fatalf("part 1 concat = %+v", d1)
	}
	if d2.ConcatSeq != 2 {
		t.Fatalf("part 2 concat = %+v", d2)
	}
}

func TestSMSegments(t *testing.T) {
	cases := []struct {
		text string
		segs int
		ucs2 bool
	}{
		{"hello", 1, false},
		{strings.Repeat("A", 160), 1, false},
		{strings.Repeat("A", 161), 2, false},
		{strings.Repeat("A", 306), 2, false},
		{strings.Repeat("A", 307), 3, false},
		{strings.Repeat("A", 459), 3, false},
		{strings.Repeat("A", 460), 4, false},
		{"Zażółć", 1, true}, // Polish diacritics → UCS-2
		{strings.Repeat("ż", 70), 1, true},
		{strings.Repeat("ż", 71), 2, true},
		{strings.Repeat("ż", 201), 3, true},
		{strings.Repeat("ż", 202), 4, true},
	}
	for _, c := range cases {
		segs, ucs2 := SMSegments(c.text)
		if segs != c.segs || ucs2 != c.ucs2 {
			t.Errorf("SMSegments(%d runes) = %d/%v, want %d/%v", len([]rune(c.text)), segs, ucs2, c.segs, c.ucs2)
		}
	}
}

// TestPDURoundTrip encodes a message and decodes it as a delivered PDU
// (re-packaging the user data) — the packed text must survive.
func TestPDURoundTrip(t *testing.T) {
	for _, text := range []string{
		"hellohello",
		"Wiadomość testowa z polskimi znakami ąćęłńóśźż",
		strings.Repeat("A", 153*2+40),
	} {
		pdus, err := encodePDU("+48600111222", text, 7)
		if err != nil {
			t.Fatalf("encode %q: %v", text, err)
		}
		var joined string
		for i, p := range pdus {
			// Extract the UD from the submit TPDU and re-wrap it as a
			// deliver for the decoder.
			raw, _ := hex.DecodeString(p.Hex)
			tpdu := raw[1:] // skip empty SMSC
			// tpdu: first(1) mr(1) da(1+daLen) pid(1) dcs(1) vp(1) udl(1) ud
			daLen := int(tpdu[2])
			udOff := 1 + 1 + 1 + daLen + 1 + 1 + 1 + 1
			ud := tpdu[udOff:]
			dcs := tpdu[udOff-3]
			first := tpdu[0]
			var body []byte
			udhi := first&0x40 != 0
			var ref, total, seq int
			if udhi {
				udhl := int(ud[0])
				udh := ud[1 : 1+udhl]
				if udh[0] == 0x00 && udh[1] == 3 {
					ref, total, seq = int(udh[2]), int(udh[3]), int(udh[4])
				}
				body = ud[1+udhl:]
			} else {
				body = ud
			}
			d := DeliverPDU{}
			d.Text = decodeUD(dcs, body)
			if udhi {
				d.ConcatRef, d.ConcatTotal, d.ConcatSeq = uint16(ref), total, seq
			}
			if i > 0 {
				joined += d.Text
			} else {
				joined = d.Text
			}
		}
		if joined != text {
			t.Fatalf("round trip = %q, want %q", joined, text)
		}
	}
}
