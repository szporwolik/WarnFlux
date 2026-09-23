package aprs

import (
	"bytes"
	"testing"
	"time"
)

func TestKISSEncodeDecodeRoundtrip(t *testing.T) {
	payloads := [][]byte{
		{},
		[]byte("hello"),
		[]byte{0xC0, 0xDB, 0x00, 0xC0, 0xDB},
	}
	for _, payload := range payloads {
		encoded := EncodeKISS(payload)
		if encoded[0] != kissFEND || encoded[1] != kissData || encoded[len(encoded)-1] != kissFEND {
			t.Fatalf("bad KISS framing for %v: % x", payload, encoded)
		}
		var dec KISSDecoder
		var got [][]byte
		// Feed byte by byte to exercise fragmentation.
		for _, b := range encoded {
			got = append(got, dec.Feed([]byte{b})...)
		}
		if len(payload) == 0 {
			// Empty frames are never emitted (nothing to deliver).
			if len(got) != 0 {
				t.Fatalf("empty payload emitted frames: % x", got)
			}
			continue
		}
		if len(got) != 1 || !bytes.Equal(got[0], payload) {
			t.Fatalf("roundtrip = % x, want % x", got, payload)
		}
	}
}

func TestKISSDecoderMultipleFrames(t *testing.T) {
	all := append(EncodeKISS([]byte("one")), EncodeKISS([]byte("two"))...)
	var dec KISSDecoder
	got := dec.Feed(all)
	if len(got) != 2 || string(got[0]) != "one" || string(got[1]) != "two" {
		t.Fatalf("frames = %q", got)
	}
}

func TestUIFrameRoundtrip(t *testing.T) {
	frame, err := BuildUIFrame("SP9MOA-10", "SP9XYZ-7", []string{"WIDE1-1", "SR9NR"}, []byte(":SP9XYZ-7  :hello{12345}"))
	if err != nil {
		t.Fatalf("BuildUIFrame: %v", err)
	}
	src, dst, digis, info, ok := DecodeUIFrame(frame)
	if !ok {
		t.Fatal("DecodeUIFrame rejected a valid frame")
	}
	if src != "SP9MOA-10" || dst != "SP9XYZ-7" {
		t.Errorf("src/dst = %q/%q", src, dst)
	}
	if len(digis) != 2 || digis[0] != "WIDE1-1" || digis[1] != "SR9NR" {
		t.Errorf("digis = %v", digis)
	}
	if string(info) != ":SP9XYZ-7  :hello{12345}" {
		t.Errorf("info = %q", info)
	}
}

func TestUIFrameRejectsNonUI(t *testing.T) {
	// I-frame control (0x00) must be rejected.
	frame, _ := BuildUIFrame("SP9MOA-10", "APRS", nil, []byte("!5056.25N/01952.50E-"))
	frame[len(frame)-len("!5056.25N/01952.50E-")-2] = 0x00 // control
	if _, _, _, _, ok := DecodeUIFrame(frame); ok {
		t.Error("non-UI frame accepted")
	}
}

func TestFrameToFeedLineParsesPosition(t *testing.T) {
	line := FrameToFeedLine("SP9XYZ-7", "APRS", []string{"WIDE1-1*"}, []byte("!5056.25N/01952.50E-"))
	p := ParseFeedLine(line, time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC))
	if p.Src != "SP9XYZ-7" || p.Kind != KindPosition {
		t.Fatalf("parsed = %+v", p)
	}
	if p.Position == nil {
		t.Fatal("position missing")
	}
}
