package aprs

import (
	"testing"
	"time"
)

var fixedNow = time.Date(2026, 9, 24, 10, 0, 0, 0, time.UTC)

func TestIsInfrastructure(t *testing.T) {
	parse := func(line string) Packet {
		return ParseFeedLine(line, fixedNow)
	}
	cases := []struct {
		line string
		want bool
	}{
		// Real ham stations.
		{"SP9OK>APRS:!5056.25N/01952.50E-", false}, // house
		{"SP9OK>APRS:!5056.25N/01952.50E>", false}, // car
		{"SP9OK>APRS:!5056.25N/01952.50E_", false}, // WX station
		{"SP9OK>APRS:>hello there", false},         // status
		// Alternate-table person markers stay visible (overlayed car).
		{"SP9OK>APRS:!5056.25N\\01952.50E>", false},
		// Infrastructure.
		{"SR9NR>APRS:!5056.25N/01952.50E#", true},                 // digipeater
		{"SR9IG>APRS:!5056.25N/01952.50EI", true},                 // TCP/IP node
		{"SR9HF>APRS:!5056.25N/01952.50E&", true},                 // gateway
		{"SR9RP>APRS:!5056.25N/01952.50Er", true},                 // repeater
		{"SR9NO>APRS:!5056.25N/01952.50En", true},                 // node
		{"SR9BB>APRS:!5056.25N/01952.50EB", true},                 // BBS
		{"SR9VO>APRS:!5056.25N/01952.50E0", true},                 // circle
		{"SR9VO>APRS:!5056.25N\\01952.50E0", true},                // IRLP/Echolink circle (alternate)
		{"SP9MOA>APRS:;SR9NR  *111111z5056.25N/01952.50Er", true}, // object
		{"SP9OK>APRS:?APRS?", true},                               // query
	}
	for _, c := range cases {
		if got := IsInfrastructure(parse(c.line)); got != c.want {
			t.Errorf("IsInfrastructure(%q) = %v, want %v", c.line, got, c.want)
		}
	}
}
