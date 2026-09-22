package web

import (
	"testing"
	"time"
)

func TestDurFormatting(t *testing.T) {
	dur := templateFuncs()["dur"].(func(time.Duration) string)
	cases := map[time.Duration]string{
		0:                      "0ms",
		999 * time.Millisecond: "999ms",
		time.Second:            "1s",
		59 * time.Second:       "59s",
		time.Minute:            "1m0s",
		time.Minute + 29*time.Second + 721*time.Microsecond: "1m29s",
		time.Hour:                    "1h0m",
		2*time.Hour + 3*time.Minute:  "2h3m",
		24 * time.Hour:               "1d0h",
		3*24*time.Hour + 4*time.Hour: "3d4h",
		-5 * time.Second:             "0ms", // negative clamps to zero
	}
	for in, want := range cases {
		if got := dur(in); got != want {
			t.Errorf("dur(%s) = %q, want %q", in, got, want)
		}
	}
}
