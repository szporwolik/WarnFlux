package web

import (
	"testing"
	"time"
)

func TestDurFormatting(t *testing.T) {
	dur := (&Server{}).templateFuncs()["dur"].(func(time.Duration) string)
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

func TestShortCommit(t *testing.T) {
	short := (&Server{}).templateFuncs()["shortCommit"].(func(string) string)
	for in, want := range map[string]string{
		"87f3c4279dbb69e6e680fdebea6d0d7988956af6": "87f3c42",
		"abc1234": "abc1234",
		"unknown": "unknown",
		"":        "",
	} {
		if got := short(in); got != want {
			t.Errorf("shortCommit(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestContainsFunc(t *testing.T) {
	contains := (&Server{}).templateFuncs()["contains"].(func([]string, string) bool)
	list := []string{"log-alerts", "mqtt-spok"}
	if !contains(list, "log-alerts") {
		t.Error("contains(list, present) = false, want true")
	}
	if contains(list, "sms") {
		t.Error("contains(list, absent) = true, want false")
	}
	if contains(nil, "anything") {
		t.Error("contains(nil, s) = true, want false")
	}
}

// TestTimezoneDisplay pins the configured display timezone: a UTC-locked
// server shows UTC wall times, an explicit zone shows local ones (the
// prod containers run UTC, which must not leak into the panels).
func TestTimezoneDisplay(t *testing.T) {
	// 2026-10-08 00:23:45 UTC == 02:23:45 Europe/Warsaw (CEST).
	instant := time.Date(2026, 10, 8, 0, 23, 45, 0, time.UTC)

	utc := &Server{}
	utc.SetTimezone(time.UTC)
	if got := utc.templateFuncs()["timeFull"].(func(time.Time) string)(instant); got != "2026-10-08 00:23:45" {
		t.Errorf("utc timeFull = %q", got)
	}

	warsaw, err := time.LoadLocation("Europe/Warsaw")
	if err != nil {
		t.Fatal(err)
	}
	pl := &Server{}
	pl.SetTimezone(warsaw)
	if got := pl.templateFuncs()["timeFull"].(func(time.Time) string)(instant); got != "2026-10-08 02:23:45" {
		t.Errorf("warsaw timeFull = %q", got)
	}
	if got := pl.templateFuncs()["timeShort"].(func(time.Time) string)(instant); got != "02:23:45" {
		t.Errorf("warsaw timeShort = %q", got)
	}
	if got := pl.templateFuncs()["timeHMS"].(func(string) string)("2026-10-08T00:23:45Z"); got != "02:23:45" {
		t.Errorf("warsaw timeHMS = %q", got)
	}
	if got := pl.templateFuncs()["timeFullStr"].(func(string) string)("2026-10-08T00:23:45Z"); got != "2026-10-08 02:23:45" {
		t.Errorf("warsaw timeFullStr = %q", got)
	}
}
