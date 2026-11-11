package gddkiadatex

import (
	"testing"
	"time"
)

// TestParseTimeUsesTheProviderZone is WF-REV-07: KPD writes Polish local time
// with no offset, so a station whose time.Local is UTC must still read
// "2026-07-01T12:00:00" as 10:00 UTC (CEST) and the winter form as 11:00 UTC.
// An explicit offset in the payload keeps its own meaning.
func TestParseTimeUsesTheProviderZone(t *testing.T) {
	saved := time.Local
	time.Local = time.UTC // the failure mode: a container that never got a TZ
	t.Cleanup(func() { time.Local = saved })

	cases := []struct {
		in   string
		want time.Time
	}{
		{"2026-07-01T12:00:00", time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)},
		{"2026-01-15T12:00:00", time.Date(2026, 1, 15, 11, 0, 0, 0, time.UTC)},
		{"2026-07-01T12:00:00+02:00", time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)},
		{"2026-01-15T12:00:00Z", time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC)},
	}
	for _, tc := range cases {
		got, ok := parseTime(tc.in)
		if !ok {
			t.Errorf("parseTime(%q) failed", tc.in)
			continue
		}
		if !got.Equal(tc.want) {
			t.Errorf("parseTime(%q) = %s, want %s", tc.in, got.UTC(), tc.want)
		}
	}
}

// TestParseTimeIsIndependentOfStationLocal pins the same instant under two
// different station zones: the provider convention alone decides.
func TestParseTimeIsIndependentOfStationLocal(t *testing.T) {
	warsaw, err := time.LoadLocation("Europe/Warsaw")
	if err != nil {
		t.Skip("no zoneinfo database on this host")
	}
	saved := time.Local
	t.Cleanup(func() { time.Local = saved })

	const in = "2026-07-01T12:00:00"
	var instants []time.Time
	for _, local := range []*time.Location{time.UTC, warsaw} {
		time.Local = local
		got, ok := parseTime(in)
		if !ok {
			t.Fatalf("parseTime(%q) failed with time.Local=%s", in, local)
		}
		instants = append(instants, got.UTC())
	}
	if !instants[0].Equal(instants[1]) {
		t.Errorf("the same payload read %s under UTC and %s under Warsaw; the zone must not matter",
			instants[0], instants[1])
	}
}
