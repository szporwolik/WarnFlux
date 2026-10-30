package timesync

import (
	"testing"
	"time"
)

// TestParseNMERMC pins the sentence the discipline actually uses: the date
// comes from RMC, the status decides whether the time may be trusted, and the
// sub-second part is kept.
func TestParseNMERMC(t *testing.T) {
	line := "$GNRMC,235958.00,A,5002.08,N,02013.04,E,0.03,0.0,291026,,,A*76"
	fix, ok, err := ParseNMEA(line)
	if err != nil || !ok {
		t.Fatalf("ParseNMEA = (%v, %v), want a usable sentence", ok, err)
	}
	want := time.Date(2026, 10, 29, 23, 59, 58, 0, time.UTC)
	if !fix.Time.Equal(want) {
		t.Fatalf("time = %s, want %s", fix.Time, want)
	}
	if !fix.Valid {
		t.Fatal("status A must be a valid fix")
	}
	if fix.Satellites != 0 {
		t.Errorf("RMC carries no satellite count, got %d", fix.Satellites)
	}
}

// TestParseNMEAVoidFix documents the rule that protects the station: a
// receiver without a fix still prints its own clock, and that clock must never
// discipline anything.
func TestParseNMEAVoidFix(t *testing.T) {
	fix, ok, err := ParseNMEA("$GNRMC,120000.00,V,,,,,,,010120,,,N*62")
	if err != nil || !ok {
		t.Fatalf("ParseNMEA = (%v, %v)", ok, err)
	}
	if fix.Valid {
		t.Fatal("status V must not be a valid fix")
	}
	if fix.Time.Year() != 2020 {
		t.Fatalf("time = %s, want the sentence's own date parsed anyway", fix.Time)
	}
}

// TestParseNMEAZDA covers the second time-bearing sentence.
func TestParseNMEAZDA(t *testing.T) {
	fix, ok, err := ParseNMEA("$GNZDA,101530.25,29,10,2026,00,00*75")
	if err != nil || !ok {
		t.Fatalf("ParseNMEA = (%v, %v)", ok, err)
	}
	want := time.Date(2026, 10, 29, 10, 15, 30, 250000000, time.UTC)
	if !fix.Time.Equal(want) {
		t.Fatalf("time = %s, want %s", fix.Time, want)
	}
	if !fix.Valid {
		t.Fatal("ZDA always carries a usable date")
	}
}

// TestParseNMEAGGAReadsSatellites keeps the satellite count, which the status
// file reports so the operator can see why a GPS fix is missing.
func TestParseNMEAGGAReadsSatellites(t *testing.T) {
	fix, ok, err := ParseNMEA("$GNGGA,101530.00,5002.08,N,02013.04,E,1,09,0.9,200.0,M,0.0,M,,*4D")
	if err != nil || !ok {
		t.Fatalf("ParseNMEA = (%v, %v)", ok, err)
	}
	if fix.Satellites != 9 {
		t.Fatalf("satellites = %d, want 9", fix.Satellites)
	}
	if !fix.Time.IsZero() {
		t.Fatal("GGA carries no date, so it must never yield a time")
	}
}

// TestParseNMEARejectsBadChecksumAndJunk guards the reader against noise:
// a corrupted sentence must be reported, unrelated lines ignored.
func TestParseNMEARejectsBadChecksumAndJunk(t *testing.T) {
	if _, ok, err := ParseNMEA("$GNRMC,235958.00,A,5002.08,N,02013.04,E,0.03,0.0,291026,,,A*00"); ok || err == nil {
		t.Fatalf("bad checksum = (ok %v, err %v), want an error", ok, err)
	}
	if _, ok, err := ParseNMEA("some log line"); ok || err != nil {
		t.Fatalf("junk = (ok %v, err %v), want (false, nil)", ok, err)
	}
}
