package core

import (
	"testing"
	"time"
)

func sampleEvent() HazardEvent {
	eff := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	exp := eff.Add(24 * time.Hour)
	lat, lon := 50.06, 19.94
	return HazardEvent{
		Source:      "meteoalarm",
		SourceID:    "2.49.0.1.616.0.DEU",
		Category:    "met",
		Event:       "Rain",
		Severity:    "orange",
		Urgency:     "immediate",
		Certainty:   "likely",
		Headline:    "Heavy rain expected",
		Description: "Widespread heavy rain.",
		Instruction: "Avoid flooded areas.",
		EffectiveAt: &eff,
		ExpiresAt:   &exp,
		Latitude:    &lat,
		Longitude:   &lon,
		Areas:       []string{"DE-NW", "DE-RP"},
		Status:      StatusActive,
		SourceURL:   "https://example.invalid/alert",
	}
}

func TestFingerprintDeterministic(t *testing.T) {
	e := sampleEvent()
	a := Fingerprint(e)
	b := Fingerprint(e)
	if a != b {
		t.Errorf("fingerprint not deterministic: %q != %q", a, b)
	}
	if len(a) != 64 {
		t.Errorf("fingerprint length = %d, want 64 (SHA-256 hex)", len(a))
	}
}

func TestFingerprintIgnoresIngestionMetadata(t *testing.T) {
	e := sampleEvent()
	base := Fingerprint(e)

	// ReceivedAt and UpdatedAt must not affect the fingerprint.
	e.ReceivedAt = time.Now()
	e.UpdatedAt = time.Now().Add(time.Hour)
	if got := Fingerprint(e); got != base {
		t.Error("ReceivedAt/UpdatedAt must not change the fingerprint")
	}

	// Source and SourceID are identity, not content; changing them must
	// not change the fingerprint either.
	e.Source = "other"
	e.SourceID = "other-id"
	if got := Fingerprint(e); got != base {
		t.Error("Source/SourceID must not change the fingerprint")
	}
}

func TestFingerprintChangesOnContentChange(t *testing.T) {
	base := Fingerprint(sampleEvent())

	cases := []struct {
		name   string
		mutate func(*HazardEvent)
	}{
		{"severity", func(e *HazardEvent) { e.Severity = "red" }},
		{"description", func(e *HazardEvent) { e.Description = "Changed text." }},
		{"headline", func(e *HazardEvent) { e.Headline = "New headline" }},
		{"expiry", func(e *HazardEvent) { t := time.Now(); e.ExpiresAt = &t }},
		{"location", func(e *HazardEvent) { v := 12.3; e.Latitude = &v }},
		{"areas", func(e *HazardEvent) { e.Areas = []string{"PL-MA"} }},
		{"source url", func(e *HazardEvent) { e.SourceURL = "https://other.invalid" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := sampleEvent()
			tc.mutate(&e)
			if got := Fingerprint(e); got == base {
				t.Errorf("fingerprint did not change when %s changed", tc.name)
			}
		})
	}
}

func TestFingerprintIgnoresLifecycleStatus(t *testing.T) {
	// The fingerprint represents provider content only; lifecycle state is
	// handled separately so transitions (active -> cancelled) can be
	// detected without corrupting content hashes.
	base := Fingerprint(sampleEvent())
	e := sampleEvent()
	e.Status = StatusCancelled
	if got := Fingerprint(e); got != base {
		t.Error("lifecycle status must not affect the content fingerprint")
	}
}

func TestFingerprintAreaOrderStable(t *testing.T) {
	a := sampleEvent()
	a.Areas = []string{"DE-NW", "DE-RP"}
	b := sampleEvent()
	b.Areas = []string{"DE-RP", "DE-NW"}
	if Fingerprint(a) != Fingerprint(b) {
		t.Error("area order must not affect the fingerprint")
	}
}

func TestFingerprintNilAndEmptyAreasEqual(t *testing.T) {
	a := sampleEvent()
	a.Areas = nil
	b := sampleEvent()
	b.Areas = []string{}
	if Fingerprint(a) != Fingerprint(b) {
		t.Error("nil and empty areas must produce the same fingerprint")
	}
}

func TestFingerprintZeroTimeEqualsNil(t *testing.T) {
	a := sampleEvent()
	a.ExpiresAt = nil
	b := sampleEvent()
	zero := time.Time{}
	b.ExpiresAt = &zero
	if Fingerprint(a) != Fingerprint(b) {
		t.Error("nil and zero ExpiresAt must produce the same fingerprint")
	}
}

func TestFingerprintBoundaryCoordinatesStable(t *testing.T) {
	// Validation rejects NaN/Inf; extreme-but-valid coordinates must
	// fingerprint deterministically (marshal cannot fail).
	for _, coords := range [][2]float64{
		{-90, -180},
		{90, 180},
		{0, 0},
		{49.999999, 19.123456},
	} {
		e := sampleEvent()
		lat, lon := coords[0], coords[1]
		e.Latitude, e.Longitude = &lat, &lon
		a := Fingerprint(e)
		b := Fingerprint(e)
		if a != b {
			t.Fatalf("fingerprint unstable for %v", coords)
		}
	}
}
