package core

import (
	"strings"
	"testing"
	"time"
)

// TestWeatherRadiationValidation pins the radiation contract: non-negative
// magnitudes only, and the values survive the wire roundtrip.
func TestWeatherRadiationValidation(t *testing.T) {
	usvh, cpm := 0.12, 15.0
	snap := WeatherSnapshot{
		SchemaVersion: WeatherSchemaVersion,
		GeneratedAt:   time.Now().UTC(),
		Provider:      WeatherProvider{ID: "aprs", Name: "APRS", Attribution: "test"},
		Location:      WeatherLocation{ID: "sp9wx", Name: "SP9WX", Latitude: 50.9, Longitude: 19.8, Timezone: "UTC"},
		Current: &WeatherCurrent{
			Time:          time.Now().UTC(),
			Condition:     ConditionUnknown,
			RadiationUSvh: &usvh,
			RadiationCPM:  &cpm,
		},
	}
	if err := snap.Validate(); err != nil {
		t.Fatalf("valid snapshot rejected: %v", err)
	}
	wire, err := MarshalWeatherSnapshot(snap)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	for _, want := range []string{`"radiation_usv_h":0.12`, `"radiation_cpm":15`} {
		if !strings.Contains(string(wire), want) {
			t.Fatalf("wire missing %s: %s", want, wire)
		}
	}

	neg := -0.1
	snap.Current.RadiationUSvh = &neg
	if err := snap.Validate(); err == nil || !strings.Contains(err.Error(), "radiation") {
		t.Fatalf("negative radiation accepted: %v", err)
	}
}
