package aprs

import (
	"math"
	"testing"
)

func TestParseGridSquare(t *testing.T) {
	cases := []struct {
		locator   string
		wantLat   float64
		wantLon   float64
		wantError bool
	}{
		// JO90 is the Kraków field: 50-51N, 18-20E.
		{"JO90", 50.5, 19.0, false},
		{"JO90WW", 50 + 22.0/24 + 0.5/24, 18 + 44.0/24 + 1.0/24, false},
		{"jo90ww", 50 + 22.0/24 + 0.5/24, 18 + 44.0/24 + 1.0/24, false},
		{"JO90WW55", 50 + 22.0/24 + 5.0/240 + 0.5/240, 18 + 44.0/24 + 10.0/240 + 1.0/240, false},
		{"AA00", -89.5, -179, false},
		{"RR99", 89.5, 179, false},
		{"", 0, 0, true},
		{"J", 0, 0, true},
		{"J0", 0, 0, true},
		{"XYZ", 0, 0, true},
		{"JO90YY", 0, 0, true},  // Y is not a valid subsquare letter
		{"JO90WW5", 0, 0, true}, // odd length
	}
	for _, c := range cases {
		lat, lon, ok := ParseGridSquare(c.locator)
		if c.wantError {
			if ok {
				t.Errorf("ParseGridSquare(%q) = ok, want error", c.locator)
			}
			continue
		}
		if !ok {
			t.Errorf("ParseGridSquare(%q) failed", c.locator)
			continue
		}
		if math.Abs(lat-c.wantLat) > 1e-9 || math.Abs(lon-c.wantLon) > 1e-9 {
			t.Errorf("ParseGridSquare(%q) = (%v, %v), want (%v, %v)", c.locator, lat, lon, c.wantLat, c.wantLon)
		}
	}
}

func TestDistanceKM(t *testing.T) {
	// Warsaw (JO90WW center-ish) to Kraków (JO90WC center-ish): the
	// latitude difference is 0.5°, roughly 55.6 km.
	d := DistanceKM(50.875, 19.5, 50.375, 19.5)
	if d < 55 || d > 56.5 {
		t.Errorf("DistanceKM = %v km, want ~55.6 km", d)
	}
	// Same point.
	if d := DistanceKM(50, 20, 50, 20); d != 0 {
		t.Errorf("DistanceKM(same) = %v, want 0", d)
	}
}
