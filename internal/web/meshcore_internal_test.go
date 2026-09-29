package web

import "testing"

// TestCardinalDirection pins the 8-wind mapping used in the nodes table.
func TestCardinalDirection(t *testing.T) {
	cases := []struct {
		bearing float64
		want    string
	}{
		{0, "N"}, {22, "N"}, {23, "NE"}, {45, "NE"}, {89, "E"},
		{90, "E"}, {135, "SE"}, {180, "S"}, {225, "SW"}, {270, "W"},
		{315, "NW"}, {337, "NW"}, {338, "N"}, {360, "N"},
	}
	for _, c := range cases {
		if got := cardinalDirection(c.bearing); got != c.want {
			t.Errorf("cardinalDirection(%v) = %q, want %q", c.bearing, got, c.want)
		}
	}
}
