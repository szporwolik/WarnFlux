package aprs

import "testing"

func TestOriginFromPath(t *testing.T) {
	cases := []struct {
		path []string
		want Origin
	}{
		// Heard over the radio by an i-gate.
		{[]string{"WIDE1-1", "SR9NR*", "qAR", "SR9NR"}, OriginRF},
		{[]string{"qAU", "SR9IG"}, OriginRF},
		// Injected directly from the internet.
		{[]string{"TCPIP*", "qAC", "T2POLAND"}, OriginInternet},
		{[]string{"TCPXX*", "qAX", "T2POLAND"}, OriginInternet},
		{[]string{"qAC", "T2POLAND"}, OriginInternet},
		{[]string{"qAo"}, OriginInternet},
		{[]string{"qAZ", "T2POLAND"}, OriginInternet},
		{[]string{"qAS", "T2POLAND"}, OriginInternet},
		// No usable marker.
		{nil, OriginUnknown},
		{[]string{"WIDE1-1", "SR9NR*"}, OriginUnknown},
	}
	for _, c := range cases {
		if got := OriginFromPath(c.path); got != c.want {
			t.Errorf("OriginFromPath(%v) = %q, want %q", c.path, got, c.want)
		}
	}
}
