package mqttreceiver

import "testing"

func TestMatchFilter(t *testing.T) {
	cases := []struct {
		topic  string
		filter string
		want   bool
	}{
		{"home/living/temp", "home/#", true},
		{"home", "home/#", true},
		{"home/living/temp", "home/+/temp", true},
		{"home/living/temp", "home/+/humidity", false},
		{"home/living/deep/temp", "home/+/temp", false},
		{"alarm/door/state", "alarm/+/state", true},
		{"alarm/door/latch", "alarm/+/state", false},
		{"club/alarm/door", "club/alarm/#", true},
		{"other/club/alarm", "club/#", false},
		{"a/b/c", "a/b/c", true},
		{"a/b/c", "a/b", false},
		{"a/b", "a/b/c", false},
	}
	for _, c := range cases {
		if got := MatchFilter(c.topic, c.filter); got != c.want {
			t.Errorf("MatchFilter(%q, %q) = %v, want %v", c.topic, c.filter, got, c.want)
		}
	}
}
