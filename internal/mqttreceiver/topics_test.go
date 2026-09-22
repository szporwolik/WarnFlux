package mqttreceiver

import "testing"

func TestParseTopicStrict(t *testing.T) {
	cases := []struct {
		topic string
		want  TopicKind
	}{
		{"warnflux/events", TopicEvents},
		{"warnflux/status", TopicStatus},
		{"warnflux/active/imgw-meteo/" + TopicHash("imgw-meteo:123"), TopicActive},
		{"warnflux/info/openmeteo/weather-home/home/weather", TopicInfo},
		// Strict rejections.
		{"warnflux/bogus", TopicUnknown},
		{"otherprefix/events", TopicUnknown},
		{"warnflux/events/extra", TopicUnknown},
		{"warnflux/active/imgw-meteo", TopicUnknown},
		{"warnflux/active/imgw-meteo/nothex", TopicUnknown},
		{"warnflux/active/UPPER/" + TopicHash("x:y"), TopicUnknown},
		{"warnflux/info/too/few/segments", TopicUnknown},
		{"warnflux/info/a/b/c/d/e", TopicUnknown},
		{"warnflux/status/", TopicUnknown},
	}
	for _, c := range cases {
		if got := ParseTopic("warnflux", c.topic).Kind; got != c.want {
			t.Errorf("ParseTopic(%q) = %v, want %v", c.topic, got, c.want)
		}
	}
}

func TestParseTopicActiveFields(t *testing.T) {
	pt := ParseTopic("warnflux", "warnflux/active/imgw-meteo/"+TopicHash("imgw-meteo:123"))
	if pt.Kind != TopicActive || pt.Source != "imgw-meteo" || pt.Hash != TopicHash("imgw-meteo:123") {
		t.Errorf("parsed = %+v", pt)
	}
}

func TestParseTopicInfoFields(t *testing.T) {
	pt := ParseTopic("warnflux", "warnflux/info/openmeteo/weather-home/home/weather")
	if pt.Kind != TopicInfo || pt.InfoSource != "openmeteo" || pt.ProducerID != "weather-home" || pt.Key != "home" || pt.InfoKind != "weather" {
		t.Errorf("parsed = %+v", pt)
	}
}

func TestSubscriptions(t *testing.T) {
	subs := Subscriptions("warnflux")
	want := []string{
		"warnflux/active/#",
		"warnflux/info/#",
		"warnflux/status",
		"warnflux/events",
	}
	if len(subs) != len(want) {
		t.Fatalf("subscriptions = %v", subs)
	}
	for i := range want {
		if subs[i] != want[i] {
			t.Errorf("subscriptions[%d] = %q, want %q", i, subs[i], want[i])
		}
	}
}
