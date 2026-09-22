package mqttreceiver

import "strings"

// MatchFilter reports whether topic matches an MQTT subscription filter
// using standard MQTT wildcard semantics ('+' = one level, '#' = any
// number of trailing levels). Filter validation happens in configuration;
// this function is only called with already-validated filters.
func MatchFilter(topic, filter string) bool {
	tsegs := strings.Split(topic, "/")
	fsegs := strings.Split(filter, "/")
	for i, f := range fsegs {
		if f == "#" {
			// '#' matches every remaining level, including zero, but must
			// be the last segment (guaranteed by config validation).
			return true
		}
		if i >= len(tsegs) {
			return false
		}
		if f != "+" && f != tsegs[i] {
			return false
		}
	}
	return len(fsegs) == len(tsegs)
}
