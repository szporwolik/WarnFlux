package aprs

import (
	"regexp"
	"strings"
)

// callsignRE is the accepted APRS callsign shape: 1-6 alphanumeric
// characters plus an optional -SSID (0..15; we accept 0..99 to stay
// tolerant of sloppy stations).
var callsignRE = regexp.MustCompile(`^[A-Z0-9]{1,6}(-[0-9]{1,2})?$`)

// NormalizeCallsign uppercases and trims a callsign so that all call sites
// compare the same identity. It does not validate.
func NormalizeCallsign(s string) string {
	return strings.ToUpper(strings.TrimSpace(s))
}

// ValidCallsign reports whether the callsign (after normalization) has an
// accepted shape. The callsign becomes an MQTT topic segment, so the shape
// is deliberately restrictive.
func ValidCallsign(s string) bool {
	return callsignRE.MatchString(NormalizeCallsign(s))
}
