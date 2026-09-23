// Package severity owns WarnFlux's single canonical severity scale. It is
// the ONLY vocabulary the routing engine understands:
//
//	unknown < minor < moderate < severe < extreme
//
// Source adapters map their provider scales onto it (e.g. IMGW degree 1 →
// moderate, 2 → severe, 3 → extreme); the raw provider value may be kept
// alongside as ProviderSeverity for diagnostics, but it never participates
// in routing decisions.
package severity

import "strings"

// Canonical severity values, lowest to highest.
const (
	Unknown  = "unknown"
	Minor    = "minor"
	Moderate = "moderate"
	Severe   = "severe"
	Extreme  = "extreme"
)

// All lists the canonical values in ascending order.
func All() []string {
	return []string{Unknown, Minor, Moderate, Severe, Extreme}
}

// rank maps each canonical value to its total order.
var rank = map[string]int{
	Unknown:  0,
	Minor:    1,
	Moderate: 2,
	Severe:   3,
	Extreme:  4,
}

// Valid reports whether s is exactly one of the canonical values.
func Valid(s string) bool {
	_, ok := rank[s]
	return ok
}

// Rank returns the canonical rank of a canonical value and whether the
// value is valid at all. Invalid strings report 0, false.
func Rank(s string) (int, bool) {
	r, ok := rank[s]
	return r, ok
}

// Normalize trims and lowercases s and reports whether the result is
// canonical. Providers publishing "SEVERE" or " Severe " still land on
// the canonical value; anything outside the vocabulary is rejected, so
// provider text can never leak into routing decisions.
func Normalize(s string) (string, bool) {
	s = strings.ToLower(strings.TrimSpace(s))
	return s, Valid(s)
}
