package storage

// Severity values flow through WarnFlux as free-form provider strings, but
// group routing needs a stable total order for threshold comparisons. The
// canonical ranking (lowest to highest) is the IMGW degree vocabulary:
//
//	unknown < minor < moderate < severe < extreme
//
// Providers that use other vocabularies are simply unranked: their events
// satisfy only the "unknown" (deliver everything) threshold.
var severityRank = map[string]int{
	"unknown":  0,
	"minor":    1,
	"moderate": 2,
	"severe":   3,
	"extreme":  4,
}

// ValidSeverity reports whether s is a canonical severity value accepted
// as a group routing threshold.
func ValidSeverity(s string) bool {
	_, ok := severityRank[s]
	return ok
}

// SeverityRank returns the canonical rank of a severity value and whether
// the value is ranked at all. Unknown strings report 0, false.
func SeverityRank(s string) (int, bool) {
	r, ok := severityRank[s]
	return r, ok
}
