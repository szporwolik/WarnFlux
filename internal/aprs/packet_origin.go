package aprs

import "strings"

// Origin classifies how a packet reached APRS-IS — whether it was
// transmitted over the radio and heard by an i-gate, or injected directly
// from the internet (APRSdroid, aprs.fi, clients without RF).
type Origin string

// Origin values.
const (
	// OriginRF marks packets heard over the radio by an i-gate
	// (qAR/qAU path constructs).
	OriginRF Origin = "rf"
	// OriginInternet marks packets injected directly via the internet
	// (TCPIP* paths or client q constructs qAC/qAX/qAo/qAZ/qAS).
	OriginInternet Origin = "internet"
	// OriginUnknown marks packets whose path carries no usable marker.
	OriginUnknown Origin = ""
)

// OriginFromPath classifies one APRS-IS path. Radio-originated packets
// (i-gated) win: a qAR/qAU construct means an i-gate heard the frame over
// the air, even when other segments exist.
func OriginFromPath(path []string) Origin {
	has := func(pred func(string) bool) bool {
		for _, seg := range path {
			if pred(seg) {
				return true
			}
		}
		return false
	}
	if has(func(s string) bool {
		u := strings.ToUpper(s)
		return strings.HasPrefix(u, "QAR") || strings.HasPrefix(u, "QAU")
	}) {
		return OriginRF
	}
	if has(func(s string) bool {
		u := strings.ToUpper(s)
		switch {
		case strings.HasPrefix(u, "TCPIP"), strings.HasPrefix(u, "TCPXX"):
			return true
		case strings.HasPrefix(u, "QAC"), strings.HasPrefix(u, "QAX"),
			strings.HasPrefix(u, "QAO"), strings.HasPrefix(u, "QAZ"),
			strings.HasPrefix(u, "QAS"):
			return true
		}
		return false
	}) {
		return OriginInternet
	}
	return OriginUnknown
}
