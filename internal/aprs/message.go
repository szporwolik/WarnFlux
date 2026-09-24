package aprs

import (
	"fmt"
	"strings"
)

// EncodeMessageLine renders an outbound APRS message frame for injection
// into APRS-IS:
//
//	SP9MOA-10>APRS,TCPIP*::SP9XYZ    :text
//
// The addressee is padded to the protocol's fixed 9-character field.
func EncodeMessageLine(src, to, text string) string {
	return fmt.Sprintf("%s>APRS,TCPIP*::%-9s:%s", NormalizeCallsign(src), NormalizeCallsign(to), text)
}

// AckSuffixLen is the length of the longest ack suffix ("{12345}"). The
// hub appends it after the message text, so text trimmed for ack-tracked
// delivery must reserve this room.
const AckSuffixLen = 7

// diacritics maps Polish diacritics to their ASCII base letters; other
// non-ASCII characters are dropped so APRS frames stay clean 7-bit ASCII.
var diacritics = map[rune]rune{
	'ą': 'a', 'ć': 'c', 'ę': 'e', 'ł': 'l', 'ń': 'n', 'ó': 'o', 'ś': 's', 'ź': 'z', 'ż': 'z',
	'Ą': 'A', 'Ć': 'C', 'Ę': 'E', 'Ł': 'L', 'Ń': 'N', 'Ó': 'O', 'Ś': 'S', 'Ź': 'Z', 'Ż': 'Z',
}

// TrimMessageText normalizes outbound message text to the APRS limit
// (see LimitMessageText with no reserved room).
func TrimMessageText(text string) string {
	return LimitMessageText(text, 0)
}

// LimitMessageText normalizes outbound APRS message text: surrounding
// whitespace removed, newlines collapsed, Polish diacritics transliterated
// (other non-ASCII dropped — APRS is a 7-bit medium), and the result
// shortened to MaxMessageText-reserve bytes. Shortening prefers cutting at
// the last word boundary so a frame never looks chopped mid-word.
func LimitMessageText(text string, reserve int) string {
	var b strings.Builder
	for _, r := range text {
		switch {
		case r < 0x80:
			b.WriteRune(r)
		case diacritics[r] != 0:
			b.WriteRune(diacritics[r])
		default:
			// Drop other non-ASCII characters entirely.
		}
	}
	text = strings.Join(strings.Fields(b.String()), " ")
	max := MaxMessageText - reserve
	if max < 10 {
		max = 10
	}
	if len(text) <= max {
		return text
	}
	if i := strings.LastIndexByte(text[:max], ' '); i > max/2 {
		text = text[:i]
	} else {
		text = text[:max]
	}
	return strings.TrimSpace(text)
}

// SeverityAbbrev maps the canonical severity to a compact radio-safe
// abbreviation used inside APRS messages.
func SeverityAbbrev(severity string) string {
	switch strings.ToUpper(strings.TrimSpace(severity)) {
	case "SEVERE":
		return "SEV"
	case "MODERATE":
		return "MOD"
	case "MINOR":
		return "MIN"
	case "EXTREME":
		return "EXT"
	case "UNKNOWN":
		return "UNK"
	}
	return ""
}

// BuildAlertMessage renders an APRS notification from event metadata in
// priority order: the headline is the most valuable part and is never
// shortened unless it alone exceeds the limit; less important components
// (event, severity, prefix) are dropped first. reserve is the room the
// caller needs for a trailing {id} ack suffix.
func BuildAlertMessage(prefix, severity, event, headline string, reserve int) string {
	prefix = strings.TrimSpace(prefix)
	event = strings.TrimSpace(event)
	headline = strings.TrimSpace(headline)
	if headline == "" {
		if event != "" {
			headline, event = event, ""
		} else {
			headline = "WarnFlux alert"
		}
	}
	sev := SeverityAbbrev(severity)

	candidates := [][]string{
		{prefix, sev, event + ":", headline},
		{prefix, sev + ":", headline},
		{sev, event + ":", headline},
		{sev + ":", headline},
		{headline},
	}
	max := MaxMessageText - reserve
	for _, parts := range candidates {
		var partsOut []string
		for _, p := range parts {
			if t := strings.TrimSpace(p); t != "" && t != ":" {
				partsOut = append(partsOut, p)
			}
		}
		text := strings.Join(partsOut, " ")
		if text == "" {
			continue
		}
		if len(text) <= max {
			return LimitMessageText(text, reserve)
		}
	}
	// The headline alone exceeds the limit: shorten at a word boundary.
	return LimitMessageText(headline, reserve)
}
