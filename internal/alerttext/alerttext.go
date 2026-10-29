// Package alerttext holds the wording rules shared by every outbound channel
// that has to fit a hazard into a few dozen characters (APRS, SMS, Meshtastic).
package alerttext

import "strings"

// summaryCap bounds what Summary returns: provider descriptions open with the
// location and the incident and then drift into boilerplate, and every channel
// caps its text anyway.
const summaryCap = 200

// Summary condenses a provider description into the one paragraph a short
// channel can carry: a trailing note block after a blank line is dropped
// ("IMGW probability: 70%."), whitespace is collapsed, and the result is cut at
// a word boundary. Punctuation is deliberately NOT used to end the summary:
// Polish descriptions are full of abbreviations ("ul.", "pow.", "al."), and
// cutting at the first period left "Jerzmanowice ul" as the whole message.
func Summary(text string) string {
	if i := strings.Index(text, "\n\n"); i > 0 {
		text = text[:i]
	}
	text = strings.Join(strings.Fields(text), " ")
	if len(text) > summaryCap {
		if i := strings.LastIndexByte(text[:summaryCap], ' '); i > summaryCap/2 {
			text = text[:i]
		} else {
			text = text[:summaryCap]
		}
	}
	return strings.TrimSpace(text)
}

// Body picks what an outbound message says about a hazard beyond its severity
// and prefix. A headline that differs from the event label carries
// information; a headline that repeats it (GDDKiA, RSO and some IMGW records
// publish the same string in both fields) does not, so the first sentence of
// the description is used instead. Returns "" when neither carries anything.
func Body(event, headline, description string) string {
	event = strings.Join(strings.Fields(event), " ")
	headline = strings.Join(strings.Fields(headline), " ")
	if headline != "" && !strings.EqualFold(headline, event) {
		return headline
	}
	return Summary(description)
}
