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

// TrimMessageText normalizes outbound message text to the APRS limit:
// surrounding whitespace removed, newlines collapsed, truncated to
// MaxMessageText bytes.
func TrimMessageText(text string) string {
	text = strings.Join(strings.Fields(text), " ")
	if len(text) > MaxMessageText {
		text = text[:MaxMessageText]
	}
	return text
}
