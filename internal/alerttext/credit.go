package alerttext

import "strings"

// creditBySource is the short provider tag a constrained channel (SMS, APRS,
// Meshtastic) appends to a message. Borrowed data has to say whose it is, but
// those channels have room for a name and no room for a link: the credit is
// the provider's short form, the same one operators say on the air.
//
// Only providers whose data we republish need an entry; a source missing here
// is our own observation (or a warning we render ourselves) and carries no
// credit. The long form of the credit — display name and link — lives in the
// web layer, where there is room for it.
var creditBySource = map[string]string{
	"gddkia-datex": "GDDKiA",
	"gddkia":       "GDDKiA",
}

// Credit returns the provider tag for a source id, or "" when the message
// needs no credit.
func Credit(source string) string {
	return creditBySource[strings.ToLower(strings.TrimSpace(source))]
}

// IDSuffix is what a radio frame carries after the alert text: the short
// message ID (operators cite it on the air) and, when the data are borrowed,
// the provider credit. Returns "" when neither applies.
func IDSuffix(msgID, source string) string {
	out := ""
	if id := strings.TrimSpace(msgID); id != "" {
		out = "ID:" + id
	}
	if c := Credit(source); c != "" {
		if out == "" {
			return c
		}
		out += " " + c
	}
	return out
}
