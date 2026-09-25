// Package notify describes the user-selectable delivery channels
// (notification media). Every recipient starts subscribed to all channels;
// the account page lets them opt out per channel, and the rule engine
// filters per-channel recipient lists accordingly. Group-scoped channels
// (like Discord) are listed for visibility even though their delivery is
// not per-recipient.
package notify

// ChannelDef is one user-selectable delivery channel. Kind is the stable
// identifier persisted in user settings; Label is the human-readable name
// shown in the UI.
type ChannelDef struct {
	Kind  string
	Label string
}

// Channels is the ordered set of known delivery channels; the order is
// the UI order. A recipient-medium channel additionally needs the
// corresponding recipient filter in the store (see GroupRecipientEmails
// and GroupRecipientAPRS).
var Channels = []ChannelDef{
	{Kind: "aprs", Label: "APRS (radio)"},
	{Kind: "smtp", Label: "Email (SMTP)"},
	{Kind: "discord", Label: "Discord"},
}

// Known reports whether kind is one of the registered channels.
func Known(kind string) bool {
	for _, c := range Channels {
		if c.Kind == kind {
			return true
		}
	}
	return false
}
