// Package notify describes the user-selectable delivery channels
// (notification media). Every recipient starts subscribed to all channels;
// the account page lets them opt out per channel, and the rule engine
// filters per-channel recipient lists accordingly.
package notify

// ChannelDef is one user-selectable delivery channel. Kind is the stable
// identifier persisted in user settings; Label is the human-readable name
// shown in the UI.
type ChannelDef struct {
	Kind  string
	Label string
}

// Channels is the ordered set of known delivery channels; the order is
// the UI order. Adding a future medium only needs an entry here plus the
// corresponding recipient filter in the store.
var Channels = []ChannelDef{
	{Kind: "aprs", Label: "APRS (radio)"},
	{Kind: "smtp", Label: "Email (SMTP)"},
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
