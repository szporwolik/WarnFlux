package web

import (
	"sort"

	"github.com/szporwolik/WarnFlux/internal/action"
)

// channelKind describes one delivery medium in plain language. The
// internal "logger" action is never shown to the public.
type channelKind struct {
	Icon        string
	Name        string
	Description string
}

// channelKinds maps action types onto their public presentation.
var channelKinds = map[string]channelKind{
	"smtp": {
		Icon:        "i-box-arrow-right",
		Name:        "Email",
		Description: "Hazard alerts land in the mailboxes of the group members.",
	},
	"aprs": {
		Icon:        "i-broadcast-pin",
		Name:        "APRS radio",
		Description: "Alerts go out as text messages to ham radio operators over the air.",
	},
	"aprs-out": {
		Icon:        "i-broadcast-pin",
		Name:        "APRS radio",
		Description: "Alerts go out as text messages to ham radio operators over the air.",
	},
	"discord": {
		Icon:        "i-people",
		Name:        "Discord",
		Description: "Alerts are posted on the community Discord channel.",
	},
	"http_webhook": {
		Icon:        "i-puzzle",
		Name:        "Webhook",
		Description: "Alerts are delivered to an external system.",
	},
}

// publicChannels builds the friendly public channel list from action
// statuses: enabled instances only, one row per medium (duplicate
// instances of one type collapse), internal types hidden, in a stable
// friendly order.
func publicChannels(statuses []action.Status) []publicChannelView {
	seen := make(map[string]bool, len(statuses))
	var out []publicChannelView
	for _, st := range statuses {
		if !st.Enabled || seen[st.Type] {
			continue
		}
		kind, ok := channelKinds[st.Type]
		if !ok {
			continue
		}
		seen[st.Type] = true
		out = append(out, publicChannelView{
			Icon:        kind.Icon,
			Name:        kind.Name,
			Description: kind.Description,
		})
	}
	// The statuses arrive sorted by instance ID, not by medium priority:
	// re-sort by the fixed friendly order.
	sort.SliceStable(out, func(i, j int) bool {
		return typeRank(out[i]) < typeRank(out[j])
	})
	return out
}

// typeRank orders the rendered view rows: email first, then radio, then
// chat, then generic integrations.
func typeRank(v publicChannelView) int {
	switch v.Name {
	case "Email":
		return 1
	case "APRS radio":
		return 2
	case "Discord":
		return 3
	default:
		return 4
	}
}
