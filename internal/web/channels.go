package web

import (
	"sort"

	"github.com/szporwolik/WarnFlux/internal/action"
	"github.com/szporwolik/WarnFlux/internal/plugin"
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

// sourceKinds maps source plugin types onto their public presentation
// (the "where the data comes from" list). Informational feeds (weather,
// aircraft) are listed too — they enrich the map, but the descriptions
// say so.
var sourceKinds = map[string]channelKind{
	"rso": {
		Icon:        "i-exclamation-triangle",
		Name:        "RSO / Alert RCB",
		Description: "Official government emergency alerts (Alert RCB) from the RSO service.",
	},
	"imgw": {
		Icon:        "i-cloud-sun",
		Name:        "IMGW-PIB",
		Description: "Official weather and hydrological warnings from the Polish meteo service.",
	},
	"gddkia": {
		Icon:        "i-speedometer2",
		Name:        "GDDKiA",
		Description: "Road works, closures and difficulties on national roads.",
	},
	"gios": {
		Icon:        "i-shield-lock",
		Name:        "GIOŚ (industrial accidents)",
		Description: "Serious industrial accidents reported to the environmental inspectorate.",
	},
	"giosaq": {
		Icon:        "i-heart-pulse",
		Name:        "GIOŚ (air quality)",
		Description: "Official air-pollution exceedances: PM10, PM2.5, ozone and more.",
	},
	"aprs-inet": {
		Icon:        "i-broadcast-pin",
		Name:        "APRS network",
		Description: "Radio amateurs' stations and messages heard over the air.",
	},
	"aprs-radio": {
		Icon:        "i-broadcast-pin",
		Name:        "APRS network",
		Description: "Radio amateurs' stations and messages heard over the air.",
	},
	"openmeteo": {
		Icon:        "i-sun",
		Name:        "Open-Meteo",
		Description: "Weather forecasts for the region (informational).",
	},
	"metar": {
		Icon:        "i-cloud-sun",
		Name:        "Aviation weather (METAR)",
		Description: "Current weather at regional airports (informational).",
	},
	"adsb": {
		Icon:        "i-lightning-charge",
		Name:        "ADS-B aircraft",
		Description: "Aircraft traffic over the area (informational).",
	},
}

// sourceRank orders the public source list: official alert feeds first,
// informational feeds last.
func sourceRank(v publicChannelView) int {
	switch v.Name {
	case "RSO / Alert RCB":
		return 1
	case "IMGW-PIB":
		return 2
	case "GDDKiA":
		return 3
	case "GIOŚ (industrial accidents)":
		return 4
	case "GIOŚ (air quality)":
		return 5
	case "APRS network":
		return 6
	default:
		return 7
	}
}

// publicSources builds the friendly public source list from plugin
// statuses: enabled SOURCE instances only, one row per feed (duplicate
// instances of one type collapse), unknown types hidden, in a stable
// friendly order.
func publicSources(statuses []plugin.PluginStatus) []publicChannelView {
	seen := make(map[string]bool, len(statuses))
	var out []publicChannelView
	for _, st := range statuses {
		if st.Kind != plugin.KindSource || st.State == plugin.StateDisabled || seen[st.Type] {
			continue
		}
		kind, ok := sourceKinds[st.Type]
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
	sort.SliceStable(out, func(i, j int) bool {
		return sourceRank(out[i]) < sourceRank(out[j])
	})
	return out
}
