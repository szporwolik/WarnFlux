package web

import (
	"sort"

	"github.com/szporwolik/WarnFlux/internal/action"
	"github.com/szporwolik/WarnFlux/internal/i18n"
	"github.com/szporwolik/WarnFlux/internal/plugin"
)

// channelKind describes one delivery medium's presentation: the icon and
// the i18n key prefix for its name and description. The internal
// "logger" action is never shown to the public.
type channelKind struct {
	Icon string
	Key  string
	// URL is the provider's public page (sources only). The sources page
	// links to it: the data belongs to the provider, and a reader who wants
	// the details — or a copy for another application — needs the address.
	// The burze.dzis.net API terms require exactly this attribution.
	URL string
}

// channelKinds maps action types onto their public presentation. Several
// types share one key when they are the same medium to the public
// ("aprs" and "aprs-out" are both APRS radio).
var channelKinds = map[string]channelKind{
	"smtp":         {Icon: "i-box-arrow-right", Key: "channels.email"},
	"aprs":         {Icon: "i-broadcast-pin", Key: "channels.aprs"},
	"aprs-out":     {Icon: "i-broadcast-pin", Key: "channels.aprs"},
	"meshtastic":   {Icon: "i-broadcast", Key: "channels.meshtastic"},
	"discord":      {Icon: "i-people", Key: "channels.discord"},
	"sms":          {Icon: "i-telephone", Key: "channels.sms"},
	"http_webhook": {Icon: "i-puzzle", Key: "channels.webhook"},
}

// channelRank orders the rendered view rows: email first, then radio
// (APRS, then mesh), then chat, then generic integrations.
var channelRank = map[string]int{
	"smtp": 1, "aprs": 2, "aprs-out": 2, "meshtastic": 3, "sms": 4,
	"discord": 5, "http_webhook": 6,
}

// publicChannels builds the friendly public channel list from action
// statuses: enabled instances only, one row per medium (duplicate
// instances of one type collapse), internal types hidden, in a stable
// friendly order. Names and descriptions come from the page language
// catalog.
func publicChannels(statuses []action.Status, lang string) []publicChannelView {
	// Deduplication is by the PRESENTATION key, not by plugin type: two
	// types behind one medium (aprs and aprs-out) must yield one row, which
	// is what the reader sees.
	seen := make(map[string]bool, len(statuses))
	type ranked struct {
		view publicChannelView
		rank int
	}
	var out []ranked
	for _, st := range statuses {
		if !st.Enabled {
			continue
		}
		kind, ok := channelKinds[st.Type]
		if !ok || seen[kind.Key] {
			continue
		}
		seen[kind.Key] = true
		out = append(out, ranked{
			view: publicChannelView{
				Icon:        kind.Icon,
				Name:        i18n.T(lang, kind.Key+".name"),
				Description: i18n.T(lang, kind.Key+".desc"),
			},
			rank: channelRank[st.Type],
		})
	}
	// The statuses arrive sorted by instance ID, not by medium priority:
	// re-sort by the fixed friendly order.
	sort.SliceStable(out, func(i, j int) bool { return out[i].rank < out[j].rank })
	views := make([]publicChannelView, len(out))
	for i, r := range out {
		views[i] = r.view
	}
	return views
}

// sourceKinds maps source plugin types onto their public presentation
// (the "where the data comes from" list). Informational feeds (weather,
// aircraft) are listed too — they enrich the map, but the descriptions
// say so.
var sourceKinds = map[string]channelKind{
	"rso":        {Icon: "i-exclamation-triangle", Key: "sources.rso", URL: "https://komunikaty.tvp.pl/"},
	"imgw":       {Icon: "i-cloud-sun", Key: "sources.imgw", URL: "https://danepubliczne.imgw.pl/"},
	"gddkia":     {Icon: "i-speedometer2", Key: "sources.gddkia", URL: "https://www.gov.pl/web/gddkia"},
	"gios":       {Icon: "i-shield-lock", Key: "sources.gios", URL: "https://www.gios.gov.pl/"},
	"giosaq":     {Icon: "i-heart-pulse", Key: "sources.giosaq", URL: "https://powietrze.gios.gov.pl/"},
	"burzedzis":  {Icon: "i-lightning-charge", Key: "sources.burzedzis", URL: "https://burze.dzis.net/"},
	"aprs-inet":  {Icon: "i-broadcast-pin", Key: "sources.aprs", URL: "https://www.aprs-is.net/"},
	"aprs-radio": {Icon: "i-broadcast-pin", Key: "sources.aprs", URL: "https://www.aprs-is.net/"},
	"meshtastic": {Icon: "i-broadcast", Key: "sources.meshtastic", URL: "https://meshtastic.org/"},
	"openmeteo":  {Icon: "i-sun", Key: "sources.openmeteo", URL: "https://open-meteo.com/"},
	"metar":      {Icon: "i-cloud-sun", Key: "sources.metar", URL: "https://aviationweather.gov/"},
	"adsb":       {Icon: "i-lightning-charge", Key: "sources.adsb", URL: "https://adsb.lol/"},
	// Feeds that are NOT plugins: the station's own messages, the operator's
	// public HTTP endpoints and the EMCOM readiness levels. They are rows of
	// the routing matrix too, so the public list must not pretend the
	// station only listens to foreign providers.
	"compose": {Icon: "i-pencil-square", Key: "sources.compose"},
	"ingest":  {Icon: "i-puzzle", Key: "sources.ingest"},
	"emcom":   {Icon: "i-megaphone", Key: "sources.emcom"},
}

// sourceRank orders the public source list: official alert feeds first,
// informational feeds last.
var sourceRank = map[string]int{
	"rso": 1, "imgw": 2, "gddkia": 3, "burzedzis": 4, "gios": 5, "giosaq": 6,
	"aprs-inet": 7, "aprs-radio": 7, "meshtastic": 8, "openmeteo": 9,
	"adsb": 10, "metar": 11,
	// The station's own feeds close the list: they are not providers.
	"compose": 12, "ingest": 13, "emcom": 14,
}

// operatorSources is what the public source list needs beyond the plugin
// statuses: the feeds the station produces itself rather than receiving from
// a provider. They are shown only when they exist (an endpoint, a network),
// except the station's own messages, which every installation can issue.
type operatorSources struct {
	// Ingest is the number of configured public HTTP ingest endpoints.
	Ingest int
	// Emcom is the number of managed EMCOM networks.
	Emcom int
}

// publicSources builds the friendly public source list: every enabled SOURCE
// instance, one row per feed (duplicate instances of one type collapse),
// unknown types hidden, plus the operator-side feeds, in a stable friendly
// order. Names and descriptions come from the page language catalog.
func publicSources(statuses []plugin.PluginStatus, op operatorSources, lang string) []publicChannelView {
	// One row per FEED, not per plugin type: the two APRS flavours are one
	// network to the reader, and a station running both must not list it
	// twice.
	seen := make(map[string]bool, len(statuses))
	type ranked struct {
		view publicChannelView
		rank int
	}
	var out []ranked
	for _, st := range statuses {
		if st.Kind != plugin.KindSource || st.State == plugin.StateDisabled {
			continue
		}
		kind, ok := sourceKinds[st.Type]
		if !ok || seen[kind.Key] {
			continue
		}
		seen[kind.Key] = true
		out = append(out, ranked{
			view: publicChannelView{
				Icon:        kind.Icon,
				Name:        i18n.T(lang, kind.Key+".name"),
				Description: i18n.T(lang, kind.Key+".desc"),
				URL:         kind.URL,
			},
			rank: sourceRank[st.Type],
		})
	}
	// The station's own feeds, appended before the ordering so the rank map
	// stays the single place that decides the reading order.
	add := func(slug string) {
		kind := sourceKinds[slug]
		out = append(out, ranked{
			view: publicChannelView{
				Icon:        kind.Icon,
				Name:        i18n.T(lang, kind.Key+".name"),
				Description: i18n.T(lang, kind.Key+".desc"),
				URL:         kind.URL,
			},
			rank: sourceRank[slug],
		})
	}
	add("compose") // every station can issue its own messages
	if op.Ingest > 0 {
		add("ingest")
	}
	if op.Emcom > 0 {
		add("emcom")
	}

	sort.SliceStable(out, func(i, j int) bool { return out[i].rank < out[j].rank })
	views := make([]publicChannelView, len(out))
	for i, r := range out {
		views[i] = r.view
	}
	return views
}
