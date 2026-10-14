package web

import (
	"net/http"
	"sort"
	"strings"

	"github.com/szporwolik/WarnFlux/internal/plugin"
)

// aprsContactView is one recipient suggestion for the message form: the
// registered callsign (SSID kept) plus the owning username.
type aprsContactView struct {
	Callsign string
	Username string
}

// aprsMapView is the /aprs admin section model: a combined page with two
// tabs — the station map (every station the APRS hub currently holds,
// with an ALL / APRS-IS / APRS-RF filter) and the durable message
// history.
type aprsMapView struct {
	Lang     string
	AppTitle string
	Name     string
	Header1  string
	Header2  string
	Tagline  string
	Version  string
	Commit   string
	RepoURL  string
	CSRF     string
	Username string
	Role     string

	AprsEnabled   bool
	AprsCenterLat float64
	AprsCenterLon float64
	AprsOwnLat    float64
	AprsOwnLon    float64
	AprsRadiusKM  float64
	AprsCallsign  string

	// Links is the APRS-IS / RF status card (the GSM modem pill of this
	// page): one row per backend, refreshed by the poll below the h2.
	Links []aprsLinkView

	OfflineMode bool
	ForceTiles  bool

	// Messages tab (history payload + feedback; see aprsMessagesData).
	aprsMessagesData
	// Calls carries the recipient suggestions of the send form: every
	// APRS callsign registered for the users (SSID kept, sorted).
	Calls  []aprsContactView
	Error  string
	Sent   bool
	Beacon bool
	Tab    string // map | msgs

	NavDashboard     bool
	NavUsers         bool
	NavGroups        bool
	NavCompose       bool
	NavEmcom         bool
	NavHelp          bool
	NavMass          bool
	NavAccount       bool
	NavLogs          bool
	NavAudit         bool
	NavMessages      bool
	NavAPRS          bool
	NavGSM           bool
	NavMeshtastic    bool
	NavMeshMap       bool
	NavTraffic       bool
	NavWebsite       bool
	NavNotifications bool
	NavHealth        bool
	NavConfig        bool
}

// handleAPRSPage renders the combined APRS admin section: the station map
// (tab "map", the default) and the durable message history (tab "msgs")
// behind one sidebar entry. Map data comes from the retained hub state
// through the shared /api/aprs/stations?all=1 endpoint (which also
// includes weather stations, unlike the public home endpoint).
func (s *Server) handleAPRSPage(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	v := aprsMapView{
		AppTitle:    s.cfg.Title,
		Name:        s.displayName(),
		Header1:     s.displayHeader1(),
		Header2:     s.DisplayHeader2(),
		Tagline:     s.DisplayTagline(),
		Version:     s.version,
		Commit:      s.commit,
		RepoURL:     repoURL,
		CSRF:        sess.csrf,
		Username:    sess.username,
		Role:        sess.role,
		NavAPRS:     true,
		Tab:         "map",
		OfflineMode: s.OfflineMode(),
		ForceTiles:  s.forceTiles.Load(),
	}
	if r.URL.Query().Get("tab") == "msgs" {
		v.Tab = "msgs"
	}
	if errMsg := r.URL.Query().Get("err"); errMsg != "" {
		v.Error = errMsg
	}
	v.Sent = r.URL.Query().Get("sent") != ""
	v.Beacon = r.URL.Query().Get("beacon") != ""
	v.Dir = "all"
	if s.users != nil {
		if owners, err := s.users.APRSCallsignOwners(); err == nil {
			v.Calls = make([]aprsContactView, 0, len(owners))
			for callsign, username := range owners {
				v.Calls = append(v.Calls, aprsContactView{Callsign: callsign, Username: username})
			}
			sort.Slice(v.Calls, func(i, j int) bool {
				ci, cj := strings.ToLower(v.Calls[i].Callsign), strings.ToLower(v.Calls[j].Callsign)
				if ci != cj {
					return ci < cj
				}
				return v.Calls[i].Callsign < v.Calls[j].Callsign
			})
		}
	}
	if s.aprsMsgs != nil {
		s.fillAPRSMessages(r, &v.aprsMessagesData)
	}
	s.fillAPRSLinks(&v)
	if s.aprs != nil && s.aprs.Enabled() {
		v.AprsEnabled = true
		v.AprsCenterLat = s.aprs.AreaLat()
		v.AprsCenterLon = s.aprs.AreaLon()
		v.AprsOwnLat = s.aprs.OwnLat()
		v.AprsOwnLon = s.aprs.OwnLon()
		v.AprsRadiusKM = s.aprs.AreaRadius()
		v.AprsCallsign = s.aprs.Callsign()
	}
	w.Header().Set("Cache-Control", "no-store")
	s.renderL(w, r, "aprs", v)
}

// aprsLinkView is one APRS backend in the status card: which link it is, how
// its plugin reports itself and the raw detail that goes with the badge. The
// card mirrors the GSM modem pill, so an operator reads the same shape on
// every radio page.
type aprsLinkView struct {
	// LabelKey names the link ("aprs.is" / "aprs.rf").
	LabelKey string
	// TextKey is the state line ("aprs.link_ok" / "_down" / "_off").
	TextKey string
	// Badge is the badge class: ok (running), warn (configured, not running)
	// or muted (not configured at all).
	Badge string
	// Detail carries the plugin's own report: its one-line stats summary
	// while it runs, or the error that took it down.
	Detail string
	// State is the plugin state name, shown next to the badge while the link
	// is not running (starting, degraded, stopped, failed).
	State string
}

// backendPluginTypes maps the two APRS links onto the plugin types that
// provide them: the internet link and the KISS radio (Direwolf) one.
const (
	aprsInetPluginType  = "aprs-inet"
	aprsRadioPluginType = "aprs-radio"
)

// fillAPRSLinks reports the state of both APRS backends. Each link is a plugin
// of its own (aprs-inet talks to APRS-IS, aprs-radio to a KISS TNC such as
// Direwolf), so the card answers the two questions an operator asks separately:
// is the internet feed up, and is the radio reachable.
func (s *Server) fillAPRSLinks(v *aprsMapView) {
	v.Links = []aprsLinkView{
		s.aprsLink("aprs.is", aprsInetPluginType),
		s.aprsLink("aprs.rf", aprsRadioPluginType),
	}
}

// aprsLink summarizes every plugin instance of one backend type: the link is
// UP only when one of them is running, and a link with no plugin at all is
// simply not configured (muted, not a warning).
func (s *Server) aprsLink(labelKey, pluginType string) aprsLinkView {
	link := aprsLinkView{LabelKey: labelKey, TextKey: "aprs.link_off", Badge: "muted"}
	if s.router == nil {
		return link
	}
	configured, running := false, false
	for _, st := range s.router.Statuses() {
		if st.Type != pluginType {
			continue
		}
		configured = true
		if st.State == plugin.StateRunning {
			running = true
			if link.Detail == "" {
				link.Detail = st.LastSummary
			}
			continue
		}
		// A backend that is not running explains itself: the last error it
		// hit, or the state it is stuck in.
		if link.State == "" {
			link.State = string(st.State)
			link.Detail = st.LastError
		}
	}
	switch {
	case running:
		link.TextKey, link.Badge = "aprs.link_ok", "ok"
		link.State = ""
	case configured:
		link.TextKey, link.Badge = "aprs.link_down", "warn"
	default:
		link.Detail = ""
	}
	return link
}

// handlePartialAPRSLinks serves the polled APRS status card: a link that comes
// up, a TNC that drops and a Direwolf that is not there yet all show up in
// place, without a reload.
func (s *Server) handlePartialAPRSLinks(w http.ResponseWriter, r *http.Request) {
	v := aprsMapView{}
	s.fillAPRSLinks(&v)
	w.Header().Set("Cache-Control", "no-store")
	s.renderL(w, r, "aprs_links", v)
}
