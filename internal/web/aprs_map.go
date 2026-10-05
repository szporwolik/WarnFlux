package web

import "net/http"

// aprsMapView is the /aprs admin map page model: a simple browser over
// every station the APRS hub currently holds, with an ALL / APRS-IS /
// APRS-RF filter.
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

	OfflineMode bool

	NavDashboard     bool
	NavUsers         bool
	NavGroups        bool
	NavCompose       bool
	NavEmcom         bool
	NavAccount       bool
	NavLogs          bool
	NavAudit         bool
	NavMessages      bool
	NavAPRS          bool
	NavMeshtastic    bool
	NavTraffic       bool
	NavNotifications bool
	NavHealth        bool
	NavConfig        bool
}

// handleAPRSMapPage renders the admin APRS station-map browser. The
// station data comes from the retained hub state through the shared
// /api/aprs/stations?all=1 endpoint (which also includes weather
// stations, unlike the public home endpoint).
func (s *Server) handleAPRSMapPage(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	v := aprsMapView{
		AppTitle:    s.cfg.Title,
		Name:        s.displayName(),
		Header1:     s.displayHeader1(),
		Header2:     s.cfg.Header2,
		Tagline:     s.cfg.Tagline,
		Version:     s.version,
		Commit:      s.commit,
		RepoURL:     repoURL,
		CSRF:        sess.csrf,
		Username:    sess.username,
		Role:        sess.role,
		NavAPRS:     true,
		OfflineMode: s.OfflineMode(),
	}
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
	s.renderL(w, r, "aprsmap", v)
}
