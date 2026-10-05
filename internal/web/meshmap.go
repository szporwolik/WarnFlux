package web

import "net/http"

// meshMapView is the /meshmap admin map page model: a simple browser
// over EVERY node the Meshtastic radio heard — no operational ring, no
// node TTL — with the hop count of the last packet per node.
type meshMapView struct {
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

	MeshEnabled bool
	SelfLat     float64
	SelfLon     float64
	SelfName    string
	CenterLat   float64
	CenterLon   float64

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
	NavMeshMap       bool
	NavTraffic       bool
	NavNotifications bool
	NavHealth        bool
	NavConfig        bool
}

// handleMeshMapPage renders the admin Meshtastic node-map browser. The
// node data comes from the shared /api/meshtastic/stations?all=1
// endpoint (no operational ring, no TTL — the whole heard directory).
func (s *Server) handleMeshMapPage(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	v := meshMapView{
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
		NavMeshMap:  true,
		OfflineMode: s.OfflineMode(),
	}
	if s.meshtastic != nil && s.meshtastic.Enabled() {
		v.MeshEnabled = true
		snap := s.meshtastic.Snapshot()
		v.SelfLat = snap.Self.Lat
		v.SelfLon = snap.Self.Lon
		v.SelfName = snap.Self.LongName
		v.CenterLat = snap.Self.Lat
		v.CenterLon = snap.Self.Lon
	}
	// Without our own position the initial view falls back to the
	// operational area (the APRS territory center).
	if v.CenterLat == 0 && v.CenterLon == 0 && s.aprs != nil && s.aprs.Enabled() {
		v.CenterLat = s.aprs.AreaLat()
		v.CenterLon = s.aprs.AreaLon()
	}
	w.Header().Set("Cache-Control", "no-store")
	s.renderL(w, r, "meshmap", v)
}
