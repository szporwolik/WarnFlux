package web

import (
	"net/http"
)

// websiteView is the /website page model: the visitor analytics moved
// out of the MQTT traffic page into their own admin section.
type websiteView struct {
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

	NavDashboard     bool
	NavUsers         bool
	NavGroups        bool
	NavLogs          bool
	NavAudit         bool
	NavAPRS          bool
	NavMessages      bool
	NavGSM           bool
	NavMeshtastic    bool
	NavMeshMap       bool
	NavTraffic       bool
	NavWebsite       bool
	NavNotifications bool

	NavHealth  bool
	NavConfig  bool
	NavCompose bool
	NavEmcom   bool
	NavHelp    bool
	NavMass    bool
	NavAccount bool
}

// handleWebsitePage renders the admin Website analytics section.
func (s *Server) handleWebsitePage(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	v := websiteView{
		AppTitle:   s.cfg.Title,
		Name:       s.displayName(),
		Header1:    s.displayHeader1(),
		Header2:    s.DisplayHeader2(),
		Tagline:    s.DisplayTagline(),
		Version:    s.version,
		Commit:     s.commit,
		RepoURL:    repoURL,
		CSRF:       sess.csrf,
		Username:   sess.username,
		Role:       sess.role,
		NavWebsite: true,
	}
	w.Header().Set("Cache-Control", "no-store")
	s.renderL(w, r, "website", v)
}
