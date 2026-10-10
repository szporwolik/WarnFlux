package web

import (
	"net/http"
)

// helpSection is one block of the Help manual: a title, an icon and a
// short list of instructions (i18n keys, resolved in the page language).
type helpSection struct {
	Title string // i18n key
	Icon  string // sprite id (without the i- prefix? kept full: "i-house")
	Items []string
}

// helpView is the Help page model — a very short manual for users and
// operators, rendered in the signed-in user's UI language.
type helpView struct {
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

	Sections []helpSection

	NavDashboard     bool
	NavUsers         bool
	NavGroups        bool
	NavCompose       bool
	NavEmcom         bool
	NavAccount       bool
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
	NavHealth        bool
	NavConfig        bool
	NavHelp          bool
	NavMass          bool
}

// buildHelpView assembles the manual sections (content lives in i18n).
func (s *Server) buildHelpView(lang string) helpView {
	return helpView{
		Lang:     lang,
		AppTitle: s.cfg.Title,
		Name:     s.displayName(),
		Header1:  s.displayHeader1(),
		Header2:  s.DisplayHeader2(),
		Tagline:  s.DisplayTagline(),
		Version:  s.version,
		Commit:   s.commit,
		RepoURL:  repoURL,
		NavHelp:  true,
		Sections: []helpSection{
			{Title: "help.general.title", Icon: "i-info-circle", Items: []string{
				"help.general.1", "help.general.2",
			}},
			{Title: "help.dashboard.title", Icon: "i-house", Items: []string{
				"help.dashboard.1", "help.dashboard.2",
			}},
			{Title: "help.messages.title", Icon: "i-pencil-square", Items: []string{
				"help.messages.1", "help.messages.2",
			}},
			{Title: "help.emcom.title", Icon: "i-megaphone", Items: []string{
				"help.emcom.1", "help.emcom.2", "help.emcom.3",
			}},
			{Title: "help.aprs.title", Icon: "i-broadcast-pin", Items: []string{
				"help.aprs.1",
			}},
			{Title: "help.gsm.title", Icon: "i-telephone", Items: []string{
				"help.gsm.1", "help.gsm.2",
			}},
			{Title: "help.meshtastic.title", Icon: "i-broadcast", Items: []string{
				"help.meshtastic.1",
			}},
			{Title: "help.account.title", Icon: "i-person", Items: []string{
				"help.account.1",
			}},
			{Title: "help.access.title", Icon: "i-people", Items: []string{
				"help.access.1",
			}},
			{Title: "help.config.title", Icon: "i-gear", Items: []string{
				"help.config.1",
			}},
		},
	}
}

// handleHelpPage renders the Help manual for any signed-in user.
func (s *Server) handleHelpPage(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	view := s.buildHelpView(s.langFor(r))
	view.CSRF = sess.csrf
	view.Username = sess.username
	view.Role = sess.role
	w.Header().Set("Cache-Control", "no-store")
	s.renderL(w, r, "help", view)
}
