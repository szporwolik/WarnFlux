package web

import (
	"net/http"
	"strconv"
	"strings"
)

// accessView is the merged /access page model: the user directory and
// the group administration share one page as two tabs.
type accessView struct {
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

	// Tab selects the active panel: "users" (default) or "groups".
	Tab string
	// Users / Groups carry the two panel models.
	Users  usersView
	Groups groupsView

	NavDashboard  bool
	NavUsers      bool
	NavGroups     bool
	NavLogs       bool
	NavAPRS       bool
	NavMessages   bool
	NavGSM        bool
	NavMeshtastic bool
	NavMeshMap    bool
	NavTraffic    bool
	NavWebsite    bool
	NavConfig     bool
	NavCompose    bool
	NavEmcom      bool
	NavHelp       bool
	NavMass       bool
	NavAccount    bool
}

// handleAccessPage renders the merged access page (?tab=users|groups).
func (s *Server) handleAccessPage(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	tab := r.URL.Query().Get("tab")
	if tab != "groups" {
		tab = "users"
	}
	view := s.buildAccessView(r, sess, tab)
	w.Header().Set("Cache-Control", "no-store")
	s.renderL(w, r, "access", view)
}

// buildAccessView assembles both panels and applies the active tab's
// ?edit= dialog prefill.
func (s *Server) buildAccessView(r *http.Request, sess *session, tab string) accessView {
	errMsg := s.csrfFlashMessage(r)
	users := s.buildUsersView(r, userForm{}, 0, errMsg)
	groups := s.buildGroupsView(r, groupForm{}, 0, errMsg)
	// Each panel hides itself when the ACTIVE tab is the other one.
	users.Tab = tab
	groups.Tab = tab

	switch tab {
	case "groups":
		if raw := r.URL.Query().Get("edit"); raw != "" {
			if id, err := strconv.ParseInt(raw, 10, 64); err == nil && id > 0 {
				if g, err := s.users.GetGroup(id); err == nil {
					groups.EditID = g.ID
					groups.Form = groupForm{Name: g.Name}
				}
			}
		}
	default:
		if raw := r.URL.Query().Get("edit"); raw != "" {
			if id, err := strconv.ParseInt(raw, 10, 64); err == nil && id > 0 {
				if u, err := s.users.GetUser(id); err == nil {
					users.EditID = u.ID
					users.AdminEdit = u.IsAdmin
					users.DialogOpen = true
					users.Form = userForm{
						Username:      u.Username,
						Phone:         u.Phone,
						Email:         u.Email,
						Discord:       u.Discord,
						Role:          u.Role,
						NotifLang:     u.Lang,
						APRSCallsigns: strings.Join(u.APRSCallsigns, " "),
						MeshtasticIDs: strings.Join(u.MeshtasticIDs, " "),
						GroupSet:      s.userGroupSet(u.ID),
						ChannelSet:    s.userChannelSet(u.ID),
					}
				}
			}
		}
	}

	return accessView{
		AppTitle:  s.cfg.Title,
		Name:      s.displayName(),
		Header1:   s.displayHeader1(),
		Header2:   s.DisplayHeader2(),
		Tagline:   s.DisplayTagline(),
		Version:   s.version,
		Commit:    s.commit,
		RepoURL:   repoURL,
		CSRF:      sess.csrf,
		Username:  sess.username,
		Role:      sess.role,
		Tab:       tab,
		Users:     users,
		Groups:    groups,
		NavUsers:  true,
		NavGroups: true,
	}
}
