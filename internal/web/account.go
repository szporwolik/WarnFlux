// Self-service account page: member and emcom sessions edit their own
// contact data and password here. The role is never self-assignable —
// admins manage roles on the Users page.
package web

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/szporwolik/WarnFlux/internal/notify"
	"github.com/szporwolik/WarnFlux/internal/storage"
)

// accountFlash maps the post-action redirect marker to a banner message.
var accountFlash = map[string]string{
	"saved": "Account updated.",
}

// accountFlashErr is reserved for future error flash markers.
var accountFlashErr = map[string]string{}

// accountView is the /account page model.
type accountView struct {
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

	// ManagedAccount marks the configured admin account: its contact
	// data, username, role and password always come from configuration
	// and the form renders read-only (no server-side writes either).
	ManagedAccount bool

	Phone   string
	Email   string
	Discord string

	// Groups is the notification-group list with the current membership
	// mirrored in GroupSet: every new user is subscribed to all groups by
	// default and can unsubscribe here.
	Groups   []storage.Group
	GroupSet map[int64]bool

	// Channels is the delivery-channel list (APRS, email, future media);
	// ChannelSet mirrors the enabled kinds — unchecked boxes become
	// per-user opt-outs.
	Channels   []notify.ChannelDef
	ChannelSet map[string]bool

	Msg   string
	Error string

	NavDashboard     bool
	NavUsers         bool
	NavGroups        bool
	NavCompose       bool
	NavAccount       bool
	NavLogs          bool
	NavAudit         bool
	NavTraffic       bool
	NavNotifications bool
	NavHealth        bool
}

// landingForRole maps an authenticated session's role to its landing page:
// the main dashboard is the shared home surface for every role (admin,
// emcom, member alike).
func landingForRole(_ string) string {
	return "/dashboard"
}

// handleAccountPage renders the signed-in user's own data.
func (s *Server) handleAccountPage(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	if sess == nil {
		http.Redirect(w, r, "/login", http.StatusSeeOther)
		return
	}
	u, err := s.users.GetUserByUsername(sess.username)
	if err != nil {
		if err == storage.ErrUserNotFound {
			// The configured admin account has no editable record.
			http.Redirect(w, r, "/users", http.StatusSeeOther)
			return
		}
		s.logger.Warn("web: account lookup failed", "username", sess.username, "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	groups, err := s.users.ListAllGroups()
	if err != nil {
		s.logger.Warn("web: account group list failed", "username", sess.username, "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	memberIDs, err := s.users.GroupIDsForUser(u.ID)
	if err != nil {
		s.logger.Warn("web: account membership lookup failed", "username", sess.username, "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	groupSet := make(map[int64]bool, len(memberIDs))
	for _, id := range memberIDs {
		groupSet[id] = true
	}
	channelSet := make(map[string]bool, len(notify.Channels))
	for _, c := range notify.Channels {
		channelSet[c.Kind] = true
	}
	opts, err := s.users.UserChannelOptOuts(u.ID)
	if err != nil {
		s.logger.Warn("web: account channel opt-out lookup failed", "username", sess.username, "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	for kind := range opts {
		channelSet[kind] = false
	}

	v := accountView{
		AppTitle:       s.cfg.Title,
		Name:           s.displayName(),
		Header1:        s.displayHeader1(),
		Header2:        s.cfg.Header2,
		Tagline:        s.cfg.Tagline,
		Version:        s.version,
		Commit:         s.commit,
		RepoURL:        repoURL,
		CSRF:           sess.csrf,
		Username:       u.Username,
		Role:           sess.role,
		ManagedAccount: sess.role == "admin",
		Phone:          u.Phone,
		Email:          u.Email,
		Discord:        u.Discord,
		Groups:         groups,
		GroupSet:       groupSet,
		Channels:       notify.Channels,
		ChannelSet:     channelSet,
		NavAccount:     true,
	}
	msg := r.URL.Query().Get("msg")
	v.Msg = accountFlash[msg]
	v.Error = accountFlashErr[r.URL.Query().Get("error")]

	w.Header().Set("Cache-Control", "no-store")
	s.render(w, "account", v)
}

// handleAccountSave applies the self-service edit: contact fields and an
// optional new password. The username and role stay untouched.
func (s *Server) handleAccountSave(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	if sess == nil {
		http.Redirect(w, r, "/login", http.StatusSeeOther)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if !csrfOK(r.PostFormValue("csrf"), sess.csrf) {
		http.Error(w, "invalid csrf token", http.StatusForbidden)
		return
	}

	// The configured admin account is managed in configuration: no
	// self-service write may touch it, whatever the form says.
	if sess.role == "admin" {
		http.Error(w, "the configured admin account is managed in configuration and cannot be edited here", http.StatusForbidden)
		return
	}

	u, err := s.users.GetUserByUsername(sess.username)
	if err != nil {
		if err == storage.ErrUserNotFound {
			http.Redirect(w, r, "/users", http.StatusSeeOther)
			return
		}
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	phone := strings.TrimSpace(r.PostFormValue("phone"))
	email := strings.TrimSpace(r.PostFormValue("email"))
	discord := strings.TrimSpace(r.PostFormValue("discord"))
	password := r.PostFormValue("password")

	// Group subscriptions: checked boxes stay subscribed; everything
	// else is an unsubscribe.
	var wantGroups []int64
	seen := make(map[int64]bool)
	for _, v := range r.PostForm["groups"] {
		id, err := strconv.ParseInt(v, 10, 64)
		if err != nil || id <= 0 || seen[id] {
			continue
		}
		seen[id] = true
		wantGroups = append(wantGroups, id)
	}

	// Delivery channels: checked boxes stay enabled; every known channel
	// left unchecked becomes a per-user opt-out.
	checked := make(map[string]bool)
	for _, v := range r.PostForm["channels"] {
		if notify.Known(v) {
			checked[v] = true
		}
	}
	var optOuts []string
	for _, c := range notify.Channels {
		if !checked[c.Kind] {
			optOuts = append(optOuts, c.Kind)
		}
	}

	if msg := validateAccountForm(phone, email, discord, password); msg != "" {
		groups, gerr := s.users.ListAllGroups()
		if gerr != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		v := accountView{
			AppTitle:   s.cfg.Title,
			Name:       s.displayName(),
			Header1:    s.displayHeader1(),
			Header2:    s.cfg.Header2,
			Tagline:    s.cfg.Tagline,
			Version:    s.version,
			Commit:     s.commit,
			RepoURL:    repoURL,
			CSRF:       sess.csrf,
			Username:   u.Username,
			Role:       sess.role,
			Phone:      phone,
			Email:      email,
			Discord:    discord,
			Groups:     groups,
			GroupSet:   make(map[int64]bool),
			Channels:   notify.Channels,
			ChannelSet: checked,
			Error:      msg,
			NavAccount: true,
		}
		for _, id := range wantGroups {
			v.GroupSet[id] = true
		}
		w.Header().Set("Cache-Control", "no-store")
		w.WriteHeader(http.StatusBadRequest)
		s.render(w, "account", v)
		return
	}

	if _, err := s.users.UpdateUser(u.ID, u.Username, phone, email, discord, u.Role, password); err != nil {
		s.logger.Warn("web: account update failed", "username", sess.username, "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if err := s.users.SetUserGroups(u.ID, wantGroups); err != nil {
		s.logger.Warn("web: account subscription update failed", "username", sess.username, "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if err := s.users.SetUserChannelOptOuts(u.ID, optOuts); err != nil {
		s.logger.Warn("web: account channel update failed", "username", sess.username, "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	s.audit(sess.username, "account-update", "")
	http.Redirect(w, r, "/account?msg=saved", http.StatusSeeOther)
}

// validateAccountForm returns a human-readable problem or "".
func validateAccountForm(phone, email, discord, password string) string {
	if len(phone) > 32 || strings.ContainsAny(phone, "\r\n") {
		return "phone must be at most 32 characters"
	}
	if len(email) > 128 || strings.ContainsAny(email, " \r\n") {
		return "email must be at most 128 characters and contain no spaces"
	}
	if email != "" && !strings.Contains(email, "@") {
		return "email must contain '@'"
	}
	if len(discord) > 128 || strings.ContainsAny(discord, "\r\n") {
		return "discord must be at most 128 characters"
	}
	if password != "" && (len(password) < 8 || len(password) > 72) {
		return "password must be 8-72 characters"
	}
	return ""
}
