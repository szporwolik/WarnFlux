// Self-service account page: member and emcom sessions edit their own
// contact data and password here. The role is never self-assignable —
// admins manage roles on the Users page.
package web

import (
	"net/http"
	"strings"

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

	Phone   string
	Email   string
	Discord string

	Msg   string
	Error string

	NavDashboard     bool
	NavUsers         bool
	NavGroups        bool
	NavTest          bool
	NavCompose       bool
	NavAccount       bool
	NavLogs          bool
	NavAudit         bool
	NavTraffic       bool
	NavNotifications bool
	NavHealth        bool
}

// landingForRole maps an authenticated session's role to its landing page.
func landingForRole(role string) string {
	switch role {
	case "emcom":
		return "/compose"
	case "member":
		return "/account"
	default:
		return "/dashboard"
	}
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
		Role:       u.Role,
		Phone:      u.Phone,
		Email:      u.Email,
		Discord:    u.Discord,
		NavAccount: true,
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
	if r.PostFormValue("csrf") == "" || r.PostFormValue("csrf") != sess.csrf {
		http.Error(w, "invalid csrf token", http.StatusForbidden)
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

	if msg := validateAccountForm(phone, email, discord, password); msg != "" {
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
			Role:       u.Role,
			Phone:      phone,
			Email:      email,
			Discord:    discord,
			Error:      msg,
			NavAccount: true,
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
