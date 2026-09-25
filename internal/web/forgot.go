// Self-service password recovery: a user requests a reset from the login
// page, receives a one-time token link by email and sets a new password.
// The configured admin account is explicitly out of scope — its password
// lives in configuration.
package web

import (
	"crypto/rand"
	"errors"
	"net/http"
	"net/url"
	"strings"

	"github.com/szporwolik/WarnFlux/internal/storage"
)

// SetPasswordResetMailer installs the delivery function for reset emails
// (to, subject, plain body). A nil mailer disables email delivery: the
// self-service flow then tells the user to contact an administrator.
func (s *Server) SetPasswordResetMailer(fn func(to, subject, text string) error) {
	s.resetMailer = fn
}

// forgotView is the /forgot page model.
type forgotView struct {
	AppTitle string
	Name     string
	Header1  string
	Header2  string
	Tagline  string
	Version  string
	Commit   string
	RepoURL  string
	CSRF     string

	Error   string
	Message string

	Username string
	Email    string
}

// handleForgotPage renders the request form.
func (s *Server) handleForgotPage(w http.ResponseWriter, r *http.Request) {
	if s.sessions.currentSession(r) != nil {
		http.Redirect(w, r, "/dashboard", http.StatusSeeOther)
		return
	}
	csrf, err := newCSRFCookiePath(w, s.cfg.Auth.SecureCookie, "/")
	if err != nil {
		s.logger.Error("web: forgot csrf token generation failed", "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	s.render(w, "forgot", forgotView{
		AppTitle: s.cfg.Title,
		Name:     s.displayName(),
		Header1:  s.displayHeader1(),
		Header2:  s.cfg.Header2,
		Tagline:  s.cfg.Tagline,
		Version:  s.version,
		Commit:   s.commit,
		RepoURL:  repoURL,
		CSRF:     csrf,
	})
}

// handleForgotSubmit validates the request and mails a reset link. The
// response is deliberately identical for unknown accounts (no username
// enumeration); only the configured admin account gets an explicit
// message: its password is defined in the configuration file.
func (s *Server) handleForgotSubmit(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	cookie, _ := r.Cookie(csrfCookie)
	if cookie == nil || !csrfOK(r.PostFormValue("csrf"), cookie.Value) {
		http.Error(w, "invalid csrf token", http.StatusForbidden)
		return
	}
	username := strings.ToLower(strings.TrimSpace(r.PostFormValue("username")))
	email := strings.TrimSpace(r.PostFormValue("email"))

	view := func(errMsg, okMsg string) {
		csrf, err := newCSRFCookiePath(w, s.cfg.Auth.SecureCookie, "/")
		if err != nil {
			s.logger.Error("web: forgot csrf token generation failed", "error", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Cache-Control", "no-store")
		s.render(w, "forgot", forgotView{
			AppTitle: s.cfg.Title,
			Name:     s.displayName(),
			Header1:  s.displayHeader1(),
			Header2:  s.cfg.Header2,
			Tagline:  s.cfg.Tagline,
			Version:  s.version,
			Commit:   s.commit,
			RepoURL:  repoURL,
			CSRF:     csrf,
			Error:    errMsg,
			Message:  okMsg,
			Username: username,
			Email:    email,
		})
	}

	// Rate limit: same exponential-lockout gate as the login form.
	limiterKey := "forgot\x00" + username + "\x00" + r.RemoteAddr
	if wait := s.loginLimiter.retryIn(limiterKey); wait > 0 {
		s.logger.Warn("web: forgot throttled", "remote", r.RemoteAddr, "username", username)
		w.Header().Set("Retry-After", "30")
		http.Error(w, "too many attempts, retry later", http.StatusTooManyRequests)
		return
	}

	// The configured admin account is managed in configuration.
	if checkUsername(username, s.cfg.Auth.Username) {
		s.loginLimiter.record(limiterKey, false)
		view("admin pass is defined in the configuration file", "")
		return
	}

	u, err := s.users.GetUserByUsername(username)
	if err != nil || u.IsAdmin || strings.TrimSpace(u.Email) == "" ||
		!strings.EqualFold(strings.TrimSpace(u.Email), email) {
		s.loginLimiter.record(limiterKey, false)
		// Generic answer — no account enumeration.
		view("", "If the account exists and the email matches, a reset link has been sent to its registered address.")
		return
	}

	token, err := s.users.CreatePasswordReset(u.ID)
	if err != nil {
		s.logger.Error("web: password reset token failed", "username", username, "error", err)
		s.loginLimiter.record(limiterKey, false)
		view("", "If the account exists and the email matches, a reset link has been sent to its registered address.")
		return
	}

	if s.resetMailer != nil {
		link := s.resetLink(r) + "/reset?token=" + url.QueryEscape(token)
		subject := s.displayName() + " — password reset"
		body := "A password reset was requested for the WarnFlux account \"" + u.Username + "\".\n\n" +
			"Open this link to set a new password (it expires in one hour):\n" + link + "\n\n" +
			"If you did not request this, ignore this message — your password stays unchanged.\n"
		if err := s.resetMailer(strings.TrimSpace(u.Email), subject, body); err != nil {
			s.logger.Warn("web: password reset email failed", "username", username, "error", err)
			s.loginLimiter.record(limiterKey, false)
			view("The reset email could not be sent — please contact an administrator.", "")
			return
		}
	}
	s.loginLimiter.record(limiterKey, true)
	view("", "If the account exists and the email matches, a reset link has been sent to its registered address.")
}

// resetLink builds the public base URL for reset links: the configured
// domain (production identity) or the request host.
func (s *Server) resetLink(r *http.Request) string {
	scheme := "http"
	if s.cfg.Auth.SecureCookie {
		scheme = "https"
	}
	if d := strings.TrimSpace(s.cfg.Domain); d != "" {
		return scheme + "://" + d
	}
	return scheme + "://" + r.Host
}

// resetView is the /reset page model.
type resetView struct {
	AppTitle string
	Name     string
	Header1  string
	Header2  string
	Tagline  string
	Version  string
	Commit   string
	RepoURL  string
	CSRF     string

	Token   string
	Error   string
	Expired bool
}

// handleResetPage renders the new-password form for a token link. An
// invalid token renders the expired state instead of the form.
func (s *Server) handleResetPage(w http.ResponseWriter, r *http.Request) {
	token := strings.TrimSpace(r.URL.Query().Get("token"))
	csrf, err := newCSRFCookiePath(w, s.cfg.Auth.SecureCookie, "/")
	if err != nil {
		s.logger.Error("web: reset csrf token generation failed", "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	view := resetView{
		AppTitle: s.cfg.Title,
		Name:     s.displayName(),
		Header1:  s.displayHeader1(),
		Header2:  s.cfg.Header2,
		Tagline:  s.cfg.Tagline,
		Version:  s.version,
		Commit:   s.commit,
		RepoURL:  repoURL,
		CSRF:     csrf,
		Token:    token,
	}
	if token == "" {
		view.Expired = true
		view.Error = "This link is invalid or has expired — request a new one."
	} else if err := s.users.PeekPasswordReset(token); errors.Is(err, storage.ErrPasswordResetInvalid) {
		view.Expired = true
		view.Error = "This link is invalid or has expired — request a new one."
	}
	w.Header().Set("Cache-Control", "no-store")
	s.render(w, "reset", view)
}

// handleResetSubmit consumes the token and replaces the password.
func (s *Server) handleResetSubmit(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	cookie, _ := r.Cookie(csrfCookie)
	if cookie == nil || !csrfOK(r.PostFormValue("csrf"), cookie.Value) {
		http.Error(w, "invalid csrf token", http.StatusForbidden)
		return
	}
	token := strings.TrimSpace(r.PostFormValue("token"))
	password := r.PostFormValue("password")

	renderErr := func(msg string, expired bool) {
		csrf, err := newCSRFCookiePath(w, s.cfg.Auth.SecureCookie, "/")
		if err != nil {
			s.logger.Error("web: reset csrf token generation failed", "error", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Cache-Control", "no-store")
		w.WriteHeader(http.StatusUnprocessableEntity)
		s.render(w, "reset", resetView{
			AppTitle: s.cfg.Title,
			Name:     s.displayName(),
			Header1:  s.displayHeader1(),
			Header2:  s.cfg.Header2,
			Tagline:  s.cfg.Tagline,
			Version:  s.version,
			Commit:   s.commit,
			RepoURL:  repoURL,
			CSRF:     csrf,
			Token:    token,
			Error:    msg,
			Expired:  expired,
		})
	}

	if password == "" || len(password) < 8 || len(password) > 72 {
		renderErr("password must be 8-72 characters", false)
		return
	}

	userID, err := s.users.ConsumePasswordReset(token)
	if errors.Is(err, storage.ErrPasswordResetInvalid) {
		renderErr("This link is invalid or has expired — request a new one.", true)
		return
	}
	if err != nil {
		s.logger.Error("web: reset token consume failed", "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	u, err := s.users.GetUser(userID)
	if err != nil {
		s.logger.Error("web: reset user lookup failed", "user", userID, "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if err := s.users.SetUserPassword(userID, password); err != nil {
		if errors.Is(err, storage.ErrUserProtected) {
			renderErr("admin pass is defined in the configuration file", true)
			return
		}
		s.logger.Error("web: reset password set failed", "user", userID, "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	s.audit(u.Username, "password-reset", "")
	http.Redirect(w, r, "/login?reset=1", http.StatusSeeOther)
}

// resetPasswordAlphabet avoids ambiguous characters (0/O, 1/l/I, ...).
const resetPasswordAlphabet = "abcdefghjkmnpqrstuvwxyzABCDEFGHJKMNPQRSTUVWXYZ23456789"

// newRandomPassword returns a 16-character password drawn from a
// confusion-free alphabet.
func newRandomPassword() (string, error) {
	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	out := make([]byte, 16)
	for i, b := range raw {
		out[i] = resetPasswordAlphabet[int(b)%len(resetPasswordAlphabet)]
	}
	return string(out), nil
}
