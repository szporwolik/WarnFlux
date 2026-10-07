package web

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/szporwolik/WarnFlux/internal/i18n"
)

const (
	sessionCookie = "wf_session"
	csrfCookie    = "wf_csrf"
	sessionTTL    = 24 * time.Hour

	// maxSessions is the hard global capacity of the session store.
	maxSessions = 4096
	// maxSessionsPerUser bounds one account's concurrent sessions; at the
	// cap the OLDEST session of that user is evicted (explicit eviction),
	// so a login always succeeds for its owner.
	maxSessionsPerUser = 16
)

// errSessionCapacity is returned by newSession when the global session
// cap is reached and every existing session is still valid.
var errSessionCapacity = errors.New("web: session capacity exhausted")

// session is one authenticated session.
type session struct {
	// userID is the directory user's immutable ID (0 = the configured
	// admin account, which has no directory row). Account changes
	// (password, role, username, deletion) revoke every session of the
	// user ID, so stale sessions never keep old privileges.
	userID   int64
	username string
	// role is the access tier: "admin" (everything) or "emcom" (compose
	// only). Set once at login, never from the client.
	role      string
	csrf      string
	createdAt time.Time
	expires   time.Time
}

// sessionStore is a server-side, in-memory session store. Sessions being
// lost on restart is acceptable for v1.
type sessionStore struct {
	mu     sync.Mutex
	m      map[string]*session
	secure bool
}

func newSessionStore(secure bool) *sessionStore {
	return &sessionStore{m: make(map[string]*session), secure: secure}
}

// newSession creates a cryptographically random session token. Capacity
// is enforced explicitly: at the global cap with every session still
// valid the creation is REJECTED (caller returns 503), and at the
// per-user cap the OLDEST session of that user is EVICTED.
func (s *sessionStore) newSession(userID int64, username, role string) (token string, sess *session, err error) {
	tok, err := randomToken()
	if err != nil {
		return "", nil, err
	}
	csrf, err := randomToken()
	if err != nil {
		return "", nil, err
	}
	sess = &session{
		userID:    userID,
		username:  username,
		role:      role,
		csrf:      csrf,
		createdAt: time.Now(),
		expires:   time.Now().Add(sessionTTL),
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sweepLocked()

	// Per-user cap: evict the oldest session of the same account so a
	// login never fails because of its own leftovers.
	userSessions := 0
	var oldestTok string
	var oldestAt time.Time
	for t, sess := range s.m {
		if sess.userID != userID {
			continue
		}
		userSessions++
		if oldestTok == "" || sess.createdAt.Before(oldestAt) {
			oldestTok, oldestAt = t, sess.createdAt
		}
	}
	if userSessions >= maxSessionsPerUser {
		delete(s.m, oldestTok)
	}

	// Global cap: reject instead of silently growing beyond it.
	if len(s.m) >= maxSessions {
		return "", nil, errSessionCapacity
	}
	s.m[tok] = sess
	return tok, sess, nil
}

// get returns the session for a token, deleting expired sessions.
func (s *sessionStore) get(token string) *session {
	if token == "" {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.m[token]
	if !ok {
		return nil
	}
	if time.Now().After(sess.expires) {
		delete(s.m, token)
		return nil
	}
	return sess
}

func (s *sessionStore) delete(token string) {
	s.mu.Lock()
	delete(s.m, token)
	s.mu.Unlock()
}

// revokeUser invalidates every live session of one directory user
// (password reset, role/username change or deletion) and reports how many
// sessions were dropped. The configured admin (userID 0) is never revoked
// this way — its credentials live in configuration.
func (s *sessionStore) revokeUser(userID int64) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for tok, sess := range s.m {
		if sess.userID == userID {
			delete(s.m, tok)
			n++
		}
	}
	return n
}

func (s *sessionStore) sweepLocked() {
	now := time.Now()
	for tok, sess := range s.m {
		if now.After(sess.expires) {
			delete(s.m, tok)
		}
	}
}

// setSessionCookie writes the session cookie. The password or username are
// never stored in the cookie — only the opaque random token.
func (s *sessionStore) setSessionCookie(w http.ResponseWriter, token string) {
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookie,
		Value:    token,
		Path:     "/",
		HttpOnly: true,
		Secure:   s.secure,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   int(sessionTTL.Seconds()),
	})
}

func (s *sessionStore) clearSessionCookie(w http.ResponseWriter) {
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookie,
		Value:    "",
		Path:     "/",
		HttpOnly: true,
		Secure:   s.secure,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   -1,
	})
}

// currentSession extracts the authenticated session from the request.
func (s *sessionStore) currentSession(r *http.Request) *session {
	c, err := r.Cookie(sessionCookie)
	if err != nil {
		return nil
	}
	return s.get(c.Value)
}

// checkPassword compares the submitted password in constant time.
func checkPassword(got, want string) bool {
	if len(got) != len(want) {
		// Keep behavior timing-similar; the compare below is constant-time.
		subtle.ConstantTimeCompare([]byte(got), []byte(want))
		return false
	}
	return subtle.ConstantTimeCompare([]byte(got), []byte(want)) == 1
}

// checkUsername compares the submitted username in constant time.
func checkUsername(got, want string) bool {
	if len(got) != len(want) {
		subtle.ConstantTimeCompare([]byte(got), []byte(want))
		return false
	}
	return subtle.ConstantTimeCompare([]byte(got), []byte(want)) == 1
}

// csrfOK compares two CSRF tokens in constant time.
func csrfOK(got, want string) bool {
	if got == "" || want == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(got), []byte(want)) == 1
}

// requireStateChange is the common gate for every state-changing POST:
// the session must exist, the CSRF token must match in constant time,
// and — when the browser sends an Origin header — it must match the
// request's own host. The token stops the classical cross-site POST;
// the Origin check additionally stops same-site sibling origins that
// could submit a form without ever being able to read the token. On
// failure the response is written and false is returned.
func (s *Server) requireStateChange(w http.ResponseWriter, r *http.Request, sess *session) bool {
	if sess == nil || !csrfOK(r.PostFormValue("csrf"), sess.csrf) {
		http.Error(w, "invalid csrf token", http.StatusForbidden)
		return false
	}
	// "null" is the opaque origin of sandboxed contexts (e.g. embedded
	// webviews): it carries no cross-site information, so it is treated
	// like an absent Origin header. The CSRF token — which a cross-site
	// form can never read — remains the primary gate.
	if origin := r.Header.Get("Origin"); origin != "" && origin != "null" && !sameOriginHost(origin, r.Host) {
		http.Error(w, "cross-origin request rejected", http.StatusForbidden)
		return false
	}
	return true
}

// csrfMismatch reports whether the submitted token does NOT match the
// session token (without writing anything). Form endpoints use it to
// send the admin back with a friendly flash instead of a bare 403.
func csrfMismatch(r *http.Request, sess *session) bool {
	return sess == nil || !csrfOK(r.PostFormValue("csrf"), sess.csrf)
}

// redirectAfterCSRFMismatch sends the browser back to a form page with a
// flash: an admin usually hits this after signing in again in another
// tab — the open form still carried the previous session's token.
func (s *Server) redirectAfterCSRFMismatch(w http.ResponseWriter, r *http.Request, target string) {
	http.Redirect(w, r, target+"?err=csrf", http.StatusSeeOther)
}

// csrfFlashMessage resolves the ?err=csrf marker of a form page into the
// friendly session-changed flash ("" otherwise).
func (s *Server) csrfFlashMessage(r *http.Request) string {
	if r.URL.Query().Get("err") != "csrf" {
		return ""
	}
	return i18n.T(s.langFor(r), "common.session_changed")
}

// sameOriginHost reports whether the Origin header's host matches the
// request's own host. Scheme-agnostic: reverse proxies may terminate
// TLS, so only the host part is compared (a same-site different-origin
// request carries a DIFFERENT host and is rejected).
func sameOriginHost(origin, host string) bool {
	u, err := url.Parse(origin)
	if err != nil || u.Host == "" {
		return false
	}
	return strings.EqualFold(u.Host, host)
}

// loginLimiter slows brute-force guessing of the admin/directory
// passwords with three independent budgets — per account, per client IP
// and a global one over the costly password-hash computations — plus a
// concurrency gate for the hashes themselves. Exponential backoff on
// failures, cleared on success for the account key. In-memory only: a
// restart clears the counters, which is acceptable for a single-process
// deployment.
type loginLimiter struct {
	mu    sync.Mutex
	fails map[string]loginFail
	// hashGate bounds concurrent costly password-hash computations
	// (bcrypt/argon2 are deliberately expensive).
	hashGate chan struct{}
}

type loginFail struct {
	count  int
	locked time.Time
}

// Brute-force budgets and the global fleet-wide failure window.
const (
	// loginGlobalBudget is the total number of consecutive failed login
	// attempts (across every account and source) before the login form
	// locks for everyone: a distributed guessing run can spread across
	// accounts and IPs, but not across the global budget.
	loginGlobalBudget = 30
	// loginGlobalLockout is the fixed lock window once the global
	// budget is spent.
	loginGlobalLockout = 30 * time.Second
	// maxLoginHashConcurrency bounds simultaneous password-hash
	// computations; further attempts are shed before burning CPU.
	maxLoginHashConcurrency = 4
)

func newLoginLimiter() *loginLimiter {
	return &loginLimiter{
		fails:    make(map[string]loginFail),
		hashGate: make(chan struct{}, maxLoginHashConcurrency),
	}
}

// loginBackoff maps consecutive failures to a lockout: the first four
// attempts stay free (typos happen), the fifth locks for 30s and every
// further failure extends it by 30s up to 5 minutes.
func loginBackoff(n int) time.Duration {
	if n < 5 {
		return 0
	}
	d := time.Duration(n-4) * 30 * time.Second
	if d > 5*time.Minute {
		return 5 * time.Minute
	}
	return d
}

// retryIn reports how long the key must wait before the next attempt;
// zero means it may try now. Early failures (below the lockout threshold)
// keep their counter; an elapsed lockout clears the key so the next burst
// starts from zero again.
func (l *loginLimiter) retryIn(key string) time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()
	f, ok := l.fails[key]
	if !ok {
		return 0
	}
	if d := time.Until(f.locked); d > 0 {
		return d
	}
	if !f.locked.IsZero() {
		delete(l.fails, key) // the lockout window elapsed
	}
	return 0
}

// record stores one attempt outcome. Failures push the lockout window
// out (below the threshold the key has no lockout at all); a success
// clears the key.
func (l *loginLimiter) record(key string, ok bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if ok {
		delete(l.fails, key)
		return
	}
	f := l.fails[key]
	f.count++
	if d := loginBackoff(f.count); d > 0 {
		f.locked = time.Now().Add(d)
	}
	l.fails[key] = f
	l.sweepLocked()
}

// recordCooldown stamps a fixed cooldown on a key after a SUCCESSFUL
// action whose repetition must be limited too (password-reset emails):
// unlike a login success, the key is NOT cleared — repeat requests wait.
func (l *loginLimiter) recordCooldown(key string, d time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	f := l.fails[key]
	f.locked = time.Now().Add(d)
	l.fails[key] = f
	l.sweepLocked()
}

// recordGlobalFailure counts one failed attempt against the fleet-wide
// budget: below the budget nothing happens, past it the login form locks
// for everyone for loginGlobalLockout (extended by further failures).
func (l *loginLimiter) recordGlobalFailure() {
	l.mu.Lock()
	defer l.mu.Unlock()
	f := l.fails["global"]
	f.count++
	if f.count >= loginGlobalBudget {
		f.locked = time.Now().Add(loginGlobalLockout)
	}
	l.fails["global"] = f
	l.sweepLocked()
}

// sweepLocked drops expired lockouts when the map grows large.
func (l *loginLimiter) sweepLocked() {
	if len(l.fails) <= 1024 {
		return
	}
	now := time.Now()
	for k, v := range l.fails {
		if now.After(v.locked) {
			delete(l.fails, k)
		}
	}
}

// acquireHash takes one password-hash slot; the caller MUST release it
// with releaseHash. False means the gate is full and the request must
// be shed before any costly verification runs.
func (l *loginLimiter) acquireHash() bool {
	select {
	case l.hashGate <- struct{}{}:
		return true
	default:
		return false
	}
}

// releaseHash returns one password-hash slot.
func (l *loginLimiter) releaseHash() { <-l.hashGate }

// randomToken returns a 32-byte cryptographically random URL-safe token.
func randomToken() (string, error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}

// newCSRFCookie issues a fresh CSRF cookie for the login form
// (double-submit), scoped to /login.
func newCSRFCookie(w http.ResponseWriter, secure bool) (string, error) {
	return newCSRFCookiePath(w, secure, "/login")
}

// newCSRFCookiePath issues a CSRF cookie scoped to the given path (the
// public password-recovery flow spans /forgot and /reset, so it uses "/").
func newCSRFCookiePath(w http.ResponseWriter, secure bool, path string) (string, error) {
	tok, err := randomToken()
	if err != nil {
		return "", err
	}
	http.SetCookie(w, &http.Cookie{
		Name:     csrfCookie,
		Value:    tok,
		Path:     path,
		HttpOnly: false, // the form reads nothing; the hidden field is rendered server-side
		Secure:   secure,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   int((15 * time.Minute).Seconds()),
	})
	return tok, nil
}
