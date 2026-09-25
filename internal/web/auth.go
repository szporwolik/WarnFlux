package web

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"net/http"
	"sync"
	"time"
)

const (
	sessionCookie = "wf_session"
	csrfCookie    = "wf_csrf"
	sessionTTL    = 24 * time.Hour
	maxSessions   = 4096
)

// session is one authenticated session.
type session struct {
	username string
	// role is the access tier: "admin" (everything) or "emcom" (compose
	// only). Set once at login, never from the client.
	role    string
	csrf    string
	expires time.Time
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

// newSession creates a cryptographically random session token.
func (s *sessionStore) newSession(username, role string) (token string, sess *session, err error) {
	tok, err := randomToken()
	if err != nil {
		return "", nil, err
	}
	csrf, err := randomToken()
	if err != nil {
		return "", nil, err
	}
	sess = &session{
		username: username,
		role:     role,
		csrf:     csrf,
		expires:  time.Now().Add(sessionTTL),
	}
	s.mu.Lock()
	if len(s.m) >= maxSessions {
		s.sweepLocked()
	}
	s.m[tok] = sess
	s.mu.Unlock()
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

// loginLimiter slows brute-force guessing of the admin/directory
// passwords: per-key (username + remote address) exponential backoff on
// failures, cleared on success. In-memory only: a restart clears the
// counters, which is acceptable for a single-process deployment.
type loginLimiter struct {
	mu    sync.Mutex
	fails map[string]loginFail
}

type loginFail struct {
	count  int
	locked time.Time
}

func newLoginLimiter() *loginLimiter {
	return &loginLimiter{fails: make(map[string]loginFail)}
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
	if f.count >= 5 {
		delete(l.fails, key) // the lockout window elapsed
	}
	return 0
}

// record stores one attempt outcome. Failures push the lockout window
// out; a success clears the key.
func (l *loginLimiter) record(key string, ok bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if ok {
		delete(l.fails, key)
		return
	}
	f := l.fails[key]
	f.count++
	f.locked = time.Now().Add(loginBackoff(f.count))
	l.fails[key] = f
	if len(l.fails) > 1024 {
		now := time.Now()
		for k, v := range l.fails {
			if now.After(v.locked) {
				delete(l.fails, k)
			}
		}
	}
}

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
