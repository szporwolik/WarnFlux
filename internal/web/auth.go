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

// randomToken returns a 32-byte cryptographically random URL-safe token.
func randomToken() (string, error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}

// newCSRFCookie issues a fresh CSRF cookie for the login form (double-submit).
func newCSRFCookie(w http.ResponseWriter, secure bool) (string, error) {
	tok, err := randomToken()
	if err != nil {
		return "", err
	}
	http.SetCookie(w, &http.Cookie{
		Name:     csrfCookie,
		Value:    tok,
		Path:     "/login",
		HttpOnly: false, // the form reads nothing; the hidden field is rendered server-side
		Secure:   secure,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   int((15 * time.Minute).Seconds()),
	})
	return tok, nil
}
