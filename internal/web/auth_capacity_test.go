package web

import (
	"strconv"
	"testing"
	"time"
)

// fillSession plants one valid session directly into the store.
func fillSession(s *sessionStore, tok string, userID int64, username string, createdAt time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[tok] = &session{
		userID: userID, username: username, role: "member", csrf: "x",
		createdAt: createdAt, expires: time.Now().Add(sessionTTL),
	}
}

// TestSessionCapacityRejected pins the global cap: when every existing
// session is still valid, creation is rejected instead of growing beyond
// maxSessions; an expired row is swept first and frees capacity.
func TestSessionCapacityRejected(t *testing.T) {
	s := newSessionStore(false)
	for i := 0; i < maxSessions; i++ {
		fillSession(s, "tok-"+strconv.Itoa(i), int64(i), "user"+strconv.Itoa(i), time.Now())
	}

	// All valid: explicit rejection at capacity.
	if _, _, err := s.newSession(9999, "newuser", "member"); err == nil {
		t.Fatal("newSession succeeded at capacity, want rejection")
	}
	s.mu.Lock()
	got := len(s.m)
	s.mu.Unlock()
	if got != maxSessions {
		t.Fatalf("store size = %d, want %d", got, maxSessions)
	}

	// One expired row: the sweep frees capacity and creation succeeds.
	s.mu.Lock()
	for _, sess := range s.m {
		sess.expires = time.Now().Add(-time.Minute)
		break
	}
	s.mu.Unlock()
	if _, _, err := s.newSession(9999, "newuser", "member"); err != nil {
		t.Fatalf("newSession after sweep: %v", err)
	}
}

// TestSessionPerUserEviction pins the per-user cap: at the limit the
// OLDEST session of the same account is evicted, so a login always
// succeeds for its owner and the account keeps at most
// maxSessionsPerUser sessions.
func TestSessionPerUserEviction(t *testing.T) {
	s := newSessionStore(false)
	base := time.Now()
	for i := 0; i < maxSessionsPerUser; i++ {
		fillSession(s, "u-"+strconv.Itoa(i), 42, "ops", base.Add(time.Duration(i)*time.Second))
	}

	if _, _, err := s.newSession(42, "ops", "emcom"); err != nil {
		t.Fatalf("newSession: %v", err)
	}
	s.mu.Lock()
	_, oldest := s.m["u-0"]
	got := len(s.m)
	s.mu.Unlock()
	if oldest {
		t.Fatal("oldest per-user session was not evicted")
	}
	if got != maxSessionsPerUser {
		t.Fatalf("sessions = %d, want %d", got, maxSessionsPerUser)
	}

	// Another user is unaffected: their sessions are never evicted here.
	fillSession(s, "other", 7, "someone", time.Now())
	if _, _, err := s.newSession(7, "someone", "member"); err != nil {
		t.Fatalf("other user session: %v", err)
	}
	s.mu.Lock()
	_, kept := s.m["other"]
	s.mu.Unlock()
	if !kept {
		t.Fatal("unrelated user's session was evicted")
	}
}
