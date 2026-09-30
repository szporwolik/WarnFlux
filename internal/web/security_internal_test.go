package web

import (
	"net/http"
	"testing"
	"time"
)

func TestLoginBackoff(t *testing.T) {
	if loginBackoff(1) != 0 || loginBackoff(4) != 0 {
		t.Error("first failures must stay free")
	}
	if loginBackoff(5) != 30*time.Second {
		t.Errorf("fifth failure = %v", loginBackoff(5))
	}
	if loginBackoff(6) != 60*time.Second {
		t.Errorf("sixth failure = %v", loginBackoff(6))
	}
	if loginBackoff(100) != 5*time.Minute {
		t.Errorf("cap = %v", loginBackoff(100))
	}
}

func TestLoginLimiterCycle(t *testing.T) {
	l := newLoginLimiter()
	if l.retryIn("admin\x00x") != 0 {
		t.Error("fresh key must not wait")
	}
	for i := 0; i < 5; i++ {
		l.record("admin\x00x", false)
	}
	if l.retryIn("admin\x00x") <= 0 {
		t.Error("five failures must lock the key")
	}
	l.record("admin\x00x", true)
	if l.retryIn("admin\x00x") != 0 {
		t.Error("success must clear the lockout")
	}
}

func TestLoginLimiterGlobalBudget(t *testing.T) {
	l := newLoginLimiter()
	for i := 0; i < loginGlobalBudget-1; i++ {
		l.recordGlobalFailure()
		if w := l.retryIn("global"); w != 0 {
			t.Fatalf("attempt %d locked early: %v", i+1, w)
		}
	}
	l.recordGlobalFailure()
	if w := l.retryIn("global"); w <= 0 {
		t.Fatalf("global budget spent but not locked: %v", w)
	}
}

func TestLoginLimiterHashGate(t *testing.T) {
	l := newLoginLimiter()
	for i := 0; i < maxLoginHashConcurrency; i++ {
		if !l.acquireHash() {
			t.Fatalf("slot %d denied", i+1)
		}
	}
	if l.acquireHash() {
		t.Fatal("gate must be full at the concurrency cap")
	}
	l.releaseHash()
	if !l.acquireHash() {
		t.Fatal("released slot must be reusable")
	}
}

func TestParseTrustedProxies(t *testing.T) {
	trusted, err := parseTrustedProxies([]string{"10.0.0.0/8", "192.168.1.5"})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(trusted) != 2 {
		t.Fatalf("parsed %d entries, want 2", len(trusted))
	}
	if _, err := parseTrustedProxies([]string{"not-an-ip"}); err == nil {
		t.Fatal("garbage proxy entry accepted")
	}
}

func TestClientIPNormalization(t *testing.T) {
	trusted, err := parseTrustedProxies([]string{"10.0.0.0/8", "192.168.1.5"})
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{trustedProxies: trusted}

	// The port is never part of the client identity.
	req := &http.Request{RemoteAddr: "1.2.3.4:56789", Header: http.Header{}}
	if got := s.clientIP(req); got != "1.2.3.4" {
		t.Errorf("client = %q, want 1.2.3.4 (no port)", got)
	}
	// Proxy headers from an UNTRUSTED peer are ignored.
	req = &http.Request{RemoteAddr: "1.2.3.4:56789", Header: http.Header{"X-Forwarded-For": {"9.9.9.9"}}}
	if got := s.clientIP(req); got != "1.2.3.4" {
		t.Errorf("untrusted XFF honored: %q", got)
	}
	// A trusted proxy forwards the original client (leftmost entry).
	req = &http.Request{RemoteAddr: "10.1.1.1:443", Header: http.Header{"X-Forwarded-For": {"9.9.9.9, 10.1.1.1"}}}
	if got := s.clientIP(req); got != "9.9.9.9" {
		t.Errorf("trusted XFF = %q, want 9.9.9.9", got)
	}
	// Exact-IP trusted entries work the same way.
	req = &http.Request{RemoteAddr: "192.168.1.5:80", Header: http.Header{"X-Forwarded-For": {"8.8.8.8"}}}
	if got := s.clientIP(req); got != "8.8.8.8" {
		t.Errorf("exact-IP proxy XFF = %q, want 8.8.8.8", got)
	}
}

func TestCSRFOK(t *testing.T) {
	if csrfOK("a", "a") != true || csrfOK("a", "b") != false {
		t.Error("csrfOK comparison broken")
	}
	if csrfOK("", "") != false || csrfOK("x", "") != false {
		t.Error("empty tokens must never match")
	}
}
