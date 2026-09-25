package web

import (
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

func TestCSRFOK(t *testing.T) {
	if csrfOK("a", "a") != true || csrfOK("a", "b") != false {
		t.Error("csrfOK comparison broken")
	}
	if csrfOK("", "") != false || csrfOK("x", "") != false {
		t.Error("empty tokens must never match")
	}
}
