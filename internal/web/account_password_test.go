package web_test

import (
	"errors"
	"net/http"
	"net/url"
	"testing"
)

// TestAccountPasswordChangeRevokesSessionsBeforePreferenceWrites pins the
// ticket: the password is stored FIRST, then the notification preferences are
// written — and a failure there used to end the request before the revocation
// ran, leaving every earlier session alive under a password that had already
// changed. The revocation must not depend on the writes that follow it.
func TestAccountPasswordChangeRevokesSessionsBeforePreferenceWrites(t *testing.T) {
	env := newTestEnv(t)
	if _, err := env.users.CreateUser("member1", "", "member1@example.com", "", "member", "old-password-1"); err != nil {
		t.Fatal(err)
	}

	// Two sessions of the SAME account: a browser and, say, a phone.
	signIn := func(password string) {
		t.Helper()
		_, html := env.get("/login")
		resp, _ := env.postForm("/login", url.Values{
			"csrf": {extractCSRF(t, html)}, "username": {"member1"}, "password": {password},
		})
		if resp.StatusCode != http.StatusSeeOther {
			t.Fatalf("login as member1 = %d, want 303", resp.StatusCode)
		}
	}
	signIn("old-password-1")
	if resp, _ := env.get("/dashboard"); resp.StatusCode != http.StatusOK {
		t.Fatalf("first session = %d, want 200", resp.StatusCode)
	}
	other := env.newBrowser()
	other.signIn("member1", "old-password-1")
	if resp, _ := other.get("/dashboard"); resp.StatusCode != http.StatusOK {
		t.Fatalf("second session = %d, want 200", resp.StatusCode)
	}

	// The preference write blows up right after the password is stored.
	fake := env.users.(*fakeUsers)
	fake.failChannelOptOuts = errors.New("store is down")

	_, html := env.get("/account")
	resp, _ := env.postForm("/account", url.Values{
		"csrf":  {extractCSRF(t, html)},
		"phone": {}, "email": {"member1@example.com"}, "discord": {},
		"lang": {"pl"}, "password": {"new-password-2"},
		"channels": {"email"},
	})
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("account save with a failing preference write = %d, want 500", resp.StatusCode)
	}

	// The password IS the new one, and NO session survives it — not even the
	// second one the operator left open elsewhere.
	for _, path := range []string{"/dashboard", "/account"} {
		if resp, _ := env.get(path); resp.StatusCode == http.StatusOK {
			t.Fatalf("the session that asked for the change still opens %s (200)", path)
		}
		if resp, _ := other.get(path); resp.StatusCode == http.StatusOK {
			t.Fatalf("another session of the same account still opens %s (200)", path)
		}
	}
	fake.failChannelOptOuts = nil
	_, html = env.get("/login")
	resp, _ = env.postForm("/login", url.Values{
		"csrf": {extractCSRF(t, html)}, "username": {"member1"}, "password": {"old-password-1"},
	})
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("the OLD password still signs in: %d, want 401", resp.StatusCode)
	}
	_, html = env.get("/login")
	resp, _ = env.postForm("/login", url.Values{
		"csrf": {extractCSRF(t, html)}, "username": {"member1"}, "password": {"new-password-2"},
	})
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("the NEW password does not sign in: %d, want 303", resp.StatusCode)
	}
}
