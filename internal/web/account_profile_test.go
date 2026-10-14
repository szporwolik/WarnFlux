package web_test

import (
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"
)

// TestAccountSaveDoesNotUndoARoleChange pins the ticket at the panel: the
// account form used to write back the username and the role it had read when
// the page was rendered, so an administrator's role change landing while the
// form was open was overwritten by the operator saving their phone number.
// The self-service path writes the profile only — no identity at all.
func TestAccountSaveDoesNotUndoARoleChange(t *testing.T) {
	env := newTestEnv(t)
	u, err := env.users.CreateUser("member1", "+48600111111", "member1@example.com", "", "emcom", "password123")
	if err != nil {
		t.Fatal(err)
	}

	// The operator is signed in with the form in hand.
	me := env.newBrowser()
	me.signIn("member1", "password123")
	_, page := me.get("/account")
	if !strings.Contains(page, "member1@example.com") {
		t.Fatalf("the account page does not show the account: %s", page)
	}

	fake := env.users.(*fakeUsers)
	pause := make(chan struct{})
	resume := make(chan struct{})
	fake.pauseNextRead(pause, resume)
	defer fake.pauseNextRead(nil, nil)

	// The save runs on the operator's own session but not through the
	// browser helper: those helpers call t.Fatal, which a goroutine may not.
	form := url.Values{
		"csrf":    {extractCSRF(t, page)},
		"phone":   {"+48600333333"},
		"email":   {"member1@example.com"},
		"discord": {""},
		"lang":    {""},
	}
	done := make(chan int, 1)
	go func() {
		req, err := http.NewRequest(http.MethodPost, env.srv.URL+"/account", strings.NewReader(form.Encode()))
		if err != nil {
			done <- -1
			return
		}
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		resp, err := me.client.Do(req)
		if err != nil {
			done <- -1
			return
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		done <- resp.StatusCode
	}()

	// The profile save has read the account (role emcom) and has not written
	// anything yet.
	select {
	case <-pause:
	case <-time.After(5 * time.Second):
		t.Fatal("the profile save never reached the paused lookup")
	}
	// The administrator demotes the account in that window.
	fake.overrideRole(u.ID, "member")
	close(resume)
	fake.pauseNextRead(nil, nil)

	if status := <-done; status != http.StatusSeeOther {
		t.Fatalf("the profile save = %d, want 303", status)
	}

	after, err := env.users.GetUser(u.ID)
	if err != nil {
		t.Fatal(err)
	}
	// The profile edit landed...
	if after.Phone != "+48600333333" {
		t.Fatalf("phone after the profile save = %q, want the saved one", after.Phone)
	}
	// ...and the administrator's decision stands.
	if after.Role != "member" {
		t.Fatalf("role after the profile save = %q, want the administrator's %q", after.Role, "member")
	}
	if after.Username != "member1" {
		t.Fatalf("username after the profile save = %q, want it untouched", after.Username)
	}
}
