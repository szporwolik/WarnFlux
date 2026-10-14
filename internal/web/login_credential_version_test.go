package web_test

import (
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

// newRaceClient is an independent browser (own cookie jar, no redirect
// following) for the logins these tests pause mid-flight: the helpers of
// testEnv/browser call t.Fatal, which a background goroutine may not do.
func newRaceClient(t *testing.T) *http.Client {
	t.Helper()
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	return &http.Client{Jar: jar, CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}
}

// loginPausedDuring starts a directory login, parks it between the password
// check and the session insert, runs change() in that window and lets the
// login finish. It returns the client holding the cookie the login ended with.
func loginPausedDuring(t *testing.T, env *testEnv, username, password string, change func()) *http.Client {
	t.Helper()
	client := newRaceClient(t)

	// The login page is fetched first: only the POST has to run while the
	// account is parked, and the CSRF cookie must already be in the jar.
	page, err := client.Get(env.srv.URL + "/login")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(page.Body)
	page.Body.Close()
	csrf := extractCSRF(t, string(body))

	fake := env.users.(*fakeUsers)
	pause := make(chan struct{})
	resume := make(chan struct{})
	fake.pauseNextRead(pause, resume)
	defer fake.pauseNextRead(nil, nil)

	type result struct {
		status int
		err    error
	}
	done := make(chan result, 1)
	go func() {
		resp, err := client.PostForm(env.srv.URL+"/login", url.Values{
			"csrf": {csrf}, "username": {username}, "password": {password},
		})
		if err != nil {
			done <- result{err: err}
			return
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		done <- result{status: resp.StatusCode}
	}()

	// The password is verified and the session does not exist yet.
	select {
	case <-pause:
	case <-time.After(5 * time.Second):
		t.Fatal("the login never reached the paused verification")
	}
	change()
	close(resume)

	res := <-done
	if res.err != nil {
		t.Fatalf("the paused login failed: %v", res.err)
	}
	if res.status != http.StatusSeeOther {
		t.Fatalf("the paused login = %d, want 303 (it may answer, but its session must be dead)", res.status)
	}
	return client
}

// mustBeLoggedOut asserts the cookie a paused login received does not open the
// panel.
func mustBeLoggedOut(t *testing.T, client *http.Client, url string, what string) {
	t.Helper()
	resp, err := client.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		t.Fatalf("%s = 200 with the session of a login that was paused during %s", url, what)
	}
	if resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/login" {
		t.Fatalf("%s = %d (Location %q), want a redirect to /login", url, resp.StatusCode, resp.Header.Get("Location"))
	}
}

// TestLoginPausedByPasswordResetGetsNoSession pins the ticket: checking the
// password and storing the session are separate steps, so a login suspended
// between them stored its session AFTER a reset had already revoked every
// earlier one — the old password bought a working session again. The session
// carries the credential version its password was verified against and dies
// on its first use once the account carries a newer one.
func TestLoginPausedByPasswordResetGetsNoSession(t *testing.T) {
	env := newTestEnv(t)
	u, err := env.users.CreateUser("member1", "", "member1@example.com", "", "member", "old-password-1")
	if err != nil {
		t.Fatal(err)
	}
	env.login() // the admin session that performs the reset
	csrf := env.csrfFromPage("/users")

	client := loginPausedDuring(t, env, "member1", "old-password-1", func() {
		resp, _ := env.postForm("/users/"+strconv.FormatInt(u.ID, 10)+"/reset", url.Values{"csrf": {csrf}})
		if resp.StatusCode != http.StatusOK {
			t.Errorf("admin reset while the login was paused = %d, want 200", resp.StatusCode)
		}
	})

	mustBeLoggedOut(t, client, env.srv.URL+"/dashboard", "a password reset")
}

// TestLoginPausedByRoleChangeGetsNoSession covers the same window for a
// permission edit: a session opened from the version before the change must
// not keep the old role.
func TestLoginPausedByRoleChangeGetsNoSession(t *testing.T) {
	env := newTestEnv(t)
	u, err := env.users.CreateUser("member1", "", "member1@example.com", "", "member", "old-password-1")
	if err != nil {
		t.Fatal(err)
	}

	client := loginPausedDuring(t, env, "member1", "old-password-1", func() {
		if _, err := env.users.UpdateUser(u.ID, u.Username, u.Phone, u.Email, u.Discord, "emcom", ""); err != nil {
			t.Errorf("role change while the login was paused: %v", err)
		}
	})

	mustBeLoggedOut(t, client, env.srv.URL+"/dashboard", "a role change")
}

// TestForgotPausedRequestIssuesNoTokenForTheOldAddress pins the ticket at the
// panel: /forgot reads the account (address A), then issues the token and
// mails it to that address. A request suspended in between must not mint a
// link once the account has moved to B, because the link would land in a
// mailbox the account has left — and the second token would also cancel a link
// the operator may just have requested legitimately.
func TestForgotPausedRequestIssuesNoTokenForTheOldAddress(t *testing.T) {
	env := newTestEnv(t)
	u, err := env.users.CreateUser("member1", "", "old@example.com", "", "member", "password123")
	if err != nil {
		t.Fatal(err)
	}
	sends := 0
	env.server.SetPasswordResetMailer(func(to, subject, text string) error {
		sends++
		return nil
	})
	env.login() // the admin session that reads the audit trail at the end

	fake := env.users.(*fakeUsers)
	pause := make(chan struct{})
	resume := make(chan struct{})
	fake.pauseNextRead(pause, resume)
	defer fake.pauseNextRead(nil, nil)

	client := newRaceClient(t)
	page, err := client.Get(env.srv.URL + "/forgot")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(page.Body)
	page.Body.Close()

	done := make(chan int, 1)
	go func() {
		resp, err := client.PostForm(env.srv.URL+"/forgot", url.Values{
			"csrf":     {extractCSRF(t, string(body))},
			"username": {"member1"},
			"email":    {"old@example.com"},
		})
		if err != nil {
			done <- -1
			return
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		done <- resp.StatusCode
	}()

	// The account was read (with the old address) and nothing was sent yet.
	select {
	case <-pause:
	case <-time.After(5 * time.Second):
		t.Fatal("the reset request never reached the paused lookup")
	}
	// The mailbox moves while the request is in flight.
	if _, err := env.users.UpdateUser(u.ID, "member1", "", "new@example.com", "", "member", ""); err != nil {
		t.Fatal(err)
	}
	close(resume)

	if status := <-done; status != http.StatusOK {
		t.Fatalf("the paused /forgot request = %d, want the rendered page", status)
	}
	if sends != 0 {
		t.Fatalf("%d reset mails went to the address the account left, want none", sends)
	}
	// The answer stays neutral (no enumeration), but the station records why
	// the request went nowhere.
	_, audit := env.get("/partials/audit?after=0")
	if !strings.Contains(audit, "address-changed") {
		t.Errorf("audit trail does not mention the address change: %s", audit)
	}
}
