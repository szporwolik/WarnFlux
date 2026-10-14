package web_test

import (
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"sync"
	"testing"
	"time"
)

// maxHashBudget mirrors the server's own limit on simultaneous password
// computations (maxLoginHashConcurrency): the login form and the public reset
// endpoint share it, so neither can multiply the cost of a hash.
const maxHashBudget = 4

// TestLoginAndResetShareTheHashGate pins the reported P2: /reset validated the
// link (a cheap filter) and only then computed the password hash, so many
// parallel submissions of ONE valid link each paid for a PBKDF2 computation —
// a token holder could buy an unbound CPU burst. The reset now takes the same
// gate as the login form, and the excess is shed before the hash is computed.
func TestLoginAndResetShareTheHashGate(t *testing.T) {
	env := newTestEnv(t)
	u, err := env.users.CreateUser("member1", "", "member1@example.com", "", "member", "password123")
	if err != nil {
		t.Fatal(err)
	}
	fake := env.users.(*fakeUsers)
	fake.setHashDelay(50 * time.Millisecond)
	token, err := env.users.CreatePasswordReset(u.ID, "member1@example.com")
	if err != nil {
		t.Fatal(err)
	}

	const parallel = 8
	type attempt struct {
		client *http.Client
		path   string
		form   url.Values
	}
	attempts := make([]attempt, 0, 2*parallel)
	for i := 0; i < parallel; i++ {
		jar, err := cookiejar.New(nil)
		if err != nil {
			t.Fatal(err)
		}
		client := &http.Client{Jar: jar, CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		}}
		if i%2 == 0 {
			// A login: the page carries the CSRF cookie and token.
			page, err := client.Get(env.srv.URL + "/login")
			if err != nil {
				t.Fatal(err)
			}
			body, _ := io.ReadAll(page.Body)
			page.Body.Close()
			attempts = append(attempts, attempt{client: client, path: "/login", form: url.Values{
				"csrf": {extractCSRF(t, string(body))}, "username": {"member1"}, "password": {"password123"},
			}})
			continue
		}
		// A submission of the SAME reset link.
		page, err := client.Get(env.srv.URL + "/reset?token=" + url.QueryEscape(token))
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(page.Body)
		page.Body.Close()
		attempts = append(attempts, attempt{client: client, path: "/reset", form: url.Values{
			"csrf": {extractCSRF(t, string(body))}, "token": {token}, "password": {"replaced-password-2"},
		}})
	}

	// Fire every request at once, so the expensive part overlaps as much as
	// the gate allows.
	start := make(chan struct{})
	statuses := make(chan int, len(attempts))
	var wg sync.WaitGroup
	for _, a := range attempts {
		wg.Add(1)
		go func(a attempt) {
			defer wg.Done()
			<-start
			resp, err := a.client.PostForm(env.srv.URL+a.path, a.form)
			if err != nil {
				statuses <- -1
				return
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			statuses <- resp.StatusCode
		}(a)
	}
	close(start)
	wg.Wait()
	close(statuses)

	accepted := 0
	for status := range statuses {
		switch status {
		case http.StatusSeeOther:
			accepted++
		case http.StatusTooManyRequests, http.StatusUnprocessableEntity:
			// Shed by the gate, or the shared link was already spent.
		case http.StatusUnauthorized:
			// A login carrying the password the winning reset just replaced.
		default:
			t.Errorf("unexpected status %d, want 303, 429, 422 or 401", status)
		}
	}
	if peak := fake.hashPeakObserved(); peak > maxHashBudget {
		t.Fatalf("%d simultaneous password computations, want at most %d", peak, maxHashBudget)
	}
	// The gate sheds the excess, it does not stop the work: the link is
	// spendable and the login succeeds.
	if accepted == 0 {
		t.Fatal("every parallel request was shed, want at least one to go through")
	}
	if _, err := env.users.Authenticate("member1", "replaced-password-2"); err != nil {
		t.Fatalf("the reset link did not set the new password: %v", err)
	}
}
