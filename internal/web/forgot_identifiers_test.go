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

// TestForgotIdentifiersShareOneCooldown pins the ticket: the reset form takes
// BOTH identifiers of an account, but the send cooldown was keyed on what was
// TYPED. Asking once by username and then by e-mail bought two mails inside one
// cooldown — and the second reset cancelled the link the first one mailed. The
// cooldown now belongs to the resolved account.
func TestForgotIdentifiersShareOneCooldown(t *testing.T) {
	env := newTestEnv(t)
	if _, err := env.users.CreateUser("member1", "", "member1@example.com", "", "member", "password123"); err != nil {
		t.Fatal(err)
	}
	sends := 0
	env.server.SetPasswordResetMailer(func(to, subject, text string) error {
		sends++
		return nil
	})

	// First request: the username in the username field.
	_, html := env.get("/forgot")
	resp := env.postFormClose("/forgot", url.Values{
		"csrf":     {extractCSRF(t, html)},
		"username": {"member1"},
		"email":    {"member1@example.com"},
	}, nil)
	if resp.StatusCode != http.StatusOK || sends != 1 {
		t.Fatalf("request by username = %d (%d sends), want 200 with 1 send", resp.StatusCode, sends)
	}

	// Second request: the SAME account addressed by its e-mail address, still
	// inside the cooldown. It must not produce a second mail (and with it a
	// second token, which cancels the link the first request just sent).
	_, html = env.get("/forgot")
	resp = env.postFormClose("/forgot", url.Values{
		"csrf":     {extractCSRF(t, html)},
		"username": {"member1@example.com"},
		"email":    {"member1@example.com"},
	}, nil)
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("request by e-mail inside the cooldown = %d, want 429", resp.StatusCode)
	}
	if sends != 1 {
		t.Fatalf("the two identifiers produced %d mails, want 1", sends)
	}

	// The same address typed in another case is still the same pair.
	_, html = env.get("/forgot")
	resp = env.postFormClose("/forgot", url.Values{
		"csrf":     {extractCSRF(t, html)},
		"username": {"MEMBER1@Example.com"},
		"email":    {"member1@example.com"},
	}, nil)
	if resp.StatusCode != http.StatusTooManyRequests || sends != 1 {
		t.Fatalf("re-typed address = %d (%d sends), want 429 with 1 send", resp.StatusCode, sends)
	}
}

// TestForgotIdentifiersInParallelSendOnce pins the same rule under a burst:
// half the requests address the account by username, half by e-mail, and the
// reservation is atomic on the ACCOUNT — so mixed identifiers still produce
// exactly one email. The mailer is held open while the burst arrives, which is
// precisely the window a per-identifier key leaked through.
func TestForgotIdentifiersInParallelSendOnce(t *testing.T) {
	env := newTestEnv(t)
	if _, err := env.users.CreateUser("member1", "", "member1@example.com", "", "member", "password123"); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	sends := 0
	release := make(chan struct{})
	env.server.SetPasswordResetMailer(func(to, subject, text string) error {
		mu.Lock()
		sends++
		mu.Unlock()
		<-release
		return nil
	})

	// Each request needs its own CSRF cookie + token (the page rotates the
	// cookie), so every goroutine acts like a separate client.
	const parallel = 8
	type attempt struct {
		client *http.Client
		login  string
		csrf   string
	}
	attempts := make([]attempt, 0, parallel)
	for i := 0; i < parallel; i++ {
		jar, err := cookiejar.New(nil)
		if err != nil {
			t.Fatal(err)
		}
		client := &http.Client{Jar: jar}
		resp, err := client.Get(env.srv.URL + "/forgot")
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		login := "member1"
		if i%2 == 1 {
			login = "member1@example.com"
		}
		attempts = append(attempts, attempt{client: client, login: login, csrf: extractCSRF(t, string(body))})
	}

	statuses := make(chan int, parallel)
	for _, a := range attempts {
		go func(a attempt) {
			form := url.Values{
				"csrf":     {a.csrf},
				"username": {a.login},
				"email":    {"member1@example.com"},
			}
			resp, err := a.client.PostForm(env.srv.URL+"/forgot", form)
			if err != nil {
				statuses <- -1
				return
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			statuses <- resp.StatusCode
		}(a)
	}

	// Let the burst arrive. The winner parks inside the mailer; every request
	// that is shed answers without it. (The grace only bounds how long the
	// burst needs to land — nothing waits for the mailer to return.)
	seen := make([]int, 0, parallel)
	grace := time.After(500 * time.Millisecond)
collect:
	for len(seen) < parallel {
		select {
		case status := <-statuses:
			seen = append(seen, status)
		case <-grace:
			break collect
		}
	}
	close(release)
	for len(seen) < parallel {
		seen = append(seen, <-statuses)
	}

	accepted := 0
	for _, status := range seen {
		switch status {
		case http.StatusOK:
			accepted++
		case http.StatusTooManyRequests:
		default:
			t.Errorf("unexpected status %d, want 200 or 429", status)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if sends != 1 {
		t.Fatalf("mixed identifiers produced %d mails, want exactly 1", sends)
	}
	if accepted != 1 {
		t.Fatalf("%d of %d parallel requests were accepted, want exactly 1", accepted, parallel)
	}
}
