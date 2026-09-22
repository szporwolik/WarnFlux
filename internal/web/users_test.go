package web_test

import (
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/storage"
)

// fakeUsers is an in-memory storage.UserStore for handler tests.
type fakeUsers struct {
	mu     sync.Mutex
	nextID int64
	rows   []storage.User
}

func newFakeUsers() *fakeUsers { return &fakeUsers{nextID: 1} }

func (f *fakeUsers) EnsureAdminUser(username string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, u := range f.rows {
		if strings.EqualFold(u.Username, username) {
			return nil
		}
	}
	now := time.Now()
	f.rows = append(f.rows, storage.User{
		ID: f.nextID, Username: username, IsAdmin: true,
		CreatedAt: now, UpdatedAt: now,
	})
	f.nextID++
	return nil
}

func (f *fakeUsers) ListUsers(page, perPage int) ([]storage.User, int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]storage.User, len(f.rows))
	copy(out, f.rows)
	// Admin first, then username.
	sortUsers(out)
	if perPage < 1 {
		perPage = 1
	}
	total := len(out)
	pages := (total + perPage - 1) / perPage
	if pages < 1 {
		pages = 1
	}
	if page < 1 {
		page = 1
	}
	if page > pages {
		page = pages
	}
	start := (page - 1) * perPage
	end := start + perPage
	if end > total {
		end = total
	}
	return out[start:end], total, nil
}

func (f *fakeUsers) GetUser(id int64) (storage.User, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, u := range f.rows {
		if u.ID == id {
			return u, nil
		}
	}
	return storage.User{}, storage.ErrUserNotFound
}

func (f *fakeUsers) CreateUser(username, phone, email, discord string) (storage.User, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, u := range f.rows {
		if strings.EqualFold(u.Username, username) {
			return storage.User{}, storage.ErrUsernameTaken
		}
	}
	now := time.Now()
	u := storage.User{
		ID: f.nextID, Username: username, Phone: phone, Email: email, Discord: discord,
		CreatedAt: now, UpdatedAt: now,
	}
	f.nextID++
	f.rows = append(f.rows, u)
	return u, nil
}

func (f *fakeUsers) UpdateUser(id int64, username, phone, email, discord string) (storage.User, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := range f.rows {
		if f.rows[i].ID != id {
			continue
		}
		if f.rows[i].IsAdmin {
			return storage.User{}, storage.ErrUserProtected
		}
		for j := range f.rows {
			if f.rows[j].ID != id && strings.EqualFold(f.rows[j].Username, username) {
				return storage.User{}, storage.ErrUsernameTaken
			}
		}
		f.rows[i].Username = username
		f.rows[i].Phone = phone
		f.rows[i].Email = email
		f.rows[i].Discord = discord
		f.rows[i].UpdatedAt = time.Now()
		return f.rows[i], nil
	}
	return storage.User{}, storage.ErrUserNotFound
}

func (f *fakeUsers) DeleteUser(id int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := range f.rows {
		if f.rows[i].ID != id {
			continue
		}
		if f.rows[i].IsAdmin {
			return storage.ErrUserProtected
		}
		f.rows = append(f.rows[:i], f.rows[i+1:]...)
		return nil
	}
	return storage.ErrUserNotFound
}

func sortUsers(rows []storage.User) {
	for i := 1; i < len(rows); i++ {
		for j := i; j > 0 && userBefore(rows[j], rows[j-1]); j-- {
			rows[j], rows[j-1] = rows[j-1], rows[j]
		}
	}
}

func userBefore(a, b storage.User) bool {
	if a.IsAdmin != b.IsAdmin {
		return a.IsAdmin
	}
	return strings.ToLower(a.Username) < strings.ToLower(b.Username)
}

// postForm performs an authenticated POST with CSRF.
func (e *testEnv) postForm(path string, values url.Values) (*http.Response, string) {
	req, err := http.NewRequest(http.MethodPost, e.srv.URL+path, strings.NewReader(values.Encode()))
	if err != nil {
		e.t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := e.client.Do(req)
	if err != nil {
		e.t.Fatal(err)
	}
	defer resp.Body.Close()
	return resp, bodyString(resp)
}

// bodyString reads the response body without closing it.
func bodyString(resp *http.Response) string {
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return ""
	}
	return string(b)
}

func TestUsersPageListsAdminReadOnly(t *testing.T) {
	env := newTestEnv(t)
	env.login()

	resp, html := env.get("/users")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if !strings.Contains(html, testUsername) {
		t.Fatalf("admin user not listed: %s", html)
	}
	if !strings.Contains(html, "read-only") {
		t.Errorf("admin row should be read-only: %s", html)
	}
	if strings.Contains(html, "/users/1/delete") {
		t.Errorf("admin row must not offer delete: %s", html)
	}
}

func TestUsersCRUDFlow(t *testing.T) {
	env := newTestEnv(t)
	env.login()
	csrf := env.csrfFromPage("/users")

	// Create.
	resp, _ := env.postForm("/users", url.Values{
		"csrf": {csrf}, "username": {"alice"}, "phone": {"+48 600 100 200"},
		"email": {"alice@example.com"}, "discord": {"alice#1234"},
	})
	if resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/users" {
		t.Fatalf("create = %d %q, want redirect to /users", resp.StatusCode, resp.Header.Get("Location"))
	}
	_, html := env.get("/users")
	if !strings.Contains(html, "alice") || !strings.Contains(html, "alice@example.com") {
		t.Fatalf("created user not listed: %s", html)
	}

	// Duplicate username rejected.
	resp, html = env.postForm("/users", url.Values{"csrf": {csrf}, "username": {"alice"}})
	if resp.StatusCode != http.StatusConflict || !strings.Contains(html, "already exists") {
		t.Fatalf("duplicate = %d %s", resp.StatusCode, html)
	}

	// Edit prefills the form.
	_, html = env.get("/users?edit=2")
	if !strings.Contains(html, `value="alice"`) || !strings.Contains(html, `name="edit_id" value="2"`) {
		t.Fatalf("edit prefill missing: %s", html)
	}

	// Save the edit.
	resp, _ = env.postForm("/users", url.Values{
		"csrf": {csrf}, "edit_id": {"2"}, "username": {"alice"},
		"phone": {"+48 600 999 999"}, "email": {"alice@example.com"}, "discord": {"alice#9999"},
	})
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("update = %d, want redirect", resp.StatusCode)
	}
	_, html = env.get("/users")
	if !strings.Contains(html, "+48 600 999 999") {
		t.Fatalf("updated phone not listed: %s", html)
	}

	// Delete.
	csrf = env.csrfFromPage("/users")
	resp, _ = env.postForm("/users/2/delete", url.Values{"csrf": {csrf}})
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("delete = %d, want redirect", resp.StatusCode)
	}
	_, html = env.get("/users")
	if strings.Contains(html, "alice") {
		t.Fatalf("deleted user still listed: %s", html)
	}
}

func TestUsersAdminCannotBeDeletedOrEdited(t *testing.T) {
	env := newTestEnv(t)
	env.login()
	csrf := env.csrfFromPage("/users")

	resp, html := env.postForm("/users/1/delete", url.Values{"csrf": {csrf}})
	if resp.StatusCode != http.StatusForbidden || !strings.Contains(html, "read-only") {
		t.Fatalf("admin delete = %d %s, want 403 read-only", resp.StatusCode, html)
	}

	resp, html = env.postForm("/users", url.Values{"csrf": {csrf}, "edit_id": {"1"}, "username": {"admin"}, "phone": {"x"}})
	if resp.StatusCode != http.StatusForbidden || !strings.Contains(html, "read-only") {
		t.Fatalf("admin edit = %d %s, want 403 read-only", resp.StatusCode, html)
	}
}

func TestUsersValidation(t *testing.T) {
	env := newTestEnv(t)
	env.login()
	csrf := env.csrfFromPage("/users")

	for name, values := range map[string]url.Values{
		"bad username": {"csrf": {csrf}, "username": {"UPPER CASE"}},
		"bad email":    {"csrf": {csrf}, "username": {"okuser"}, "email": {"no-at-sign"}},
		"missing csrf": {"username": {"okuser"}},
	} {
		resp, html := env.postForm("/users", values)
		if resp.StatusCode < 400 {
			t.Errorf("%s: status = %d, want 4xx (%s)", name, resp.StatusCode, html)
		}
	}
}

// csrfFromPage extracts the session CSRF token embedded in a rendered page.
func (e *testEnv) csrfFromPage(path string) string {
	_, html := e.get(path)
	idx := strings.Index(html, `name="csrf" value="`)
	if idx < 0 {
		e.t.Fatalf("csrf token not found on %s: %s", path, html)
	}
	rest := html[idx+len(`name="csrf" value="`):]
	end := strings.Index(rest, `"`)
	if end < 0 {
		e.t.Fatal("unterminated csrf token")
	}
	return rest[:end]
}
