package web_test

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/notify"
	"github.com/szporwolik/WarnFlux/internal/severity"
	"github.com/szporwolik/WarnFlux/internal/storage"
)

// fakeUsers is an in-memory storage.DirectoryStore for handler tests.
type fakeUsers struct {
	mu          sync.Mutex
	nextID      int64
	nextGroupID int64
	rows        []storage.User
	groups      []storage.Group
	membership  map[int64]map[int64]bool       // userID -> groupID set
	routing     map[int64]storage.GroupRouting // groupID -> routing
	passwords   map[string]string              // username -> plaintext (fake)
	channelOpts map[int64]map[string]bool      // userID -> disabled delivery-channel kinds
	resets      map[string]int64               // plaintext reset token -> userID
}

func newFakeUsers() *fakeUsers {
	return &fakeUsers{
		nextID: 1, nextGroupID: 1,
		membership:  make(map[int64]map[int64]bool),
		routing:     make(map[int64]storage.GroupRouting),
		passwords:   make(map[string]string),
		channelOpts: make(map[int64]map[string]bool),
		resets:      make(map[string]int64),
	}
}

// Ping satisfies the optional dbPinger assertion for the health page.
func (f *fakeUsers) Ping(context.Context) error { return nil }

// PendingStats satisfies the optional storageProbe assertion for /metrics.
func (f *fakeUsers) PendingStats(context.Context) (int, time.Duration, error) {
	return 3, time.Minute, nil
}

// CountActive satisfies the optional storageProbe assertion for /metrics.
func (f *fakeUsers) CountActive(context.Context) (int, error) { return 4, nil }

func (f *fakeUsers) EnsureAdminUser(username, _ string) error {
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

func (f *fakeUsers) GetUserByUsername(username string) (storage.User, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, u := range f.rows {
		if strings.EqualFold(u.Username, username) {
			return u, nil
		}
	}
	return storage.User{}, storage.ErrUserNotFound
}

func (f *fakeUsers) CreateUser(username, phone, email, discord, role, password string) (storage.User, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, u := range f.rows {
		if strings.EqualFold(u.Username, username) {
			return storage.User{}, storage.ErrUsernameTaken
		}
	}
	now := time.Now()
	u := storage.User{
		ID: f.nextID, Username: username, Phone: phone, Email: email, Discord: discord, Role: role,
		CreatedAt: now, UpdatedAt: now,
	}
	f.nextID++
	f.rows = append(f.rows, u)
	f.passwords[username] = password
	// New users are subscribed to all channels by default (mirrors the
	// SQLite store).
	f.membership[u.ID] = make(map[int64]bool)
	for _, g := range f.groups {
		f.membership[u.ID][g.ID] = true
	}
	return u, nil
}

func (f *fakeUsers) UpdateUser(id int64, username, phone, email, discord, role, password string) (storage.User, error) {
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
		f.rows[i].Role = role
		f.rows[i].UpdatedAt = time.Now()
		if password != "" {
			f.passwords[username] = password
		}
		return f.rows[i], nil
	}
	return storage.User{}, storage.ErrUserNotFound
}

func (f *fakeUsers) Authenticate(username, password string) (storage.User, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	want, ok := f.passwords[username]
	if !ok || want == "" {
		return storage.User{}, storage.ErrBadCredentials
	}
	if subtle.ConstantTimeCompare([]byte(want), []byte(password)) != 1 {
		return storage.User{}, storage.ErrBadCredentials
	}
	for _, u := range f.rows {
		if strings.EqualFold(u.Username, username) && !u.IsAdmin && u.Role != "" {
			return u, nil
		}
	}
	return storage.User{}, storage.ErrBadCredentials
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

// SetUserAPRS replaces the user's registered APRS callsigns (uppercase,
// de-duplicated, sorted).
func (f *fakeUsers) SetUserAPRS(userID int64, callsigns []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	seen := make(map[string]bool, len(callsigns))
	var clean []string
	for _, c := range callsigns {
		c = strings.ToUpper(strings.TrimSpace(c))
		if c == "" || seen[c] {
			continue
		}
		seen[c] = true
		clean = append(clean, c)
	}
	sort.Strings(clean)
	for i := range f.rows {
		if f.rows[i].ID != userID {
			continue
		}
		if f.rows[i].IsAdmin {
			return storage.ErrUserProtected
		}
		f.rows[i].APRSCallsigns = clean
		f.rows[i].UpdatedAt = time.Now()
		return nil
	}
	return storage.ErrUserNotFound
}

// AllAPRSCallsigns returns the distinct base callsigns registered for any
// user (SSID stripped), sorted.
func (f *fakeUsers) AllAPRSCallsigns() ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	seen := make(map[string]bool)
	var out []string
	for _, u := range f.rows {
		for _, c := range u.APRSCallsigns {
			base := strings.ToUpper(c)
			if i := strings.IndexByte(base, '-'); i >= 0 {
				base = base[:i]
			}
			if base != "" && !seen[base] {
				seen[base] = true
				out = append(out, base)
			}
		}
	}
	sort.Strings(out)
	return out, nil
}

// SetUserPassword replaces a regular user's password; the admin row is
// protected.
func (f *fakeUsers) SetUserPassword(userID int64, password string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, u := range f.rows {
		if u.ID != userID {
			continue
		}
		if u.IsAdmin {
			return storage.ErrUserProtected
		}
		f.passwords[u.Username] = password
		return nil
	}
	return storage.ErrUserNotFound
}

// CreatePasswordReset issues a one-time token (fake: stored in plaintext).
func (f *fakeUsers) CreatePasswordReset(userID int64) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, u := range f.rows {
		if u.ID != userID {
			continue
		}
		if u.IsAdmin {
			return "", storage.ErrUserProtected
		}
		raw := make([]byte, 16)
		if _, err := rand.Read(raw); err != nil {
			return "", err
		}
		token := hex.EncodeToString(raw)
		f.resets[token] = userID
		return token, nil
	}
	return "", storage.ErrUserNotFound
}

// PeekPasswordReset validates a token without consuming it.
func (f *fakeUsers) PeekPasswordReset(token string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.resets[token]; !ok {
		return storage.ErrPasswordResetInvalid
	}
	return nil
}

// ConsumePasswordReset validates and consumes a one-time token.
func (f *fakeUsers) ConsumePasswordReset(token string) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	id, ok := f.resets[token]
	if !ok {
		return 0, storage.ErrPasswordResetInvalid
	}
	delete(f.resets, token)
	return id, nil
}

// UserChannelOptOuts returns the disabled delivery-channel kinds for a user.
func (f *fakeUsers) UserChannelOptOuts(userID int64) (map[string]bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[string]bool, len(f.channelOpts[userID]))
	for k, v := range f.channelOpts[userID] {
		out[k] = v
	}
	return out, nil
}

// SetUserChannelOptOuts replaces the disabled delivery-channel kinds.
func (f *fakeUsers) SetUserChannelOptOuts(userID int64, kinds []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	found := false
	for _, u := range f.rows {
		if u.ID == userID {
			found = true
			break
		}
	}
	if !found {
		return storage.ErrUserNotFound
	}
	set := make(map[string]bool, len(kinds))
	for _, k := range kinds {
		set[k] = true
	}
	f.channelOpts[userID] = set
	return nil
}

// GroupRecipientAPRS returns the distinct APRS callsigns of the group's
// members (minus users who opted out of the aprs channel), sorted.
func (f *fakeUsers) GroupRecipientAPRS(groupID int64) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	seen := make(map[string]bool)
	var out []string
	for _, u := range f.rows {
		if !f.membership[u.ID][groupID] || f.channelOpts[u.ID]["aprs"] {
			continue
		}
		for _, c := range u.APRSCallsigns {
			if !seen[c] {
				seen[c] = true
				out = append(out, c)
			}
		}
	}
	sort.Strings(out)
	return out, nil
}

// GroupRecipientDiscord returns the distinct Discord handles of the
// group's members (minus users who opted out of the discord channel),
// sorted.
func (f *fakeUsers) GroupRecipientDiscord(groupID int64) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	seen := make(map[string]bool)
	var out []string
	for _, u := range f.rows {
		if !f.membership[u.ID][groupID] || f.channelOpts[u.ID]["discord"] || u.Discord == "" {
			continue
		}
		if !seen[u.Discord] {
			seen[u.Discord] = true
			out = append(out, u.Discord)
		}
	}
	sort.Strings(out)
	return out, nil
}

func sortUsers(rows []storage.User) {
	for i := 1; i < len(rows); i++ {
		for j := i; j > 0 && userBefore(rows[j], rows[j-1]); j-- {
			rows[j], rows[j-1] = rows[j-1], rows[j]
		}
	}
}

// ---- GroupStore (in-memory) ----

func (f *fakeUsers) memberCount(groupID int64) int64 {
	n := 0
	for _, set := range f.membership {
		if set[groupID] {
			n++
		}
	}
	return int64(n)
}

func (f *fakeUsers) ListGroups(page, perPage int) ([]storage.Group, int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	gs := append([]storage.Group(nil), f.groups...)
	sort.Slice(gs, func(i, j int) bool { return strings.ToLower(gs[i].Name) < strings.ToLower(gs[j].Name) })
	total := len(gs)
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
	if start > total {
		start = total
	}
	end := start + perPage
	if end > total {
		end = total
	}
	out := append([]storage.Group(nil), gs[start:end]...)
	for i := range out {
		out[i].Members = f.memberCount(out[i].ID)
	}
	return out, total, nil
}

func (f *fakeUsers) ListAllGroups() ([]storage.Group, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	gs := append([]storage.Group(nil), f.groups...)
	sort.Slice(gs, func(i, j int) bool { return strings.ToLower(gs[i].Name) < strings.ToLower(gs[j].Name) })
	for i := range gs {
		gs[i].Members = f.memberCount(gs[i].ID)
	}
	return gs, nil
}

func (f *fakeUsers) GetGroup(id int64) (storage.Group, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, g := range f.groups {
		if g.ID == id {
			g.Members = f.memberCount(id)
			return g, nil
		}
	}
	return storage.Group{}, storage.ErrGroupNotFound
}

func (f *fakeUsers) CreateGroup(name string) (storage.Group, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, g := range f.groups {
		if strings.EqualFold(g.Name, name) {
			return storage.Group{}, storage.ErrGroupNameTaken
		}
	}
	now := time.Now()
	g := storage.Group{
		ID:        f.nextGroupID,
		Name:      name,
		CreatedAt: now,
		UpdatedAt: now,
	}
	f.nextGroupID++
	f.groups = append(f.groups, g)
	return g, nil
}

func (f *fakeUsers) UpdateGroup(id int64, name string) (storage.Group, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, g := range f.groups {
		if g.ID != id && strings.EqualFold(g.Name, name) {
			return storage.Group{}, storage.ErrGroupNameTaken
		}
	}
	for i := range f.groups {
		if f.groups[i].ID == id {
			f.groups[i].Name = name
			f.groups[i].UpdatedAt = time.Now()
			return f.groups[i], nil
		}
	}
	return storage.Group{}, storage.ErrGroupNotFound
}

func (f *fakeUsers) DeleteGroup(id int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i, g := range f.groups {
		if g.ID == id {
			f.groups = append(f.groups[:i], f.groups[i+1:]...)
			for _, set := range f.membership {
				delete(set, id)
			}
			return nil
		}
	}
	return storage.ErrGroupNotFound
}

func (f *fakeUsers) GroupIDsForUser(userID int64) ([]int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	set := f.membership[userID]
	out := make([]int64, 0, len(set))
	for id := range set {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

func (f *fakeUsers) SetUserGroups(userID int64, groupIDs []int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	set := make(map[int64]bool, len(groupIDs))
	for _, id := range groupIDs {
		set[id] = true
	}
	if len(set) == 0 {
		delete(f.membership, userID)
		return nil
	}
	f.membership[userID] = set
	return nil
}

func (f *fakeUsers) GroupRouting(groupID int64) (storage.GroupRouting, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, g := range f.groups {
		if g.ID != groupID {
			continue
		}
		r := f.routing[groupID]
		r.GroupID = g.ID
		r.Name = g.Name
		return r, nil
	}
	return storage.GroupRouting{}, storage.ErrGroupNotFound
}

func (f *fakeUsers) SetGroupRouting(groupID int64, actions []storage.ChannelAssignment) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, a := range actions {
		if !severity.Valid(a.MinSeverity) {
			return storage.ErrInvalidSeverity
		}
	}
	found := false
	for i := range f.groups {
		if f.groups[i].ID == groupID {
			f.groups[i].UpdatedAt = time.Now()
			found = true
		}
	}
	if !found {
		return storage.ErrGroupNotFound
	}
	f.routing[groupID] = storage.GroupRouting{
		GroupID: groupID,
		Actions: append([]storage.ChannelAssignment(nil), actions...),
	}
	return nil
}

func (f *fakeUsers) ListGroupRoutings() ([]storage.GroupRouting, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]storage.GroupRouting, 0, len(f.groups))
	for _, g := range f.groups {
		r := f.routing[g.ID]
		r.GroupID = g.ID
		r.Name = g.Name
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return strings.ToLower(out[i].Name) < strings.ToLower(out[j].Name) })
	return out, nil
}

func (f *fakeUsers) GroupRecipientEmails(groupID int64) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	emails := map[string]bool{}
	for userID, set := range f.membership {
		if !set[groupID] || f.channelOpts[userID]["smtp"] {
			continue
		}
		for _, u := range f.rows {
			if u.ID == userID && strings.TrimSpace(u.Email) != "" {
				emails[strings.ToLower(strings.TrimSpace(u.Email))] = true
			}
		}
	}
	out := make([]string, 0, len(emails))
	for e := range emails {
		out = append(out, e)
	}
	sort.Strings(out)
	return out, nil
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

func TestUsersAPRSCallsignValidation(t *testing.T) {
	env := newTestEnv(t)
	env.login()
	csrf := env.csrfFromPage("/users")

	// Malformed callsign rejected.
	resp, html := env.postForm("/users", url.Values{
		"csrf": {csrf}, "username": {"bad-cs"},
		"aprs_callsigns": {"SP9MOA-16 not-a-call"},
	})
	if resp.StatusCode != http.StatusUnprocessableEntity || !strings.Contains(html, "not a valid APRS callsign") {
		t.Fatalf("invalid callsign = %d %q", resp.StatusCode, html)
	}

	// Too many callsigns rejected.
	resp, _ = env.postForm("/users", url.Values{
		"csrf": {csrf}, "username": {"too-many"},
		"aprs_callsigns": {"SP1AAA SP1BBB SP1CCC SP1DDD SP1EEE SP1FFF SP1GGG SP1HHH SP1III"},
	})
	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("too many callsigns = %d, want 422", resp.StatusCode)
	}
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

// TestUsersNotificationPrefs pins the admin-side per-user notification
// override: the row popover posts groups (checked = subscribed) and
// delivery channels (checked = enabled, everything else is an opt-out).
func TestUsersNotificationPrefs(t *testing.T) {
	env := newTestEnv(t)
	env.login()
	csrf := env.csrfFromPage("/users")

	group, err := env.users.CreateGroup("hams")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := env.users.CreateUser("alice", "", "", "", "member", "password123"); err != nil {
		t.Fatal(err)
	}

	// alice = user 2. Subscribe her to the group and keep only aprs on.
	resp, _ := env.postForm("/users/2/prefs", url.Values{
		"csrf":     {csrf},
		"groups":   {strconv.FormatInt(group.ID, 10)},
		"channels": {"aprs"},
	})
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("prefs = %d, want redirect", resp.StatusCode)
	}
	ids, err := env.users.GroupIDsForUser(2)
	if err != nil || len(ids) != 1 || ids[0] != group.ID {
		t.Fatalf("membership = %v, %v", ids, err)
	}
	opts, err := env.users.UserChannelOptOuts(2)
	if err != nil {
		t.Fatal(err)
	}
	if !opts["smtp"] || opts["aprs"] {
		t.Fatalf("channel opt-outs = %v, want smtp disabled and aprs enabled", opts)
	}

	// The row renders both checked states back through the edit dialog
	// (membership and channel boxes live in the modal now).
	_, html := env.get("/users?edit=2")
	if !strings.Contains(html, `name="groups" value="`+strconv.FormatInt(group.ID, 10)+`" checked`) {
		t.Errorf("edit dialog missing checked group row: %s", html)
	}
	if got := strings.Count(html, `name="channels" value="aprs" checked`); got != 1 {
		t.Errorf("aprs checked rows = %d, want 1 (alice only)", got)
	}
	if strings.Contains(html, `name="channels" value="smtp" checked`) {
		t.Errorf("smtp must be unchecked for alice: %s", html)
	}
	// The table itself no longer carries the inline prefs popover.
	_, html = env.get("/users")
	if strings.Contains(html, "group-picker") {
		t.Errorf("users table still renders the inline prefs popover: %s", html)
	}

	// Clearing everything unsubscribes all groups and disables every channel.
	csrf = env.csrfFromPage("/users")
	resp, _ = env.postForm("/users/2/prefs", url.Values{"csrf": {csrf}})
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("clear prefs = %d, want redirect", resp.StatusCode)
	}
	if ids, _ := env.users.GroupIDsForUser(2); len(ids) != 0 {
		t.Fatalf("membership after clear = %v, want empty", ids)
	}
	if opts, _ := env.users.UserChannelOptOuts(2); len(opts) != len(notify.Channels) {
		t.Fatalf("opt-outs after clear = %v, want every channel disabled", opts)
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
		"aprs_callsigns": {"sp9moa-16, SR9KR"},
	})
	if resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/users" {
		t.Fatalf("create = %d %q, want redirect to /users", resp.StatusCode, resp.Header.Get("Location"))
	}
	_, html := env.get("/users")
	if !strings.Contains(html, "alice") || !strings.Contains(html, "alice@example.com") {
		t.Fatalf("created user not listed: %s", html)
	}
	for _, want := range []string{"SP9MOA-16", "SR9KR"} {
		if !strings.Contains(html, want) {
			t.Errorf("APRS callsign %s not listed for the user: %s", want, html)
		}
	}

	// Edit prefill carries the callsigns back into the form.
	_, html = env.get("/users?edit=2")
	if !strings.Contains(html, `value="SP9MOA-16 SR9KR"`) {
		t.Errorf("edit prefill missing callsigns: %s", html)
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
	if !strings.Contains(html, "&#43;48 600 999 999") {
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

// TestUsersModalPrefsApply pins the modal path of the save endpoint: with
// the prefs marker the submitted membership and channel boxes are applied;
// without it (plain saves from other flows) they stay untouched.
func TestUsersModalPrefsApply(t *testing.T) {
	env := newTestEnv(t)
	env.login()
	csrf := env.csrfFromPage("/users")

	group, err := env.users.CreateGroup("hams")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := env.users.CreateUser("alice", "", "", "", "member", "password123"); err != nil {
		t.Fatal(err)
	}

	// Save with the prefs marker: alice joins hams and keeps only aprs.
	resp, _ := env.postForm("/users", url.Values{
		"csrf": {csrf}, "edit_id": {"2"}, "username": {"alice"}, "prefs": {"1"},
		"groups":   {strconv.FormatInt(group.ID, 10)},
		"channels": {"aprs"},
	})
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("save = %d, want redirect", resp.StatusCode)
	}
	if ids, err := env.users.GroupIDsForUser(2); err != nil || len(ids) != 1 || ids[0] != group.ID {
		t.Fatalf("membership = %v, %v", ids, err)
	}
	if opts, err := env.users.UserChannelOptOuts(2); err != nil || !opts["smtp"] || opts["aprs"] {
		t.Fatalf("opt-outs = %v, %v", opts, err)
	}

	// A plain save without the marker must not wipe the prefs.
	resp, _ = env.postForm("/users", url.Values{
		"csrf": {csrf}, "edit_id": {"2"}, "username": {"alice"}, "phone": {"600700800"},
	})
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("plain save = %d, want redirect", resp.StatusCode)
	}
	if ids, _ := env.users.GroupIDsForUser(2); len(ids) != 1 {
		t.Fatalf("membership after plain save = %v, want unchanged", ids)
	}
	if opts, _ := env.users.UserChannelOptOuts(2); !opts["smtp"] || opts["aprs"] {
		t.Fatalf("opt-outs after plain save = %v, want unchanged", opts)
	}
}
