package sqlite

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/storage"
)

func newUsersStore(t *testing.T) *Store {
	t.Helper()
	store, _, err := Open(filepath.Join(t.TempDir(), "users.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

func TestUsersTableMigratesToV5(t *testing.T) {
	store := newUsersStore(t)
	var v int
	if err := store.db.QueryRow("PRAGMA user_version").Scan(&v); err != nil {
		t.Fatal(err)
	}
	if v != len(migrations) {
		t.Fatalf("schema version = %d, want %d", v, len(migrations))
	}
}

func TestEnsureAdminUserIsIdempotent(t *testing.T) {
	store := newUsersStore(t)
	if err := store.EnsureAdminUser("admin", "secret123"); err != nil {
		t.Fatal(err)
	}
	if err := store.EnsureAdminUser("admin", "secret123"); err != nil {
		t.Fatal(err)
	}
	users, total, err := store.ListUsers(1, 10)
	if err != nil {
		t.Fatal(err)
	}
	if total != 1 || len(users) != 1 || !users[0].IsAdmin || users[0].Username != "admin" {
		t.Fatalf("users = %+v total=%d, want one admin row", users, total)
	}
}

// TestEnsureAdminUserSyncsPassword pins the config-authoritative contract:
// the admin row stores the YAML password, an unchanged password does not
// churn the row, and a changed password re-syncs the hash.
func TestEnsureAdminUserSyncsPassword(t *testing.T) {
	store := newUsersStore(t)
	if err := store.EnsureAdminUser("admin", "secret-one"); err != nil {
		t.Fatal(err)
	}

	salt, hash, updated := func() (string, string, int64) {
		var s, h string
		var u int64
		if err := store.db.QueryRow(`SELECT password_salt, password_hash, updated_at_ms FROM users WHERE username = 'admin'`).Scan(&s, &h, &u); err != nil {
			t.Fatal(err)
		}
		return s, h, u
	}()
	if !verifyPassword("secret-one", salt, hash) {
		t.Fatal("admin row does not verify the configured password")
	}

	// Same password: no change.
	if err := store.EnsureAdminUser("admin", "secret-one"); err != nil {
		t.Fatal(err)
	}
	var updatedAfter int64
	if err := store.db.QueryRow(`SELECT updated_at_ms FROM users WHERE username = 'admin'`).Scan(&updatedAfter); err != nil {
		t.Fatal(err)
	}
	if updatedAfter != updated {
		t.Fatal("unchanged password still churned the admin row")
	}

	// Changed password: the stored hash follows the config.
	if err := store.EnsureAdminUser("admin", "secret-two"); err != nil {
		t.Fatal(err)
	}
	var s2, h2 string
	if err := store.db.QueryRow(`SELECT password_salt, password_hash FROM users WHERE username = 'admin'`).Scan(&s2, &h2); err != nil {
		t.Fatal(err)
	}
	if !verifyPassword("secret-two", s2, h2) {
		t.Fatal("admin row does not verify the new configured password")
	}
	if verifyPassword("secret-one", s2, h2) {
		t.Fatal("admin row still verifies the old password")
	}
}

func TestUsersCRUDAndProtection(t *testing.T) {
	store := newUsersStore(t)
	if err := store.EnsureAdminUser("admin", "secret123"); err != nil {
		t.Fatal(err)
	}

	// Create.
	alice, err := store.CreateUser("alice", "+48 600 100 200", "alice@example.com", "alice#1234", "", "")
	if err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	if alice.ID == 0 || alice.Username != "alice" || alice.IsAdmin {
		t.Fatalf("created user = %+v", alice)
	}
	if alice.CreatedAt.IsZero() || alice.UpdatedAt.IsZero() {
		t.Fatalf("timestamps missing: %+v", alice)
	}

	// Duplicate (case-insensitive).
	if _, err := store.CreateUser("Alice", "", "", "", "", ""); !errors.Is(err, storage.ErrUsernameTaken) {
		t.Fatalf("duplicate create = %v, want ErrUsernameTaken", err)
	}

	// Update.
	alice, err = store.UpdateUser(alice.ID, "alice", "+48 600 999 999", "alice@example.com", "alice#9999", "", "")
	if err != nil {
		t.Fatalf("UpdateUser: %v", err)
	}
	if alice.Phone != "+48 600 999 999" || alice.Discord != "alice#9999" {
		t.Fatalf("updated user = %+v", alice)
	}

	// Admin row is protected.
	if _, err := store.UpdateUser(1, "admin", "x", "", "", "", ""); !errors.Is(err, storage.ErrUserProtected) {
		t.Fatalf("admin update = %v, want ErrUserProtected", err)
	}
	if err := store.DeleteUser(1); !errors.Is(err, storage.ErrUserProtected) {
		t.Fatalf("admin delete = %v, want ErrUserProtected", err)
	}

	// Missing row.
	if err := store.DeleteUser(999); !errors.Is(err, storage.ErrUserNotFound) {
		t.Fatalf("missing delete = %v, want ErrUserNotFound", err)
	}

	// Delete the regular user.
	if err := store.DeleteUser(alice.ID); err != nil {
		t.Fatalf("DeleteUser: %v", err)
	}
	users, total, err := store.ListUsers(1, 10)
	if err != nil {
		t.Fatal(err)
	}
	if total != 1 || users[0].Username != "admin" {
		t.Fatalf("after delete: %+v total=%d", users, total)
	}
}

// TestUserAuthenticate pins password-based sign-in for directory users:
// correct credentials return the user with their role, wrong or unknown
// credentials return ErrBadCredentials, and role-less users cannot sign in.
func TestUserAuthenticate(t *testing.T) {
	store := newUsersStore(t)
	if err := store.EnsureAdminUser("admin", "secret123"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateUser("emcom-user", "", "", "", "emcom", "hunter2secret"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateUser("no-role", "", "", "", "", ""); err != nil {
		t.Fatal(err)
	}

	u, err := store.Authenticate("emcom-user", "hunter2secret")
	if err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	if u.Username != "emcom-user" || u.Role != "emcom" {
		t.Fatalf("authenticated user = %+v", u)
	}

	if _, err := store.Authenticate("emcom-user", "wrong"); !errors.Is(err, storage.ErrBadCredentials) {
		t.Fatalf("wrong password = %v, want ErrBadCredentials", err)
	}
	if _, err := store.Authenticate("nobody", "x"); !errors.Is(err, storage.ErrBadCredentials) {
		t.Fatalf("unknown user = %v, want ErrBadCredentials", err)
	}
	if _, err := store.Authenticate("no-role", ""); !errors.Is(err, storage.ErrBadCredentials) {
		t.Fatalf("role-less user = %v, want ErrBadCredentials", err)
	}
}

// TestUserAPRSCallsigns pins the per-user APRS callsign registry: store,
// normalize/dedupe, replace, protect and group-recipient collection.
func TestUserAPRSCallsigns(t *testing.T) {
	store := newUsersStore(t)
	if err := store.EnsureAdminUser("admin", "secret123"); err != nil {
		t.Fatal(err)
	}
	alice, err := store.CreateUser("alice", "", "", "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateGroup("ops"); err != nil {
		t.Fatal(err)
	}

	// Store, normalize and dedupe.
	if err := store.SetUserAPRS(alice.ID, []string{"sp9moa-16", "SR9KR", " sp9moa-16 "}); err != nil {
		t.Fatalf("SetUserAPRS: %v", err)
	}
	got, err := store.GetUser(alice.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.APRSCallsigns) != 2 || got.APRSCallsigns[0] != "SP9MOA-16" || got.APRSCallsigns[1] != "SR9KR" {
		t.Fatalf("callsigns = %v, want [SP9MOA-16 SR9KR]", got.APRSCallsigns)
	}

	// Visible through ListUsers too.
	users, _, err := store.ListUsers(1, 10)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, u := range users {
		if u.ID == alice.ID {
			found = len(u.APRSCallsigns) == 2
		}
	}
	if !found {
		t.Fatalf("ListUsers missing callsigns: %+v", users)
	}

	// Replace.
	if err := store.SetUserAPRS(alice.ID, []string{"SP9ABC"}); err != nil {
		t.Fatal(err)
	}
	got, _ = store.GetUser(alice.ID)
	if len(got.APRSCallsigns) != 1 || got.APRSCallsigns[0] != "SP9ABC" {
		t.Fatalf("after replace = %v", got.APRSCallsigns)
	}

	// Group recipients collect the member callsigns.
	if err := store.SetUserGroups(alice.ID, []int64{1}); err != nil {
		t.Fatal(err)
	}
	calls, err := store.GroupRecipientAPRS(1)
	if err != nil {
		t.Fatal(err)
	}
	if len(calls) != 1 || calls[0] != "SP9ABC" {
		t.Fatalf("group aprs recipients = %v", calls)
	}
	if calls, err := store.GroupRecipientAPRS(999); err != nil || len(calls) != 0 {
		t.Fatalf("unknown group = %v, %v", calls, err)
	}

	// Protected and missing users.
	if err := store.SetUserAPRS(1, []string{"SP9MOA-16"}); !errors.Is(err, storage.ErrUserProtected) {
		t.Fatalf("admin SetUserAPRS = %v, want ErrUserProtected", err)
	}
	if err := store.SetUserAPRS(999, []string{"SP9MOA-16"}); !errors.Is(err, storage.ErrUserNotFound) {
		t.Fatalf("missing SetUserAPRS = %v, want ErrUserNotFound", err)
	}
}

func TestUsersPagination(t *testing.T) {
	clock := time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)
	store, _, err := Open(filepath.Join(t.TempDir(), "users.db"), WithClock(func() time.Time { return clock }))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()
	if err := store.EnsureAdminUser("admin", "secret123"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 12; i++ {
		name := "user" + string(rune('a'+i))
		if _, err := store.CreateUser(name, "", "", "", "", ""); err != nil {
			t.Fatalf("CreateUser %s: %v", name, err)
		}
	}
	users, total, err := store.ListUsers(1, 5)
	if err != nil {
		t.Fatal(err)
	}
	if total != 13 || len(users) != 5 || users[0].Username != "admin" {
		t.Fatalf("page1 = %d/%d first=%+v", len(users), total, users[0])
	}
	users, _, err = store.ListUsers(3, 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(users) != 3 {
		t.Fatalf("page3 = %d rows, want 3 (clamped last page)", len(users))
	}
}
