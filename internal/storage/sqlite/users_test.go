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
	if err := store.EnsureAdminUser("admin"); err != nil {
		t.Fatal(err)
	}
	if err := store.EnsureAdminUser("admin"); err != nil {
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

func TestUsersCRUDAndProtection(t *testing.T) {
	store := newUsersStore(t)
	if err := store.EnsureAdminUser("admin"); err != nil {
		t.Fatal(err)
	}

	// Create.
	alice, err := store.CreateUser("alice", "+48 600 100 200", "alice@example.com", "alice#1234")
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
	if _, err := store.CreateUser("Alice", "", "", ""); !errors.Is(err, storage.ErrUsernameTaken) {
		t.Fatalf("duplicate create = %v, want ErrUsernameTaken", err)
	}

	// Update.
	alice, err = store.UpdateUser(alice.ID, "alice", "+48 600 999 999", "alice@example.com", "alice#9999")
	if err != nil {
		t.Fatalf("UpdateUser: %v", err)
	}
	if alice.Phone != "+48 600 999 999" || alice.Discord != "alice#9999" {
		t.Fatalf("updated user = %+v", alice)
	}

	// Admin row is protected.
	if _, err := store.UpdateUser(1, "admin", "x", "", ""); !errors.Is(err, storage.ErrUserProtected) {
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

func TestUsersPagination(t *testing.T) {
	clock := time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)
	store, _, err := Open(filepath.Join(t.TempDir(), "users.db"), WithClock(func() time.Time { return clock }))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()
	if err := store.EnsureAdminUser("admin"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 12; i++ {
		name := "user" + string(rune('a'+i))
		if _, err := store.CreateUser(name, "", "", ""); err != nil {
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
