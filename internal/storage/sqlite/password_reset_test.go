package sqlite

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/szporwolik/WarnFlux/internal/storage"
)

// TestPasswordChangePurgesResetTokens pins the reported P1 at the storage
// boundary: a password change invalidates every reset link issued BEFORE
// it, in the same transaction as the new hash. Otherwise a link stolen
// earlier stays a working way back into the account — the owner changes
// the password, and the intruder still holds a recovery token.
func TestPasswordChangePurgesResetTokens(t *testing.T) {
	store, _, err := Open(filepath.Join(t.TempDir(), "reset.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()
	if err := store.EnsureAdminUser("admin", "secret123"); err != nil {
		t.Fatal(err)
	}
	u, err := store.CreateUser("alice", "", "alice@example.com", "", "member", "password123")
	if err != nil {
		t.Fatal(err)
	}

	// A reset link issued before the change dies with it.
	token, err := store.CreatePasswordReset(u.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SetUserPassword(u.ID, "first-change-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ConsumePasswordReset(token); !errors.Is(err, storage.ErrPasswordResetInvalid) {
		t.Errorf("token issued before SetUserPassword = %v, want ErrPasswordResetInvalid", err)
	}

	// The same holds for the edit path an administrator uses to set a new
	// password (UpdateUser with a password).
	token, err = store.CreatePasswordReset(u.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.UpdateUser(u.ID, "alice", "", "alice@example.com", "", "member", "second-change-2"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ConsumePasswordReset(token); !errors.Is(err, storage.ErrPasswordResetInvalid) {
		t.Errorf("token issued before UpdateUser(password) = %v, want ErrPasswordResetInvalid", err)
	}
	if _, err := store.Authenticate("alice", "second-change-2"); err != nil {
		t.Fatalf("the new password does not authenticate: %v", err)
	}

	// An edit that leaves the password alone must NOT invalidate a link
	// the owner asked for: only a password change ends the recovery path.
	token, err = store.CreatePasswordReset(u.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.UpdateUser(u.ID, "alice", "+48600000000", "alice@example.com", "", "member", ""); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ConsumePasswordReset(token); err != nil {
		t.Errorf("a password-less edit invalidated a live reset link: %v", err)
	}
}
