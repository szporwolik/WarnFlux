package sqlite

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/szporwolik/WarnFlux/internal/storage"
)

// TestAuthVersionRetiresOlderSessions pins the version a session is bound to.
// Every change that ends sessions (a new password, a new role, a new username)
// raises the account's credential version, other edits leave it alone, and
// Authenticate reports the version it verified the password against — the
// value a session must carry to stay valid.
func TestAuthVersionRetiresOlderSessions(t *testing.T) {
	store, _, err := Open(filepath.Join(t.TempDir(), "version.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()
	if err := store.EnsureAdminUser("admin", "secret123"); err != nil {
		t.Fatal(err)
	}
	u, err := store.CreateUser("alice", "", "alice@example.com", "", "member", "start-password-1")
	if err != nil {
		t.Fatal(err)
	}
	version := func() int64 {
		t.Helper()
		v, err := store.AuthVersion(u.ID)
		if err != nil {
			t.Fatalf("AuthVersion: %v", err)
		}
		return v
	}
	start := version()
	if start == 0 {
		t.Fatalf("a fresh account carries version %d, want a real one", start)
	}

	// A verification carries the version of the row it read the hash from.
	auth, err := store.Authenticate("alice", "start-password-1")
	if err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	if auth.AuthVersion != start {
		t.Fatalf("verified version = %d, want %d", auth.AuthVersion, start)
	}

	// An edit of the contact fields only keeps the version: saving the
	// account page must not sign the operator out.
	if _, err := store.UpdateUser(u.ID, "alice", "+48600000000", "alice@example.com", "", "member", ""); err != nil {
		t.Fatal(err)
	}
	if got := version(); got != start {
		t.Fatalf("a contact-only edit moved the version %d -> %d", start, got)
	}

	// A new password retires the sessions opened with the old one.
	if err := store.SetUserPassword(u.ID, "second-password-2"); err != nil {
		t.Fatal(err)
	}
	afterPassword := start + 1
	if got := version(); got != afterPassword {
		t.Fatalf("version after SetUserPassword = %d, want %d", got, afterPassword)
	}
	auth, err = store.Authenticate("alice", "second-password-2")
	if err != nil {
		t.Fatalf("Authenticate with the new password: %v", err)
	}
	if auth.AuthVersion != afterPassword {
		t.Fatalf("verified version after the change = %d, want %d", auth.AuthVersion, afterPassword)
	}

	// A role change retires them too, without touching the hash.
	if _, err := store.UpdateUser(u.ID, "alice", "+48600000000", "alice@example.com", "", "emcom", ""); err != nil {
		t.Fatal(err)
	}
	afterRole := afterPassword + 1
	if got := version(); got != afterRole {
		t.Fatalf("version after a role change = %d, want %d", got, afterRole)
	}

	// So does a rename: the session names the old account.
	if _, err := store.UpdateUser(u.ID, "alice2", "+48600000000", "alice@example.com", "", "emcom", ""); err != nil {
		t.Fatal(err)
	}
	afterRename := afterRole + 1
	if got := version(); got != afterRename {
		t.Fatalf("version after a rename = %d, want %d", got, afterRename)
	}

	// The password handed to UpdateUser counts as a password change.
	if _, err := store.UpdateUser(u.ID, "alice2", "", "alice@example.com", "", "emcom", "third-password-3"); err != nil {
		t.Fatal(err)
	}
	afterEditPassword := afterRename + 1
	if got := version(); got != afterEditPassword {
		t.Fatalf("version after UpdateUser(password) = %d, want %d", got, afterEditPassword)
	}

	// And the self-service reset spends a token to do the same.
	token := issueResetToken(t, store, u.ID)
	if _, err := store.ResetPasswordWithToken(token, "fourth-password-4"); err != nil {
		t.Fatal(err)
	}
	if got := version(); got != afterEditPassword+1 {
		t.Fatalf("version after ResetPasswordWithToken = %d, want %d", got, afterEditPassword+1)
	}

	// An unknown account has no version at all: a session naming it must die.
	if _, err := store.AuthVersion(9999); !errors.Is(err, storage.ErrUserNotFound) {
		t.Fatalf("AuthVersion of a missing account = %v, want ErrUserNotFound", err)
	}
}
