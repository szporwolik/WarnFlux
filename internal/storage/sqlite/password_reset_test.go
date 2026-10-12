package sqlite

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
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

// TestResetTokenIsSingleUseUnderConcurrency pins the reported P2: checking
// the token and marking it used were separate statements, so one link
// submitted from many places at once was accepted over and over (the
// reproduction saw 12 hits in 50 parallel attempts) and every accepted
// submission set its own password. Now the claim is one conditional
// statement: exactly one caller wins, the rest see an invalid link.
//
// This test drives the whole reset path; the atomicity of the claim
// itself is pinned by TestConsumePasswordResetIsAtomic, which exercises
// the claim without a surrounding transaction.
func TestResetTokenIsSingleUseUnderConcurrency(t *testing.T) {
	store, _, err := Open(filepath.Join(t.TempDir(), "race.db"))
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
	token, err := store.CreatePasswordReset(u.ID)
	if err != nil {
		t.Fatal(err)
	}
	const attempts = 50
	passwords := make([]string, attempts)
	for i := range passwords {
		passwords[i] = fmt.Sprintf("attempted-password-%02d", i)
	}
	var winners, unexpected atomic.Int64
	winner := atomic.Int64{}
	winner.Store(-1)
	var wg sync.WaitGroup
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, err := store.ResetPasswordWithToken(token, passwords[i]); err != nil {
				if !errors.Is(err, storage.ErrPasswordResetInvalid) {
					unexpected.Add(1)
				}
				return
			}
			winners.Add(1)
			winner.Store(int64(i))
		}(i)
	}
	wg.Wait()

	if winners.Load() != 1 {
		t.Fatalf("%d of %d parallel resets were accepted, want exactly 1", winners.Load(), attempts)
	}
	if unexpected.Load() != 0 {
		t.Errorf("%d attempts failed with an unexpected error, want an invalid link", unexpected.Load())
	}
	// The stored password is the winner's — no other submission's password
	// can have landed, and the old one is gone.
	w := int(winner.Load())
	if w < 0 {
		t.Fatal("no winner recorded")
	}
	if _, err := store.Authenticate("alice", passwords[w]); err != nil {
		t.Errorf("the winning password does not authenticate: %v", err)
	}
	if _, err := store.Authenticate("alice", "start-password-1"); err == nil {
		t.Error("the password from before the reset still works")
	}
	// The link is spent for everyone, including a later attempt.
	if _, err := store.ResetPasswordWithToken(token, "late-attempt-1"); !errors.Is(err, storage.ErrPasswordResetInvalid) {
		t.Errorf("late reuse of the spent link = %v, want ErrPasswordResetInvalid", err)
	}
}

// TestConsumePasswordResetIsAtomic pins the same claim through the
// standalone consume API (the panel's step-by-step path): one success, the
// rest invalid.
func TestConsumePasswordResetIsAtomic(t *testing.T) {
	store, _, err := Open(filepath.Join(t.TempDir(), "consume.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()
	if err := store.EnsureAdminUser("admin", "secret123"); err != nil {
		t.Fatal(err)
	}
	u, err := store.CreateUser("bob", "", "", "", "member", "start-password-1")
	if err != nil {
		t.Fatal(err)
	}
	token, err := store.CreatePasswordReset(u.ID)
	if err != nil {
		t.Fatal(err)
	}

	const attempts = 50
	var okCount, unexpected atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := store.ConsumePasswordReset(token); err != nil {
				if !errors.Is(err, storage.ErrPasswordResetInvalid) {
					unexpected.Add(1)
				}
				return
			}
			okCount.Add(1)
		}()
	}
	wg.Wait()
	if okCount.Load() != 1 {
		t.Fatalf("the same token was consumed %d times, want exactly 1", okCount.Load())
	}
	if unexpected.Load() != 0 {
		t.Errorf("%d consumes failed with an unexpected error", unexpected.Load())
	}
}
