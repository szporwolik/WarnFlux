package sqlite

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

// TestDigestCountersCountsOnlyTheWindow proves the report covers the last 24
// hours and nothing else: a message one millisecond outside the window must not
// appear, and the receive/send split comes from the direction column.
func TestDigestCountersCountsOnlyTheWindow(t *testing.T) {
	store, _, err := Open(filepath.Join(t.TempDir(), "digest.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	ctx := context.Background()
	now := time.Now()

	in := now.Add(-time.Hour)
	out := now.Add(-25 * time.Hour) // just outside the window

	if err := store.RecordGSMMessage(ctx, "rx", "+48600111222", "self", "in", in); err != nil {
		t.Fatalf("RecordGSMMessage: %v", err)
	}
	if err := store.RecordGSMMessage(ctx, "tx", "self", "+48600111222", "out", in); err != nil {
		t.Fatalf("RecordGSMMessage: %v", err)
	}
	if err := store.RecordGSMMessage(ctx, "rx", "+48600111222", "self", "old", out); err != nil {
		t.Fatalf("RecordGSMMessage: %v", err)
	}

	c, err := store.DigestCounters(ctx, now.Add(-24*time.Hour), now)
	if err != nil {
		t.Fatalf("DigestCounters: %v", err)
	}
	if c.SMSReceived != 1 || c.SMSSent != 1 {
		t.Errorf("SMS rx/tx = %d/%d, want 1/1 (the 25-hour-old row must not count)", c.SMSReceived, c.SMSSent)
	}
}

// TestAdminEmailsListsAdministratorsOnly pins the fallback recipient list.
func TestAdminEmailsListsAdministratorsOnly(t *testing.T) {
	store, _, err := Open(filepath.Join(t.TempDir(), "admins.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	ctx := context.Background()
	if err := store.EnsureAdminUser("root", "secret-password"); err != nil {
		t.Fatalf("EnsureAdminUser: %v", err)
	}
	root, err := store.GetUserByUsername("root")
	if err != nil {
		t.Fatalf("GetUserByUsername: %v", err)
	}
	if _, err := store.UpdateUser(root.ID, "root", "", "root@example.org", "", "admin", ""); err != nil {
		t.Fatalf("UpdateUser: %v", err)
	}
	if _, err := store.CreateUser("member", "", "member@example.org", "", "user", "secret-password"); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}

	got, err := store.AdminEmails(ctx)
	if err != nil {
		t.Fatalf("AdminEmails: %v", err)
	}
	if len(got) != 1 || got[0] != "root@example.org" {
		t.Fatalf("AdminEmails = %v, want only the administrator", got)
	}
}
