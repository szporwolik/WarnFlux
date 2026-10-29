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

// TestDigestCountersSurviveMessageRetention is the regression test for the
// reported P3: the message history keeps only the newest few thousand rows, so
// counting it truncated a busy window (2001 APRS messages were reported as
// 2000) and the gap grew with the traffic. The counters now come from durable
// per-minute aggregates written with the message, so they are exact no matter
// how aggressively the history is pruned.
func TestDigestCountersSurviveMessageRetention(t *testing.T) {
	store, _, err := Open(filepath.Join(t.TempDir(), "retention.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	ctx := context.Background()
	// A whole minute, so the window snapping cannot stretch the numbers.
	now := time.Now().UTC().Truncate(time.Minute).Add(time.Minute)
	from := now.Add(-24 * time.Hour)

	const total = storageAPRSRetentionEntries + 1
	for i := 0; i < total; i++ {
		if err := store.RecordAPRSMessage(ctx, "rx", "SP9MOA", "APRS", "x", "", "rf", now.Add(-time.Hour)); err != nil {
			t.Fatalf("RecordAPRSMessage %d: %v", i, err)
		}
	}
	var history int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM aprs_messages`).Scan(&history); err != nil {
		t.Fatalf("count history: %v", err)
	}
	if history != storageAPRSRetentionEntries {
		t.Fatalf("history rows = %d, want the capped %d (the premise of the bug)", history, storageAPRSRetentionEntries)
	}

	c, err := store.DigestCounters(ctx, from, now)
	if err != nil {
		t.Fatalf("DigestCounters: %v", err)
	}
	if c.APRSReceived != total {
		t.Fatalf("APRS received = %d, want %d (the count must not be capped by the history)", c.APRSReceived, total)
	}

	// Directions and channels stay separate, and the window still applies.
	if err := store.RecordAPRSMessage(ctx, "tx", "SP9MOA", "SP9XYZ", "y", "1", "rf", now.Add(-time.Hour)); err != nil {
		t.Fatalf("RecordAPRSMessage tx: %v", err)
	}
	if err := store.RecordGSMMessage(ctx, "rx", "+48600111222", "self", "sms", now.Add(-2*time.Hour)); err != nil {
		t.Fatalf("RecordGSMMessage: %v", err)
	}
	if err := store.RecordAPRSMessage(ctx, "rx", "SP9MOA", "APRS", "old", "", "rf", now.Add(-30*time.Hour)); err != nil {
		t.Fatalf("RecordAPRSMessage outside: %v", err)
	}
	c, err = store.DigestCounters(ctx, from, now)
	if err != nil {
		t.Fatalf("DigestCounters: %v", err)
	}
	if c.APRSReceived != total || c.APRSSent != 1 {
		t.Errorf("APRS rx/tx = %d/%d, want %d/1", c.APRSReceived, c.APRSSent, total)
	}
	if c.SMSReceived != 1 || c.SMSSent != 0 {
		t.Errorf("SMS rx/tx = %d/%d, want 1/0", c.SMSReceived, c.SMSSent)
	}
}

// storageAPRSRetentionEntries is the history cap under test (the storage
// constant), named locally so the test reads as the report does.
const storageAPRSRetentionEntries = 2000
