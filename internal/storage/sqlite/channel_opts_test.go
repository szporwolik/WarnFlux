package sqlite

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/szporwolik/WarnFlux/internal/storage"
)

// TestUserChannelOptOuts pins the opt-out round trip: replace semantics,
// normalization, protected/missing-user errors, and the effect on the
// per-channel recipient lists used by the rule engine.
func TestUserChannelOptOuts(t *testing.T) {
	store, _, err := Open(filepath.Join(t.TempDir(), "opts.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()
	if err := store.EnsureAdminUser("admin", "secret123"); err != nil {
		t.Fatal(err)
	}
	ada, err := store.CreateUser("ada", "", "ada@example.com", "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	bea, err := store.CreateUser("bea", "", "bea@example.com", "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	g, err := store.CreateGroup("ops")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SetUserGroups(ada.ID, []int64{g.ID}); err != nil {
		t.Fatal(err)
	}
	if err := store.SetUserGroups(bea.ID, []int64{g.ID}); err != nil {
		t.Fatal(err)
	}
	if err := store.SetUserAPRS(ada.ID, []string{"SP9MOA-16"}); err != nil {
		t.Fatal(err)
	}
	if err := store.SetUserAPRS(bea.ID, []string{"SP9BEA"}); err != nil {
		t.Fatal(err)
	}

	// Default: nothing disabled, every channel reaches every member.
	opts, err := store.UserChannelOptOuts(ada.ID)
	if err != nil || len(opts) != 0 {
		t.Fatalf("default opt-outs = %v, %v", opts, err)
	}
	if emails, err := store.GroupRecipientEmails(g.ID); err != nil || len(emails) != 2 {
		t.Fatalf("default emails = %v, %v", emails, err)
	}
	if calls, err := store.GroupRecipientAPRS(g.ID); err != nil || len(calls) != 2 {
		t.Fatalf("default callsigns = %v, %v", calls, err)
	}

	// Opt ada out of smtp only: email list drops her, APRS still gets her.
	if err := store.SetUserChannelOptOuts(ada.ID, []string{"smtp"}); err != nil {
		t.Fatalf("SetUserChannelOptOuts: %v", err)
	}
	opts, err = store.UserChannelOptOuts(ada.ID)
	if err != nil || len(opts) != 1 || !opts["smtp"] {
		t.Fatalf("opt-outs = %v, %v", opts, err)
	}
	emails, err := store.GroupRecipientEmails(g.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(emails) != 1 || emails[0] != "bea@example.com" {
		t.Fatalf("emails after smtp opt-out = %v, want [bea@example.com]", emails)
	}
	calls, err := store.GroupRecipientAPRS(g.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(calls) != 2 {
		t.Fatalf("callsigns after smtp opt-out = %v, want both members", calls)
	}

	// Opt ada out of aprs as well: both lists drop her.
	if err := store.SetUserChannelOptOuts(ada.ID, []string{"aprs", "smtp"}); err != nil {
		t.Fatalf("SetUserChannelOptOuts both: %v", err)
	}
	if calls, err := store.GroupRecipientAPRS(g.ID); err != nil || len(calls) != 1 || calls[0] != "SP9BEA" {
		t.Fatalf("callsigns after aprs opt-out = %v, %v", calls, err)
	}

	// Replace semantics: clearing the opt-outs restores full delivery,
	// and kinds are normalized (lowercase) and de-duplicated.
	if err := store.SetUserChannelOptOuts(ada.ID, []string{"APRS", "aprs"}); err != nil {
		t.Fatalf("SetUserChannelOptOuts dedupe: %v", err)
	}
	opts, _ = store.UserChannelOptOuts(ada.ID)
	if len(opts) != 1 || !opts["aprs"] || opts["smtp"] {
		t.Fatalf("after replace = %v, want only aprs", opts)
	}
	if err := store.SetUserChannelOptOuts(ada.ID, nil); err != nil {
		t.Fatalf("clear: %v", err)
	}
	if emails, err := store.GroupRecipientEmails(g.ID); err != nil || len(emails) != 2 {
		t.Fatalf("emails after clear = %v, %v", emails, err)
	}

	// Protected and missing users.
	if err := store.SetUserChannelOptOuts(1, []string{"smtp"}); !errors.Is(err, storage.ErrUserProtected) {
		t.Fatalf("admin opt-outs = %v, want ErrUserProtected", err)
	}
	if err := store.SetUserChannelOptOuts(999, []string{"smtp"}); !errors.Is(err, storage.ErrUserNotFound) {
		t.Fatalf("missing opt-outs = %v, want ErrUserNotFound", err)
	}
}
