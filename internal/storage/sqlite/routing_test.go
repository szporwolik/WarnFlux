package sqlite

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/storage"
)

func newRoutingStore(t *testing.T) *Store {
	t.Helper()
	store, _, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

func TestGroupRoutingRoundTrip(t *testing.T) {
	store := newRoutingStore(t)

	g, err := store.CreateGroup("spok-ops")
	if err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}

	// A fresh group defaults to the permissive threshold and no channels.
	r, err := store.GroupRouting(g.ID)
	if err != nil {
		t.Fatalf("GroupRouting: %v", err)
	}
	if len(r.Actions) != 0 {
		t.Fatalf("default routing = %+v, want no actions", r)
	}

	err = store.SetGroupRouting(g.ID,
		[]storage.ChannelAssignment{
			{ID: "log-alerts", MinSeverity: "severe"},
			{Source: "imgw-meteo", ID: "log-alerts", MinSeverity: "minor"},    // same ID, other source: kept
			{Source: "imgw-meteo", ID: "log-alerts", MinSeverity: "moderate"}, // duplicate cell: first kept
			{Source: "rso", ID: "sms", MinSeverity: "moderate"},
		})
	if err != nil {
		t.Fatalf("SetGroupRouting: %v", err)
	}

	r, err = store.GroupRouting(g.ID)
	if err != nil {
		t.Fatalf("GroupRouting after set: %v", err)
	}
	wantActions := []storage.ChannelAssignment{
		{ID: "log-alerts", MinSeverity: "severe"},
		{Source: "imgw-meteo", ID: "log-alerts", MinSeverity: "minor"},
		{Source: "rso", ID: "sms", MinSeverity: "moderate"},
	}
	if !reflect.DeepEqual(r.Actions, wantActions) {
		t.Errorf("Actions = %+v, want %+v (deduplicated per source+ID, first severity kept, sorted)", r.Actions, wantActions)
	}

	// Empty assignment clears the matrix but keeps the group.
	if err := store.SetGroupRouting(g.ID, nil); err != nil {
		t.Fatalf("SetGroupRouting clear: %v", err)
	}
	r, err = store.GroupRouting(g.ID)
	if err != nil {
		t.Fatalf("GroupRouting after clear: %v", err)
	}
	if len(r.Actions) != 0 {
		t.Fatalf("routing after clear = %+v, want empty", r)
	}
}

func TestGroupRoutingErrors(t *testing.T) {
	store := newRoutingStore(t)

	if err := store.SetGroupRouting(999, nil); !errors.Is(err, storage.ErrGroupNotFound) {
		t.Fatalf("missing group error = %v, want ErrGroupNotFound", err)
	}
	if _, err := store.GroupRouting(999); !errors.Is(err, storage.ErrGroupNotFound) {
		t.Fatalf("GroupRouting missing = %v, want ErrGroupNotFound", err)
	}

	g, err := store.CreateGroup("rsp")
	if err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
	if err := store.SetGroupRouting(g.ID, []storage.ChannelAssignment{{ID: "a", MinSeverity: "orange"}}); !errors.Is(err, storage.ErrInvalidSeverity) {
		t.Fatalf("invalid action severity error = %v, want ErrInvalidSeverity", err)
	}
	if err := store.SetGroupRouting(g.ID, []storage.ChannelAssignment{{ID: "a", MinSeverity: "EXTREME"}}); !errors.Is(err, storage.ErrInvalidSeverity) {
		t.Fatalf("non-canonical severity error = %v, want ErrInvalidSeverity", err)
	}
}

// TestClaimActionFire pins the durable delivery ledger: a (group, action,
// event) claim succeeds exactly once and PruneActionFires removes rows by
// age only.
func TestClaimActionFire(t *testing.T) {
	store := newRoutingStore(t)

	g, err := store.CreateGroup("spok")
	if err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
	at := time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)

	first, err := store.ClaimActionFire(g.ID, "log", "imgw:1", "c:imgw:imgw:1:7", at)
	if err != nil || !first {
		t.Fatalf("first claim = (%v, %v), want (true, nil)", first, err)
	}
	second, err := store.ClaimActionFire(g.ID, "log", "imgw:1", "c:imgw:imgw:1:7", at.Add(time.Second))
	if err != nil || second {
		t.Fatalf("second claim = (%v, %v), want (false, nil)", second, err)
	}
	// Same dedup key but a different action or group is a new claim.
	otherAction, err := store.ClaimActionFire(g.ID, "sms", "imgw:1", "c:imgw:imgw:1:7", at)
	if err != nil || !otherAction {
		t.Fatalf("other action claim = (%v, %v), want (true, nil)", otherAction, err)
	}
	h, err := store.CreateGroup("rsp")
	if err != nil {
		t.Fatalf("CreateGroup rsp: %v", err)
	}
	otherGroup, err := store.ClaimActionFire(h.ID, "log", "imgw:1", "c:imgw:imgw:1:7", at)
	if err != nil || !otherGroup {
		t.Fatalf("other group claim = (%v, %v), want (true, nil)", otherGroup, err)
	}

	// Prune with a cutoff before every row: nothing removed.
	n, err := store.PruneActionFires(at.Add(-time.Minute))
	if err != nil || n != 0 {
		t.Fatalf("young prune = (%d, %v), want (0, nil)", n, err)
	}
	// A later change from the same event key is a fresh claim.
	fresh, err := store.ClaimActionFire(g.ID, "log", "imgw:1", "c:imgw:imgw:1:8", at.Add(time.Hour))
	if err != nil || !fresh {
		t.Fatalf("fresh change claim = (%v, %v), want (true, nil)", fresh, err)
	}
	// Cutoff after the first three rows but before the fresh change:
	// exactly the old rows go.
	n, err = store.PruneActionFires(at.Add(30 * time.Minute))
	if err != nil || n != 3 {
		t.Fatalf("partial prune = (%d, %v), want (3, nil)", n, err)
	}
	// Pruning everything: the ledger is empty and dedup resets.
	n, err = store.PruneActionFires(at.Add(2 * time.Hour))
	if err != nil || n != 1 {
		t.Fatalf("full prune = (%d, %v), want (1, nil)", n, err)
	}
	again, err := store.ClaimActionFire(g.ID, "log", "imgw:1", "c:imgw:imgw:1:7", at)
	if err != nil || !again {
		t.Fatalf("claim after prune = (%v, %v), want (true, nil)", again, err)
	}
}

func TestListGroupRoutings(t *testing.T) {
	store := newRoutingStore(t)

	a, err := store.CreateGroup("alpha")
	if err != nil {
		t.Fatalf("CreateGroup alpha: %v", err)
	}
	b, err := store.CreateGroup("bravo")
	if err != nil {
		t.Fatalf("CreateGroup bravo: %v", err)
	}
	if err := store.SetGroupRouting(a.ID,
		[]storage.ChannelAssignment{{ID: "log", MinSeverity: "moderate"}}); err != nil {
		t.Fatalf("route alpha: %v", err)
	}
	// bravo stays untouched: no actions.

	all, err := store.ListGroupRoutings()
	if err != nil {
		t.Fatalf("ListGroupRoutings: %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("ListGroupRoutings returned %d rules, want 2", len(all))
	}
	if all[0].Name != "alpha" ||
		!reflect.DeepEqual(all[0].Actions, []storage.ChannelAssignment{{ID: "log", MinSeverity: "moderate"}}) {
		t.Errorf("alpha rule = %+v", all[0])
	}
	if all[1].Name != "bravo" || len(all[1].Actions) != 0 {
		t.Errorf("bravo rule = %+v", all[1])
	}

	// Deleting a group removes its routing rows (FK cascade).
	if err := store.DeleteGroup(a.ID); err != nil {
		t.Fatalf("DeleteGroup: %v", err)
	}
	all, err = store.ListGroupRoutings()
	if err != nil {
		t.Fatalf("ListGroupRoutings after delete: %v", err)
	}
	if len(all) != 1 || all[0].GroupID != b.ID {
		t.Fatalf("routings after delete = %+v, want only bravo", all)
	}
}

func TestGroupRecipientEmails(t *testing.T) {
	store := newRoutingStore(t)

	g, err := store.CreateGroup("spok")
	if err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
	// An empty group has no recipients (and a missing group is equally
	// empty, never an error).
	got, err := store.GroupRecipientEmails(g.ID)
	if err != nil || len(got) != 0 {
		t.Fatalf("empty group recipients = %v, %v", got, err)
	}
	got, err = store.GroupRecipientEmails(999)
	if err != nil || len(got) != 0 {
		t.Fatalf("missing group recipients = %v, %v", got, err)
	}

	// Four users: one without email, one duplicate address (different
	// case), all in the group.
	users := []struct{ name, email string }{
		{"ada", "ada@example.com"},
		{"bea", ""},
		{"cya", "cya@example.com"},
		{"dea", "ADA@example.com"},
	}
	for _, u := range users {
		u2, err := store.CreateUser(u.name, "", u.email, "", "", "")
		if err != nil {
			t.Fatalf("CreateUser %s: %v", u.name, err)
		}
		if err := store.SetUserGroups(u2.ID, []int64{g.ID}); err != nil {
			t.Fatalf("SetUserGroups %s: %v", u.name, err)
		}
	}

	got, err = store.GroupRecipientEmails(g.ID)
	if err != nil {
		t.Fatalf("recipients: %v", err)
	}
	// Sorted, distinct, non-empty: ada + cya only.
	if len(got) != 2 || got[0] != "ada@example.com" || got[1] != "cya@example.com" {
		t.Fatalf("recipients = %v, want [ada@example.com cya@example.com]", got)
	}
}

// TestGroupRecipientDiscord mirrors the email test for Discord handles:
// sorted, distinct, non-empty, and never an error for missing groups.
func TestGroupRecipientDiscord(t *testing.T) {
	store := newRoutingStore(t)

	g, err := store.CreateGroup("spok")
	if err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
	got, err := store.GroupRecipientDiscord(g.ID)
	if err != nil || len(got) != 0 {
		t.Fatalf("empty group handles = %v, %v", got, err)
	}
	got, err = store.GroupRecipientDiscord(999)
	if err != nil || len(got) != 0 {
		t.Fatalf("missing group handles = %v, %v", got, err)
	}

	users := []struct{ name, handle string }{
		{"ada", "ada#1234"},
		{"bea", ""},
		{"cya", "@cya"},
		{"dea", "ADA#1234"},
	}
	for _, u := range users {
		u2, err := store.CreateUser(u.name, "", "", u.handle, "", "")
		if err != nil {
			t.Fatalf("CreateUser %s: %v", u.name, err)
		}
		if err := store.SetUserGroups(u2.ID, []int64{g.ID}); err != nil {
			t.Fatalf("SetUserGroups %s: %v", u.name, err)
		}
	}

	got, err = store.GroupRecipientDiscord(g.ID)
	if err != nil {
		t.Fatalf("handles: %v", err)
	}
	// Sorted, distinct (case-insensitive), non-empty: @cya + ada only.
	if len(got) != 2 || got[0] != "@cya" || got[1] != "ada#1234" {
		t.Fatalf("handles = %v, want [@cya ada#1234]", got)
	}
}
