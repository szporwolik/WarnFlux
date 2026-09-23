package sqlite

import (
	"errors"
	"testing"

	"github.com/szporwolik/WarnFlux/internal/storage"
)

func newGroupsStore(t *testing.T) *Store {
	t.Helper()
	store := newUsersStore(t)
	if err := store.EnsureAdminUser("admin"); err != nil {
		t.Fatal(err)
	}
	return store
}

func TestGroupsCRUDAndMembership(t *testing.T) {
	store := newGroupsStore(t)
	alice, err := store.CreateUser("alice", "", "alice@example.com", "", "", "")
	if err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	bob, err := store.CreateUser("bob", "", "bob@example.com", "", "", "")
	if err != nil {
		t.Fatalf("CreateUser: %v", err)
	}

	// Create groups.
	ops, err := store.CreateGroup("ops")
	if err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
	news, err := store.CreateGroup("news")
	if err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}

	// Duplicate name rejected (case-insensitive).
	if _, err := store.CreateGroup("OPS"); !errors.Is(err, storage.ErrGroupNameTaken) {
		t.Fatalf("duplicate group = %v, want ErrGroupNameTaken", err)
	}

	// Rename.
	news, err = store.UpdateGroup(news.ID, "alerts")
	if err != nil {
		t.Fatalf("UpdateGroup: %v", err)
	}
	if news.Name != "alerts" {
		t.Fatalf("group name = %q, want alerts", news.Name)
	}
	if _, err := store.UpdateGroup(9999, "x"); !errors.Is(err, storage.ErrGroupNotFound) {
		t.Fatalf("update missing group = %v, want ErrGroupNotFound", err)
	}

	// Membership: alice in both groups, bob in none.
	if err := store.SetUserGroups(alice.ID, []int64{ops.ID, news.ID}); err != nil {
		t.Fatalf("SetUserGroups: %v", err)
	}
	ids, err := store.GroupIDsForUser(alice.ID)
	if err != nil || len(ids) != 2 {
		t.Fatalf("alice groups = %v (%v), want 2", ids, err)
	}
	ids, err = store.GroupIDsForUser(bob.ID)
	if err != nil || len(ids) != 0 {
		t.Fatalf("bob groups = %v (%v), want none", ids, err)
	}

	// Member counts on listing.
	groups, total, err := store.ListGroups(1, 10)
	if err != nil || total != 2 {
		t.Fatalf("ListGroups = %d groups (%v), want 2", total, err)
	}
	counts := map[string]int64{}
	for _, g := range groups {
		counts[g.Name] = g.Members
	}
	if counts["ops"] != 1 || counts["alerts"] != 1 {
		t.Fatalf("member counts = %v, want ops:1 alerts:1", counts)
	}

	// Clearing membership.
	if err := store.SetUserGroups(alice.ID, nil); err != nil {
		t.Fatalf("clear membership: %v", err)
	}
	if ids, _ := store.GroupIDsForUser(alice.ID); len(ids) != 0 {
		t.Fatalf("alice still has %d groups after clear", len(ids))
	}

	// Deleting a group cascades membership rows away.
	if err := store.SetUserGroups(bob.ID, []int64{news.ID}); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteGroup(news.ID); err != nil {
		t.Fatalf("DeleteGroup: %v", err)
	}
	if ids, _ := store.GroupIDsForUser(bob.ID); len(ids) != 0 {
		t.Fatalf("bob still has %d groups after group delete", len(ids))
	}
	if err := store.DeleteGroup(news.ID); !errors.Is(err, storage.ErrGroupNotFound) {
		t.Fatalf("delete missing group = %v, want ErrGroupNotFound", err)
	}

	// Deleting a user cascades its membership rows away.
	if err := store.SetUserGroups(alice.ID, []int64{ops.ID}); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteUser(alice.ID); err != nil {
		t.Fatal(err)
	}
	if ids, _ := store.GroupIDsForUser(alice.ID); len(ids) != 0 {
		t.Fatalf("deleted user still has %d memberships", len(ids))
	}
}
