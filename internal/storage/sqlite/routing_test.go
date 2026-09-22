package sqlite

import (
	"errors"
	"reflect"
	"testing"

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
	if r.MinSeverity != "unknown" || len(r.Actions) != 0 || len(r.Outputs) != 0 {
		t.Fatalf("default routing = %+v, want unknown threshold, no channels", r)
	}

	err = store.SetGroupRouting(g.ID, "severe",
		[]string{"log-alerts", "log-alerts", "sms"},
		[]string{"mqtt-spok", "mqtt-spok"})
	if err != nil {
		t.Fatalf("SetGroupRouting: %v", err)
	}

	r, err = store.GroupRouting(g.ID)
	if err != nil {
		t.Fatalf("GroupRouting after set: %v", err)
	}
	if r.MinSeverity != "severe" {
		t.Errorf("MinSeverity = %q, want severe", r.MinSeverity)
	}
	if want := []string{"log-alerts", "sms"}; !reflect.DeepEqual(r.Actions, want) {
		t.Errorf("Actions = %v, want %v (deduplicated)", r.Actions, want)
	}
	if want := []string{"mqtt-spok"}; !reflect.DeepEqual(r.Outputs, want) {
		t.Errorf("Outputs = %v, want %v (deduplicated)", r.Outputs, want)
	}

	// The group list carries the threshold for the admin page.
	listed, err := store.ListAllGroups()
	if err != nil {
		t.Fatalf("ListAllGroups: %v", err)
	}
	if len(listed) != 1 || listed[0].MinSeverity != "severe" {
		t.Fatalf("ListAllGroups = %+v, want one group with severe threshold", listed)
	}

	// Empty assignment clears the routing but keeps the group.
	if err := store.SetGroupRouting(g.ID, "unknown", nil, nil); err != nil {
		t.Fatalf("SetGroupRouting clear: %v", err)
	}
	r, err = store.GroupRouting(g.ID)
	if err != nil {
		t.Fatalf("GroupRouting after clear: %v", err)
	}
	if r.MinSeverity != "unknown" || len(r.Actions) != 0 || len(r.Outputs) != 0 {
		t.Fatalf("routing after clear = %+v, want empty", r)
	}
}

func TestGroupRoutingErrors(t *testing.T) {
	store := newRoutingStore(t)

	if err := store.SetGroupRouting(999, "severe", nil, nil); !errors.Is(err, storage.ErrGroupNotFound) {
		t.Fatalf("missing group error = %v, want ErrGroupNotFound", err)
	}
	if _, err := store.GroupRouting(999); !errors.Is(err, storage.ErrGroupNotFound) {
		t.Fatalf("GroupRouting missing = %v, want ErrGroupNotFound", err)
	}

	g, err := store.CreateGroup("rsp")
	if err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
	if err := store.SetGroupRouting(g.ID, "orange", nil, nil); !errors.Is(err, storage.ErrInvalidSeverity) {
		t.Fatalf("invalid severity error = %v, want ErrInvalidSeverity", err)
	}
	if err := store.SetGroupRouting(g.ID, "EXTREME", nil, nil); err == nil {
		t.Fatal("non-canonical case severity must be rejected")
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
	if err := store.SetGroupRouting(a.ID, "moderate", []string{"log"}, []string{"mqtt"}); err != nil {
		t.Fatalf("route alpha: %v", err)
	}
	// bravo stays untouched: default threshold, no channels.

	all, err := store.ListGroupRoutings()
	if err != nil {
		t.Fatalf("ListGroupRoutings: %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("ListGroupRoutings returned %d rules, want 2", len(all))
	}
	if all[0].Name != "alpha" || all[0].MinSeverity != "moderate" ||
		!reflect.DeepEqual(all[0].Actions, []string{"log"}) ||
		!reflect.DeepEqual(all[0].Outputs, []string{"mqtt"}) {
		t.Errorf("alpha rule = %+v", all[0])
	}
	if all[1].Name != "bravo" || all[1].MinSeverity != "unknown" ||
		len(all[1].Actions) != 0 || len(all[1].Outputs) != 0 {
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
