package main

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/config"
	"github.com/szporwolik/WarnFlux/internal/core"
	"github.com/szporwolik/WarnFlux/internal/plugin"
	"github.com/szporwolik/WarnFlux/internal/storage"
)

// fakeStore records what the reconciliation withdrew. The withdrawal runs on
// the startup goroutine but tests poll it, so it is guarded.
type fakeStore struct {
	mu        sync.Mutex
	events    []core.HazardEvent
	withdrawn []string
}

func (f *fakeStore) ListActiveEvents(context.Context, string, int) ([]core.HazardEvent, error) {
	return f.events, nil
}

func (f *fakeStore) WithdrawEvent(_ context.Context, ev core.HazardEvent, _ time.Time) (*storage.Change, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.withdrawn = append(f.withdrawn, ev.Producer+"/"+ev.Key())
	return &storage.Change{ChangeType: core.ChangeCancelled}, nil
}

func (f *fakeStore) withdrawnKeys() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.withdrawn...)
}

// TestWithdrawKeepsEnabledIMGWEvents is the review's P1: a running IMGW instance
// publishes "imgw-meteo"/"imgw-hydro" under the plugin type "imgw", and the
// reconciliation used to compare those slugs with the type, decide both feeds
// were orphaned and withdraw VALID warnings at every restart.
func TestWithdrawKeepsEnabledIMGWEvents(t *testing.T) {
	store := &fakeStore{events: []core.HazardEvent{
		{Source: "imgw-meteo", SourceID: "1", Producer: "imgw-warnings"},
		{Source: "imgw-hydro", SourceID: "2", Producer: "imgw-warnings"},
		// A legacy row without a producer: cannot be attributed, left alone.
		{Source: "imgw-meteo", SourceID: "3"},
	}}
	withdrawDisabledSources(context.Background(), store, store,
		[]config.Source{{ID: "imgw-warnings", Type: "imgw", Enabled: true}}, nil)

	if got := store.withdrawnKeys(); len(got) != 0 {
		t.Fatalf("withdrew %v, want nothing while IMGW is enabled", got)
	}
}

// TestWithdrawUsesProducerInstance: two instances of one type, one switched off
// — only the switched-off instance's events go.
func TestWithdrawUsesProducerInstance(t *testing.T) {
	store := &fakeStore{events: []core.HazardEvent{
		{Source: "imgw-meteo", SourceID: "1", Producer: "imgw-warnings"},
		{Source: "imgw-meteo", SourceID: "2", Producer: "imgw-backup"},
	}}
	withdrawDisabledSources(context.Background(), store, store, []config.Source{
		{ID: "imgw-warnings", Type: "imgw", Enabled: true},
		{ID: "imgw-backup", Type: "imgw", Enabled: false},
	}, nil)

	got := store.withdrawnKeys()
	if len(got) != 1 || got[0] != "imgw-backup/imgw-meteo:2" {
		t.Fatalf("withdrew %v, want only the disabled instance", got)
	}
}

// TestWithdrawSkipsUnattributable: a row without a producer whose source no
// enabled plugin declares is never withdrawn — a stale warning is cheaper than
// hiding a valid alarm.
func TestWithdrawSkipsUnattributable(t *testing.T) {
	store := &fakeStore{events: []core.HazardEvent{{Source: "meteoalarm", SourceID: "pl-1"}}}
	withdrawDisabledSources(context.Background(), store, store, nil, nil)
	if got := store.withdrawnKeys(); len(got) != 0 {
		t.Fatalf("withdrew %v, want the unattributable row left alone", got)
	}
}

// TestWithdrawSwitchedOffSource: the original purpose still works — a source the
// operator switched off has its active events withdrawn, and the withdrawal goes
// through the journalled store call (durable removal), not a best-effort loop.
func TestWithdrawSwitchedOffSource(t *testing.T) {
	store := &fakeStore{events: []core.HazardEvent{
		{Source: "gddkia", SourceID: "road-1", Producer: "gddkia-roads"},
		{Source: "imgw-meteo", SourceID: "w-1", Producer: "imgw-warnings"},
	}}
	withdrawDisabledSources(context.Background(), store, store, []config.Source{
		{ID: "gddkia-roads", Type: "gddkia", Enabled: false},
		{ID: "imgw-warnings", Type: "imgw", Enabled: true},
	}, nil)

	got := store.withdrawnKeys()
	if len(got) != 1 || got[0] != "gddkia-roads/gddkia:road-1" {
		t.Fatalf("withdrew %v, want only the gddkia row", got)
	}
}

// TestOrphanedUsesDeclaredSlugs pins the mapping that keeps a producer-less row
// from looking orphaned: IMGW's events are named per feed, so the instance's
// declared slugs (not its type) decide ownership. Without the registration the
// station would mistake a RUNNING IMGW for a switched-off one.
func TestOrphanedUsesDeclaredSlugs(t *testing.T) {
	slugs := map[string]bool{}
	for _, slug := range plugin.EventSourcesOf("imgw") {
		slugs[slug] = true
	}
	if !slugs["imgw-meteo"] || !slugs["imgw-hydro"] {
		t.Fatalf("imgw declares %v, want both feeds", slugs)
	}
	legacy := core.HazardEvent{Source: "imgw-meteo", SourceID: "3"}
	if orphaned(legacy, map[string]bool{"imgw-warnings": true}, slugs) {
		t.Fatal("a producer-less IMGW row of a running instance must not look orphaned")
	}
	other := core.HazardEvent{Source: "meteoalarm", SourceID: "pl-1"}
	if !orphaned(other, nil, slugs) {
		t.Fatal("an unknown source without a producer is orphaned (and then left alone)")
	}
}

// TestPluginTypeWithoutRegistrationOwnsItsName documents the default: a plugin
// whose event source IS its type needs no registration.
func TestPluginTypeWithoutRegistrationOwnsItsName(t *testing.T) {
	got := plugin.EventSourcesOf("gddkia")
	if len(got) != 1 || got[0] != "gddkia" {
		t.Fatalf("slugs = %v, want [gddkia]", got)
	}
}
