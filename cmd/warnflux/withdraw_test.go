package main

import (
	"context"
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/config"
	"github.com/szporwolik/WarnFlux/internal/core"
)

// fakeWithdrawer records what the startup pass cancelled.
type fakeWithdrawer struct {
	events    []core.HazardEvent
	withdrawn map[string]int
}

func (f *fakeWithdrawer) ListActiveEvents(context.Context, string, int) ([]core.HazardEvent, error) {
	return f.events, nil
}

func (f *fakeWithdrawer) WithdrawSourceEvents(_ context.Context, source string, _ time.Time) ([]core.HazardEvent, error) {
	if f.withdrawn == nil {
		f.withdrawn = map[string]int{}
	}
	var out []core.HazardEvent
	for _, ev := range f.events {
		if ev.Source == source {
			f.withdrawn[source]++
			out = append(out, ev)
		}
	}
	return out, nil
}

// fakeRetirer records the retained deletes.
type fakeRetirer struct{ keys []string }

func (f *fakeRetirer) ExpireActive(source, key string) error {
	f.keys = append(f.keys, source+"/"+key)
	return nil
}

// TestWithdrawDisabledSources pins the production case: nine road works from a
// source that was switched off kept showing as "live" for a month.
func TestWithdrawDisabledSources(t *testing.T) {
	store := &fakeWithdrawer{events: []core.HazardEvent{
		{Source: "gddkia", SourceID: "abc"},
		{Source: "imgw-meteo", SourceID: "def"},
	}}
	retirer := &fakeRetirer{}

	withdrawDisabledSources(context.Background(), store, store, retirer,
		[]config.Source{
			{ID: "gddkia-roads", Type: "gddkia", Enabled: false},
			{ID: "imgw-warnings", Type: "imgw-meteo", Enabled: true},
		}, nil)

	if store.withdrawn["gddkia"] != 1 {
		t.Fatalf("gddkia withdrawals = %d, want 1", store.withdrawn["gddkia"])
	}
	if store.withdrawn["imgw-meteo"] != 0 {
		t.Fatalf("an enabled source was withdrawn: %v", store.withdrawn)
	}
	// The broker half runs in a goroutine.
	deadline := time.Now().Add(2 * time.Second)
	for len(retirer.keys) == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if len(retirer.keys) != 1 || retirer.keys[0] != "gddkia/gddkia:abc" {
		t.Fatalf("retired = %v, want the withdrawn event only", retirer.keys)
	}
}

// TestWithdrawDisabledSourcesIgnoresConfiguredSources: nothing stale, nothing
// withdrawn — the pass must be silent on a healthy station.
func TestWithdrawDisabledSourcesIgnoresConfiguredSources(t *testing.T) {
	store := &fakeWithdrawer{events: []core.HazardEvent{{Source: "imgw-meteo", SourceID: "x"}}}
	retirer := &fakeRetirer{}
	withdrawDisabledSources(context.Background(), store, store, retirer,
		[]config.Source{{Type: "imgw-meteo", Enabled: true}}, nil)
	if len(store.withdrawn) != 0 || len(retirer.keys) != 0 {
		t.Fatalf("withdrew %v / retired %v, want nothing", store.withdrawn, retirer.keys)
	}
}

// TestWithdrawDisabledSourcesCatchesRemovedSource: a source deleted from the
// configuration has no entry at all, and its events must still be withdrawn.
func TestWithdrawDisabledSourcesCatchesRemovedSource(t *testing.T) {
	store := &fakeWithdrawer{events: []core.HazardEvent{{Source: "meteoalarm", SourceID: "z"}}}
	retirer := &fakeRetirer{}
	withdrawDisabledSources(context.Background(), store, store, retirer, nil, nil)
	if store.withdrawn["meteoalarm"] != 1 {
		t.Fatalf("removed source was not withdrawn: %v", store.withdrawn)
	}
	deadline := time.Now().Add(2 * time.Second)
	for len(retirer.keys) == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if len(retirer.keys) != 1 {
		t.Fatalf("retired = %v, want one", retirer.keys)
	}
}
