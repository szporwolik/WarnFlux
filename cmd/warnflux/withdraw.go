package main

import (
	"context"
	"log/slog"
	"sort"
	"time"

	"github.com/szporwolik/WarnFlux/internal/config"
	"github.com/szporwolik/WarnFlux/internal/core"
	"github.com/szporwolik/WarnFlux/internal/plugin"
	"github.com/szporwolik/WarnFlux/internal/storage"
)

// eventWithdrawer is the storage half of the withdrawal (implemented by the
// SQLite store): it cancels one active event AND records the cancellation in
// the durable journal, so the retained document disappears even when the broker
// was unreachable at that moment.
type eventWithdrawer interface {
	WithdrawEvent(ctx context.Context, event core.HazardEvent, now time.Time) (*storage.Change, error)
}

// activeEventLister lists the active events.
type activeEventLister interface {
	ListActiveEvents(ctx context.Context, afterKey string, limit int) ([]core.HazardEvent, error)
}

// withdrawDisabledSources cancels the active events of every source instance
// that is switched off (or no longer configured).
//
// A disabled source never publishes again, so its events would stay "live" on
// the panel and the public page until their own expiry — road works carry
// expiries months ahead, so one source switched off kept nine stale warnings
// visible for a month (reported P2). This runs once at startup, before the
// plugins publish anything, and is cheap: no active event, no work.
//
// Ownership is decided by the event's PRODUCER (the configured source instance,
// e.g. "imgw-warnings"), never by comparing the event's source slug with a
// plugin type: an IMGW instance publishes "imgw-meteo"/"imgw-hydro" under the
// type "imgw", so the naive comparison withdrew the warnings of a RUNNING
// plugin at every restart (reported P1).
//
// Events that carry no producer (rows written before the stamp existed) are
// never withdrawn here: without an owner the station cannot tell a switched-off
// source from a running one, and leaving a stale warning is far cheaper than
// hiding a valid alarm. They are reported once, so the operator can act.
func withdrawDisabledSources(ctx context.Context, store eventWithdrawer, lister activeEventLister,
	sources []config.Source, logger *slog.Logger) {
	enabledInstances := make(map[string]bool, len(sources))
	enabledSlugs := make(map[string]bool, len(sources))
	for _, s := range sources {
		if !s.Enabled {
			continue
		}
		enabledInstances[s.ID] = true
		for _, slug := range plugin.EventSourcesOf(s.Type) {
			enabledSlugs[slug] = true
		}
	}

	after := ""
	total := map[string]int{}
	unattributable := map[string]int{}
	for {
		if ctx.Err() != nil {
			return
		}
		batch, err := lister.ListActiveEvents(ctx, after, 256)
		if err != nil {
			if logger != nil && ctx.Err() == nil {
				logger.Warn("withdraw disabled sources: cannot list active events", "error", err)
			}
			return
		}
		for _, ev := range batch {
			if !orphaned(ev, enabledInstances, enabledSlugs) {
				continue
			}
			owner := ev.Producer
			if owner == "" {
				unattributable[ev.Source]++
				continue
			}
			change, err := store.WithdrawEvent(ctx, ev, time.Now())
			if err != nil {
				if logger != nil {
					logger.Warn("withdraw disabled sources: cancellation failed",
						"source", ev.Source, "producer", owner, "event_key", ev.Key(), "error", err)
				}
				continue
			}
			if change != nil {
				total[owner]++
			}
		}
		if len(batch) < 256 {
			break
		}
		after = batch[len(batch)-1].Key()
	}

	logWithdrawn(total, logger)
	logUnattributable(unattributable, logger)
}

// orphaned reports whether an active event belongs to a source that is no
// longer enabled. A missing producer means "cannot tell" — never orphaned.
func orphaned(ev core.HazardEvent, enabledInstances, enabledSlugs map[string]bool) bool {
	if ev.Producer != "" {
		return !enabledInstances[ev.Producer]
	}
	// Rows without a producer: fall back to the declared slug ownership, which
	// the source plugins register, so a running plugin is never mistaken for a
	// switched-off one.
	return !enabledSlugs[ev.Source]
}

// logWithdrawn reports the reconciliation per instance, sorted for a stable log.
func logWithdrawn(counts map[string]int, logger *slog.Logger) {
	if logger == nil || len(counts) == 0 {
		return
	}
	owners := make([]string, 0, len(counts))
	for owner := range counts {
		owners = append(owners, owner)
	}
	sort.Strings(owners)
	for _, owner := range owners {
		logger.Info("source is switched off: its active events were withdrawn",
			"source_instance", owner, "events", counts[owner])
	}
}

// logUnattributable reports events the station refused to touch.
func logUnattributable(counts map[string]int, logger *slog.Logger) {
	if logger == nil || len(counts) == 0 {
		return
	}
	slugs := make([]string, 0, len(counts))
	for slug := range counts {
		slugs = append(slugs, slug)
	}
	sort.Strings(slugs)
	for _, slug := range slugs {
		logger.Warn("active events without a producer were left alone (cannot tell whether their source still runs)",
			"source", slug, "events", counts[slug])
	}
}
