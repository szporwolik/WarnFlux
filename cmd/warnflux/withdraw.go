package main

import (
	"context"
	"log/slog"
	"sort"
	"time"

	"github.com/szporwolik/WarnFlux/internal/config"
	"github.com/szporwolik/WarnFlux/internal/core"
)

// sourceWithdrawer is the storage half of the withdrawal (implemented by the
// SQLite store): it cancels every active event of one source and returns what
// it withdrew.
type sourceWithdrawer interface {
	WithdrawSourceEvents(ctx context.Context, source string, now time.Time) ([]core.HazardEvent, error)
}

// activeEventLister lists the active events, used only to learn which sources
// still have something published.
type activeEventLister interface {
	ListActiveEvents(ctx context.Context, afterKey string, limit int) ([]core.HazardEvent, error)
}

// retainedRetirer removes one retained active document from the broker.
type retainedRetirer interface {
	ExpireActive(source, eventKey string) error
}

// withdrawDisabledSources cancels the active events of every source that is
// switched off (or no longer configured) and removes their retained documents.
//
// A disabled source never publishes again, so its events would stay "live" on
// the panel and the public page until their own expiry — road works carry
// expiries months ahead, so one source switched off kept nine stale warnings
// visible for a month (reported P2). This runs once at startup, before the
// plugins publish anything, and is cheap: no active event, no work.
//
// The broker half is best effort with a few retries, because the receivers may
// still be connecting; a retained delete is idempotent, so re-running it is
// harmless.
func withdrawDisabledSources(ctx context.Context, store sourceWithdrawer, lister activeEventLister,
	retirer retainedRetirer, sources []config.Source, logger *slog.Logger) {
	enabled := make(map[string]bool, len(sources))
	for _, s := range sources {
		if s.Enabled {
			enabled[s.Type] = true
		}
	}

	// Which sources still own active events? Read from the authoritative
	// table instead of the configuration: a source removed from the file is
	// exactly the case this must catch, so it has no configuration entry.
	stale := map[string][]core.HazardEvent{}
	after := ""
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
			if !enabled[ev.Source] {
				stale[ev.Source] = append(stale[ev.Source], ev)
			}
		}
		if len(batch) < 256 {
			break
		}
		after = batch[len(batch)-1].Key()
	}
	if len(stale) == 0 {
		return
	}

	names := make([]string, 0, len(stale))
	for source := range stale {
		names = append(names, source)
	}
	sort.Strings(names)

	for _, source := range names {
		withdrawn, err := store.WithdrawSourceEvents(ctx, source, time.Now())
		if err != nil {
			if logger != nil {
				logger.Warn("withdraw disabled sources: cancellation failed", "source", source, "error", err)
			}
			continue
		}
		if len(withdrawn) == 0 {
			continue
		}
		if logger != nil {
			logger.Info("source is switched off: its active events were withdrawn",
				"source", source, "events", len(withdrawn))
		}
		if retirer != nil {
			go retireRetained(ctx, retirer, withdrawn, logger)
		}
	}
}

// retireRetained deletes the retained documents of withdrawn events, retrying
// while the receivers are still connecting.
func retireRetained(ctx context.Context, retirer retainedRetirer, events []core.HazardEvent, logger *slog.Logger) {
	const attempts = 12
	pending := append([]core.HazardEvent(nil), events...)
	for round := 0; round < attempts && len(pending) > 0; round++ {
		var failed []core.HazardEvent
		for _, ev := range pending {
			if err := retirer.ExpireActive(ev.Source, ev.Key()); err != nil {
				failed = append(failed, ev)
			}
		}
		pending = failed
		if len(pending) == 0 {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
		}
	}
	if logger != nil && len(pending) > 0 {
		logger.Warn("withdraw disabled sources: retained documents not removed",
			"pending", len(pending), "first_key", pending[0].Key())
	}
}
