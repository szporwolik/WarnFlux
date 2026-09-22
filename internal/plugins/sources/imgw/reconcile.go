package imgw

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/szporwolik/WarnFlux/internal/core"
	"github.com/szporwolik/WarnFlux/internal/plugin"
)

// reconcileSnapshots cancels the source's currently-active SQLite events
// that no longer appear in a COMPLETE provider snapshot. This is the
// disappearance reconciliation that makes the full-snapshot semantics
// correct — especially for hydrological drought warnings with ExpiresAt =
// nil that would otherwise stay active forever after being withdrawn
// upstream. It is safe across process restarts because it reads the
// authoritative SQLite current state, never plugin-local memory.
//
// Events whose ExpiresAt has already passed are left alone: the core
// expiration worker owns their EXPIRED transition.
func reconcileSnapshots(ctx context.Context, emit plugin.Emitter, source string, snapshotKeys map[string]bool, now time.Time) (int, error) {
	reader, ok := emit.(plugin.SourceActiveEventReader)
	if !ok {
		return 0, fmt.Errorf("emitter does not provide the source active-event reader; refusing to reconcile %s disappearances", source)
	}
	existing, err := reader.ListSourceActiveEvents(ctx, source)
	if err != nil {
		return 0, fmt.Errorf("read current active events: %w", err)
	}

	cancelled := 0
	for _, ev := range existing {
		if ctx.Err() != nil {
			return cancelled, ctx.Err()
		}
		if snapshotKeys[ev.Key()] {
			continue // still present in the complete current snapshot
		}
		if ev.ExpiresAt != nil && !ev.ExpiresAt.After(now) {
			// Natural end: the expiration worker classifies it EXPIRED.
			continue
		}
		c := ev.Clone()
		c.Status = core.StatusCancelled
		if err := emit.Emit(ctx, c); err != nil {
			if ctx.Err() != nil {
				return cancelled, ctx.Err()
			}
			slog.Warn("cancellation emit failed", "source", source, "event_key", ev.Key(), "error", err)
			continue
		}
		cancelled++
	}
	return cancelled, nil
}
