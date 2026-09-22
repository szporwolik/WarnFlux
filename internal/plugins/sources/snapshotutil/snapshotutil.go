// Package snapshotutil holds the tiny shared helpers used by full-snapshot
// hazard sources (IMGW, RSO): Polish-local-time parsing and
// disappearance reconciliation against the authoritative SQLite active
// state. It is deliberately NOT a generic provider framework — just the
// two algorithms that more than one snapshot source needs verbatim.
package snapshotutil

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/szporwolik/WarnFlux/internal/core"
	"github.com/szporwolik/WarnFlux/internal/plugin"
)

// warsawLocation is Europe/Warsaw (CET/CEST, DST-aware). Polish providers
// publish naive local timestamps that must never be interpreted as UTC.
var warsawLocation = func() *time.Location {
	loc, err := time.LoadLocation("Europe/Warsaw")
	if err != nil {
		// The embedded zoneinfo always carries Europe/Warsaw; this
		// fallback exists only for exotic stripped runtimes and
		// intentionally stays fixed-zone (never UTC-misparsed).
		return time.FixedZone("CET", 3600)
	}
	return loc
}()

// ParseWarsawLocal parses a naive Polish local timestamp
// ("2006-01-02 15:04:05") in Europe/Warsaw.
func ParseWarsawLocal(s string) (time.Time, error) {
	t, err := time.ParseInLocation("2006-01-02 15:04:05", strings.TrimSpace(s), warsawLocation)
	if err != nil {
		return time.Time{}, fmt.Errorf("malformed Warsaw-local timestamp %q", s)
	}
	return t, nil
}

// Reconcile cancels the source's currently-active SQLite events that no
// longer appear in a COMPLETE provider snapshot. It reads the authoritative
// SQLite current state (via the optional SourceActiveEventReader), so it is
// safe across process restarts and never depends on plugin-local memory.
//
// Events whose ExpiresAt has already passed are left alone: the core
// expiration worker owns their EXPIRED transition.
func Reconcile(ctx context.Context, emit plugin.Emitter, source string, snapshotKeys map[string]bool, now time.Time) (int, error) {
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
