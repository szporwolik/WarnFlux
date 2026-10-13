package burzedzis

import (
	"context"
	"log/slog"
	"time"

	"github.com/szporwolik/WarnFlux/internal/burzedzis"
	"github.com/szporwolik/WarnFlux/internal/plugin"
	"github.com/szporwolik/WarnFlux/internal/plugins/sources/snapshotutil"
)

// stormState remembers what was last published for one locality. The
// lightning facts move with every poll, and a fingerprint change is a
// notification, so the plugin publishes a detection when something that
// matters to a reader changed (severity, validity bucket) or when the
// configured update interval has passed — never merely because a count
// drifted by one.
type stormState struct {
	severity string
	expires  time.Time
	at       time.Time
}

// pollStorm searches for lightning around every point, publishes the
// detections that pass the filter and reconciles the nowcast against the
// durable state: a locality the search no longer finds has its event
// cancelled by this poll instead of waiting for its validity to lapse.
// reconciliation is false when the caller's warning pass was incomplete —
// an incomplete search must never look like a quiet sky.
func (s *Source) pollStorm(ctx context.Context, emit plugin.Emitter, points []burzedzis.Locality, reconcile bool) (int, error) {
	now := time.Now()
	keys := make(map[string]bool, len(points))
	emitted, failed, skipped := 0, 0, 0
	var firstErr error

	for _, at := range points {
		if ctx.Err() != nil {
			return emitted, ctx.Err()
		}
		st, err := s.client.Storm(ctx, at, s.cfg.StormRadiusKM)
		if err != nil {
			failed++
			slog.Warn("burzedzis lightning search failed", "locality", at.Name, "error", err)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if st.Count < s.cfg.StormMinLightnings {
			continue
		}
		ev := s.stormEvent(st, now)
		keys[ev.Key()] = true

		id := ev.SourceID
		s.mu.Lock()
		prev, seen := s.storm[id]
		s.mu.Unlock()
		if seen && prev.severity == ev.Severity && prev.expires.Equal(*ev.ExpiresAt) &&
			now.Sub(prev.at) < s.cfg.StormUpdateInterval {
			skipped++
			continue
		}
		if err := emit.Emit(ctx, ev); err != nil {
			if ctx.Err() != nil {
				return emitted, ctx.Err()
			}
			slog.Warn("burzedzis lightning emit failed; retrying on the next poll", "event_key", ev.Key(), "error", err)
			continue
		}
		s.mu.Lock()
		s.storm[id] = stormState{severity: ev.Severity, expires: *ev.ExpiresAt, at: now}
		s.mu.Unlock()
		emitted++
	}

	if reconcile && failed == 0 {
		n, err := snapshotutil.Reconcile(ctx, emit, TypeStorm, keys, now)
		if err != nil {
			slog.Warn("burzedzis lightning reconciliation failed", "error", err)
		} else if n > 0 {
			slog.Info("burzedzis storms cancelled (no discharges around those localities)", "count", n)
		}
	}
	if skipped > 0 {
		slog.Debug("burzedzis lightning detections unchanged; published once per update interval", "skipped", skipped)
	}
	return emitted, firstErr
}
