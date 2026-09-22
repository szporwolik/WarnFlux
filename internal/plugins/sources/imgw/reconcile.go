package imgw

import (
	"context"
	"time"

	"github.com/szporwolik/WarnFlux/internal/plugin"
	"github.com/szporwolik/WarnFlux/internal/plugins/sources/snapshotutil"
)

// reconcileSnapshots cancels the source's currently-active SQLite events
// that no longer appear in a COMPLETE provider snapshot. This is the
// disappearance reconciliation that makes the full-snapshot semantics
// correct — especially for hydrological drought warnings with ExpiresAt =
// nil that would otherwise stay active forever after being withdrawn
// upstream. It is safe across process restarts because it reads the
// authoritative SQLite current state, never plugin-local memory.
//
// The algorithm is shared with the RSO source via snapshotutil.Reconcile.
func reconcileSnapshots(ctx context.Context, emit plugin.Emitter, source string, snapshotKeys map[string]bool, now time.Time) (int, error) {
	return snapshotutil.Reconcile(ctx, emit, source, snapshotKeys, now)
}
