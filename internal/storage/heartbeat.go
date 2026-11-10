package storage

import (
	"context"
)

// HeartbeatSample is one minute of the station's own health, as the panel
// assessed it at the time.
//
// The station already knows whether it is working RIGHT NOW (the health card);
// this table is what turns that into a history. A minute without a row is a
// minute the process was not running — that is the whole point of the seven-day
// strip on the dashboard: "was it working then?" has to be answerable without
// reading the journal.
type HeartbeatSample struct {
	// MinuteMS is the local minute, truncated to its start, in Unix
	// milliseconds. It is the primary key: one row per minute, ever.
	MinuteMS int64
	// Level is the worst finding of that minute: "ok", "warn" or "bad".
	Level string
	// Problems lists the subsystems that were not OK, loudest first, as the
	// health assessment names them ("plugins", "receivers", ...). Empty when
	// the station was healthy.
	Problems []string
}

// HeartbeatRetentionDays bounds the history. The dashboard strip shows
// seven days; the extra day absorbs the "was today complete?" edge without
// keeping a record nobody reads.
const HeartbeatRetentionDays = 8

// HeartbeatStore persists the per-minute station health history.
type HeartbeatStore interface {
	// RecordHeartbeat writes (or rewrites) one minute. Recording the same
	// minute twice keeps the newest assessment, so a tick that fires twice
	// cannot duplicate a row.
	RecordHeartbeat(ctx context.Context, sample HeartbeatSample) error
	// HeartbeatsSince returns the samples with MinuteMS >= sinceMS, oldest
	// first.
	HeartbeatsSince(ctx context.Context, sinceMS int64) ([]HeartbeatSample, error)
	// PruneHeartbeats deletes the samples older than beforeMS and reports how
	// many rows went away.
	PruneHeartbeats(ctx context.Context, beforeMS int64) (int64, error)
}
