package sqlite

import (
	"context"
	"time"
)

// Traffic channels of the durable traffic aggregates: the same identifiers the
// message history tables use.
const (
	trafficChannelAPRS = "aprs"
	trafficChannelGSM  = "gsm"
	trafficChannelMesh = "meshtastic"
)

// TrafficCounterRetention bounds the aggregate ledger. It must outlive the
// longest reporting window by a wide margin (the daily mail needs a day, a
// trend a year), while the message history it replaces is capped at a few
// thousand rows.
const TrafficCounterRetention = 400 * 24 * time.Hour

// trafficMinuteBucket returns the UTC minute containing t: the aggregate
// granularity. A reporting window is snapped to the same boundaries, so the
// numbers a report shows are exactly what it counted.
func trafficMinuteBucket(t time.Time) int64 {
	return t.UTC().Truncate(time.Minute).UnixMilli()
}

// bumpTrafficCounter records one message in the minute bucket of its channel
// and direction. It runs in the SAME transaction as the history insert, so the
// aggregate can never disagree with the rows it summarises.
func bumpTrafficCounter(ctx context.Context, ex execer, channel, direction string, at time.Time) error {
	_, err := ex.ExecContext(ctx, `
		INSERT INTO traffic_counters (minute_ms, channel, direction, messages)
		VALUES (?, ?, ?, 1)
		ON CONFLICT(minute_ms, channel, direction) DO UPDATE SET messages = messages + 1`,
		trafficMinuteBucket(at), channel, direction)
	return err
}

// PruneTrafficCounters drops aggregate buckets older than the cutoff: they no
// longer describe any window a report can ask for.
func (s *Store) PruneTrafficCounters(ctx context.Context, cutoff time.Time) (int64, error) {
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM traffic_counters WHERE minute_ms < ?`, trafficMinuteBucket(cutoff))
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
