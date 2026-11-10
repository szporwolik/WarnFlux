package sqlite

import (
	"context"
	"fmt"
	"strings"

	"github.com/szporwolik/WarnFlux/internal/storage"
)

// RecordHeartbeat writes one minute of station health. The second tick of the
// same minute rewrites the row instead of adding one: the strip counts minutes,
// so a duplicate would make an hour look busier than it was.
func (s *Store) RecordHeartbeat(ctx context.Context, sample storage.HeartbeatSample) error {
	if sample.MinuteMS <= 0 {
		return fmt.Errorf("record heartbeat: empty minute")
	}
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO station_heartbeat(minute_ms, level, problems) VALUES(?, ?, ?)
		ON CONFLICT(minute_ms) DO UPDATE SET level = excluded.level, problems = excluded.problems`,
		sample.MinuteMS, sample.Level, strings.Join(sample.Problems, ",")); err != nil {
		return fmt.Errorf("record heartbeat: %w", err)
	}
	return nil
}

// HeartbeatsSince returns the samples from sinceMS on, oldest first.
func (s *Store) HeartbeatsSince(ctx context.Context, sinceMS int64) ([]storage.HeartbeatSample, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT minute_ms, level, problems FROM station_heartbeat
		WHERE minute_ms >= ? ORDER BY minute_ms`, sinceMS)
	if err != nil {
		return nil, fmt.Errorf("list heartbeats: %w", err)
	}
	defer rows.Close()
	var out []storage.HeartbeatSample
	for rows.Next() {
		var (
			sample   storage.HeartbeatSample
			problems string
		)
		if err := rows.Scan(&sample.MinuteMS, &sample.Level, &problems); err != nil {
			return nil, fmt.Errorf("scan heartbeat: %w", err)
		}
		if problems != "" {
			sample.Problems = strings.Split(problems, ",")
		}
		out = append(out, sample)
	}
	return out, rows.Err()
}

// PruneHeartbeats deletes everything older than beforeMS.
func (s *Store) PruneHeartbeats(ctx context.Context, beforeMS int64) (int64, error) {
	res, err := s.db.ExecContext(ctx, `DELETE FROM station_heartbeat WHERE minute_ms < ?`, beforeMS)
	if err != nil {
		return 0, fmt.Errorf("prune heartbeats: %w", err)
	}
	return res.RowsAffected()
}
