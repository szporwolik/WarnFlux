// Package ingest implements the core ingestion pipeline: it accepts
// normalized hazard events, deduplicates them by stable identity and content
// fingerprint, persists meaningful changes, and expires stale events.
package ingest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"warnflux/internal/core"
	"warnflux/internal/storage"
)

// Result describes what ingestion did with a HazardEvent.
type Result int

const (
	// ResultNew: the event was previously unknown and has been persisted.
	ResultNew Result = iota + 1
	// ResultDuplicate: the event was already stored with identical content;
	// only last_seen_at was refreshed.
	ResultDuplicate
	// ResultUpdated: an existing event's content changed.
	ResultUpdated
	// ResultCancelled: the incoming event cancels an existing event.
	ResultCancelled
)

// String returns the wire-style name of the result.
func (r Result) String() string {
	switch r {
	case ResultNew:
		return "new"
	case ResultDuplicate:
		return "duplicate"
	case ResultUpdated:
		return "updated"
	case ResultCancelled:
		return "cancelled"
	default:
		return fmt.Sprintf("Result(%d)", int(r))
	}
}

// Ingester accepts normalized hazard events, persists them and reports what
// changed. It is safe for concurrent use by multiple source workers.
type Ingester struct {
	store  storage.EventStore
	logger *slog.Logger

	// now is the clock used for timestamps; injectable in tests.
	now func() time.Time
}

// NewIngester creates an Ingester backed by the given store.
func NewIngester(store storage.EventStore, logger *slog.Logger) *Ingester {
	return &Ingester{store: store, logger: logger, now: time.Now}
}

// Ingest normalizes, validates and persists the event and reports the
// outcome. Only meaningful changes produce a non-zero EventChange:
// duplicates never do.
//
// The store is the source of truth, so concurrent ingestion of the same new
// event results in exactly one ResultNew; the other calls resolve safely
// against the already-inserted row.
func (s *Ingester) Ingest(ctx context.Context, event core.HazardEvent) (Result, core.EventChange, error) {
	event.Normalize()
	if event.ReceivedAt.IsZero() {
		event.ReceivedAt = s.now().UTC()
	}
	if err := event.Validate(); err != nil {
		return 0, core.EventChange{}, fmt.Errorf("invalid hazard event: %w", err)
	}

	key := event.Key()
	fingerprint := core.Fingerprint(event)

	stored, err := s.store.Get(ctx, key)
	switch {
	case errors.Is(err, storage.ErrNotFound):
		inserted, err := s.store.Insert(ctx, event, fingerprint)
		if err != nil {
			return 0, core.EventChange{}, err
		}
		if inserted {
			s.logger.Info("new hazard event",
				"source", event.Source, "source_id", event.SourceID,
				"event_key", key, "event", event.Event, "severity", event.Severity)
			return ResultNew, core.EventChange{Type: core.ChangeNew, Event: event}, nil
		}
		// Another ingest inserted the same key concurrently; fall through
		// to the existing-event comparison.
		stored, err = s.store.Get(ctx, key)
		if err != nil {
			return 0, core.EventChange{}, fmt.Errorf("reload event %q after insert conflict: %w", key, err)
		}
	case err != nil:
		return 0, core.EventChange{}, fmt.Errorf("get event %q: %w", key, err)
	}

	// Existing event: identical content is a duplicate.
	if stored.Fingerprint == fingerprint {
		if err := s.store.Touch(ctx, key); err != nil {
			return 0, core.EventChange{}, err
		}
		s.logger.Debug("duplicate ignored",
			"source", event.Source, "source_id", event.SourceID, "event_key", key)
		return ResultDuplicate, core.EventChange{}, nil
	}

	// Content changed: persist it, preserving first-seen identity.
	event.ReceivedAt = stored.Event.ReceivedAt
	event.UpdatedAt = s.now().UTC()

	if event.Status == core.StatusCancelled {
		if err := s.store.Update(ctx, event, fingerprint); err != nil {
			return 0, core.EventChange{}, err
		}
		s.logger.Info("hazard event cancelled",
			"source", event.Source, "source_id", event.SourceID,
			"event_key", key, "event", event.Event)
		return ResultCancelled, core.EventChange{Type: core.ChangeCancelled, Event: event}, nil
	}

	if err := s.store.Update(ctx, event, fingerprint); err != nil {
		return 0, core.EventChange{}, err
	}
	s.logger.Info("hazard event updated",
		"source", event.Source, "source_id", event.SourceID,
		"event_key", key, "event", event.Event, "severity", event.Severity)
	return ResultUpdated, core.EventChange{Type: core.ChangeUpdated, Event: event}, nil
}

// Expire marks active events whose ExpiresAt is at or before now as expired
// and returns a change for each of them.
func (s *Ingester) Expire(ctx context.Context, now time.Time) ([]core.EventChange, error) {
	expired, err := s.store.MarkExpired(ctx, now)
	if err != nil {
		return nil, err
	}
	changes := make([]core.EventChange, 0, len(expired))
	for _, e := range expired {
		s.logger.Info("hazard event expired",
			"source", e.Source, "source_id", e.SourceID, "event_key", e.Key())
		changes = append(changes, core.EventChange{Type: core.ChangeExpired, Event: e})
	}
	return changes, nil
}

// RunExpiration periodically expires stale events until ctx is cancelled.
// It must be run in a managed goroutine; it never leaks its own.
func (s *Ingester) RunExpiration(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := s.Expire(ctx, s.now()); err != nil && ctx.Err() == nil {
				s.logger.Warn("expiration check failed", "error", err)
			}
		}
	}
}
