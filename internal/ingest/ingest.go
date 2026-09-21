// Package ingest implements the core ingestion pipeline: it accepts
// normalized hazard events, classifies them atomically through the store,
// persists meaningful changes and reports what happened.
package ingest

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/szporwolik/WarnFlux/internal/core"
	"github.com/szporwolik/WarnFlux/internal/storage"
)

// Result describes what ingestion did with a HazardEvent.
type Result int

const (
	// ResultNew: the event was previously unknown and has been persisted.
	ResultNew Result = iota + 1
	// ResultDuplicate: identical content and lifecycle state; only
	// last_seen_at was refreshed and no change was journaled.
	ResultDuplicate
	// ResultUpdated: an existing event's content or lifecycle changed.
	ResultUpdated
	// ResultCancelled: the incoming event cancels an event (also for
	// previously unknown events).
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

// Ingester accepts normalized hazard events and persists them atomically.
// It is safe for concurrent use; the store guarantees that classification,
// event state and journal writes happen in one transaction.
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

// Ingest normalizes, validates and atomically persists the event. Only
// meaningful changes produce an EventChange carrying the journal change ID;
// duplicates never do.
func (s *Ingester) Ingest(ctx context.Context, event core.HazardEvent) (Result, core.EventChange, error) {
	event.Normalize()
	if event.ReceivedAt.IsZero() {
		event.ReceivedAt = s.now().UTC()
	}
	event.UpdatedAt = s.now().UTC()
	if err := event.Validate(); err != nil {
		return 0, core.EventChange{}, fmt.Errorf("invalid hazard event: %w", err)
	}

	fingerprint := core.Fingerprint(event)
	outcome, change, err := s.store.Ingest(ctx, event, fingerprint)
	if err != nil {
		return 0, core.EventChange{}, err
	}

	key := event.Key()
	switch outcome {
	case storage.OutcomeNew:
		s.logger.Info("new hazard event",
			"change_type", core.ChangeNew,
			"source", event.Source, "source_id", event.SourceID,
			"event_key", key, "event", event.Event, "severity", event.Severity)
		return ResultNew, changeToEventChange(change), nil
	case storage.OutcomeDuplicate:
		s.logger.Debug("duplicate ignored",
			"source", event.Source, "source_id", event.SourceID, "event_key", key)
		return ResultDuplicate, core.EventChange{}, nil
	case storage.OutcomeUpdated:
		s.logger.Info("hazard event updated",
			"change_type", core.ChangeUpdated,
			"source", event.Source, "source_id", event.SourceID,
			"event_key", key, "event", event.Event, "severity", event.Severity)
		return ResultUpdated, changeToEventChange(change), nil
	case storage.OutcomeCancelled:
		s.logger.Info("hazard event cancelled",
			"change_type", core.ChangeCancelled,
			"source", event.Source, "source_id", event.SourceID,
			"event_key", key, "event", event.Event)
		return ResultCancelled, changeToEventChange(change), nil
	default:
		return 0, core.EventChange{}, fmt.Errorf("unknown ingestion outcome %v", outcome)
	}
}

// Expire atomically marks stale active events expired and returns a change
// for each of them.
func (s *Ingester) Expire(ctx context.Context, now time.Time) ([]core.EventChange, error) {
	changes, err := s.store.Expire(ctx, now)
	if err != nil {
		return nil, err
	}
	out := make([]core.EventChange, 0, len(changes))
	for _, c := range changes {
		s.logger.Info("hazard event expired",
			"change_type", core.ChangeExpired,
			"source", c.Event.Source, "source_id", c.Event.SourceID, "event_key", c.Event.Key())
		out = append(out, storageChangeToEventChange(c))
	}
	return out, nil
}

func changeToEventChange(c *storage.Change) core.EventChange {
	if c == nil {
		return core.EventChange{}
	}
	return storageChangeToEventChange(*c)
}

func storageChangeToEventChange(c storage.Change) core.EventChange {
	return core.EventChange{ID: c.ID, Type: c.ChangeType, Event: c.Event}
}
