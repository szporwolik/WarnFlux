// Package storage defines the persistence contract for hazard events and
// the durable change journal. The only implementation today is
// internal/storage/sqlite.
package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/szporwolik/WarnFlux/internal/core"
)

// ErrNotFound is returned by EventStore.Get when no event matches the key.
var ErrNotFound = errors.New("event not found")

// OutputRef identifies one enabled output instance. The durable journal
// consumer identity is the (ID, Type) pair: changing the plugin type under
// the same ID resets the cursor — a new consumer that replays retained
// history.
type OutputRef struct {
	ID   string
	Type string
}

// Outcome describes the result of an atomic ingestion transaction.
type Outcome int

const (
	// OutcomeNew: the event was previously unknown (and active).
	OutcomeNew Outcome = iota + 1
	// OutcomeDuplicate: identical content and lifecycle state; only
	// last_seen_at was refreshed and no change was journaled.
	OutcomeDuplicate
	// OutcomeUpdated: an existing event's content or lifecycle changed.
	OutcomeUpdated
	// OutcomeCancelled: the incoming event cancels the event (also for
	// events that were previously unknown).
	OutcomeCancelled
)

// String returns the wire-style name of the outcome.
func (o Outcome) String() string {
	switch o {
	case OutcomeNew:
		return "new"
	case OutcomeDuplicate:
		return "duplicate"
	case OutcomeUpdated:
		return "updated"
	case OutcomeCancelled:
		return "cancelled"
	default:
		return fmt.Sprintf("Outcome(%d)", int(o))
	}
}

// StoredEvent is a HazardEvent plus the persistence metadata attached to it.
type StoredEvent struct {
	Event core.HazardEvent

	// Fingerprint is the content hash of the event at its last update.
	Fingerprint string
	// FirstSeenAt is when WarnFlux first persisted the event.
	FirstSeenAt time.Time
	// LastSeenAt is the last time the event was ingested, even when the
	// content was unchanged.
	LastSeenAt time.Time
}

// Change is a durable journal record of a meaningful event transition.
// Delivery semantics are at-least-once: outputs acknowledge by change ID
// and redelivery is possible after a restart.
type Change struct {
	// ID is the stable, monotonic journal identifier.
	ID int64
	// ChangeType is the transition that happened.
	ChangeType core.ChangeType
	// Event is the event state after the transition.
	Event core.HazardEvent
}

// EventStore persists normalized hazard events together with a durable
// change journal. Implementations must be safe for concurrent use and must
// make each Ingest / Expire call atomic (event state + journal record in
// one transaction).
type EventStore interface {
	// Ingest atomically classifies and persists one normalized event and,
	// when meaningful, writes its change record in the same transaction.
	// Duplicates never produce a change.
	Ingest(ctx context.Context, event core.HazardEvent, fingerprint string) (Outcome, *Change, error)

	// Expire atomically marks stale active events expired, creating a
	// durable ChangeExpired record for each of them.
	Expire(ctx context.Context, now time.Time) ([]Change, error)

	// PollChanges returns unacknowledged journal changes for an output in
	// stable ID order (at-least-once delivery).
	PollChanges(ctx context.Context, outputID string, limit int) ([]Change, error)

	// AckChanges records that an output has successfully delivered every
	// change up to lastChangeID.
	AckChanges(ctx context.Context, outputID string, lastChangeID int64) error

	// SyncOutputs makes the output_cursors table match the set of enabled
	// outputs exactly: missing cursors are created at 0 (a newly enabled
	// output receives all changes still present in the retained journal),
	// cursors of outputs that are no longer enabled are removed (so they
	// stop blocking cleanup), and a cursor whose plugin type changed is
	// reset to 0 (a new durable consumer). It is called once at
	// application startup, before any runtime worker starts.
	SyncOutputs(ctx context.Context, enabled []OutputRef) error

	// CleanupChanges deletes journal rows older than olderThan that have
	// been acknowledged by every output. Returns the number of deleted rows.
	CleanupChanges(ctx context.Context, olderThan time.Time) (int64, error)

	// CleanupEvents deletes cancelled/expired current-state records whose
	// updated_at is older than olderThan. Active events are never touched.
	CleanupEvents(ctx context.Context, olderThan time.Time) (int64, error)

	// Get returns the stored event for key, or ErrNotFound.
	Get(ctx context.Context, key string) (*StoredEvent, error)

	// Count returns the number of stored events.
	Count(ctx context.Context) (int, error)

	// PendingStats reports the number of undelivered changes and the age of
	// the oldest one.
	PendingStats(ctx context.Context) (pending int, oldest time.Duration, err error)

	// ListArchiveEvents returns current-state events last seen on or after
	// since (any status), newest first, as one page (offset/limit). The
	// public home-page archive browses the history of communications
	// through it.
	ListArchiveEvents(ctx context.Context, since time.Time, offset, limit int) ([]StoredEvent, error)

	// CountArchiveEvents reports how many current-state events were last
	// seen on or after since (the archive total for pagination).
	CountArchiveEvents(ctx context.Context, since time.Time) (int, error)

	// Close releases the underlying resources.
	Close() error
}

// ActiveEventLister is an OPTIONAL EventStore capability: enumerating the
// CURRENT active events directly from the authoritative current-state
// table (NOT by replaying the historical change journal). It exists for
// materialized active-state views (e.g. retained MQTT topics) that must be
// reconstructed after a process restart even when every journal change is
// already acknowledged.
type ActiveEventLister interface {
	// ListActiveEvents returns current-state events whose status is
	// active, in stable event_key order. afterKey is exclusive ("" for
	// the first page) and limit bounds the page size, so callers page
	// through large datasets in bounded batches.
	ListActiveEvents(ctx context.Context, afterKey string, limit int) ([]core.HazardEvent, error)
}
