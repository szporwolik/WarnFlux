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
	"github.com/szporwolik/WarnFlux/internal/dispatch"
)

// ErrNotFound is returned by EventStore.Get when no event matches the key.
var ErrNotFound = errors.New("event not found")

// OutboxCapacity hard-bounds the pending rows of the durable ingest
// outbox (rows awaiting broker confirmation). Capacity pressure may
// remove only PROVABLY OBSOLETE rows — expired or superseded by a newer
// version of the same event identity. When the pending backlog alone
// exceeds the bound there is nothing to evict and appends fail with
// ErrOutboxFull (explicit backpressure): a still-valid alarm accepted by
// the system is never dropped before its sync.
const OutboxCapacity = 10000

// ErrOutboxFull reports that the durable ingest outbox has no room left:
// the pending backlog (still-valid events and corrective sync) exceeds
// the capacity bound and cannot be evicted without losing a still-valid
// notification — the append fails explicitly instead of dropping rows
// (503 at the ingress, no partial write).
var ErrOutboxFull = errors.New("outbox full: pending sync cannot be evicted")

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
	// Publisher is the persistent UUID of the WarnFlux instance that
	// produced this journal entry; it rides on the /events wire contract
	// so independent publishers never collide in deduplication.
	Publisher string
	// InboxID is the durable dispatch-inbox row written for this change
	// IN THE SAME TRANSACTION as the journal record (0 when the store
	// has no inbox integration). It guarantees that a journaled alert
	// always has a local acceptance row, so a crash between the journal
	// commit and the live dispatch can never strand a notification: the
	// inbox row is recovered and evaluated after a restart.
	InboxID int64
}

// InboxItem is one durable dispatch-inbox row: a canonical event accepted
// for routing but not yet acknowledged by the routing engine.
type InboxItem struct {
	ID    int64
	Event dispatch.Event
}

// OutboxItem is one durable HTTP-ingest outbox row: an accepted request
// whose broker publication is still pending. The outbox worker publishes
// it (mask applied) and deletes the row only after the broker confirmed,
// so a broker outage never loses the cross-instance sync of an accepted
// request.
type OutboxItem struct {
	ID      int64
	Topic   string
	Payload []byte
}

// PendingDelete is one durable unresolved retained-topic deletion of an
// output: the retained broker document must not exist, but the confirming
// DELETE has not happened yet (masked category, broker outage or a failed
// publish). The output worker persists the set before the journal ack and
// restores it on startup, so a restart can replay deletes the journal no
// longer remembers.
type PendingDelete struct {
	Key   string
	Topic string
}

// PendingDeleteStore is the OPTIONAL durable surface for outputs that
// maintain retained broker documents (implemented by the SQLite store).
type PendingDeleteStore interface {
	// SavePendingDeletes atomically replaces the unresolved-deletion set
	// of one output with the given snapshot (the plugin's in-memory map
	// is authoritative; the rows are its durable mirror).
	SavePendingDeletes(ctx context.Context, outputID string, deletes []PendingDelete) error
	// LoadPendingDeletes returns the persisted unresolved deletions of
	// one output (stable key order).
	LoadPendingDeletes(ctx context.Context, outputID string) ([]PendingDelete, error)
}

// EmcomNetwork is one EMCOM operational-readiness network persisted
// locally: the panel edits it without any broker dependency and the
// retained MQTT document is only an asynchronous sync copy.
type EmcomNetwork struct {
	Slug      string
	Name      string
	Level     int
	UpdatedBy string
	UpdatedAt time.Time
}

// ComposeHazard is one panel-issued communication persisted locally:
// the transition is routed from the local record and the retained MQTT
// document is only an asynchronous sync copy. State is the JSON shape of
// the web-owned hazard (opaque to storage).
type ComposeHazard struct {
	EventKey  string
	State     []byte
	Status    string
	UpdatedAt time.Time
}

// EventStore persists normalized hazard events together with a durable
// change journal. Implementations must be safe for concurrent use and must
// make each Ingest / Expire call atomic (event state + journal record in
// one transaction).
//
// LOCAL-FIRST CONTRACT: implementations with a dispatch inbox must write
// the canonical transition of every journal change into the durable inbox
// INSIDE the same transaction as the journal record (Change.InboxID holds
// the row ID). A crash between the journal commit and the live dispatch
// therefore leaves the inbox row behind, and inbox recovery re-delivers
// the alert after a restart — a re-polled duplicate can never silently
// strand a journaled notification.
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

	// EventTimes returns the stored lifecycle anchor (effective /
	// expires) of one event, if the row exists and carries one. Radio
	// gateways use it to re-publish a retransmitted command with
	// byte-identical content, so the store deduplicates it into a
	// single delivery across restarts.
	EventTimes(ctx context.Context, key string) (eff, exp time.Time, ok bool, err error)

	// Count returns the number of stored events.
	Count(ctx context.Context) (int, error)

	// PendingStats reports the number of undelivered changes and the age of
	// the oldest one.
	PendingStats(ctx context.Context) (pending int, oldest time.Duration, err error)

	// ListArchiveEvents returns current-state ENDED events last seen on or
	// after since (active events excluded — they are the Active hazards
	// section), newest first, as one page (offset/limit). The public
	// home-page archive browses the history of ended communications
	// through it.
	ListArchiveEvents(ctx context.Context, since time.Time, offset, limit int) ([]StoredEvent, error)

	// CountArchiveEvents reports how many current-state ENDED events were
	// last seen on or after since (the archive total for pagination).
	CountArchiveEvents(ctx context.Context, since time.Time) (int, error)

	// Close releases the underlying resources.
	Close() error
}

// DeliveryRecord is one action-fire ledger row for the notification
// details view: which action attempted one event's delivery, with what
// result, how many attempts and when. The ledger rows are the durable
// half of the delivery history; retention applies via PruneActionFires.
type DeliveryRecord struct {
	EventKey      string
	ActionID      string
	Status        string
	Attempts      int
	FiredAt       time.Time
	NextAttemptAt time.Time
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
