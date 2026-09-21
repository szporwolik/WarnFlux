// Package storage defines the persistence contract for hazard events. The
// only implementation today is internal/storage/sqlite.
package storage

import (
	"context"
	"errors"
	"time"

	"warnflux/internal/core"
)

// ErrNotFound is returned by EventStore.Get when no event matches the key.
var ErrNotFound = errors.New("event not found")

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

// EventStore persists normalized hazard events. Implementations must be
// safe for concurrent use.
type EventStore interface {
	// Get returns the stored event for key, or ErrNotFound.
	Get(ctx context.Context, key string) (*StoredEvent, error)

	// Insert stores a new event. It reports false without error when an
	// event with the same key already exists, so concurrent ingestion of
	// the same new event inserts exactly one row.
	Insert(ctx context.Context, event core.HazardEvent, fingerprint string) (bool, error)

	// Update replaces the content of an existing event. FirstSeenAt and
	// ReceivedAt are preserved; LastSeenAt and UpdatedAt are refreshed.
	Update(ctx context.Context, event core.HazardEvent, fingerprint string) error

	// Touch records that an unchanged event was seen again. Only LastSeenAt
	// is refreshed.
	Touch(ctx context.Context, key string) error

	// MarkExpired flips active events whose ExpiresAt is at or before now
	// to expired and returns them.
	MarkExpired(ctx context.Context, now time.Time) ([]core.HazardEvent, error)

	// Close releases the underlying resources.
	Close() error
}
