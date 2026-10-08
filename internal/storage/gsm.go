package storage

import (
	"context"
	"time"
)

// GSMMessageRetentionEntries bounds the durable SMS history: the oldest
// rows evict beyond this many newest entries (pruned on every insert,
// at startup and hourly).
const GSMMessageRetentionEntries = 2000

// GSMMessage is one SMS history row (rx or tx).
type GSMMessage struct {
	ID        int64
	Direction string // rx | tx
	// From/To are the raw phone numbers; tx rows use "self" as the
	// sender and the addressee number as the recipient.
	From string
	To   string
	Text string
	At   time.Time
}

// GSMMessageStore persists the SMS history. Implemented by the SQLite
// store; recording failures in the hub are best-effort.
type GSMMessageStore interface {
	// RecordGSMMessage appends one SMS message and prunes the history
	// back to GSMMessageRetentionEntries.
	RecordGSMMessage(ctx context.Context, direction, from, to, text string, at time.Time) error
	// ListGSMMessages returns history rows newest first. direction is
	// "rx", "tx" or "" (both).
	ListGSMMessages(ctx context.Context, direction string, limit, offset int) ([]GSMMessage, error)
	// CountGSMMessages counts history rows, optionally per direction.
	CountGSMMessages(ctx context.Context, direction string) (int, error)
}
