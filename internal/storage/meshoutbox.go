package storage

import (
	"context"
	"time"
)

// MeshOutboxItem is one message an operator asked the station to send over
// Meshtastic but the radio could not take at that moment.
//
// The web send form used to fail outright when the node was between sessions —
// a WiFi reset, a firmware reconnect, another client taking the slot — and the
// message was simply gone. The outbox holds it instead: the dispatcher sends it
// as soon as the device is back, and the panel shows what is still waiting, so
// "did my message go out?" has an answer that does not depend on the log.
type MeshOutboxItem struct {
	ID int64
	// Kind is "dm" (a direct message to TargetID) or "channel" (a broadcast
	// on ChannelIdx).
	Kind string
	// TargetID is the 8-hex node id of a direct message; empty for a channel
	// broadcast.
	TargetID string
	// Label is what the panel shows: the node name the operator picked, or
	// the channel label at the time of queueing.
	Label string
	// ChannelIdx is the device channel index of a broadcast (1-7).
	ChannelIdx int
	Text       string
	Operator   string
	CreatedAt  time.Time
	// Attempts counts the delivery attempts that failed; LastError carries
	// the last one, so a message that keeps failing says why.
	Attempts  int
	LastError string
	// SentAt is zero while the message is still waiting.
	SentAt time.Time
}

// MeshOutboxMaxAttempts bounds the retries of one queued message. A link that
// comes back and dies again should not keep an undeliverable message forever,
// and the operator sees the abandonment in the panel.
const MeshOutboxMaxAttempts = 10

// MeshOutboxRetention bounds how long a delivered or abandoned row is kept for
// the panel's "recent" list.
const MeshOutboxRetention = 24 * time.Hour

// MeshOutboxStore persists messages waiting for the Meshtastic device.
type MeshOutboxStore interface {
	// EnqueueMeshOutbox stores one message and returns its id.
	EnqueueMeshOutbox(ctx context.Context, item MeshOutboxItem) (int64, error)
	// PendingMeshOutbox returns the messages still to send, oldest first.
	PendingMeshOutbox(ctx context.Context, limit int) ([]MeshOutboxItem, error)
	// RecentMeshOutbox returns the newest rows, sent or not, for the panel.
	RecentMeshOutbox(ctx context.Context, limit int) ([]MeshOutboxItem, error)
	// MarkMeshOutboxSent records a successful transmission.
	MarkMeshOutboxSent(ctx context.Context, id int64, at time.Time) error
	// MarkMeshOutboxAttempt records a failed attempt and its reason.
	MarkMeshOutboxAttempt(ctx context.Context, id int64, errText string) error
	// DeleteMeshOutbox removes one row (the operator cancelled it).
	DeleteMeshOutbox(ctx context.Context, id int64) error
	// PruneMeshOutbox deletes rows created before the given time.
	PruneMeshOutbox(ctx context.Context, before time.Time) error
}
