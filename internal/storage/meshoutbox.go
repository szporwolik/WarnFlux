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
	// ClaimedAt is when the dispatcher took this row for one transmission
	// attempt. A row is claimed atomically, so a cancel that arrives after the
	// claim cannot silently be ignored — and a claim left behind by a crash is
	// reclaimable after MeshOutboxClaimTTL.
	ClaimedAt time.Time
	// FinishedAt is when the row reached its final state (delivered or
	// abandoned). Retention counts from here, never from CreatedAt: a message
	// that waited a day for the radio must not be swept away instead of sent.
	FinishedAt time.Time
}

// MeshOutboxMaxAttempts bounds the retries of one queued message. A link that
// comes back and dies again should not keep an undeliverable message forever,
// and the operator sees the abandonment in the panel.
const MeshOutboxMaxAttempts = 10

// MeshOutboxRetention bounds how long a FINISHED row (delivered or abandoned)
// is kept for the panel's history. Pending rows are never pruned: a radio that
// was away for two days must still deliver what the operators queued.
const MeshOutboxRetention = 24 * time.Hour

// MeshOutboxClaimTTL is how long a claim survives without progress. A crash
// between "claim" and "send" must not strand a message forever, so an older
// claim is up for grabs again.
const MeshOutboxClaimTTL = 5 * time.Minute

// MeshOutboxStore persists messages waiting for the Meshtastic device.
type MeshOutboxStore interface {
	// EnqueueMeshOutbox stores one message and returns its id.
	EnqueueMeshOutbox(ctx context.Context, item MeshOutboxItem) (int64, error)
	// PendingMeshOutbox returns the messages still to send, oldest first.
	PendingMeshOutbox(ctx context.Context, limit int) ([]MeshOutboxItem, error)
	// CountPendingMeshOutbox counts every message still owed a delivery. The
	// panel's counter must come from here, never from a limited page: history
	// must not be able to hide the active queue (WF-R5-02).
	CountPendingMeshOutbox(ctx context.Context) (int, error)
	// RecentFinishedMeshOutbox returns the newest finished rows (delivered or
	// abandoned) for the panel's history.
	RecentFinishedMeshOutbox(ctx context.Context, limit int) ([]MeshOutboxItem, error)
	// ClaimMeshOutbox takes one pending row for a single transmission and
	// reports whether this caller won it. Losing means the operator cancelled
	// it (or another pass claimed it): the message must not be sent.
	ClaimMeshOutbox(ctx context.Context, id int64, at, staleBefore time.Time) (bool, error)
	// MarkMeshOutboxSent records a successful transmission and releases the
	// claim.
	MarkMeshOutboxSent(ctx context.Context, id int64, at time.Time) error
	// MarkMeshOutboxAttempt records a failed attempt and its reason, releases
	// the claim, and finishes the row once it has no attempts left.
	MarkMeshOutboxAttempt(ctx context.Context, id int64, errText string, at time.Time) error
	// DeletePendingMeshOutbox removes one row the operator cancelled and
	// reports whether it was still waiting. False means the dispatcher was
	// already sending it.
	DeletePendingMeshOutbox(ctx context.Context, id int64) (bool, error)
	// PruneMeshOutbox deletes FINISHED rows older than the given time.
	PruneMeshOutbox(ctx context.Context, before time.Time) error
}
