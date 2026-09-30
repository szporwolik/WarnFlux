package storage

import (
	"context"
	"time"
)

// DeliveryStatus is the persisted state of one durable delivery job
// (one group × action × event fire, keyed by dedup_key).
type DeliveryStatus int

const (
	// DeliverySaved means the job is persisted with its full payload and
	// waits for a worker (first execution or a scheduled retry). Replays
	// of the transition are deduplicated: the job sits in the durable
	// queue and WILL be executed by a worker.
	DeliverySaved DeliveryStatus = iota

	// DeliveryAccepted means the transport accepted the transmission
	// (the device/relay queued or sent it). Terminal: replays
	// deduplicate. This is the strongest stage most channels can prove.
	DeliveryAccepted

	// DeliveryConfirmed means the transport proved recipient-level
	// confirmation. Terminal: replays deduplicate. Only channels whose
	// protocol offers confirmations (e.g. MeshCore direct-message ACKs)
	// can reach it; others stay at accepted.
	DeliveryConfirmed

	// DeliveryExpired means the hazard was no longer worth transmitting
	// when the worker was about to execute it (its ExpiresAt passed
	// while the job sat in the queue, or the store knows a newer
	// cancellation/expiry). Terminal: replays deduplicate — the stale
	// alert must never hit the radio.
	DeliveryExpired

	// DeliveryFailed means the execution failed and the retry budget is
	// exhausted (terminal for the scheduler). A replayed transition
	// re-arms the job with a fresh budget instead of deduplicating it,
	// so an alert whose execution burned all attempts is never lost.
	DeliveryFailed
)

func (s DeliveryStatus) String() string {
	switch s {
	case DeliverySaved:
		return "saved"
	case DeliveryAccepted:
		return "accepted"
	case DeliveryConfirmed:
		return "confirmed"
	case DeliveryExpired:
		return "expired"
	case DeliveryFailed:
		return "failed"
	}
	return "unknown"
}

// DeliveryJob is one durable delivery: everything needed to execute the
// action after a restart.
type DeliveryJob struct {
	GroupID  int64
	ActionID string
	EventKey string
	DedupKey string
	// Payload is the JSON-encoded action request (event + recipients)
	// persisted with the job.
	Payload []byte
	// Attempts is the number of execution attempts already spent,
	// including the current claim.
	Attempts int
	// FiredAt is when the transition triggered the job.
	FiredAt time.Time
}

// DeliveryResult is the per-job outcome of one CommitInboxDelivery call.
type DeliveryResult struct {
	// Status is the stored job state after the call.
	Status DeliveryStatus
	// Queued reports whether the call made the job pending (fresh insert
	// or a re-armed terminal failure). false = the transition was
	// deduplicated (already pending or delivered).
	Queued bool
}

// InboxDeliveryStore atomically persists delivery jobs and consumes the
// inbox row they came from: either every job exists durably AND the
// inbox row is gone, or neither happened. This is the only way the
// routing engine may process an inbox event — a failed evaluation must
// leave the row pending for recovery, never silently drop it.
type InboxDeliveryStore interface {
	// CommitInboxDelivery persists every job and, when inboxID != 0,
	// deletes the inbox row — all in one transaction. It returns one
	// result per job, in order. inboxID == 0 with no jobs is a no-op
	// (live events need no ledger round trip).
	CommitInboxDelivery(ctx context.Context, inboxID int64, jobs []DeliveryJob) ([]DeliveryResult, error)
}

// DeliveryStore is the durable action-job queue backing the notification
// machine. The routing engine persists jobs; action workers claim and
// settle them. Every row carries the full payload, the attempt counter
// and the next-attempt deadline, so delivery survives any restart
// between "queued" and "result recorded".
type DeliveryStore interface {
	// EnqueueDelivery persists one job. It returns the stored job state
	// and whether THIS call made the job pending: queued=true for a
	// fresh insert or a re-armed terminal failure, queued=false when the
	// transition was deduplicated (already pending or delivered).
	EnqueueDelivery(ctx context.Context, job DeliveryJob) (DeliveryStatus, bool, error)

	// ClaimNextDelivery atomically claims the next due job of one
	// action: it becomes running, the attempt counter is incremented and
	// the claim deadline moves past now. maxAttempts bounds the retry
	// budget (terminal failures are never claimed again); a saved job
	// recovered from a crashed claim executes even when its spent
	// attempts equal the budget.
	ClaimNextDelivery(ctx context.Context, actionID string, maxAttempts int, now time.Time) (DeliveryJob, bool, error)

	// SettleDelivery records the post-execution stage. A non-terminal
	// failure carries the deadline of the next attempt.
	SettleDelivery(ctx context.Context, groupID int64, actionID, dedupKey string, stage DeliveryStatus, nextAttempt time.Time) error

	// RecoverStaleClaims resets running jobs whose claim deadline has
	// passed (the process crashed between claim and settlement) back to
	// saved, so they execute again.
	RecoverStaleClaims(ctx context.Context, now time.Time) (int64, error)

	// PendingDeliveries counts non-terminal jobs of one action (status
	// display).
	PendingDeliveries(ctx context.Context, actionID string) (int, error)
}
