package action

import (
	"context"
	"log/slog"
	"time"

	"github.com/szporwolik/WarnFlux/internal/dispatch"
	"github.com/szporwolik/WarnFlux/internal/storage"
)

// GateStore is the storage surface the delivery gate consults.
type GateStore interface {
	// LifecycleBlocks: this exact version may not go out (superseded by a
	// newer one, or itself terminal).
	LifecycleBlocks(ctx context.Context, publisher, eventKey string, changeID int64) (bool, error)
	// LifecycleSuperseded: a NEWER version is already recorded.
	LifecycleSuperseded(ctx context.Context, publisher, eventKey string, changeID int64) (bool, error)
	// HazardActive: local freshness verdict of the event key.
	HazardActive(ctx context.Context, publisher, eventKey string, at time.Time) (storage.HazardVerdict, error)
}

// NewLifecycleGate builds the staleness oracle consulted on this
// instance directly before a queued job is transmitted: an alert
// superseded by a newer version, cancelled (locally or remotely) or
// expired while its job waited in the queue must never reach the
// radio. The shared lifecycle ledger covers local, remote and panel
// messages; the local events table remains the second layer for expiry
// of locally-ingested events.
//
// A terminal transition flagged Notify is the deliberate exception, but
// a NARROW one: the stand-down notice tells people the hazard ended, so
// the terminal status of its own version cannot hold it back — yet a
// version that came after it still does. Without that rule a
// cancellation that was still queued reached the channels after the
// operator had already raised the network again (reported P1: version 9
// transmitted while version 10 was active).
//
// Every read failure fails open: a notification is never suppressed
// because the database could not answer.
func NewLifecycleGate(store GateStore, logger *slog.Logger) func(ctx context.Context, req ActionRequest) bool {
	return func(ctx context.Context, req ActionRequest) bool {
		h := req.Event.Hazard
		if h == nil {
			return true
		}
		if h.Notify {
			superseded, err := store.LifecycleSuperseded(ctx, h.Publisher, h.Key, h.ChangeID)
			if err != nil {
				logGateFailure(logger, "actions: lifecycle lookup failed", h, err)
				return true
			}
			return !superseded
		}
		blocked, err := store.LifecycleBlocks(ctx, h.Publisher, h.Key, h.ChangeID)
		if err != nil {
			logGateFailure(logger, "actions: lifecycle lookup failed", h, err)
			return true
		}
		if blocked {
			return false
		}
		verdict, err := store.HazardActive(ctx, h.Publisher, h.Key, time.Now())
		if err != nil {
			logGateFailure(logger, "actions: delivery freshness lookup failed", h, err)
			return true
		}
		return verdict != storage.HazardInactive
	}
}

// logGateFailure reports a gate read failure without suppressing the
// notification (logger may be nil in tests).
func logGateFailure(logger *slog.Logger, msg string, h *dispatch.HazardTransition, err error) {
	if logger == nil {
		return
	}
	logger.Warn(msg, "event_key", h.Key, "error", err)
}
