package storage

import (
	"context"
	"time"
)

// APRSMessageRetentionEntries bounds the persisted APRS message history
// (the admin /messages page): the store keeps the newest N rows and prunes
// the rest on insert. The value is deliberately generous — the volume is
// tiny (a few messages per hour at most).
const APRSMessageRetentionEntries = 2000

// APRSMessage is one persisted APRS message: received (rx) or sent (tx).
type APRSMessage struct {
	ID        int64
	Direction string // rx | tx
	From      string
	To        string
	Text      string
	MsgID     string
	Via       string
	At        time.Time
	// Status is the outbound delivery state ("" = transmitted, "delivered"
	// after the addressee's ack, "failed" after a rej). rx rows keep "".
	Status string
}

// APRSMessageStore persists APRS message history. The hub records both
// directions; the web admin page reads the history back. Implementations
// bound retention to APRSMessageRetentionEntries.
type APRSMessageStore interface {
	RecordAPRSMessage(ctx context.Context, direction, from, to, text, msgID, via string, at time.Time) error
	UpdateAPRSMessageStatus(ctx context.Context, msgID, status string, at time.Time) error
	// APRSMessageAddressee returns the addressee callsign of the tx row
	// carrying msgID (ok=false when no such row exists). The hub uses it
	// to bind late acks — heard after the in-memory wait is gone — to
	// their exchange before applying the delivery status.
	APRSMessageAddressee(ctx context.Context, msgID string) (string, bool, error)
	ListAPRSMessages(ctx context.Context, direction string, limit, offset int) ([]APRSMessage, error)
	CountAPRSMessages(ctx context.Context, direction string) (int, error)
}
