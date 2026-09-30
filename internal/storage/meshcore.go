package storage

import (
	"context"
	"time"
)

// MeshMessageRetentionEntries bounds the persisted mesh message history
// (the admin /meshcore page): the store keeps the newest N rows.
const MeshMessageRetentionEntries = 2000

// MeshMessage is one persisted MeshCore message: received (rx) or sent
// (tx). Sender is the hex public key prefix (contact messages) or empty
// (channel messages); Channel names the group channel (e.g. "Public");
// Hops is the radio path length when the frame carried one (0 = unknown);
// Operator is the admin username behind a tx row (rx rows are empty).
type MeshMessage struct {
	ID        int64
	Direction string
	Sender    string
	Channel   string
	Hops      int
	Operator  string
	Text      string
	At        time.Time
}

// MeshMessageStore persists MeshCore message history. Implementations
// bound retention to MeshMessageRetentionEntries.
type MeshMessageStore interface {
	RecordMeshMessage(ctx context.Context, direction, sender, channel, text, operator string, hops int, at time.Time) error
	ListMeshMessages(ctx context.Context, direction string, limit, offset int) ([]MeshMessage, error)
	CountMeshMessages(ctx context.Context, direction string) (int, error)
}
