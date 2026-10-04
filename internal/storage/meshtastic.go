package storage

import (
	"context"
	"time"
)

// MeshMessageRetentionEntries bounds the persisted mesh message history
// (the admin /meshtastic page): the store keeps the newest N rows.
const MeshMessageRetentionEntries = 2000

// MeshMessage is one persisted Meshtastic message: received (rx) or sent
// (tx). Sender is the hex public key prefix (contact messages) or empty
// (channel messages); Channel names the group channel (e.g. "Public");
// Hops is the radio path length when the frame carried one (0 = unknown);
// Operator is the admin username behind a tx row (rx rows are empty).
// Status is the tx delivery state: "" (legacy/unknown), "sent" (the
// radio transmitted it), "delivered" (the recipient acknowledged a
// direct message) or "failed" (no acknowledgment after the retries).
type MeshMessage struct {
	ID        int64
	Direction string
	Sender    string
	Channel   string
	Hops      int
	Operator  string
	Text      string
	Status    string
	At        time.Time
}

// MeshtasticMessageFilter narrows the persisted message history queries.
type MeshtasticMessageFilter struct {
	// Direction is "rx", "tx" or "" (both directions).
	Direction string
	// Channel is an exact stored channel label ("ch0", "dm", "SP9MOA",
	// ...). Empty means no channel condition.
	Channel string
	// Exclude inverts the channel condition: every channel except
	// Channel. Ignored when Channel is empty.
	Exclude bool
}

// MeshtasticMessageStore persists Meshtastic message history. Implementations
// bound retention to MeshMessageRetentionEntries.
type MeshtasticMessageStore interface {
	RecordMeshtasticMessage(ctx context.Context, direction, sender, channel, text, operator string, hops int, at time.Time) error
	// UpdateMeshtasticMessageStatus marks the delivery state of the
	// matching TX row (created at + text).
	UpdateMeshtasticMessageStatus(ctx context.Context, status string, at time.Time, text string) error
	ListMeshtasticMessages(ctx context.Context, f MeshtasticMessageFilter, limit, offset int) ([]MeshMessage, error)
	CountMeshtasticMessages(ctx context.Context, f MeshtasticMessageFilter) (int, error)
}

// MeshtasticNode is one persisted heard node from the node directory.
type MeshtasticNode struct {
	ID       string
	Name     string
	Short    string
	Lat      float64
	Lon      float64
	LastSeen time.Time
	Sends    []string
}

// MeshtasticNodeStore persists the heard-node directory across restarts:
// the hub merges it with live observations (the device node DB and fresh
// packets) and rewrites it on change, so the admin page and the map keep
// nodes that went quiet long ago.
type MeshtasticNodeStore interface {
	LoadMeshtasticNodes(ctx context.Context) ([]MeshtasticNode, error)
	SaveMeshtasticNodes(ctx context.Context, nodes []MeshtasticNode) error
}
