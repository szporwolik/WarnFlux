package core

// ChangeType describes what changed about an event so outputs can act on
// meaningful transitions only.
type ChangeType string

const (
	// ChangeNew is emitted when a previously unknown event is ingested.
	ChangeNew ChangeType = "new"
	// ChangeUpdated is emitted when an existing event's content changes.
	ChangeUpdated ChangeType = "updated"
	// ChangeCancelled is emitted when a source cancels an event.
	ChangeCancelled ChangeType = "cancelled"
	// ChangeExpired is emitted when the expiration worker expires an event.
	ChangeExpired ChangeType = "expired"
)

// String returns the change type as its wire name.
func (t ChangeType) String() string { return string(t) }

// EventChange is a meaningful state transition that outputs should receive.
// Duplicates intentionally produce no EventChange.
type EventChange struct {
	// ID is the durable journal change ID (0 when the change does not come
	// from the journal, e.g. in tests or demos).
	ID int64

	Type  ChangeType
	Event HazardEvent
}

// Clone returns a deep copy of the change so outputs can never mutate data
// shared with the core or with other outputs.
func (c EventChange) Clone() EventChange {
	return EventChange{ID: c.ID, Type: c.Type, Event: c.Event.Clone()}
}
