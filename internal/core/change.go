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
	Type  ChangeType
	Event HazardEvent
}
