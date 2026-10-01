package storage

// HazardVerdict is the local freshness verdict for one event key,
// separated so callers make explicit choices: only a confirmed inactive
// state may suppress a notification — unknown and read errors never do.
type HazardVerdict int

const (
	// HazardUnknown: no local record of this event (the transition may
	// come from a producer without local storage).
	HazardUnknown HazardVerdict = iota
	// HazardActive: the event exists, is active and not expired.
	HazardActive
	// HazardInactive: the event is known but cancelled or expired.
	HazardInactive
)
