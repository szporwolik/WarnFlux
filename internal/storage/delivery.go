package storage

// DeliveryStatus is the persisted state of one notification delivery job
// (one group × action × event fire, keyed by dedup_key).
type DeliveryStatus int

const (
	// DeliveryRetry means the job may execute: it is running (the process
	// crashed between claim and execution, or the success settle failed)
	// or failed (the action rejected the request earlier). Replaying the
	// transition re-attempts the delivery instead of deduplicating it.
	DeliveryRetry DeliveryStatus = iota

	// DeliverySucceeded means the action accepted the request. Replays of
	// the same transition are deduplicated.
	DeliverySucceeded
)
