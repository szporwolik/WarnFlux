package core

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"time"
)

// fingerprintPayload is the canonical serialization of the content fields of
// a HazardEvent. The struct field order is fixed and no maps are involved,
// so JSON marshaling is deterministic.
type fingerprintPayload struct {
	Category    string   `json:"category"`
	Event       string   `json:"event"`
	Severity    string   `json:"severity"`
	Urgency     string   `json:"urgency"`
	Certainty   string   `json:"certainty"`
	Headline    string   `json:"headline"`
	Description string   `json:"description"`
	Instruction string   `json:"instruction"`
	EffectiveAt string   `json:"effective_at,omitempty"`
	ExpiresAt   string   `json:"expires_at,omitempty"`
	Latitude    *float64 `json:"latitude,omitempty"`
	Longitude   *float64 `json:"longitude,omitempty"`
	Areas       []string `json:"areas"`
	Status      string   `json:"status"`
	SourceURL   string   `json:"source_url"`
}

// Fingerprint returns a deterministic SHA-256 hash of the fields whose
// changes should trigger an event update.
//
// Ingestion metadata such as ReceivedAt, UpdatedAt, database IDs and
// last-seen timestamps is intentionally excluded: the same normalized event
// ingested twice always produces the same fingerprint.
func Fingerprint(e HazardEvent) string {
	// Sort a copy so area ordering does not affect the fingerprint. The
	// append to a non-nil slice also normalizes nil and empty areas.
	areas := append([]string{}, e.Areas...)
	sort.Strings(areas)

	p := fingerprintPayload{
		Category:    e.Category,
		Event:       e.Event,
		Severity:    e.Severity,
		Urgency:     e.Urgency,
		Certainty:   e.Certainty,
		Headline:    e.Headline,
		Description: e.Description,
		Instruction: e.Instruction,
		Latitude:    e.Latitude,
		Longitude:   e.Longitude,
		Areas:       areas,
		Status:      string(e.Status),
		SourceURL:   e.SourceURL,
	}
	if e.EffectiveAt != nil {
		p.EffectiveAt = e.EffectiveAt.UTC().Format(time.RFC3339Nano)
	}
	if e.ExpiresAt != nil {
		p.ExpiresAt = e.ExpiresAt.UTC().Format(time.RFC3339Nano)
	}

	// Marshaling a flat struct of basic types cannot fail.
	data, _ := json.Marshal(p)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
