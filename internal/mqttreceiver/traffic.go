package mqttreceiver

import (
	"sync"
	"time"
)

// DefaultTrafficEntries bounds the in-memory MQTT traffic viewer buffer.
const DefaultTrafficEntries = 100

// TrafficEntry is one recorded inbound MQTT frame.
type TrafficEntry struct {
	Seq      int64  `json:"seq"`
	At       string `json:"at"` // local time, RFC3339
	Receiver string `json:"receiver"`
	Kind     string `json:"kind"` // events | active | info | status | generic
	Topic    string `json:"topic"`
	QoS      byte   `json:"qos"`
	Retained bool   `json:"retained"`
	Size     int    `json:"size"`
}

// TrafficBuffer is a bounded ring buffer of the inbound MQTT frames every
// receiver ingests. The web UI serves snapshots of it on the MQTT traffic
// page. Since receivers subscribe to the instance's own topic prefix, the
// buffer naturally shows both own publications and external traffic.
type TrafficBuffer struct {
	mu      sync.Mutex
	max     int
	nextSeq int64
	entries []TrafficEntry
}

// NewTrafficBuffer builds a buffer retaining at most max entries.
func NewTrafficBuffer(max int) *TrafficBuffer {
	if max < 1 {
		max = DefaultTrafficEntries
	}
	return &TrafficBuffer{max: max}
}

// Add records one inbound frame. It is a no-op for a nil receiver.
func (b *TrafficBuffer) Add(receiver, kind, topic string, qos byte, retained bool, size int) {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.nextSeq++
	b.entries = append(b.entries, TrafficEntry{
		Seq:      b.nextSeq,
		At:       time.Now().Format(time.RFC3339),
		Receiver: receiver,
		Kind:     kind,
		Topic:    topic,
		QoS:      qos,
		Retained: retained,
		Size:     size,
	})
	if len(b.entries) > b.max {
		b.entries = b.entries[len(b.entries)-b.max:]
	}
}

// Max returns the buffer capacity (0 when nil).
func (b *TrafficBuffer) Max() int {
	if b == nil {
		return 0
	}
	return b.max
}

// Snapshot returns every entry with Seq > after (0 = everything).
func (b *TrafficBuffer) Snapshot(after int64) []TrafficEntry {
	if b == nil {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]TrafficEntry, 0, len(b.entries))
	for _, e := range b.entries {
		if e.Seq > after {
			out = append(out, e)
		}
	}
	return out
}
