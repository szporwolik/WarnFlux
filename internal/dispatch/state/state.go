// Package state holds the concurrency-safe, in-memory mirror of the
// retained MQTT state received through the configured receivers.
//
// Multi-broker namespacing: every entry is keyed by receiver ID + MQTT
// topic, so two brokers publishing the same topics never overwrite each
// other. This mirror deliberately does NOT live in SQLite: it is
// reconstructed from retained topics after every MQTT reconnect, and
// WarnFlux's single SQLite database is reserved for the Router core
// (plus future durable dispatcher tables).
package state

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

// Bounds protect the maps against pathological broker input. They are
// generous: normal operation never reaches them.
const (
	MaxActiveHazards = 10_000
	MaxInfoEntries   = 10_000
)

// SeverityRank orders severities for dashboard sorting. Lower is more severe.
func SeverityRank(severity string) int {
	switch severity {
	case "extreme":
		return 0
	case "severe":
		return 1
	case "moderate":
		return 2
	case "minor":
		return 3
	case "unknown":
		return 4
	default:
		return 5
	}
}

// Hazard is the current view of one active hazard received through one
// receiver.
type Hazard struct {
	ReceiverID string
	Topic      string
	EventKey   string
	Source     string
	SourceID   string
	Category   string
	Event      string
	Severity   string
	Urgency    string
	Certainty  string
	Headline   string
	// Description is kept for future plugin/UI use; the dashboard does not
	// render the full text by default.
	Description string
	Instruction string

	EffectiveAt *time.Time
	ExpiresAt   *time.Time
	Areas       []string
	Status      string

	// Latitude/Longitude are the optional event coordinates (sources
	// that publish positions, e.g. road difficulties); the web map
	// renders hazards that carry them.
	Latitude  *float64
	Longitude *float64

	ReceivedAt time.Time
	UpdatedAt  time.Time
}

// Weather is a compact, typed snapshot of the canonical weather document
// published by WarnFlux on <prefix>/info/<source>/<producer>/<key>/weather.
type Weather struct {
	GeneratedAt time.Time
	ValidUntil  *time.Time

	ProviderName string

	LocationID   string
	LocationName string
	Latitude     float64
	Longitude    float64

	TemperatureC     *float64
	Condition        string
	HumidityPct      *float64
	WindSpeedKmh     *float64
	WindDirectionDeg *float64
	WindGustsKmh     *float64
	PressureMSLHpa   *float64
	RadiationUSvh    *float64
	RadiationCPM     *float64
	// Daily is the multi-day forecast (internet providers); APRS
	// stations only observe, so their entries have none.
	Daily []DailyWeather
}

// DailyWeather is one forecast day of a canonical weather document.
type DailyWeather struct {
	Date               string
	Condition          string
	TemperatureMaxC    *float64
	TemperatureMinC    *float64
	PrecipitationSumMm *float64
	WindSpeedMaxKmh    *float64
}

// InfoEntry is one retained informational message (usually weather).
type InfoEntry struct {
	ReceiverID string
	Topic      string
	Source     string
	ProducerID string
	Key        string
	Kind       string
	ReceivedAt time.Time
	Weather    *Weather
}

// RouterStatus is the last valid WarnFlux <prefix>/status payload from
// one receiver. Multiple WarnFlux instances (through multiple
// receivers) are tracked independently.
type RouterStatus struct {
	Valid       bool
	ReceivedAt  time.Time
	Service     string
	State       string
	GeneratedAt time.Time
	Version     string

	UptimeSeconds   int64
	DatabaseHealthy bool
	PendingChanges  int
}

// RouterHealth is the dashboard-level health view for one router status.
func (rs RouterStatus) RouterHealth() string {
	if !rs.Valid {
		return "unknown"
	}
	switch rs.State {
	case "running":
		if rs.DatabaseHealthy {
			return "healthy"
		}
		return "degraded"
	case "offline":
		return "degraded"
	default:
		return "unknown"
	}
}

// Counters tracks mirror statistics. Message/malformed/oversized counters
// are per receiver (see mqttreceiver.Stats); only capacity-drop counters
// live here.
type Counters struct {
	DroppedActive int64
	DroppedInfo   int64
}

// Snapshot is an immutable copy of the mirrored state for rendering.
type Snapshot struct {
	Hazards     []Hazard
	Weather     []InfoEntry
	ActiveCount int
	InfoCount   int
	Router      map[string]RouterStatus
	Counters    Counters
}

// State is the concurrency-safe mirror store.
type State struct {
	mu sync.RWMutex

	activeByKey map[string]Hazard
	infoByKey   map[string]InfoEntry
	router      map[string]RouterStatus

	counters Counters
}

// New returns an empty mirror store.
func New() *State {
	return &State{
		activeByKey: make(map[string]Hazard),
		infoByKey:   make(map[string]InfoEntry),
		router:      make(map[string]RouterStatus),
	}
}

// entryKey namespaces a topic by receiver so two brokers publishing the
// same topic cannot collide.
func entryKey(receiverID, topic string) string {
	return receiverID + "\x00" + topic
}

// AddOrUpdateActive stores an active hazard keyed by receiver + topic.
// Updates and deletes of existing entries are always allowed; only
// brand-new entries are subject to the size bound.
func (s *State) AddOrUpdateActive(receiverID, topic string, h Hazard) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := entryKey(receiverID, topic)
	if _, exists := s.activeByKey[key]; !exists && len(s.activeByKey) >= MaxActiveHazards {
		s.counters.DroppedActive++
		return fmt.Errorf("state: active hazard limit %d reached, rejecting %s", MaxActiveHazards, topic)
	}
	h.ReceiverID = receiverID
	h.Topic = topic
	s.activeByKey[key] = h
	return nil
}

// DeleteActive removes an active hazard after a zero-length retained
// payload (per receiver namespace).
func (s *State) DeleteActive(receiverID, topic string) {
	s.mu.Lock()
	delete(s.activeByKey, entryKey(receiverID, topic))
	s.mu.Unlock()
}

// AddOrUpdateInfo stores an informational entry keyed by receiver + topic.
func (s *State) AddOrUpdateInfo(receiverID, topic string, e InfoEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := entryKey(receiverID, topic)
	if _, exists := s.infoByKey[key]; !exists && len(s.infoByKey) >= MaxInfoEntries {
		s.counters.DroppedInfo++
		return fmt.Errorf("state: info entry limit %d reached, rejecting %s", MaxInfoEntries, topic)
	}
	e.ReceiverID = receiverID
	e.Topic = topic
	s.infoByKey[key] = e
	return nil
}

// SetRouterStatus stores the latest valid router status of one receiver.
func (s *State) SetRouterStatus(receiverID string, rs RouterStatus) {
	s.mu.Lock()
	s.router[receiverID] = rs
	s.mu.Unlock()
}

// Snapshot returns an immutable, sorted copy of the mirrored state.
// Active hazards whose ExpiresAt has already passed are pruned from the
// mirror first: a stale retained document on the broker (e.g. left behind
// by an instance that went down before publishing the expiry) must never
// leak into rendered views.
func (s *State) Snapshot() Snapshot {
	s.mu.Lock()
	now := time.Now()
	for key, h := range s.activeByKey {
		if h.Status == "active" && h.ExpiresAt != nil && !h.ExpiresAt.After(now) {
			delete(s.activeByKey, key)
		}
	}

	snap := Snapshot{
		ActiveCount: len(s.activeByKey),
		InfoCount:   len(s.infoByKey),
		Counters:    s.counters,
		Router:      make(map[string]RouterStatus, len(s.router)),
	}
	for id, rs := range s.router {
		snap.Router[id] = rs
	}

	snap.Hazards = make([]Hazard, 0, len(s.activeByKey))
	for _, h := range s.activeByKey {
		h.Areas = append([]string(nil), h.Areas...)
		snap.Hazards = append(snap.Hazards, h)
	}
	sort.Slice(snap.Hazards, func(i, j int) bool {
		a, b := snap.Hazards[i], snap.Hazards[j]
		if ra, rb := SeverityRank(a.Severity), SeverityRank(b.Severity); ra != rb {
			return ra < rb
		}
		if ka, kb := hazardSortKey(a), hazardSortKey(b); !ka.Equal(kb) {
			return ka.After(kb)
		}
		return a.ReceiverID+"\x00"+a.Topic < b.ReceiverID+"\x00"+b.Topic
	})

	snap.Weather = make([]InfoEntry, 0, len(s.infoByKey))
	for _, e := range s.infoByKey {
		if e.Weather != nil {
			snap.Weather = append(snap.Weather, e)
		}
	}
	sort.Slice(snap.Weather, func(i, j int) bool {
		a := snap.Weather[i].Weather.GeneratedAt
		b := snap.Weather[j].Weather.GeneratedAt
		if a.Equal(b) {
			ka := snap.Weather[i].ReceiverID + "\x00" + snap.Weather[i].Topic
			kb := snap.Weather[j].ReceiverID + "\x00" + snap.Weather[j].Topic
			return ka < kb
		}
		return a.After(b)
	})
	s.mu.Unlock()
	return snap
}

// hazardSortKey picks the most meaningful timestamp for ordering within a
// severity group: updated time first, then effective, then received.
func hazardSortKey(h Hazard) time.Time {
	if !h.UpdatedAt.IsZero() {
		return h.UpdatedAt
	}
	if h.EffectiveAt != nil && !h.EffectiveAt.IsZero() {
		return *h.EffectiveAt
	}
	return h.ReceivedAt
}
