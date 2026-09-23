package aprs

import (
	"time"
)

// PositionWire is the wire form of a position.
type PositionWire struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

// StationDocument is the retained MQTT state document of ONE nearby
// station. All backends (aprs-inet today, aprs-radio later) merge into the
// same document, so every station has exactly one topic.
type StationDocument struct {
	SchemaVersion int    `json:"schema_version"`
	Callsign      string `json:"callsign"`
	// Self marks the WarnFlux instance's own station document.
	Self bool `json:"self,omitempty"`
	// Name is the object name when the station is an APRS object.
	Name           string        `json:"name,omitempty"`
	Position       *PositionWire `json:"position,omitempty"`
	SymbolTable    string        `json:"symbol_table,omitempty"`
	Symbol         string        `json:"symbol,omitempty"`
	CourseDeg      int           `json:"course_deg,omitempty"`
	SpeedKMH       float64       `json:"speed_kmh,omitempty"`
	AltitudeM      *float64      `json:"altitude_m,omitempty"`
	Comment        string        `json:"comment,omitempty"`
	Status         string        `json:"status,omitempty"`
	MessageCapable bool          `json:"message_capable"`
	// DistanceKM is the distance from our configured position.
	DistanceKM   float64  `json:"distance_km,omitempty"`
	LastHeardAt  string   `json:"last_heard_at"`
	LastPacketAt string   `json:"last_packet_at,omitempty"`
	ReceivedVia  []string `json:"received_via"`
	PacketCount  int      `json:"packet_count"`
}

// PacketDocument is the non-retained MQTT document of one parsed packet.
type PacketDocument struct {
	SchemaVersion  int           `json:"schema_version"`
	Kind           string        `json:"kind"`
	Src            string        `json:"src"`
	Dst            string        `json:"dst,omitempty"`
	Path           []string      `json:"path,omitempty"`
	Timestamp      string        `json:"timestamp,omitempty"`
	Position       *PositionWire `json:"position,omitempty"`
	SymbolTable    string        `json:"symbol_table,omitempty"`
	Symbol         string        `json:"symbol,omitempty"`
	CourseDeg      int           `json:"course_deg,omitempty"`
	SpeedKMH       float64       `json:"speed_kmh,omitempty"`
	AltitudeM      *float64      `json:"altitude_m,omitempty"`
	Name           string        `json:"name,omitempty"`
	Comment        string        `json:"comment,omitempty"`
	Status         string        `json:"status,omitempty"`
	MessageCapable bool          `json:"message_capable"`
	Message        *MessageWire  `json:"message,omitempty"`
	ReceivedAt     string        `json:"received_at"`
	Via            string        `json:"via"`
}

// MessageWire is the message block inside a PacketDocument.
type MessageWire struct {
	To   string `json:"to"`
	Text string `json:"text"`
	ID   string `json:"id,omitempty"`
}

// MessageDocument is the non-retained MQTT document of one APRS text
// message, received (direction "rx") or sent (direction "tx").
type MessageDocument struct {
	SchemaVersion int    `json:"schema_version"`
	Direction     string `json:"direction"`
	From          string `json:"from"`
	To            string `json:"to"`
	Text          string `json:"text"`
	ID            string `json:"id,omitempty"`
	ReceivedAt    string `json:"received_at"`
	Via           string `json:"via"`
}

// formatTime renders a unix timestamp as RFC 3339 UTC.
func formatTime(unix int64) string {
	if unix <= 0 {
		return ""
	}
	return time.Unix(unix, 0).UTC().Format(time.RFC3339)
}
