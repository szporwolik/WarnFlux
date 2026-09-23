// Package aprs implements the shared APRS (Automatic Packet Reporting
// System) domain for WarnFlux: packet parsing, Maidenhead gridsquares,
// distance math, the merged station registry (the "APRS hub") and the
// retained MQTT wire documents it publishes.
//
// The package is deliberately transport-agnostic. Backends feed packets
// into Hub.Observe and can register as Transmitters for outbound APRS
// messages:
//
//   - aprs-inet (plugins/sources/aprsinet) receives via APRS-IS and can
//     inject messages back into the APRS-IS network;
//   - aprs-radio (future) receives and transmits via KISS/TNC radio.
//
// Because every backend funnels through the same Hub, every nearby station
// has exactly ONE retained state document on MQTT regardless of how many
// backends heard it — no duplicate topics, no divergent state.
package aprs

// MQTT topic layout (suffixes under the receiver's WarnFlux topic prefix;
// the Sink prepends the prefix):
//
//	aprs/stations/<CALLSIGN>   retained — merged state of one nearby station
//	aprs/packets               non-retained — one JSON document per packet
//	aprs/messages              non-retained — APRS messages rx/tx
const (
	// StationsTopicPrefix is the retained per-station state namespace.
	StationsTopicPrefix = "aprs/stations/"

	// PacketsTopic carries the parsed packet feed.
	PacketsTopic = "aprs/packets"

	// MessagesTopic carries APRS text messages (rx and tx).
	MessagesTopic = "aprs/messages"

	// SchemaVersion is the wire schema version of the published documents.
	SchemaVersion = 1

	// MaxMessageText is the APRS message text limit (one line).
	MaxMessageText = 67
)

// Kind classifies a parsed APRS packet.
type Kind string

// Packet kinds.
const (
	KindPosition  Kind = "position"
	KindMessage   Kind = "message"
	KindStatus    Kind = "status"
	KindWeather   Kind = "weather"
	KindTelemetry Kind = "telemetry"
	KindObject    Kind = "object"
	KindQuery     Kind = "query"
	KindOther     Kind = "other"
)

// Position is a WGS84 coordinate pair.
type Position struct {
	Latitude  float64
	Longitude float64
}

// Message is a parsed APRS text message (addressee, text, optional ack
// request id). APRS messages are the "short text message" service, not
// hazard payloads.
type Message struct {
	To   string
	Text string
	// ID is the message number when the sender requested an
	// acknowledgement ("{001" suffix). Empty when no ack is requested.
	ID string
}

// Packet is one parsed APRS-IS / TNC2 frame. Unsupported fields degrade
// gracefully: a packet keeps its kind and whatever fields parsed.
type Packet struct {
	// Raw is the original line (without the trailing CR/LF).
	Raw string
	// Src is the normalized (uppercase) source callsign with SSID.
	Src string
	// Dst is the destination field (e.g. "APRS", "APRSIS").
	Dst string
	// Path is the digipeater path (including q constructs).
	Path []string
	// Kind classifies the packet payload.
	Kind Kind
	// ReceivedAt is when WarnFlux received the packet (caller-supplied).
	ReceivedAt int64
	// Timestamp is the position timestamp embedded in the packet, if any.
	Timestamp *int64
	// Position is the decoded position, when the packet carried one.
	Position *Position
	// SymbolTable and Symbol are the APRS symbol identifiers.
	SymbolTable byte
	Symbol      byte
	// CourseDeg is the reported course in degrees (0 when unknown).
	CourseDeg int
	// SpeedKMH is the reported speed in km/h (0 when unknown).
	SpeedKMH float64
	// AltitudeM is the reported altitude in meters, when present.
	AltitudeM *float64
	// Name is the object/item name (objects only).
	Name string
	// Comment is the free-form comment / status remainder.
	Comment string
	// Status is the status text of a status packet.
	Status string
	// Message is the decoded message (message packets only).
	Message *Message
	// MessageCapable reports whether the station announced APRS
	// messaging capability.
	MessageCapable bool
}
