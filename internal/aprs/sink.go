package aprs

import (
	"context"
	"errors"
	"time"
)

// Sink is the MQTT publishing surface the hub needs. The hub builds topic
// SUFFIXES (aprs/stations/..., aprs/packets, aprs/messages); the sink owns
// the configured topic prefix and the connection. A nil payload with
// retained=true is the retained-topic delete (station expired).
type Sink interface {
	PublishRaw(suffix string, retained bool, payload []byte) error
}

// Transmitter is one backend capable of sending APRS messages (APRS-IS
// today, KISS radio later). The hub routes outbound messages to the first
// ready transmitter.
type Transmitter interface {
	// Name identifies the backend (e.g. "aprs-inet").
	Name() string
	// Ready reports whether the backend can send right now.
	Ready() bool
	// Send delivers one message to the addressed callsign.
	Send(ctx context.Context, to, text string) error
}

// BeaconTransmitter is an optional Transmitter capability: a backend that
// can force an immediate position beacon (the KISS radio backend).
type BeaconTransmitter interface {
	Transmitter
	// Beacon transmits our position packet now.
	Beacon(ctx context.Context) error
}

// ErrNoTransmitter is returned by Hub.SendMessage when no backend is
// connected and ready to transmit.
var ErrNoTransmitter = errors.New("no APRS transmitter is connected")

// ErrNoBeacon is returned by Hub.SendBeacon when no connected backend can
// transmit a position beacon.
var ErrNoBeacon = errors.New("no APRS transmitter can beacon")

// HubConfig is the shared APRS hub configuration (top-level "aprs:" YAML
// section). The hub is the merge point for every APRS backend: aprs-inet
// today, aprs-radio later.
type HubConfig struct {
	// Enabled switches the hub on. APRS plugins fail to start when the
	// hub is disabled.
	Enabled bool
	// Callsign is our identity (with optional SSID), normalized.
	Callsign string
	// Icon is the 1- or 2-character APRS symbol of our own station:
	// "<code>" (primary table) or "<table><code>" (e.g. "/j").
	Icon string
	// GridSquare is our position as a Maidenhead locator.
	GridSquare string
	// Latitude/Longitude optionally pin our exact position; they override
	// the gridsquare center when both are set.
	Latitude  *float64
	Longitude *float64
	// CenterLat/CenterLon are the center of GridSquare (computed).
	CenterLat float64
	CenterLon float64
	// RadiusKM is the nearby radius; it doubles as the default
	// operational-area radius (see AreaRadiusKM).
	RadiusKM float64
	// AreaLatitude/AreaLongitude optionally pin the operational-area
	// center (the territory we serve) independently of the antenna
	// position; must be set together.
	AreaLatitude  *float64
	AreaLongitude *float64
	// AreaRadiusKM is the operational-area radius (0 = RadiusKM).
	AreaRadiusKM float64
	// AreaLat/AreaLon are the resolved area center (computed).
	AreaLat float64
	AreaLon float64
	// StationTTL is how long a station remains in the retained MQTT
	// state after its last packet.
	StationTTL time.Duration
	// BulletinTTL is how long a heard APRS bulletin stays in the
	// retained MQTT state (aprs/bulletins/*) before it is deleted.
	BulletinTTL time.Duration
	// ExcludeInfrastructure drops APRS objects, digipeaters, gateways
	// and similar infrastructure from the station state so the map shows
	// actual ham stations only.
	ExcludeInfrastructure bool
	// Name is the optional display name of our own station (e.g. the
	// installation display name); it rides along in routed APRS messages.
	Name string
	// RouteMessages is legacy: message handling now mirrors the
	// Meshtastic hub — answers and alarms key off the sender allow-list
	// only. Kept for configuration compatibility.
	RouteMessages bool
	// Version is the WarnFlux version (used in the APRS-IS login).
	Version string

	// CmdRetryCooldown is the retry pacing of a TRANSIENTLY failed
	// command (the local pipeline rejected the alert): a retransmission
	// within the cooldown replays the failure, after it the command is
	// executed again (default cmdRetryCooldownDefault).
	CmdRetryCooldown time.Duration
	// ReplyBurst / ReplyBurstWindow bound EVERY automatic reply per
	// sender (command answers, retransmission replays, /help, denials,
	// banners): at most ReplyBurst replies per ReplyBurstWindow per
	// sender, so a flooding sender cannot make the station chatter
	// (defaults: 5 per minute).
	ReplyBurst       int
	ReplyBurstWindow time.Duration

	// MessageRecorder optionally persists the APRS message history
	// (received and sent) for the admin /messages page; nil disables it.
	MessageRecorder MessageRecorder

	// SymbolTable/Symbol are our icon, parsed from Icon.
	SymbolTable byte
	Symbol      byte
}

// MessageRecorder persists the APRS message history (rx and tx) and the
// delivery status of outbound messages. Implemented by storage stores;
// the hub treats recording failures as best-effort.
type MessageRecorder interface {
	RecordAPRSMessage(ctx context.Context, direction, from, to, text, msgID, via string, at time.Time) error
	// UpdateAPRSMessageStatus marks the tx row carrying msgID as
	// delivered (ack) or failed (rej).
	UpdateAPRSMessageStatus(ctx context.Context, msgID, status string, at time.Time) error
}

// Defaults applied by NewHub when the config omits values.
const (
	DefaultRadiusKM    = 60
	DefaultStationTTL  = 30 * time.Minute
	DefaultBulletinTTL = 24 * time.Hour
)
