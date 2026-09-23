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

// ErrNoTransmitter is returned by Hub.SendMessage when no backend is
// connected and ready to transmit.
var ErrNoTransmitter = errors.New("no APRS transmitter is connected")

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
	// CenterLat/CenterLon are the center of GridSquare (computed).
	CenterLat float64
	CenterLon float64
	// RadiusKM is the "nearby" radius around our position.
	RadiusKM float64
	// StationTTL is how long a station remains in the retained MQTT
	// state after its last packet.
	StationTTL time.Duration
	// ExcludeInfrastructure drops APRS objects, digipeaters, gateways
	// and similar infrastructure from the station state so the map shows
	// actual ham stations only.
	ExcludeInfrastructure bool
	// Version is the WarnFlux version (used in the APRS-IS login).
	Version string

	// SymbolTable/Symbol are our icon, parsed from Icon.
	SymbolTable byte
	Symbol      byte
}

// Defaults applied by NewHub when the config omits values.
const (
	DefaultRadiusKM   = 60
	DefaultStationTTL = 30 * time.Minute
)
