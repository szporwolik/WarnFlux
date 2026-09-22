// Package mqttreceiver implements the MQTT receiver subsystem: one
// independent Paho client per configured receiver, strict WarnFlux
// protocol parsing, generic subscription handling and a manager with
// per-receiver status. It is the unified dispatch ingress for local and
// remote brokers.
//
// It deliberately is NOT an MQTT output: receivers never republish
// anything, so there is no implicit broker bridge and no loop.
package mqttreceiver

import (
	"errors"
	"time"
)

// WireSchemaVersion is the schema version published by WarnFlux for
// events, active hazards, status and weather documents.
const WireSchemaVersion = 1

// Payload type markers.
const (
	TypeActiveHazard = "active_hazard"
	TypeWeather      = "weather"
	ServiceName      = "warnflux" // public MQTT wire identifier
)

// Status states published by WarnFlux.
const (
	RouterStateRunning = "running"
	RouterStateOffline = "offline"
)

// Change types on the /events stream.
const (
	ChangeNew       = "new"
	ChangeUpdated   = "updated"
	ChangeCancelled = "cancelled"
	ChangeExpired   = "expired"
)

// MaxPayload is the maximum accepted MQTT payload size (1 MiB). MQTT is an
// external trust boundary; larger messages are ignored.
const MaxPayload = 1 << 20

// Errors used for safe rejection of malformed input.
var (
	errSchemaVersion = errors.New("unsupported schema_version")
	errType          = errors.New("unexpected payload type")
	errEmptyKey      = errors.New("empty event_key")
	errBadChangeType = errors.New("unknown change_type")
	errBadState      = errors.New("unknown status state")
	errService       = errors.New("unexpected service")
)

// wireHazardEvent is the shared hazard block used by /events and /active.
// It matches WarnFlux's public wire schema exactly (snake_case).
type wireHazardEvent struct {
	Source      string   `json:"source"`
	SourceID    string   `json:"source_id"`
	Category    string   `json:"category"`
	Event       string   `json:"event"`
	Severity    string   `json:"severity"`
	Urgency     string   `json:"urgency"`
	Certainty   string   `json:"certainty"`
	Headline    string   `json:"headline"`
	Description string   `json:"description"`
	Instruction string   `json:"instruction"`
	EffectiveAt *string  `json:"effective_at,omitempty"`
	ExpiresAt   *string  `json:"expires_at,omitempty"`
	Latitude    *float64 `json:"latitude,omitempty"`
	Longitude   *float64 `json:"longitude,omitempty"`
	Areas       []string `json:"areas"`
	Status      string   `json:"status"`
	SourceURL   string   `json:"source_url"`
	ReceivedAt  string   `json:"received_at"`
	UpdatedAt   string   `json:"updated_at"`
}

// wireActiveHazard is the retained payload on <prefix>/active/<source>/<hash>.
type wireActiveHazard struct {
	SchemaVersion int             `json:"schema_version"`
	Type          string          `json:"type"`
	EventKey      string          `json:"event_key"`
	Event         wireHazardEvent `json:"event"`
}

// wireEvent is the non-retained payload on <prefix>/events.
type wireEvent struct {
	SchemaVersion int             `json:"schema_version"`
	ChangeID      int64           `json:"change_id"`
	ChangeType    string          `json:"change_type"`
	EventKey      string          `json:"event_key"`
	Event         wireHazardEvent `json:"event"`
}

// wirePluginState is one source/output entry inside the status payload.
type wirePluginState struct {
	ID                  string `json:"id"`
	Type                string `json:"type"`
	State               string `json:"state"`
	ConsecutiveFailures int    `json:"consecutive_failures"`
	RestartCount        int    `json:"restart_count"`
	LastError           string `json:"last_error,omitempty"`
}

// wireStatus is the retained payload on <prefix>/status.
type wireStatus struct {
	SchemaVersion           int               `json:"schema_version"`
	Service                 string            `json:"service"`
	State                   string            `json:"state"`
	GeneratedAt             string            `json:"generated_at"`
	Version                 string            `json:"version"`
	UptimeSeconds           int64             `json:"uptime_seconds"`
	DatabaseHealthy         bool              `json:"database_healthy"`
	PendingChanges          int               `json:"pending_changes"`
	OldestPendingAgeSeconds int64             `json:"oldest_pending_age_seconds"`
	Sources                 []wirePluginState `json:"sources"`
	Outputs                 []wirePluginState `json:"outputs"`
}

// wireWeather is the canonical weather document published raw on
// <prefix>/info/<source>/<producer>/<key>/weather.
type wireWeather struct {
	SchemaVersion int          `json:"schema_version"`
	Type          string       `json:"type"`
	GeneratedAt   string       `json:"generated_at"`
	ValidUntil    string       `json:"valid_until,omitempty"`
	Provider      wireProvider `json:"provider"`
	Location      wireLocation `json:"location"`
	Current       *wireCurrent `json:"current,omitempty"`
}

type wireProvider struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Attribution string `json:"attribution"`
}

type wireLocation struct {
	ID         string   `json:"id"`
	Name       string   `json:"name,omitempty"`
	Latitude   float64  `json:"latitude"`
	Longitude  float64  `json:"longitude"`
	ElevationM *float64 `json:"elevation_m,omitempty"`
	Timezone   string   `json:"timezone"`
}

type wireCurrent struct {
	Time                 string   `json:"time"`
	TemperatureC         *float64 `json:"temperature_c,omitempty"`
	ApparentTemperatureC *float64 `json:"apparent_temperature_c,omitempty"`
	RelativeHumidityPct  *float64 `json:"relative_humidity_pct,omitempty"`
	PressureMSLHpa       *float64 `json:"pressure_msl_hpa,omitempty"`
	SurfacePressureHpa   *float64 `json:"surface_pressure_hpa,omitempty"`
	PrecipitationMm      *float64 `json:"precipitation_mm,omitempty"`
	CloudCoverPct        *float64 `json:"cloud_cover_pct,omitempty"`
	WindSpeedKmh         *float64 `json:"wind_speed_kmh,omitempty"`
	WindDirectionDeg     *float64 `json:"wind_direction_deg,omitempty"`
	WindGustsKmh         *float64 `json:"wind_gusts_kmh,omitempty"`
	Condition            string   `json:"condition"`
	IsDay                *bool    `json:"is_day,omitempty"`
}

// parseTime parses an RFC3339(Nano) timestamp; unparseable values yield the
// zero time rather than failing the whole message.
func parseTime(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}
	}
	return t
}

// optTime parses an optional pointer timestamp.
func optTime(s *string) *time.Time {
	if s == nil {
		return nil
	}
	t := parseTime(*s)
	if t.IsZero() {
		return nil
	}
	return &t
}
