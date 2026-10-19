// Package meshtastic implements the Meshtastic node integration: the
// station's Meshtastic device plugs in over USB serial and the hub
// speaks the Meshtastic client protocol (protobuf stream) through
// github.com/kabili207/meshtastic-go. It tracks the heard nodes
// (identity, names, positions), keeps the rx/tx message history, sends
// text messages (broadcast or direct), publishes the station and message
// feeds to MQTT, and bridges direct messages from directory-known nodes
// into the canonical /events alarm pipeline.
package meshtastic

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kabili207/meshtastic-go/core"
	pb "github.com/kabili207/meshtastic-go/core/proto"
	"github.com/kabili207/meshtastic-go/transport"
	"github.com/kabili207/meshtastic-go/transport/client"
	"github.com/kabili207/meshtastic-go/transport/serial"
	"github.com/kabili207/meshtastic-go/transport/tcp"
	"google.golang.org/protobuf/proto"

	"github.com/szporwolik/WarnFlux/internal/dispatch"
	"github.com/szporwolik/WarnFlux/internal/radiocli"
	"github.com/szporwolik/WarnFlux/internal/schedule"
	"github.com/szporwolik/WarnFlux/internal/severity"
	"github.com/szporwolik/WarnFlux/internal/storage"
)

// Defaults applied when the configuration omits values.
const (
	DefaultBaud    = 115200
	DefaultNodeTTL = 30 * time.Minute
)

// Config is the hub configuration (top-level "meshtastic:").
type Config struct {
	Enabled bool
	// Device is the serial device path of the Meshtastic node (e.g.
	// /dev/ttyACM0 or /dev/ttyUSB0). A comma-separated list is
	// accepted: the hub tries the paths in order and connects to the
	// first one that opens — a node that re-enumerates between ports
	// after a USB reset (ttyACM0 <-> ttyACM1) is found on whichever
	// port it landed, without a config edit.
	Device string
	Baud   int
	// Transport selects the device link: "serial" (USB CDC, the
	// default) or "tcp" (the node's WiFi API — transport "tcp" dials
	// Host on the Meshtastic API port, 4403 by default). An empty value
	// means serial.
	Transport string
	// Host is the TCP endpoint for transport "tcp": a host, an IP or a
	// host:port pair (the default port 4403 is appended when missing).
	Host string
	// RouteMessages re-publishes direct messages from directory-known
	// senders as canonical /events documents, so they enter the normal
	// routing matrix (the alarm pipeline) like APRS messages do.
	RouteMessages bool
	// NodeTTL bounds how long an unheard neighbour stays in the node list.
	NodeTTL time.Duration
	// PinUserNodes keeps the node IDs registered in the user directory as
	// FAVORITES on the radio. The firmware protects favorites when it prunes
	// its node database and keeps them across a node-db reset, so the public
	// key a DIRECT message is encrypted with survives an unheard night — a
	// node whose key was evicted can only be messaged after it announces
	// itself again ("recipient has not announced its public key").
	PinUserNodes bool
	// PinInterval is how often the pin pass re-runs (default
	// defaultPinInterval when unset): it catches a node registered since the
	// last pass and re-pins one whose favorite was cleared on the device.
	PinInterval time.Duration
	// SilenceTimeout declares a session dead after this long without ANY
	// FromRadio traffic (defaultSilenceTimeout when unset). Tests shorten
	// it; production runs with the default.
	SilenceTimeout time.Duration
	// AckSettleTimeout is how long a device-confirmed direct message
	// waits for the recipient's acknowledgment before settling failed
	// (defaultAckSettleTimeout when unset). The firmware retransmits
	// want_ack frames itself, so this window is generous — client-side
	// resends here would only duplicate the message.
	AckSettleTimeout time.Duration
	// EmcomChannel is the device channel index (1-7) for the periodic
	// presence beacon. 0 disables the beacon. Channel 0 (the default
	// PRIMARY channel) is never used for broadcasts — broadcasts there
	// are blocked.
	EmcomChannel int
	// WatchChannel is the device channel index (1-7) whose received
	// traffic gets a dedicated read-only tab in the message history.
	// It never triggers any transmission, so a secondary install can
	// watch the production channel without beaconing on it. 0 = off.
	WatchChannel int
	// EmcomAt anchors the presence beacon to a daily clock time ("06:00",
	// the station's local time): the beacon then fires once a day at that
	// time and never at startup. It is an alternative to EmcomInterval —
	// naming both is a configuration error.
	EmcomAt string
	// EmcomInterval is the beacon SPACING: it fires every interval, the
	// first one as soon as the first session comes up (server
	// start/restart). 0 with an empty EmcomAt turns the beacon off.
	EmcomInterval time.Duration
	// EmcomHazardsInterval is the spacing of the periodic status line
	// broadcast on the emcom channel (default 1 hour). 0 disables it;
	// the presence beacon stays on its own cadence. The line is sent
	// only when at least one hazard is active.
	EmcomHazardsInterval time.Duration
	// EmcomSite is the public site the periodic status line points at
	// (e.g. https://sosna.sp9moa.pl) — where the details of the active
	// hazards live. Empty = the counters are sent without the pointer.
	EmcomSite string
	// EmcomIdentity is the one-line installation banner sent by the
	// beacon (version, installation name, public domain) — set by main.
	EmcomIdentity string
	// BannerMinInterval is the per-sender spacing of automatic banner
	// replies (default bannerMinInterval): a sender gets at most one
	// banner per window, so answering bots cannot loop each other.
	BannerMinInterval time.Duration
	// BannerGlobalInterval is the global pacing of banner replies
	// (default bannerGlobalInterval): the automatic-answer path never
	// hogs the channel, and queued banners always yield to alarm and
	// command replies.
	BannerGlobalInterval time.Duration
	// CmdRetryCooldown is the retry pacing of a TRANSIENTLY failed
	// command (the local pipeline rejected the alert): a redelivery
	// within the cooldown replays the failure, after it the command is
	// executed again (default cmdRetryCooldownDefault).
	CmdRetryCooldown time.Duration
	// ReplyBurst / ReplyBurstWindow bound EVERY automatic reply per
	// sender (command answers, redelivery replays, /help, denials,
	// banners): at most ReplyBurst replies per ReplyBurstWindow per
	// sender, so a flooding sender cannot make the station chatter
	// (defaults: 5 per minute).
	ReplyBurst       int
	ReplyBurstWindow time.Duration
}

// Recorder persists the meshtastic message history (implemented by
// storage stores). Best-effort: the hub logs recording failures and
// keeps running.
type Recorder interface {
	RecordMeshtasticMessage(ctx context.Context, direction, sender, recipient, channel, text, operator string, hops int, at time.Time) error
	// UpdateMeshtasticMessageStatus marks the tx delivery state of the
	// matching row (created at + text). Reason is the device's refusal
	// reason key when the send settled failed without ever reaching the
	// air (empty otherwise).
	UpdateMeshtasticMessageStatus(ctx context.Context, status, reason string, at time.Time, text string) error
	// RecordMeshActionProgress durably marks one successful meshtastic
	// action transmission keyed by the message VERSION (publisher +
	// event key + change id) and recipient. It is the action's resume
	// ledger — independent of the message history.
	RecordMeshActionProgress(ctx context.Context, publisher, eventKey string, changeID int64, recipient string, channel int, at time.Time) error
	// MeshActionProgressDone reports whether the exact versioned
	// transmission already has a progress row.
	MeshActionProgressDone(ctx context.Context, publisher, eventKey string, changeID int64, recipient string, channel int) (bool, error)
	// SetMeshActionFailed marks (or clears) the durable failure marker
	// of one versioned transmission of ONE CONCRETE JOB: the marker ties
	// the async modem result to the durable delivery job.
	SetMeshActionFailed(ctx context.Context, actionID string, groupID int64, dedupKey, publisher, eventKey string, changeID int64, recipient string, channel int, failed bool) error
	// MeshActionFailed reports whether ANY transmission of the version
	// of one concrete job carries a failure marker.
	MeshActionFailed(ctx context.Context, actionID string, groupID int64, dedupKey, publisher, eventKey string, changeID int64) (bool, error)
	// RecordMeshFailureAndRequeue applies the async TxFailed outcome in
	// ONE transaction: progress revoked, marker recorded and the
	// already-settled delivery job of the concrete job identity
	// re-armed as failed due now. Returns the number of re-armed jobs.
	RecordMeshFailureAndRequeue(ctx context.Context, actionID string, groupID int64, dedupKey, publisher, eventKey string, changeID int64, recipient string, channel int, now time.Time) (int64, error)
}

// NodeStore persists the heard-node directory (implemented by storage
// stores). Optional: a hub without a store keeps the directory in RAM.
type NodeStore interface {
	LoadMeshtasticNodes(ctx context.Context) ([]storage.MeshtasticNode, error)
	SaveMeshtasticNodes(ctx context.Context, nodes []storage.MeshtasticNode) error
}

// FavoriteStore persists the node numbers THIS station set as favorites on
// the radio (implemented by storage stores). Optional: without a store the
// hub still tracks its own stars for the lifetime of the process.
type FavoriteStore interface {
	LoadMeshtasticFavorites(ctx context.Context) ([]uint32, error)
	SaveMeshtasticFavorites(ctx context.Context, ids []uint32) error
}

// Tx delivery states.
const (
	TxSent      = "sent"      // the radio transmitted the frame
	TxDelivered = "delivered" // the recipient acknowledged the DM
	TxFailed    = "failed"    // no acknowledgment after the retries
)

// Delivery timers for direct messages (the channel broadcasts carry no
// acknowledgment by design).
const (
	dmAckTimeout    = 10 * time.Second
	dmMaxRetries    = 2
	dmRetryInterval = 5 * time.Second
)

// defaultSilenceTimeout is how long the session tolerates a completely
// silent device (no FromRadio traffic at all) before it is declared dead
// and reconnected. A healthy node emits queueStatus/telemetry well
// within this window.
const defaultSilenceTimeout = 2 * time.Minute

// defaultHeartbeatInterval is how often the client pings the device with
// the protocol's heartbeat frame. The heartbeat doubles as a keepalive —
// an idle TCP session the peer closes fails the write with "broken pipe"
// instead of hanging silently — and as a liveness probe: a dead link
// surfaces within one interval instead of waiting out the silence
// watchdog.
const defaultHeartbeatInterval = 30 * time.Second

// defaultPinInterval is how often the radio's favorites are reconciled with
// the user directory when the configuration does not say.
const defaultPinInterval = 6 * time.Hour

// maxPendingStationRemovals bounds the station-document removals the hub keeps
// waiting for the broker to confirm. A broker that is unreachable for long
// enough (or an installation with no station feed at all) would otherwise let
// every node that ever went quiet queue another id — in memory and, through the
// persisted directory, on disk. Past the bound the most obsolete entry is
// abandoned, counted and logged: the loss is never silent, and the bound also
// limits what a restart has to load.
const maxPendingStationRemovals = 512

// stationPublishTimeout is the budget of ONE station-feed publication. A batch
// is not bounded by it: the feed is published by its own worker, so a slow
// broker costs the feed its pace and never the radio session.
const stationPublishTimeout = 5 * time.Second

// stationFeedQueueLen bounds the operations waiting for that worker. When it is
// full the operation is dropped, never awaited: an unconfirmed removal stays
// pending (the retry pace offers it again) and a position document is refreshed
// by the next packet from that node.
const stationFeedQueueLen = maxPendingStationRemovals

// feedJob is one station-feed operation for the worker: a document to
// publish, or (doc == nil) the removal of the node's document.
type feedJob struct {
	id  string
	doc []byte
}

// defaultTombstoneRetry is how long a station-document removal the broker
// refused waits before it is attempted again. The maintenance tick is seconds
// apart, so without a pace a broker outage would retry (and log) the same
// removals several times a minute.
const defaultTombstoneRetry = time.Minute

// pinRetryDelay is how soon a pin pass that did not get through is repeated.
// A radio that was reconnecting (or a directory that was briefly unreadable)
// must not wait out a whole interval with a half-applied favorite list.
const pinRetryDelay = 5 * time.Minute

// linkSendWait bounds how long a send waits for a live session before it
// gives up. The session goroutine rebuilds the link on its own after a
// drop, so a send that lands inside the reconnect window only has to
// wait instead of failing.
const linkSendWait = 10 * time.Second

// connPollInterval is how often a waiting send re-checks the session.
const connPollInterval = 100 * time.Millisecond

// errDeviceNotConnected is the send-path error when no session is live.
// The panel shows this wording verbatim.
var errDeviceNotConnected = errors.New("meshtastic: device not connected")

// errReconnectRequested ends the current session on purpose: the link is
// known dead (a failed send or heartbeat), so the session goroutine
// rebuilds it immediately — without the failure noise or the growing
// backoff of a real outage.
var errReconnectRequested = errors.New("reconnect requested")

// nodeDirRetention bounds the heard-node directory: nodes unheard for
// this long are dropped from memory and persistence, so 24/7/365
// operation never accumulates an unbounded directory of transient
// visitors (active mesh members are re-heard well within it).
const nodeDirRetention = 30 * 24 * time.Hour

// meshTextMaxRunes bounds one outbound channel text message (133 chars
// per the Meshtastic spec).
const meshTextMaxRunes = 133

// AlertMarker is the in-band alert character (ASCII BEL, 0x07) the
// Meshtastic firmware scans every received payload for: a message
// carrying it wakes the recipient's screen even when the channel or
// sender is muted, is styled as an alert and — when the receiver enabled
// the External Notification alerts — rings its buzzer/vibrator. THIS
// byte is what makes devices react.
const AlertMarker = "\x07"

// AlertEmoji is the visual label the official clients put in front of an
// alert message (the firmware does not react to it, people do — the
// notification is recognisable at a glance).
const AlertEmoji = "🔔"

// AlertMinSeverity is the lowest hazard severity that rings the
// recipients' devices on the mesh. Severe and above carry the alert
// marker (the device wakes and rings); everything below travels as a
// quiet message — a minor notice must not wake a sleeping phone.
const AlertMinSeverity = severity.Severe

// AlertsAt reports whether one hazard severity is worth an alarm on the
// mesh. Only the canonical scale decides (severity.Rank): an unknown or
// malformed value never rings.
func AlertsAt(sev string) bool {
	r, ok := severity.Rank(sev)
	if !ok {
		return false
	}
	min, _ := severity.Rank(AlertMinSeverity)
	return r >= min
}

// PlainText bounds one quiet outbound text to the channel limit, with no
// alert marking (hazards below AlertMinSeverity).
func PlainText(text string) string {
	r := []rune(strings.TrimSpace(text))
	if len(r) > meshTextMaxRunes {
		r = r[:meshTextMaxRunes]
	}
	return string(r)
}

// alertFraming is the decoration AlertText wraps around a payload: the
// 🔔 label in front (what the official clients show) and the in-band
// marker at the end (what the firmware reacts to).
func alertFraming() (prefix, suffix string) { return AlertEmoji + " ", " " + AlertMarker }

// AlertFramingRunes is the room the alert marking needs around the
// payload. A caller that bounds a message itself (the action reserves
// room for the message ID riding at the end) subtracts it BEFORE the
// final cut, so the framing never eats the tail.
func AlertFramingRunes() int {
	prefix, suffix := alertFraming()
	return len([]rune(prefix)) + len([]rune(suffix))
}

// AlertText marks one outbound text as an alert and bounds it to the
// channel limit. The firmware scans the whole payload, so the position
// of the marker is free; both the label and the marker are framing, so
// the text is cut to the limit AFTER it — a long message never loses its
// alert flag.
func AlertText(text string) string {
	prefix, suffix := alertFraming()
	r := []rune(strings.TrimSpace(text))
	if limit := meshTextMaxRunes - AlertFramingRunes(); len(r) > limit {
		r = r[:limit]
	}
	return prefix + string(r) + suffix
}

// cmdDedupWindow is how long a command identity stays remembered: the
// same packet delivered again by the mesh (rebroadcast, router retry)
// within this window re-sends the previous reply instead of executing
// the command again.
const cmdDedupWindow = 10 * time.Minute

// cmdDedupCap bounds the remembered command outcomes.
const cmdDedupCap = 256

// Automatic banner answers are low-priority traffic: a sender gets at
// most one banner per window (answering bots cannot loop each other),
// the whole path is globally paced, and queued banners drain one at a
// time so alarm replies always go first.
const (
	// bannerMinInterval is the default per-sender banner spacing.
	bannerMinInterval = 5 * time.Minute
	// bannerGlobalInterval is the default global banner pacing.
	bannerGlobalInterval = 10 * time.Second
	// bannerQueueCap bounds queued banner replies.
	bannerQueueCap = 32
)

// cmdRetryCooldownDefault paces retries of transiently failed commands:
// within the cooldown a redelivery replays the failure; after it the
// command executes again.
const cmdRetryCooldownDefault = 15 * time.Second

// replyBurstDefault / replyBurstWindowDefault bound EVERY automatic
// reply per sender (command answers, redelivery replays, /help,
// denials, banners): a flooding sender cannot make the station chatter.
const (
	replyBurstDefault       = 5
	replyBurstWindowDefault = time.Minute
)

// defaultAckSettleTimeout is how long a device-confirmed direct message
// waits for the recipient's acknowledgment. The device firmware
// retransmits want_ack frames on the mesh itself, so the ack can arrive
// late; there is no client-side resend in this window.
const defaultAckSettleTimeout = 2 * time.Minute

// pendingSend tracks one sent message until its delivery state settles.
type pendingSend struct {
	id      uint32 // packet id assigned by the device (echo or queueStatus)
	to      uint32
	at      time.Time // history-row timestamp (the DB match key)
	lastTry time.Time // last transmit attempt (retry window anchor)
	text    string
	channel string // recorded channel label
	wantAck bool   // direct messages want an acknowledgment
	echoed  bool   // the id is known (echo or queueStatus paired)
	status  string
	retries int
	// probe marks a traceroute/node-info placeholder: it rides the send
	// bookkeeping so the device's routing acknowledgment settles IT
	// (never an unrelated text message to the same node) and is never
	// retransmitted as text.
	probe bool
	// failReason carries the device's refusal reason onto the history row
	// when the send settles failed (the raw Routing_Error is unreadable to
	// an operator).
	failReason string
	// prog carries the durable action-progress identity when the send
	// came from the meshtastic action: the ledger follows the
	// TRANSMISSION OUTCOME (recorded on sent/delivered, revoked on
	// failed) instead of the accept handoff.
	prog ProgressRef

	// favorite carries the change to the radio's favorites this frame asks
	// for when it IS one of those admin frames. It rides the send bookkeeping
	// so the DEVICE's answer settles the change: a frame written to the
	// transport is not a confirmation, and a refused one must never be
	// remembered as applied.
	favorite *favoriteChange
	// favoriteSettled marks the change as already answered (a send can settle
	// more than once: only the first verdict counts).
	favoriteSettled atomic.Bool
	// keyTried marks a message that was already re-sent once because the
	// recipient's public key arrived after this very message was refused for
	// missing it. A second refusal must not start the wait again: a peer that
	// keeps announcing a key the radio cannot use would otherwise have the
	// station re-sending the same text forever.
	keyTried bool
}

// keyWaitWindow bounds the wait for a peer key once the radio refused a
// message for missing it. The NodeInfo refresh sent at the refusal is
// answered by the peer's firmware — normally within seconds — and a key that
// never lands leaves the message failed, exactly as before. Every queued
// message carries its OWN deadline (see keyRetry.when), so a later message
// never extends the wait of an older one.
const keyWaitWindow = 5 * time.Minute

// keyRetryPerPeer and keyRetryTotal bound the wait queue: how many messages
// may wait for one peer's key, and how many may wait in the whole hub. Both
// are small on purpose — a normal refusal is one or two messages the operator
// just typed — and an overflow is REFUSED out loud (see awaitPeerKey): the
// message stays failed with its reason, and the log says the retry could not
// be remembered instead of the hub growing without a bound.
const (
	keyRetryPerPeer = 4
	keyRetryTotal   = 32
)

// keyRetry is one direct message the radio refused because the recipient's
// public key was absent from its node database. The station has already asked
// that peer to re-announce its NodeInfo; this holds the message until that
// key lands so the work is finished instead of dying on a technicality the
// operator can neither see nor fix.
type keyRetry struct {
	ps   *pendingSend
	when time.Time
}

// favoriteChange is one requested change to the radio's favorites: the star of
// node is to be set (or taken back).
type favoriteChange struct {
	node uint32
	set  bool
}

// finalOnModemAck reports whether the modem's own acknowledgment is the FINAL
// result of the frame. A favorite change is a LOCAL admin frame: no recipient
// exists to acknowledge it, unlike a direct message, so the delivery window
// does not apply.
func (ps *pendingSend) finalOnModemAck() bool { return ps.favorite != nil }

// ProgressRef identifies one durable action-progress entry (the
// versioned resume ledger of the meshtastic action). The zero value
// attaches no ledger.
type ProgressRef struct {
	ActionID  string
	GroupID   int64
	DedupKey  string
	Publisher string
	EventKey  string
	ChangeID  int64
	// recipient/channel are filled in by the hub at send time.
	recipient string
	channel   int
}

// active reports whether the reference attaches the ledger.
func (p ProgressRef) active() bool {
	return p.ActionID != "" && p.DedupKey != "" && p.Publisher != "" && p.ChangeID > 0
}

// Node signal kinds observed in packets from the node (shown as badges
// on the admin page and the home map).
const (
	SignalTelemetry = "telemetry"
	SignalPosition  = "position"
	SignalText      = "text"
)

// Telemetry is the latest telemetry data observed in a node's
// TELEMETRY_APP broadcasts: device, environment, air-quality and power
// metrics. Fields keep their previous value until the node reports a
// fresh section; zero never means "measured zero" unless the device
// really sends it every time.
type Telemetry struct {
	// Device metrics.
	BatteryLevel uint32  `json:"battery_level,omitempty"`
	Voltage      float32 `json:"voltage,omitempty"`
	ChannelUtil  float32 `json:"channel_util,omitempty"`
	AirUtilTx    float32 `json:"air_util_tx,omitempty"`
	UptimeSecs   uint32  `json:"uptime_secs,omitempty"`
	// Environment metrics.
	Temperature float32 `json:"temperature,omitempty"`
	Humidity    float32 `json:"humidity,omitempty"`
	Pressure    float32 `json:"pressure,omitempty"`
	GasResist   float32 `json:"gas_resistance,omitempty"`
	EnvVoltage  float32 `json:"env_voltage,omitempty"`
	EnvCurrent  float32 `json:"env_current,omitempty"`
	IAQ         uint32  `json:"iaq,omitempty"`
	Lux         float32 `json:"lux,omitempty"`
	WhiteLux    float32 `json:"white_lux,omitempty"`
	IRLux       float32 `json:"ir_lux,omitempty"`
	UVLux       float32 `json:"uv_lux,omitempty"`
	WindDir     uint32  `json:"wind_direction,omitempty"`
	WindSpeed   float32 `json:"wind_speed,omitempty"`
	WindGust    float32 `json:"wind_gust,omitempty"`
	WindLull    float32 `json:"wind_lull,omitempty"`
	Weight      float32 `json:"weight,omitempty"`
	Radiation   float32 `json:"radiation,omitempty"`
	Rainfall1H  float32 `json:"rainfall_1h,omitempty"`
	Rainfall24H float32 `json:"rainfall_24h,omitempty"`
	SoilMoist   uint32  `json:"soil_moisture,omitempty"`
	SoilTemp    float32 `json:"soil_temperature,omitempty"`
	// Air quality metrics (standard and environmental scales).
	PM10Std      uint32  `json:"pm10_standard,omitempty"`
	PM25Std      uint32  `json:"pm25_standard,omitempty"`
	PM100Std     uint32  `json:"pm100_standard,omitempty"`
	PM40Std      uint32  `json:"pm40_standard,omitempty"`
	PM10Env      uint32  `json:"pm10_environmental,omitempty"`
	PM25Env      uint32  `json:"pm25_environmental,omitempty"`
	PM100Env     uint32  `json:"pm100_environmental,omitempty"`
	Particles03  uint32  `json:"particles_03um,omitempty"`
	Particles05  uint32  `json:"particles_05um,omitempty"`
	Particles10  uint32  `json:"particles_10um,omitempty"`
	Particles25  uint32  `json:"particles_25um,omitempty"`
	Particles40  uint32  `json:"particles_40um,omitempty"`
	Particles50  uint32  `json:"particles_50um,omitempty"`
	Particles100 uint32  `json:"particles_100um,omitempty"`
	ParticlesTPS float32 `json:"particles_tps,omitempty"`
	CO2          uint32  `json:"co2,omitempty"`
	CO2Temp      float32 `json:"co2_temperature,omitempty"`
	CO2Hum       float32 `json:"co2_humidity,omitempty"`
	FormForm     float32 `json:"form_formaldehyde,omitempty"`
	FormHum      float32 `json:"form_humidity,omitempty"`
	FormTemp     float32 `json:"form_temperature,omitempty"`
	PMTemp       float32 `json:"pm_temperature,omitempty"`
	PMHum        float32 `json:"pm_humidity,omitempty"`
	PMVOCIdx     float32 `json:"pm_voc_idx,omitempty"`
	PMNOxIdx     float32 `json:"pm_nox_idx,omitempty"`
	// Power metrics: eight measurement channels (voltage in volts,
	// current in milliamps).
	PowerVoltage []float32 `json:"power_voltage,omitempty"`
	PowerCurrent []float32 `json:"power_current,omitempty"`
	// At is when this telemetry was received.
	At time.Time `json:"at,omitempty"`
}

// Node is one neighbour heard through the mesh.
type Node struct {
	// ID is the node's 8-hex identifier (without the leading '!').
	ID   string
	Name string
	// Short is the node's short name from the device directory.
	Short string
	// Sends lists the observed packet kinds from this node
	// (telemetry, position, text), stable order.
	Sends []string
	// Hops is how many mesh hops the LAST packet from this node
	// travelled before reaching us (0 = direct; also 0 for legacy
	// directory entries restored without a packet).
	Hops int
	Lat  float64
	Lon  float64
	// Telemetry is the latest telemetry the node broadcast (nil when
	// the node never sent any).
	Telemetry *Telemetry
	// DistKM and BearingDeg are computed relative to our station's
	// position in Snapshot (0 when either position is unknown).
	DistKM     float64
	BearingDeg float64
	LastSeen   time.Time
	// RemovedSince is the observation whose retained station document the
	// broker CONFIRMED removing. It travels with the node (and its row), so a
	// restart never queues that removal again while the node stays quiet — and
	// a node heard after it becomes eligible again.
	RemovedSince time.Time
}

// nodeTombstone is one removal the broker still owes for a node's retained
// station document. It lives OUTSIDE the node directory: the directory drops a
// node once it has been quiet for nodeDirRetention, and a removal queued just
// before that (or one the broker refused) must stay alive until the broker
// confirmed it — through a restart as well.
type nodeTombstone struct {
	// quietSince is the node's LastSeen when the removal was queued. A node
	// heard AFTER it is active again: the removal is void (and its document
	// has been published again by the very packet that woke it).
	quietSince time.Time
	// tried is the last publish attempt (zero = never), which paces the
	// retries and lets a restored entry be attempted again.
	tried time.Time
	// confirmed reports that the broker took the removal; the entry then only
	// remembers that this node's quiet period has already been handled.
	confirmed bool
}

// cmdRecord is the remembered outcome of one radio command, keyed by
// sender + packet identity: a redelivered packet (rebroadcast, router
// retry) re-sends the previous reply and never re-executes the command.
// A transient failure (retryable) admits controlled retries after the
// cooldown instead of replaying the stale failure forever.
type cmdRecord struct {
	at        time.Time
	reply     string
	retryable bool
}

// bannerReply is one queued low-priority banner answer.
type bannerReply struct {
	id   string
	text string
}

// replyWindow is one sender's sliding burst window for automatic
// replies.
type replyWindow struct {
	start time.Time
	count int
}

// SelfInfo describes our own node.
type SelfInfo struct {
	ID        string
	LongName  string
	ShortName string
	HwModel   string
	Firmware  string
	Lat       float64
	Lon       float64
}

// Snapshot is the admin page / public map model of the hub state.
type Snapshot struct {
	Connected bool
	Self      SelfInfo
	// Device is the configured link (serial path or host:port) and
	// LastError the reason the session is not running — the two facts the
	// panel puts next to the connection pill so an operator sees WHICH link
	// is down and WHY without reading the log.
	Device    string
	LastError string
	// Channels are the device channel names by index.
	Channels []string
	// Nodes is the full node directory (recent and long-gone alike);
	// LastSeen distinguishes them. The MQTT station feed only carries
	// nodes heard within NodeTTL.
	Nodes []Node
	// NodeTTL is the freshness bound used to filter the live feed.
	NodeTTL time.Duration
	// PendingRemovals is how many station documents the broker still owes a
	// removal, RemovalsWaiting how many quiet nodes are due one that had no
	// room in the ledger (they are offered again as the broker confirms what
	// is in flight — waiting is not a loss), RemovalsDropped the genuine
	// overflow: rows restored from a database whose backlog is longer than
	// the ledger bound.
	PendingRemovals int
	RemovalsWaiting int
	RemovalsDropped int
}

// routeReply is one TRACEROUTE route-discovery answer buffered for a
// waiting probe.
type routeReply struct {
	from  uint32
	route *pb.RouteDiscovery
}

// TracerouteHop is one node on the discovered route from us towards the
// destination (SNR is the link quality the hop reported, in dB).
type TracerouteHop struct {
	ID   string  `json:"id"`
	Name string  `json:"name,omitempty"`
	SNR  float32 `json:"snr,omitempty"`
}

// TracerouteResult is the outcome of one route probe: the hops from us
// to the answering node (excluding us), whether the destination itself
// answered (an intermediate reply means the destination was not reached,
// but the partial route still shows how far the probe got) and how long
// the probe window took.
type TracerouteResult struct {
	Hops      []TracerouteHop `json:"hops"`
	Reached   bool            `json:"reached"`
	ElapsedMS int64           `json:"elapsed_ms"`
}

// TraceProgress is a snapshot of a route probe that is still on the air:
// what the operator watches while the answer travels back. The probe
// refreshes it as it goes, so the panel can show that the station is
// still listening instead of leaving one frozen "tracing…" line on
// screen for the whole window.
type TraceProgress struct {
	// Destination is the probed node id (8 hex characters).
	Destination string
	// Started is when the first frame went out, Timeout the probe budget.
	Started time.Time
	Timeout time.Duration
	// Probes counts the frames sent so far (re-probes included) and
	// MaxProbes the ceiling: silence is retried on a lossy mesh.
	Probes    int
	MaxProbes int
	// Hops are the hops the answer carried (empty while nothing came
	// back). A route probe hears its answer in one frame — the nodes
	// along the way extend it — so this fills in when the radio answers.
	Hops []TracerouteHop
	// Reached reports that the destination itself answered.
	Reached bool
}

// Elapsed is how long the probe has been running.
func (p TraceProgress) Elapsed() time.Duration {
	if p.Started.IsZero() {
		return 0
	}
	return time.Since(p.Started)
}

// Message is one received or sent text message.
type Message struct {
	Direction string // rx | tx
	Sender    string // node id (8 hex) for rx; "" for tx
	// Recipient is the target node id (8 hex) of a tx direct message;
	// empty for broadcasts and rx rows.
	Recipient string
	Channel   string // channel name or "dm"
	Hops      int    // radio path length from the packet (0 = unknown/ours)
	Operator  string // admin username behind a tx (rx rows are empty)
	Text      string
	Status    string // delivery state (sent | delivered | failed)
	At        time.Time
}

// transportConn is the seam over github.com/kabili207/meshtastic-go's
// client transport: production dials a serial device; tests use the
// library's in-memory client API server.
type transportConn interface {
	Connect(ctx context.Context) error
	IsConnected() bool
	Stop() error
	State() *client.DeviceState
	SetPacketHandler(fn func(*pb.MeshPacket))
	Handle(kind proto.Message, fn func(proto.Message) error)
	SendToRadio(msg *pb.ToRadio) error
}

// clientAdapter adapts *client.Transport to transportConn.
type clientAdapter struct{ t *client.Transport }

func (a *clientAdapter) Connect(ctx context.Context) error { return a.t.Connect(ctx) }
func (a *clientAdapter) IsConnected() bool                 { return a.t.IsConnected() }
func (a *clientAdapter) Stop() error                       { return a.t.Stop() }
func (a *clientAdapter) State() *client.DeviceState        { return a.t.State() }
func (a *clientAdapter) SetPacketHandler(fn func(*pb.MeshPacket)) {
	a.t.SetPacketHandler(func(np transport.NetworkPacket) { fn(np.Packet) })
}
func (a *clientAdapter) Handle(kind proto.Message, fn func(proto.Message) error) {
	a.t.Handle(kind, client.MessageHandler(fn))
}
func (a *clientAdapter) SendToRadio(msg *pb.ToRadio) error { return a.t.SendToRadio(msg) }

// serialDial opens one serial device transport (the seam for tests).
var serialDial = func(ctx context.Context, cfg serial.Config) (transportConn, error) {
	t, err := serial.Connect(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &clientAdapter{t: t}, nil
}

// tcpDial opens one TCP transport to the node's WiFi API (the seam for
// tests).
var tcpDial = func(ctx context.Context, address string) (transportConn, error) {
	t, err := tcp.Connect(ctx, tcp.Config{Address: address})
	if err != nil {
		return nil, err
	}
	return &clientAdapter{t: t}, nil
}

// tcpAddress normalizes the configured TCP endpoint: a bare host or IP
// gets the Meshtastic default API port (4403) appended.
func tcpAddress(host string) (string, error) {
	addr := strings.TrimSpace(host)
	if addr == "" {
		return "", errors.New("meshtastic: no TCP host configured (set meshtastic.host)")
	}
	if !strings.Contains(addr, ":") {
		addr = net.JoinHostPort(addr, "4403")
	}
	return addr, nil
}

// deviceCandidates splits the device field into the ordered list of
// serial paths to try: a comma-separated list keeps the hub working
// when a node re-enumerates onto a different port number after a USB
// reset. Empty/whitespace entries are dropped.
func deviceCandidates(device string) []string {
	var out []string
	for _, p := range strings.Split(device, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// dialAttemptTimeout bounds ONE serial open attempt. The device library
// can block indefinitely in open() when the port is in a bad state (a
// hung MCU, a half-dead USB link): without the bound, a radio-silence
// reconnect would hang the whole reconnect loop forever and the node
// would never come back after a USB hiccup. Overridable in tests.
var dialAttemptTimeout = 15 * time.Second

// dialOne opens one transport with a bounded attempt window. The
// device library can block indefinitely in open() when the port is in a
// bad state (a hung MCU, a half-dead USB link): without the bound, a
// radio-silence reconnect would hang the whole reconnect loop forever
// and the node would never come back after a USB hiccup. The dial
// function runs in a goroutine because its context may be ignored on the
// blocking syscall path; the select below is the real timeout. A late
// connect (the syscall finished just after the deadline) is reaped and
// stopped so it never leaks a live transport. Overridable in tests.
func dialOne(ctx context.Context, dial func(context.Context) (transportConn, error)) (transportConn, error) {
	type res struct {
		t   transportConn
		err error
	}
	ch := make(chan res, 1)
	go func() {
		t, err := dial(ctx)
		ch <- res{t: t, err: err}
	}()
	select {
	case r := <-ch:
		return r.t, r.err
	case <-ctx.Done():
		go func() {
			select {
			case r := <-ch:
				if r.t != nil {
					_ = r.t.Stop()
				}
			case <-time.After(30 * time.Second):
			}
		}()
		return nil, ctx.Err()
	}
}

// Dial opens the device transport (the seam for tests). The serial
// transport tries the configured device paths in order and returns the
// first transport that connects; when none opens, the error names every
// candidate that failed. The tcp transport dials the configured host
// (the node's WiFi API).
var Dial = func(ctx context.Context, cfg Config) (transportConn, error) {
	switch strings.ToLower(strings.TrimSpace(cfg.Transport)) {
	case "", "serial":
		paths := deviceCandidates(cfg.Device)
		if len(paths) == 0 {
			return nil, errors.New("meshtastic: no serial device path configured")
		}
		var lastErr error
		for _, path := range paths {
			// Capture the (test-)swappable seam before the goroutine
			// starts: the load is ordered with the caller and can never
			// race with a later swap.
			sd := serialDial
			dctx, cancel := context.WithTimeout(ctx, dialAttemptTimeout)
			conn, err := dialOne(dctx, func(c context.Context) (transportConn, error) {
				return sd(c, serial.Config{Port: path, BaudRate: cfg.Baud})
			})
			cancel()
			if err == nil {
				return conn, nil
			}
			lastErr = err
		}
		if len(paths) == 1 {
			return nil, lastErr
		}
		return nil, fmt.Errorf("meshtastic: none of the device candidates connected (%s): %w",
			strings.Join(paths, ", "), lastErr)
	case "tcp":
		addr, err := tcpAddress(cfg.Host)
		if err != nil {
			return nil, err
		}
		td := tcpDial
		dctx, cancel := context.WithTimeout(ctx, dialAttemptTimeout)
		defer cancel()
		conn, err := dialOne(dctx, func(c context.Context) (transportConn, error) {
			return td(c, addr)
		})
		if err != nil {
			return nil, fmt.Errorf("meshtastic: TCP dial to %s: %w", addr, err)
		}
		return conn, nil
	default:
		return nil, fmt.Errorf("meshtastic: unknown transport %q (want \"serial\" or \"tcp\")", cfg.Transport)
	}
}

// Hub owns the serial connection to the Meshtastic node and the shared
// state: self info, neighbours and the message history. The source
// plugin runs Run; the outbound action sends through SendChannelMessage
// or SendContactMessage; the web admin page reads Snapshot.
type Hub struct {
	cfg    Config
	logger *slog.Logger
	// now is the clock the periodic work reads (nil = time.Now); the tests
	// inject it to drive the daily beacon anchor.
	now func() time.Time

	mu        sync.Mutex
	conn      transportConn
	connected bool
	lastErr   error
	self      *SelfInfo
	nodes     map[string]*Node
	channels  []string
	// cadence is the live cadence of the periodic lines (seeded from the
	// configuration, changed by the Config page). Guarded by mu.
	cadence Cadence
	// traces holds the route probes in flight, keyed by destination node
	// number: the panel reads it to report progress while the answer is
	// on the air. Guarded by traceMu.
	traces  map[uint32]*TraceProgress
	traceMu sync.Mutex
	// channelKeys holds the device channel keys by index (parallel to
	// channels): the Help page renders the installation's own channel for
	// users to join. Guarded by mu.
	channelKeys [][]byte
	recent      []Message

	recorder    Recorder
	stationSink func(ctx context.Context, topic string, retained bool, payload []byte) error
	messageSink func(ctx context.Context, topic string, retained bool, payload []byte) error
	// senderGate resolves the direct-message sender's node id onto the
	// directory username that registered it; empty = not registered (the
	// message stays off the alarm pipeline).
	senderGate func(id string) string
	eventSink  func(ctx context.Context, topic string, retained bool, payload []byte) error
	cli        *radiocli.Bot

	sendMu    sync.Mutex
	pending   map[uint32]*pendingSend // by device-assigned packet id
	echoQueue []*pendingSend          // sent but the echo (id) not seen yet

	// nodeInfoAsked rate-limits the NodeInfo refresh per peer (the peer's
	// firmware suppresses a reply to the same requester for hours). Nil
	// until the first refusal that needs a key refresh.
	nodeInfoAsked map[uint32]time.Time

	// keyRetries holds the direct messages refused for a missing peer key,
	// per peer and oldest first: the NodeInfo refresh is already on its way
	// and those messages go out again once that key lands (see retryAfterKey).
	// One peer can owe several of them (the operator sent two texts), so this
	// is a queue, not a single slot — and it is BOUNDED in both directions
	// (see keyRetryPerPeer/keyRetryTotal): a peer whose key never arrives must
	// not grow the hub's memory. Nil until the first such refusal.
	keyRetries map[uint32][]*keyRetry
	// keyRetryPerPeerMax / keyRetryTotalMax override those bounds (tests).
	keyRetryPerPeerMax int
	keyRetryTotalMax   int
	// peerKeysMax overrides the key-mark cap, peerKeysSkipped counts the marks
	// the cap refused since the last sweep (reported there, not per event).
	peerKeysMax     int
	peerKeysSkipped int
	// nodeInfoAskedMax overrides the refresh cooldown cap.
	nodeInfoAskedMax int

	// userNodes supplies the node numbers registered in the user directory
	// (nil = nothing to pin); pinNext arms the next pin pass.
	userNodes func() ([]uint32, error)
	pinNext   time.Time

	// favoriteStore is the ledger of the favorites THIS station set on the
	// radio; pinned mirrors it, pinnedLoaded marks whether the store was
	// read (a failed read stays unloaded, and the pass then touches
	// nothing — see pinUserNodes).
	favoriteStore FavoriteStore
	pinned        map[uint32]bool
	pinnedLoaded  bool
	// favoritesDirty marks ledger changes the store has not accepted yet
	// (favoritesRev guards against clearing the flag for a snapshot a newer
	// change has already made obsolete): a write that failed must not be
	// forgotten, or a restart loses which stars are the station's own.
	favoritesDirty bool
	favoritesRev   uint64
	// favoriteSaveMu serializes the WHOLE flush (snapshot + write): two flushes
	// may overlap (the pass and a device answer), and without it the older
	// snapshot could land last and overwrite the newer ledger — with the dirty
	// flag already cleared, so nothing would notice or repair it.
	favoriteSaveMu sync.Mutex

	// radioFavorites is what the DEVICE said about its own node table when
	// it last handed it over (node -> starred). Owning a star is no proof
	// the radio still carries it: a node-db reset or a swapped device
	// leaves our ledger intact and the favorites gone, so the pass
	// re-asserts an owned node unless THIS map confirms it.
	radioFavorites map[uint32]bool

	// peerKeys records which nodes announced a PUBLIC KEY — the one thing a
	// radio needs to encrypt a direct message to them (it refuses the frame
	// without it, see needsNodeInfoRefresh). It is the LATEST evidence: the
	// device's own node table at the handshake, then every NodeInfo heard.
	// It never claims more than that — a node whose key changed has to
	// announce again — and it is memory-only (radio state, not directory).
	peerKeys map[uint32]bool

	// routeWaiters buffers TRACEROUTE route replies while probes wait,
	// keyed by the replying node (the destination answers with the full
	// route; intermediate nodes may answer when it is out of reach).
	routeWaiters map[uint32][]chan routeReply

	// lastRadio is the unix-nano time of the most recent FromRadio
	// message; the session is declared dead after cfg.SilenceTimeout
	// (defaultSilenceTimeout) of total radio silence.
	lastRadio atomic.Int64

	// reconnect asks the session goroutine to rebuild a link that died
	// under a send: an operator's message must not wait out the silence
	// watchdog while the socket is already gone. Buffered — one pending
	// rebuild is enough.
	reconnect chan struct{}
	// heartbeatEvery is how often the session pings the device
	// (defaultHeartbeatInterval when unset; the tests shorten it).
	heartbeatEvery time.Duration

	// stationMu orders the retained station feed: a position publish and the
	// removal of a document for the same node must never interleave, or a node
	// that just spoke would be left without its document.
	stationMu sync.Mutex
	// tombstones holds the removals the broker has not confirmed yet, keyed by
	// node id (independent of the node directory, see nodeTombstone).
	tombstones map[string]*nodeTombstone
	// feedQueue hands the station-feed operations (a retained document, or its
	// removal) to the feed worker. Bounded and never awaited: a broker that
	// does not answer must not make the session loop — heartbeat, silence
	// watchdog, message retry, shutdown — wait for it.
	feedQueue chan feedJob
	// removalsDropped counts the removals a RESTORE could not take over
	// because the ledger bound was already reached (the only genuine overflow:
	// what a run queues is bounded, what it defers is not lost).
	removalsDropped int
	// removalsWaiting is how many quiet nodes are due a removal that had no
	// room in the ledger in the last pass (the backlog gauge, see
	// StationRemovals).
	removalsWaiting int
	// pendingRemovalsMax overrides the ledger bound (tests shrink it).
	pendingRemovalsMax int
	// tombstoneRetry paces a removal the broker refused
	// (defaultTombstoneRetry when unset; the tests shorten it).
	tombstoneRetry time.Duration

	// nodeStore persists the heard-node directory across restarts
	// (optional; nil = directory lives in RAM only).
	nodeStore   NodeStore
	nodesDirty  bool
	lastPersist time.Time

	// startedAt anchors the beacon uptime; emcomNext schedules the next
	// presence beacon on the emcom channel; hazardsNext schedules the
	// next active-hazard digest (same channel).
	startedAt   time.Time
	emcomNext   time.Time
	hazardsNext time.Time

	// hazardSummary supplies the periodic status counters (set by the
	// application from the dispatch state mirror); nil disables them.
	hazardSummary func() HazardSummary

	// cmds remembers executed radio commands by sender + packet
	// identity; guarded by mu. A redelivered packet is answered with
	// the previous result instead of re-executing the command.
	cmds map[string]*cmdRecord

	// eventAcceptor accepts a marshalled COMMAND event (/alert and
	// into the LOCAL pipeline and reports how it was accepted
	// (durable inbox, emergency RAM fallback or rejection); guarded by
	// mu. nil falls back to the event sink (tests, legacy wiring).
	eventAcceptor func(payload []byte) dispatch.Acceptance

	// banners queues low-priority automatic banner replies; guarded by
	// mu. They drain one per drain call (inbound packet / session
	// tick), so direct alarm and command replies always go first.
	banners    []bannerReply
	bannerLast map[string]time.Time // sender -> last banner answer
	bannerNext time.Time            // global pacing: next allowed banner

	// replyLast bounds ALL automatic replies per sender (command
	// answers, replays, /help, denials, banners); guarded by mu.
	replyLast map[string]replyWindow

	// eventTimes resolves the durable command registry for one command
	// key: the lifecycle anchor (effective / expires) AND the recorded
	// result of a previously accepted command; guarded by mu. It is the
	// restart-proof half of the registry: the durable anchor row is
	// written transactionally with the inbox acceptance, so a re-issued
	// command recovers the original TTL and — when a result is stored —
	// replays it instead of executing the job again.
	eventTimes func(ctx context.Context, key string) (eff, exp time.Time, result string, ok bool)
}

// NewHub validates the config and builds the hub.
func NewHub(cfg Config, logger *slog.Logger) (*Hub, error) {
	if cfg.Baud == 0 {
		cfg.Baud = DefaultBaud
	}
	if cfg.NodeTTL <= 0 {
		cfg.NodeTTL = DefaultNodeTTL
	}
	if cfg.AckSettleTimeout <= 0 {
		cfg.AckSettleTimeout = defaultAckSettleTimeout
	}
	cfg.EmcomAt = strings.TrimSpace(cfg.EmcomAt)
	if cfg.EmcomAt != "" {
		if !schedule.Valid(cfg.EmcomAt) {
			return nil, fmt.Errorf("meshtastic: emcom_at must be a clock time like %q, got %q", schedule.AtDefault, cfg.EmcomAt)
		}
		if cfg.EmcomInterval > 0 {
			return nil, errors.New("meshtastic: set emcom_at (daily beacon) or emcom_interval (spacing), not both")
		}
	}
	if cfg.EmcomChannel < 0 || cfg.EmcomChannel > 7 {
		return nil, errors.New("meshtastic: emcom channel must be 0-7")
	}
	if cfg.WatchChannel < 0 || cfg.WatchChannel > 7 {
		return nil, errors.New("meshtastic: watch channel must be 0-7")
	}
	if cfg.EmcomHazardsInterval < 0 {
		return nil, errors.New("meshtastic: emcom hazards interval must not be negative (0 disables)")
	}
	if cfg.BannerMinInterval <= 0 {
		cfg.BannerMinInterval = bannerMinInterval
	}
	if cfg.BannerGlobalInterval <= 0 {
		cfg.BannerGlobalInterval = bannerGlobalInterval
	}
	if cfg.CmdRetryCooldown <= 0 {
		cfg.CmdRetryCooldown = cmdRetryCooldownDefault
	}
	if cfg.ReplyBurst <= 0 {
		cfg.ReplyBurst = replyBurstDefault
	}
	if cfg.ReplyBurstWindow <= 0 {
		cfg.ReplyBurstWindow = replyBurstWindowDefault
	}
	if !cfg.Enabled {
		// Disabled hub: no serial device needed; the source plugin skips
		// Run and the admin page shows the device as disconnected.
		return &Hub{cfg: cfg, logger: logger, cadence: cadenceOf(cfg), nodes: make(map[string]*Node),
			tombstones: make(map[string]*nodeTombstone),
			feedQueue:  make(chan feedJob, stationFeedQueueLen),
			pending:    make(map[uint32]*pendingSend), cmds: make(map[string]*cmdRecord),
			routeWaiters: make(map[uint32][]chan routeReply),
			traces:       make(map[uint32]*TraceProgress),
			bannerLast:   make(map[string]time.Time), replyLast: make(map[string]replyWindow),
			reconnect: make(chan struct{}, 1),
			startedAt: time.Now()}, nil
	}
	if cfg.Device == "" {
		return nil, errors.New("meshtastic: device path is required")
	}
	return &Hub{
		cfg:          cfg,
		logger:       logger,
		cadence:      cadenceOf(cfg),
		nodes:        make(map[string]*Node),
		tombstones:   make(map[string]*nodeTombstone),
		feedQueue:    make(chan feedJob, stationFeedQueueLen),
		pending:      make(map[uint32]*pendingSend),
		cmds:         make(map[string]*cmdRecord),
		routeWaiters: make(map[uint32][]chan routeReply),
		traces:       make(map[uint32]*TraceProgress),
		bannerLast:   make(map[string]time.Time),
		replyLast:    make(map[string]replyWindow),
		reconnect:    make(chan struct{}, 1),
		startedAt:    time.Now(),
	}, nil
}

// Enabled reports whether the meshtastic integration is configured.
func (h *Hub) Enabled() bool { return h.cfg.Enabled }

// ActiveHazard is the minimal summary of one active hazard for the
// periodic channel digest.
type ActiveHazard struct {
	Headline    string
	Description string
	EffectiveAt *time.Time
	ExpiresAt   *time.Time
}

// SetHazardSummarySource attaches the counter supplier for the periodic
// emcom-channel status line (nil disables it). The callback must return
// the current counts — the digest posts NUMBERS, never the hazards.
func (h *Hub) SetHazardSummarySource(fn func() HazardSummary) {
	h.mu.Lock()
	h.hazardSummary = fn
	h.mu.Unlock()
}

// EmcomChannel reports the configured presence-beacon channel index
// (1-7; 0 = beacon disabled). The admin message history uses it for the
// dedicated emcom-channel tab.
func (h *Hub) EmcomChannel() int { return h.cfg.EmcomChannel }

// NodeHops reports the radio path length of the last packet heard from
// addr (0 = direct), or -1 when the node is unknown.
func (h *Hub) NodeHops(addr string) int {
	id := normalizeMeshID(addr)
	h.mu.Lock()
	defer h.mu.Unlock()
	if n := h.nodes[id]; n != nil {
		return n.Hops
	}
	return -1
}

// NodeLastSeen reports when addr's last packet was heard (zero time
// when the node is unknown).
func (h *Hub) NodeLastSeen(addr string) time.Time {
	id := normalizeMeshID(addr)
	h.mu.Lock()
	defer h.mu.Unlock()
	if n := h.nodes[id]; n != nil {
		return n.LastSeen
	}
	return time.Time{}
}

// ResolveNode maps a node query — an 8-hex id, a name or a short name,
// case-insensitive — onto the canonical 8-hex id. Unknown queries
// return "". Resolution order: id, exact name, exact short name, name
// prefix, then substring matches that are unambiguous (a single
// candidate) — an ambiguous query stays unresolved instead of sending a
// message to a random node.
func (h *Hub) ResolveNode(q string) string {
	q = strings.TrimSpace(strings.TrimPrefix(q, "!"))
	if q == "" {
		return ""
	}
	ql := strings.ToLower(q)
	h.mu.Lock()
	defer h.mu.Unlock()
	if id := normalizeMeshID(q); id != "" {
		if _, ok := h.nodes[id]; ok {
			return id
		}
	}
	var nameHit, shortHit, prefixHit string
	for id, n := range h.nodes {
		nl := strings.ToLower(n.Name)
		if nl == ql {
			if nameHit == "" {
				nameHit = id
			}
			continue
		}
		if strings.ToLower(n.Short) == ql {
			if shortHit == "" {
				shortHit = id
			}
			continue
		}
		if prefixHit == "" && strings.HasPrefix(nl, ql) {
			prefixHit = id
		}
	}
	if nameHit != "" {
		return nameHit
	}
	if shortHit != "" {
		return shortHit
	}
	if prefixHit != "" {
		return prefixHit
	}
	// Substring fallback: only when exactly one node matches, so a
	// sloppy query can never route a message to an arbitrary node. A
	// query ambiguous on names stops here — a unique short-name hit
	// would still be a guess.
	count, hit := 0, ""
	for id, n := range h.nodes {
		if n.Name != "" && strings.Contains(strings.ToLower(n.Name), ql) {
			count++
			hit = id
		}
	}
	if count == 1 {
		return hit
	}
	if count > 1 {
		return ""
	}
	count, hit = 0, ""
	for id, n := range h.nodes {
		if n.Short != "" && strings.Contains(strings.ToLower(n.Short), ql) {
			count++
			hit = id
		}
	}
	if count == 1 {
		return hit
	}
	return ""
}

// TabChannel is the channel index that owns the extra message-history
// tab: the read-only watch channel when configured, otherwise the emcom
// channel. The tab is purely a view — it never transmits.
func (h *Hub) TabChannel() int {
	if h.cfg.WatchChannel > 0 {
		return h.cfg.WatchChannel
	}
	return h.cfg.EmcomChannel
}

// Connected reports whether the serial session is currently up.
func (h *Hub) Connected() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.connected
}

// SetRecorder attaches the durable message history store (optional).
func (h *Hub) SetRecorder(r Recorder) { h.mu.Lock(); h.recorder = r; h.mu.Unlock() }

// SetNodeStore attaches the persistent heard-node directory (optional):
// the hub restores it at startup and rewrites it whenever a node is
// learned or updated.
func (h *Hub) SetNodeStore(store NodeStore) { h.mu.Lock(); h.nodeStore = store; h.mu.Unlock() }

// SetStationSink attaches the MQTT station publisher (optional): every
// heard node becomes a retained document under meshtastic/stations/<id>,
// removed once its NodeTTL expires (a removal the broker refused is retried,
// and a node heard again publishes its document again).
func (h *Hub) SetStationSink(fn func(ctx context.Context, topic string, retained bool, payload []byte) error) {
	h.mu.Lock()
	h.stationSink = fn
	h.mu.Unlock()
}

// SetMessageSink attaches the MQTT message feed publisher (optional):
// every rx/tx text message is published as a non-retained JSON document
// under meshtastic/messages.
func (h *Hub) SetMessageSink(fn func(ctx context.Context, topic string, retained bool, payload []byte) error) {
	h.mu.Lock()
	h.messageSink = fn
	h.mu.Unlock()
}

// SetSenderGate installs the direct-message sender resolution. The gate
// receives the sender's lowercase 8-hex node id and returns the
// directory username that registered it ("" = nobody: no mesh message
// becomes a hazard event).
func (h *Hub) SetSenderGate(fn func(id string) string) {
	h.mu.Lock()
	h.senderGate = fn
	h.mu.Unlock()
}

// SetEventSink attaches the /events publisher (optional): routed direct
// messages become canonical event documents on the events stream.
func (h *Hub) SetEventSink(fn func(ctx context.Context, topic string, retained bool, payload []byte) error) {
	h.mu.Lock()
	h.eventSink = fn
	h.mu.Unlock()
}

// SetEventAcceptor installs the LOCAL acceptance path for command
// events (/alert): the callback enqueues a marshalled event
// into the local pipeline and reports how it was accepted (durable
// inbox, emergency RAM fallback or rejection). Without it the hub falls
// back to the event sink (tests, legacy wiring).
func (h *Hub) SetEventAcceptor(fn func(payload []byte) dispatch.Acceptance) {
	h.mu.Lock()
	h.eventAcceptor = fn
	h.mu.Unlock()
}

// SetEventTimesResolver installs the durable command-registry lookup
// for command events: it returns the stored effective/expires times
// and the recorded result of an already-accepted command. A stored
// result proves the job executed before — the retransmission replays
// it; times without a result make a re-issued command byte-identical
// so the store deduplicates it (one delivery across restarts). nil
// disables the lookup (tests, legacy wiring).
func (h *Hub) SetEventTimesResolver(fn func(ctx context.Context, key string) (eff, exp time.Time, result string, ok bool)) {
	h.mu.Lock()
	h.eventTimes = fn
	h.mu.Unlock()
}

// SetCLI attaches the shared radio-command interpreter (optional): a
// direct message that parses as a command is answered in-band and
// stays off the alarm pipeline.
func (h *Hub) SetCLI(b *radiocli.Bot) { h.mu.Lock(); h.cli = b; h.mu.Unlock() }

// Run dials the device, performs the client handshake and pumps incoming
// messages until ctx is cancelled. It reconnects with bounded backoff so
// transient USB hiccups never kill the plugin permanently.
func (h *Hub) Run(ctx context.Context) error {
	h.restoreNodes(ctx)
	// The station feed runs NEXT TO the session, not inside it.
	go h.runStationFeed(ctx)
	backoff := time.Second
	for {
		if ctx.Err() != nil {
			return nil
		}
		err := h.runSession(ctx)
		if ctx.Err() != nil {
			return nil
		}
		h.mu.Lock()
		h.connected = false
		h.lastErr = err
		h.conn = nil
		h.mu.Unlock()
		if errors.Is(err, errReconnectRequested) {
			// The link is known dead (a failed send or heartbeat): rebuild
			// it at once and quietly instead of treating it as an outage.
			h.logger.Info("meshtastic: rebuilding the device link")
			backoff = time.Second
		} else {
			h.logger.Warn("meshtastic: device session ended, reconnecting", "error", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

// runSession runs one device session: dial, perform the handshake (the
// library requests the device config), pull the device state (self, node
// list, channels) into the hub, and pump until the context ends or the
// transport dies.
func (h *Hub) runSession(ctx context.Context) error {
	conn, err := Dial(ctx, h.cfg)
	if err != nil {
		return err
	}
	defer conn.Stop()
	h.mu.Lock()
	h.conn = conn
	h.mu.Unlock()

	conn.SetPacketHandler(h.handlePacket)
	// The device answers every client send with a queueStatus carrying the
	// assigned mesh packet id — the key that later pairs the recipient's
	// ROUTING_APP acknowledgment with our send.
	conn.Handle(&pb.QueueStatus{}, func(msg proto.Message) error {
		h.lastRadio.Store(time.Now().UnixNano())
		if qs, ok := msg.(*pb.QueueStatus); ok {
			h.pairQueueStatus(qs.GetMeshPacketId(), pb.Routing_Error(qs.GetRes()))
		}
		return nil
	})
	// The production dial (serial.Connect) already ran the handshake and
	// returns a connected transport; in-memory test transports need the
	// handshake here. A second Connect on an already-complete transport
	// would block forever (its state is already Complete, so the
	// config-complete signal never fires again) — never call it twice.
	if !conn.IsConnected() {
		if err := conn.Connect(ctx); err != nil {
			return err
		}
	}
	h.mu.Lock()
	h.connected = true
	h.mu.Unlock()
	h.lastRadio.Store(time.Now().UnixNano())
	// A reconnect request filed while this session was being dialled
	// belonged to the previous link: drop it, so it cannot tear down the
	// fresh session.
	select {
	case <-h.reconnect:
	default:
	}
	// The presence beacon is armed by the FIRST session only (later ones
	// are device reconnects and must not reschedule the cadence — it
	// belongs to the process, not the session). A configured daily anchor
	// arms the next clock time, so a restart never beacons ahead of
	// schedule; the spacing mode arms now, so the first maintenance tick
	// sends the beacon right away.
	if h.cfg.EmcomChannel > 0 && h.cfg.EmcomIdentity != "" &&
		(h.cadence.BeaconAt != "" || h.cadence.BeaconInterval > 0) {
		h.mu.Lock()
		if h.emcomNext.IsZero() {
			h.emcomNext = h.beaconArm(h.clock())
		}
		h.mu.Unlock()
	}
	// The active-hazard digest follows the same rule: the first session
	// schedules it for now so a restart resyncs the mesh promptly; the
	// hourly cadence survives device reconnects.
	if h.cfg.EmcomChannel > 0 && h.cadence.HazardsInterval > 0 {
		h.mu.Lock()
		if h.hazardsNext.IsZero() {
			h.hazardsNext = time.Now()
		}
		h.mu.Unlock()
	}
	h.populateFromState(conn.State())
	// The pin pass runs right after the handshake (now that our own node
	// number is known and the device has stated which favorites it still
	// carries) and then on its own cadence. It is FORCED here: a session is
	// the only moment the radio's node table is read, and a node-db reset or
	// a swapped device must be repaired without waiting out the interval.
	h.pinUserNodes(true)

	if h.logger != nil {
		h.mu.Lock()
		self := h.self
		h.mu.Unlock()
		if self != nil {
			h.logger.Info("meshtastic: device connected", "id", self.ID,
				"name", self.LongName, "firmware", self.Firmware, "hw", self.HwModel)
		} else {
			h.logger.Info("meshtastic: device connected")
		}
	}

	// Maintenance: expire unheard neighbours, keep the link warm and watch
	// the transport.
	ticker := time.NewTicker(dmRetryInterval)
	defer ticker.Stop()
	lastHeartbeat := time.Now()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-h.reconnect:
			return errReconnectRequested
		case <-ticker.C:
			if !conn.IsConnected() {
				return errors.New("device disconnected")
			}
			// The heartbeat is the only liveness probe that does not
			// depend on the mesh being busy. A write that fails means the
			// peer closed the socket, so end the session and let the
			// reconnect loop rebuild it instead of waiting out the
			// silence watchdog.
			every := h.heartbeatEvery
			if every <= 0 {
				every = defaultHeartbeatInterval
			}
			if time.Since(lastHeartbeat) >= every {
				if err := conn.SendToRadio(heartbeatFrame()); err != nil {
					return fmt.Errorf("heartbeat write failed: %w", err)
				}
				lastHeartbeat = time.Now()
				if h.logger != nil {
					h.logger.Debug("meshtastic: heartbeat sent")
				}
			}
			// A healthy node emits FromRadio traffic (queueStatus,
			// telemetry) constantly; total silence means the serial
			// session stalled without an error — reconnect.
			timeout := h.cfg.SilenceTimeout
			if timeout <= 0 {
				timeout = defaultSilenceTimeout
			}
			if since := time.Since(time.Unix(0, h.lastRadio.Load())); since > timeout {
				h.logger.Warn("meshtastic: device radio silent, reconnecting", "silent_for", since)
				return errors.New("device radio silent")
			}
			h.expireNodes()
			h.pinUserNodes(false)
			h.retryPending(time.Now())
			h.expireKeyRetries(time.Now())
			h.sweepPeerState(time.Now())
			h.persistNodes()
			h.beaconTick()
			h.hazardsTick()
			h.drainBanner()
		}
	}
}

// Cadence is the live cadence of the two periodic mesh lines: the
// presence beacon (a daily clock anchor or a spacing) and the hazard
// status line. It starts from the configuration and the Config page can
// change it at runtime, so an operator moves the morning beacon without a
// restart.
type Cadence struct {
	// BeaconAt is the daily clock time ("06:00"); "" = the spacing applies.
	BeaconAt string
	// BeaconInterval is the presence spacing (0 = anchor mode, or off when
	// the anchor is empty too).
	BeaconInterval time.Duration
	// HazardsInterval is the status line spacing (0 = off).
	HazardsInterval time.Duration
}

// cadenceOf seeds the live cadence from the configuration.
func cadenceOf(cfg Config) Cadence {
	return Cadence{
		BeaconAt:        cfg.EmcomAt,
		BeaconInterval:  cfg.EmcomInterval,
		HazardsInterval: cfg.EmcomHazardsInterval,
	}
}

// SetCadence replaces the live cadence and re-arms the beacon, so a new
// anchor takes effect now instead of at the previously scheduled hour.
func (h *Hub) SetCadence(c Cadence) {
	now := h.clock()
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.cadence == c {
		return
	}
	h.cadence = c
	if h.cfg.EmcomChannel > 0 && h.cfg.EmcomIdentity != "" && (c.BeaconAt != "" || c.BeaconInterval > 0) {
		h.emcomNext = h.beaconArm(now)
	}
	if c.HazardsInterval > 0 && h.hazardsNext.IsZero() {
		h.hazardsNext = now
	}
}

// ReportCadence returns the live cadence.
func (h *Hub) ReportCadence() Cadence {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.cadence
}

// StatusFacts is what the station says it does over Meshtastic: the two
// alert paths, the two periodic lines and where the group channel sits.
// The Help page renders it — the same shape as the Discord card, so the two
// surfaces describe themselves alike.
type StatusFacts struct {
	// Channel is the group channel index (0 = none: no channel line and no
	// commands on the air).
	Channel int
	// ChannelAlerts reports whether routed alerts go on the group channel.
	ChannelAlerts bool
	// DMAlerts reports whether routed alerts go to registered node ids.
	DMAlerts bool
	// HazardsInterval is the periodic status line spacing (0 = off).
	HazardsInterval time.Duration
	// BeaconAt is the daily presence anchor ("" = the spacing applies).
	BeaconAt string
	// BeaconInterval is the presence spacing (0 = anchor mode, or off when
	// the anchor is empty too).
	BeaconInterval time.Duration
}

// Status reports what the station sends over Meshtastic right now (the
// live cadence, not the one the process started with).
func (h *Hub) Status() StatusFacts {
	c := h.ReportCadence()
	return StatusFacts{
		Channel:         h.cfg.EmcomChannel,
		ChannelAlerts:   ChannelAlerts(),
		DMAlerts:        DMAlerts(),
		HazardsInterval: c.HazardsInterval,
		BeaconAt:        c.BeaconAt,
		BeaconInterval:  c.BeaconInterval,
	}
}

// clock reads the hub's clock (time.Now unless a test injected one).
func (h *Hub) clock() time.Time {
	if h.now != nil {
		return h.now()
	}
	return time.Now()
}

// beaconArm returns when the beacon becomes due: the daily anchor when one
// is configured (so a restart never beacons ahead of the clock), otherwise
// now — the spacing mode beacons as soon as the first session is up. The
// caller holds h.mu (it reads the cadence straight off the hub).
func (h *Hub) beaconArm(now time.Time) time.Time {
	if h.cadence.BeaconAt != "" {
		return schedule.NextDaily(now, h.cadence.BeaconAt)
	}
	return now
}

// beaconTick sends the periodic presence beacon on the emcom channel
// (identity + uptime + node count). The PRIMARY channel is never used.
func (h *Hub) beaconTick() {
	h.mu.Lock()
	next := h.emcomNext
	h.mu.Unlock()
	now := h.clock()
	if next.IsZero() || now.Before(next) {
		return
	}
	h.mu.Lock()
	c := h.cadence
	if c.BeaconAt != "" {
		// Re-arm for tomorrow: a slow or failed send must never fire the
		// beacon twice in one day.
		h.emcomNext = schedule.NextDailyAfter(now, c.BeaconAt)
	} else {
		h.emcomNext = now.Add(c.BeaconInterval)
	}
	nodes := len(h.nodes)
	up := now.Sub(h.startedAt).Round(time.Minute)
	h.mu.Unlock()
	text := fmt.Sprintf("%s | up %s | nodes %d", h.cfg.EmcomIdentity, up, nodes)
	if r := []rune(text); len(r) > meshTextMaxRunes {
		text = string(r[:meshTextMaxRunes])
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := h.SendChannelText(ctx, h.cfg.EmcomChannel, text, "system"); err != nil && h.logger != nil {
		h.logger.Warn("meshtastic: emcom beacon failed", "error", err)
	}
}

// HazardSummary is what the periodic emcom-channel line reports: how
// many active hazards are severe or higher (each of them already went
// out as its own alert) and how many sit below that bar. The line
// carries COUNTERS, never a list — a list would put the same hazards on
// the air a second time and drown the channel.
type HazardSummary struct {
	Severe int
	Other  int
}

// Total is the number of active hazards the summary covers.
func (s HazardSummary) Total() int { return s.Severe + s.Other }

// Alarming reports whether the status line is worth sending at all: the
// station speaks up on the channel only for severe-and-above hazards.
func (s HazardSummary) Alarming() bool { return s.Severe > 0 }

// hazardsTick sends the periodic status line on the emcom channel: the
// hazard counters plus the public site where the details are. It goes out
// ONLY while something sits at severe or higher — the level that already
// rang the devices — because an hourly "0 severe" line would train the
// channel to ignore the one that matters. The line is disabled when the
// emcom channel is off, the interval is 0 or no source is attached.
func (h *Hub) hazardsTick() {
	if h.cfg.EmcomChannel <= 0 || h.ReportCadence().HazardsInterval <= 0 {
		return
	}
	h.mu.Lock()
	next := h.hazardsNext
	src := h.hazardSummary
	h.mu.Unlock()
	if next.IsZero() || time.Now().Before(next) || src == nil {
		return
	}
	// Advance the cadence BEFORE reading the source: a slow callback or
	// a failed send must never fire the line twice per window.
	h.mu.Lock()
	h.hazardsNext = time.Now().Add(h.cadence.HazardsInterval)
	h.mu.Unlock()

	sum := src()
	if !sum.Alarming() {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	text := h.hazardSummaryLine(sum)
	if err := h.SendChannelText(ctx, h.cfg.EmcomChannel, text, "system"); err != nil && h.logger != nil {
		h.logger.Warn("meshtastic: hazard status line failed", "error", err)
	}
}

// hazardListHint tells listeners how to ask for the full list: the
// periodic line carries counters only, so the way to get the details
// (the public /hazard command, answered as a direct message) rides
// along whenever the channel budget still has room.
const hazardListHint = "DM me /hazard for the list"

// hazardSummaryLine renders the periodic channel status: the counters
// ("1 severe or more, 12 other active") and, when one is configured, the
// public site holding the details. "0 severe" is worth posting — it
// tells the channel that nothing above the alert bar is running. The
// /hazard hint is appended only while it still fits one channel message.
func (h *Hub) hazardSummaryLine(sum HazardSummary) string {
	line := fmt.Sprintf("WarnFlux: %d severe or more, %d other active", sum.Severe, sum.Other)
	if site := strings.TrimSpace(h.cfg.EmcomSite); site != "" {
		line += " — " + site
	}
	if hint := " · " + hazardListHint; len([]rune(line))+len([]rune(hint)) <= meshTextMaxRunes {
		line += hint
	}
	return line
}

// hazardDigestLine renders one hazard as a single channel line: the Zulu
// validity window, the headline and as much of the description as fits
// the Meshtastic text limit.
func hazardDigestLine(hz ActiveHazard) string {
	var b strings.Builder
	b.WriteString(zuluShort(hz.EffectiveAt))
	b.WriteString("-")
	b.WriteString(zuluShort(hz.ExpiresAt))
	headline := strings.TrimSpace(hz.Headline)
	if headline == "" {
		headline = "?"
	}
	b.WriteString(" ")
	b.WriteString(headline)
	if desc := strings.TrimSpace(hz.Description); desc != "" {
		b.WriteString(" — ")
		b.WriteString(desc)
	}
	r := []rune(b.String())
	if len(r) > meshTextMaxRunes {
		return string(r[:meshTextMaxRunes])
	}
	return string(r)
}

// HazardDigestLine is the shared one-line formatter for active hazards:
// the public /hazard radio command answers with these lines (the
// periodic channel line carries counters only).
func HazardDigestLine(hz ActiveHazard) string {
	return hazardDigestLine(hz)
}

// zuluShort renders an optional time as a compact Zulu timestamp
// (UTC date + time); a missing timestamp renders as "?".
func zuluShort(t *time.Time) string {
	if t == nil {
		return "?"
	}
	return t.UTC().Format("2006-01-02 15:04Z")
}

// restoreNodes loads the persisted node directory into the hub (once, at
// startup, before the first session).
func (h *Hub) restoreNodes(ctx context.Context) {
	h.mu.Lock()
	store := h.nodeStore
	h.mu.Unlock()
	if store == nil {
		return
	}
	rctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	nodes, err := store.LoadMeshtasticNodes(rctx)
	if err != nil {
		h.logger.Warn("meshtastic: node directory load failed", "error", err)
		return
	}
	h.mu.Lock()
	if h.tombstones == nil {
		h.tombstones = make(map[string]*nodeTombstone)
	}
	pending := 0
	for _, n := range nodes {
		if n.PendingRemoval {
			// The removal never reached the broker before the station stopped:
			// it is owed again. This entry is the only thing that remembers the
			// node when the directory has dropped it long ago.
			//
			// The reference is the observation the removal was queued AGAINST,
			// never the node's current LastSeen: the node may have been heard
			// again (and its document published again) before the save, and a
			// removal must never be applied to activity that came after it.
			since := n.PendingSince
			if since.IsZero() {
				since = n.LastSeen
			}
			if h.tombstones[n.ID] == nil {
				if h.pendingRemovalsLocked() >= maxPendingStationRemovals {
					// A backlog written by an older station can be longer than
					// what this one works off: it loads only its bound and
					// COUNTS the rest instead of carrying it forever.
					h.removalsDropped++
				} else if h.queueRemovalLocked(n.ID, since) {
					pending++
				}
			}
		}
		if cur := h.nodes[n.ID]; cur != nil {
			continue // live knowledge wins
		}
		h.nodes[n.ID] = &Node{
			ID:           n.ID,
			Name:         n.Name,
			Short:        n.Short,
			Lat:          n.Lat,
			Lon:          n.Lon,
			LastSeen:     n.LastSeen,
			Sends:        append([]string(nil), n.Sends...),
			Hops:         n.Hops,
			RemovedSince: n.RemovedSince,
		}
	}
	h.mu.Unlock()
	if len(nodes) > 0 {
		h.logger.Info("meshtastic: restored node directory", "nodes", len(nodes), "pending_removals", pending)
	}
	h.mu.Lock()
	dropped := h.removalsDropped
	h.mu.Unlock()
	if dropped > 0 && h.logger != nil {
		h.logger.Warn("meshtastic: station removal backlog over its bound, the excess was dropped",
			"dropped", dropped, "backlog", maxPendingStationRemovals)
	}
}

// persistNodes rewrites the node directory when it changed (at most once
// per 30 seconds; saves run on a background goroutine so a slow disk can
// never stall the session).
func (h *Hub) persistNodes() {
	h.mu.Lock()
	store := h.nodeStore
	if store == nil || !h.nodesDirty || time.Since(h.lastPersist) < 30*time.Second {
		h.mu.Unlock()
		return
	}
	nodes := make([]storage.MeshtasticNode, 0, len(h.nodes)+len(h.tombstones))
	cutoff := time.Now().Add(-nodeDirRetention)
	for _, n := range h.nodes {
		// Defense in depth: the directory prune in expireNodes already
		// dropped these, but a persisted save must never resurrect them —
		// unless the broker still owes their removal (below).
		if n.LastSeen.Before(cutoff) {
			continue
		}
		// A quiet node whose removal the broker has not confirmed keeps that
		// work alive across a restart — but only while the node really is
		// quiet: a removal the node has already invalidated (it spoke again
		// since) is dropped here as well, so a save can never turn fresh
		// activity into the basis of an old removal.
		pending := h.openRemovalLocked(n.ID)
		if pending != nil && n.LastSeen.After(pending.quietSince) {
			delete(h.tombstones, n.ID)
			pending = nil
		}
		row := storage.MeshtasticNode{
			ID:           n.ID,
			Name:         n.Name,
			Short:        n.Short,
			Lat:          n.Lat,
			Lon:          n.Lon,
			LastSeen:     n.LastSeen,
			Sends:        append([]string(nil), n.Sends...),
			Hops:         n.Hops,
			RemovedSince: n.RemovedSince,
		}
		if pending != nil {
			row.PendingRemoval = true
			row.PendingSince = pending.quietSince
		}
		nodes = append(nodes, row)
	}
	// A removal outlives the directory: its node may be gone from memory
	// entirely, so the pending work gets a row of its own.
	for id, tb := range h.tombstones {
		if tb.confirmed || h.nodes[id] != nil {
			continue
		}
		nodes = append(nodes, storage.MeshtasticNode{
			ID:             id,
			LastSeen:       tb.quietSince,
			PendingRemoval: true,
			PendingSince:   tb.quietSince,
		})
	}
	h.lastPersist = time.Now()
	h.nodesDirty = false
	h.mu.Unlock()
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := store.SaveMeshtasticNodes(ctx, nodes); err != nil {
			h.logger.Warn("meshtastic: node directory save failed", "error", err)
			h.mu.Lock()
			h.nodesDirty = true
			h.mu.Unlock()
		}
	}()
}

// populateFromState copies the handshake device state into the hub: our
// identity, the firmware/hardware metadata, the node directory (names
// and ids) and the channel names by index. Station documents are
// published after the lock is released (sinks do network I/O).
func (h *Hub) populateFromState(st *client.DeviceState) {
	if st == nil {
		return
	}
	var publish []Node
	h.mu.Lock()
	if mi := st.NodeInfo(); mi != nil {
		id := fmt.Sprintf("%08x", mi.GetMyNodeNum())
		if h.self == nil {
			h.self = &SelfInfo{ID: id}
		} else {
			h.self.ID = id
		}
	}
	if md := st.DeviceMetadata(); md != nil {
		if h.self == nil {
			h.self = &SelfInfo{}
		}
		h.self.Firmware = md.GetFirmwareVersion()
		h.self.HwModel = strings.TrimPrefix(md.GetHwModel().String(), "HW_MODEL_")
	}
	// The device states its own favorites here: the pass uses this (and not
	// its ledger) as the proof that a star is actually set on the radio.
	stars := make(map[uint32]bool)
	// The peer key is what a direct message needs (the device refuses the
	// frame without it), so the same handshake says which nodes the radio
	// can encrypt to at all — the panel tells an operator BEFORE they type.
	keys := make(map[uint32]bool, len(st.Nodes()))
	for _, ni := range st.Nodes() {
		if ni == nil || ni.GetNum() == 0 {
			continue
		}
		if ni.GetIsFavorite() {
			stars[ni.GetNum()] = true
		}
		if len(ni.GetUser().GetPublicKey()) > 0 {
			keys[ni.GetNum()] = true
		}
	}
	h.radioFavorites = stars
	h.peerKeys = keys
	for _, ni := range st.Nodes() {
		if ni == nil {
			continue
		}
		id := fmt.Sprintf("%08x", ni.GetNum())
		user := ni.GetUser()
		name := strings.TrimSpace(user.GetLongName())
		if name == "" {
			name = strings.TrimSpace(user.GetShortName())
		}
		if h.self != nil && id == h.self.ID {
			if name != "" {
				h.self.LongName = name
			}
			if sn := strings.TrimSpace(user.GetShortName()); sn != "" {
				h.self.ShortName = sn
			}
			if pos := ni.GetPosition(); pos != nil {
				if lat, lon := positionDeg(pos); lat != 0 || lon != 0 {
					h.self.Lat, h.self.Lon = lat, lon
				}
			}
			continue
		}
		n := h.nodes[id]
		if n == nil {
			n = &Node{ID: id}
			h.nodes[id] = n
			h.nodesDirty = true
		}
		if name != "" {
			n.Name = name
		}
		if sn := strings.TrimSpace(user.GetShortName()); sn != "" {
			n.Short = sn
		}
		// The device node DB carries the last known position.
		if pos := ni.GetPosition(); pos != nil {
			if lat, lon := positionDeg(pos); lat != 0 || lon != 0 {
				n.Lat, n.Lon = lat, lon
			}
		}
		// last_heard (epoch seconds of the last packet from this node)
		// is the honest freshness signal; fall back to now when absent.
		if heard := ni.GetLastHeard(); heard > 0 {
			if t := time.Unix(int64(heard), 0); t.After(n.LastSeen) {
				n.LastSeen = t
			}
		}
		if n.LastSeen.IsZero() {
			n.LastSeen = time.Now()
		}
		c := *n
		c.Telemetry = cloneTelemetry(n.Telemetry)
		publish = append(publish, c)
	}
	for _, ch := range st.Channels() {
		if ch == nil {
			continue
		}
		idx := int(ch.GetIndex())
		if idx >= len(h.channels) {
			grow := idx + 1 - len(h.channels)
			h.channels = append(h.channels, make([]string, grow)...)
			h.channelKeys = append(h.channelKeys, make([][]byte, grow)...)
		}
		if name := strings.TrimSpace(ch.GetSettings().GetName()); name != "" {
			h.channels[idx] = name
		}
		// The key is kept next to the name: the Help page hands the
		// installation's own channel to users (name + key + scan code),
		// which needs exactly what the device reported.
		if psk := ch.GetSettings().GetPsk(); len(psk) > 0 {
			h.channelKeys[idx] = append([]byte(nil), psk...)
		}
	}
	h.mu.Unlock()
	for i := range publish {
		h.publishStation(&publish[i])
	}
}

// positionDeg converts the 1e-7 scaled Position coordinates to degrees.
func positionDeg(pos *pb.Position) (lat, lon float64) {
	if pos == nil {
		return 0, 0
	}
	return float64(pos.GetLatitudeI()) / 1e7, float64(pos.GetLongitudeI()) / 1e7
}

// SetUserNodes supplies the node numbers registered in the user directory:
// the nodes this station is expected to be able to send a DIRECT message to.
// They are pinned as favorites on the radio (see pinUserNodes). A nil
// provider — the default — leaves the radio's favorites alone, and so does a
// provider that returns an error: a directory that cannot be read is not an
// empty directory, and reconciling against it would unpin the very keys the
// station must keep.
func (h *Hub) SetUserNodes(nodes func() ([]uint32, error)) {
	h.mu.Lock()
	h.userNodes = nodes
	h.mu.Unlock()
}

// SetFavoriteStore attaches the ledger of the favorites this station set on
// the radio (optional). Without it the hub still remembers its own stars for
// the lifetime of the process, but a restart forgets which ones were its own
// and it then refuses to unpin anything.
func (h *Hub) SetFavoriteStore(store FavoriteStore) {
	h.mu.Lock()
	h.favoriteStore = store
	h.mu.Unlock()
}

// pinUserNodes keeps the radio's node database from evicting the public keys
// of the people this station serves. The firmware refuses a direct message to
// a node whose key it does not hold and only learns it from that node's
// NodeInfo, so a key pruned out of a full node table costs the operator a
// message until the node announces itself again. Favorites are NEVER evicted
// and survive a node-db reset, which makes them the one durable place this
// pin can live.
//
// The pass reconciles in BOTH directions: every node in the directory is set
// as a favorite, and a favorite this station set for a node that has since
// left the directory is removed again — the radio keeps favorites forever, so
// without the second half the node table would slowly fill with stars for
// people the station no longer serves. Only stars this station set itself are
// ever removed; a node the operator starred on the device is left alone, which
// is exactly what the ledger of its own pins is for.
//
// The ledger is ownership, NOT the radio's state. A star it lists is
// re-asserted unless the DEVICE reports the node as favorite, so a node-db
// reset (or a swapped radio) is repaired at the next session, while a healthy
// radio — which confirms every star it kept — costs no admin traffic at all.
//
// The pass runs on the maintenance tick (and forced right after a handshake):
// at most once per PinInterval, and a radio that refuses the admin message
// only logs a warning — a failed pass must never disturb the mesh session. A
// pass that did not get through is retried after pinRetryDelay instead of a
// whole interval later.
func (h *Hub) pinUserNodes(force bool) {
	if !h.cfg.PinUserNodes {
		return
	}
	h.mu.Lock()
	provider := h.userNodes
	now := h.clock()
	if provider == nil || (!force && now.Before(h.pinNext)) {
		h.mu.Unlock()
		return
	}
	interval := h.cfg.PinInterval
	if interval <= 0 {
		interval = defaultPinInterval
	}
	// This pass CONSUMES the due time: leaving it in the past would make every
	// later tick run the pass again (the maintenance tick is seconds apart).
	// The end of the pass arms the next one, and a retry asked for while it
	// runs still wins because arming never postpones (see schedulePinNext).
	h.pinNext = time.Time{}
	h.mu.Unlock()

	next := now.Add(interval)
	// Whatever this pass wrote down goes to the store even when the pass
	// itself gives up early, and a store that refuses the write is retried
	// soon — the pending change survives in the flag, not in this call.
	defer func() {
		if !h.flushFavorites() {
			next = now.Add(pinRetryDelay)
		}
		// Never postpones a retry the device asked for while this pass ran.
		h.schedulePinNext(next)
	}()

	// The ledger first: without knowing our own stars the station cannot
	// tell them from the operator's, and must not touch the radio.
	pinned, err := h.loadFavorites()
	if err != nil {
		h.pinLog(slog.LevelWarn, "meshtastic: pinned favorite ledger load failed, favorites left unchanged", "error", err)
		return
	}
	want, err := provider()
	if err != nil {
		h.pinLog(slog.LevelWarn, "meshtastic: user node list unavailable, favorites left unchanged", "error", err)
		return
	}
	desired := make(map[uint32]bool, len(want))
	for _, id := range want {
		if id != 0 && id != core.BroadcastNodeID.Uint32() {
			desired[id] = true
		}
	}
	// The device's own report of its favorites is the only proof a star is
	// actually set; the ledger says whose star it is, never that it is still
	// there.
	h.mu.Lock()
	confirmed := make(map[uint32]bool, len(h.radioFavorites))
	for id := range h.radioFavorites {
		confirmed[id] = true
	}
	h.mu.Unlock()

	// Every node the directory requires must be a favorite on the radio. The
	// ledger does not excuse one: an owned star the device no longer reports
	// was lost with a node-db reset (or never reached a swapped device) and
	// is set again here. A star the radio confirms needs no traffic — which
	// is the normal state of a healthy installation.
	failed := false
	for id := range desired {
		if confirmed[id] {
			continue
		}
		if err := h.setFavorite(id, true); err != nil {
			h.pinLog(slog.LevelWarn, "meshtastic: pinning a user node as favorite failed",
				"node", meshIDString(id), "error", err)
			failed = true
			continue
		}
		// Nothing is recorded here: the frame has only been written, and the
		// device's answer settles what the star actually is (settleFavorite).
	}
	for _, id := range pinned {
		if desired[id] {
			continue
		}
		if err := h.setFavorite(id, false); err != nil {
			h.pinLog(slog.LevelWarn, "meshtastic: unpinning a node that left the user directory failed",
				"node", meshIDString(id), "error", err)
			failed = true
			continue
		}
		// As above: the star is taken out of the ledger and the confirmation
		// only when the device confirms it has been removed.
	}
	if failed {
		next = now.Add(pinRetryDelay)
	}
}

// loadFavorites returns the favorites this station set on the radio, reading
// the ledger from the store on first use. A failed read is NOT cached: the
// next pass tries again, and until then the pass leaves the radio alone.
func (h *Hub) loadFavorites() ([]uint32, error) {
	h.mu.Lock()
	loaded := h.pinnedLoaded
	h.mu.Unlock()
	if !loaded {
		h.mu.Lock()
		store := h.favoriteStore
		h.mu.Unlock()
		if store != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			ids, err := store.LoadMeshtasticFavorites(ctx)
			cancel()
			if err != nil {
				return nil, err
			}
			h.mu.Lock()
			// A pass that loaded the ledger while this read was in flight
			// already knows the pins AND what it did with them: merging a
			// stale snapshot could resurrect a star it just took back.
			if !h.pinnedLoaded {
				if h.pinned == nil {
					h.pinned = make(map[uint32]bool, len(ids))
				}
				for _, id := range ids {
					h.pinned[id] = true
				}
				h.pinnedLoaded = true
			}
			h.mu.Unlock()
		} else {
			h.mu.Lock()
			if h.pinned == nil {
				h.pinned = map[uint32]bool{}
			}
			h.pinnedLoaded = true
			h.mu.Unlock()
		}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]uint32, 0, len(h.pinned))
	for id := range h.pinned {
		out = append(out, id)
	}
	return out, nil
}

// rememberFavorite records (or drops) one star in the in-memory ledger and
// marks it unsaved; the pass flushes it at the end (flushFavorites), so a
// half-applied pass costs at most a repeated admin frame.
func (h *Hub) rememberFavorite(id uint32, pinned bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.pinned == nil {
		h.pinned = map[uint32]bool{}
	}
	if pinned {
		h.pinned[id] = true
	} else {
		delete(h.pinned, id)
	}
	h.favoritesRev++
	h.favoritesDirty = true
}

// dirtyFavorites reports whether the ledger holds changes the store has not
// accepted yet.
func (h *Hub) dirtyFavorites() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.favoritesDirty
}

// saveFavorites writes the ledger back (a small snapshot, at most once per
// pass). A failure only costs a stale ledger — the radio already carries the
// star — so it is logged, never propagated.
// flushFavorites writes the ledger when it holds changes the store has not
// accepted yet, and reports whether nothing is left pending. A failed write
// KEEPS the flag (that is the whole point: the radio carries the stars, but
// only the ledger makes them OURS after a restart, and only the ledger lets a
// later pass take a star back), so the next pass retries it without any new
// change from the directory.
//
// Flushes are serialized end to end: the revision check protects against a
// change made DURING a write, but it cannot protect the store from an older
// snapshot arriving after a newer one.
func (h *Hub) flushFavorites() bool {
	// The snapshot and the write that carries it are ONE step: a flush that
	// starts later must never land earlier (see favoriteSaveMu).
	h.favoriteSaveMu.Lock()
	defer h.favoriteSaveMu.Unlock()
	h.mu.Lock()
	if !h.favoritesDirty {
		h.mu.Unlock()
		return true
	}
	store := h.favoriteStore
	rev := h.favoritesRev
	ids := make([]uint32, 0, len(h.pinned))
	for id := range h.pinned {
		ids = append(ids, id)
	}
	h.mu.Unlock()
	if store == nil {
		// RAM-only mode (no store attached): nothing to write to, which is
		// not a failure — the process cannot lose what it never persisted.
		h.mu.Lock()
		if h.favoritesRev == rev {
			h.favoritesDirty = false
		}
		h.mu.Unlock()
		return true
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := store.SaveMeshtasticFavorites(ctx, ids); err != nil {
		h.pinLog(slog.LevelWarn, "meshtastic: pinned favorite ledger save failed, will retry", "error", err)
		// The change landed outside a pass (the device settles a favorite
		// whenever it answers), so the retry is scheduled here.
		h.favoriteRetry()
		return false
	}
	h.mu.Lock()
	// A change made while the write was in flight is not covered by this
	// snapshot: the flag stays set for it.
	if h.favoritesRev == rev {
		h.favoritesDirty = false
	}
	h.mu.Unlock()
	return true
}

// PinnedNodes returns the node numbers this station set as favorites on the
// radio, normalized to lowercase 8-hex ids. Nodes the operator starred on the
// device are not listed: the station only knows about its own pins. The
// ledger is read on first use, so the admin page shows the pins of a previous
// process too — before the first pass of this one.
func (h *Hub) PinnedNodes() []string {
	ids, err := h.loadFavorites()
	if err != nil {
		return nil
	}
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		out = append(out, meshIDString(id))
	}
	sort.Strings(out)
	return out
}

// PeerKeyNodes returns the node ids whose NodeInfo announced a PUBLIC KEY —
// exactly the key a radio needs to encrypt a direct message to that node.
// The list is the latest evidence the station has (the device's node table at
// the handshake, then every announcement heard); a node missing from it has
// either never announced or announced without a key, and a direct message to
// it will be refused until its firmware changes that.
func (h *Hub) PeerKeyNodes() []string {
	h.mu.Lock()
	out := make([]string, 0, len(h.peerKeys))
	for id, ok := range h.peerKeys {
		if ok {
			out = append(out, meshIDString(id))
		}
	}
	h.mu.Unlock()
	sort.Strings(out)
	return out
}

// markRadioFavorite records what the device now carries for one node. It
// mirrors BOTH directions: a star the device accepted is skipped for the rest
// of the session, and a star it took back stops counting as confirmed —
// otherwise a node that is removed and later registered again would be left
// without a star, because the confirmation of the first pin outlives the
// removal. (The next handshake reads the device's own report anyway.)
func (h *Hub) markRadioFavorite(id uint32, favorite bool) {
	h.mu.Lock()
	if h.radioFavorites == nil {
		h.radioFavorites = map[uint32]bool{}
	}
	if favorite {
		h.radioFavorites[id] = true
	} else {
		delete(h.radioFavorites, id)
	}
	h.mu.Unlock()
}

// settleFavorite applies the DEVICE's verdict on one favorite frame. Only a
// frame the device accepted changes anything: the star is then recorded as
// OURS — the ledger is what takes it back when its owner leaves the directory
// — and as CONFIRMED, so a later pass does not re-send it.
//
// A refusal, or an answer that never came, leaves both untouched, retracts a
// confirmation that no longer matches the radio, and brings the next pass
// forward: the station must never believe in a star the radio does not have.
func (h *Hub) settleFavorite(ps *pendingSend, status string) {
	op := ps.favorite
	if op == nil || !ps.favoriteSettled.CompareAndSwap(false, true) {
		return
	}
	if status == TxSent || status == TxDelivered {
		h.rememberFavorite(op.node, op.set)
		h.markRadioFavorite(op.node, op.set)
		if op.set {
			h.pinLog(slog.LevelInfo, "meshtastic: user node pinned as favorite on the radio", "node", meshIDString(op.node))
		} else {
			h.pinLog(slog.LevelInfo, "meshtastic: node left the user directory, favorite removed from the radio",
				"node", meshIDString(op.node))
		}
		h.flushFavorites()
		return
	}
	// The radio refused the frame (or never answered). The star keeps whatever
	// state the radio really has: a refused SET is not confirmed, and a failed
	// REMOVE means the star is still there.
	h.markRadioFavorite(op.node, !op.set)
	h.pinLog(slog.LevelWarn, "meshtastic: the radio did not apply a favorite change, will retry",
		"node", meshIDString(op.node), "set", op.set, "reason", ps.failReason)
	h.favoriteRetry()
}

// favoriteRetry brings the next pin pass forward: a change the radio refused
// (or never answered) is attempted again in minutes, not a whole interval
// later.
func (h *Hub) favoriteRetry() {
	h.schedulePinNext(time.Now().Add(pinRetryDelay))
}

// schedulePinNext arms the next pin pass WITHOUT ever postponing one that is
// already due earlier. A retry is asked for the moment the radio answers, and
// that answer can arrive while the pass that sent the frame is still running:
// the pass's own interval must not overwrite it on the way out, or a refused
// node would stay unprotected for a whole interval.
func (h *Hub) schedulePinNext(next time.Time) {
	h.mu.Lock()
	if h.pinNext.IsZero() || next.Before(h.pinNext) {
		h.pinNext = next
	}
	h.mu.Unlock()
}

// pinLog writes one pin-pass line; the tests build hubs without a logger.
func (h *Hub) pinLog(level slog.Level, msg string, args ...any) {
	if h.logger == nil {
		return
	}
	h.logger.Log(context.Background(), level, msg, args...)
}

// meshIDString renders a node number the way the UI and the logs do.
func meshIDString(id uint32) string { return fmt.Sprintf("%08x", id) }

// setFavorite asks the DEVICE to add or remove one node in its favorites.
// The admin frame is addressed to our own node: the firmware handles a packet
// that came in over the local API itself (no channel, no admin session, no
// encryption), which is exactly what the phone app does when a star is
// tapped.
//
// Returning nil only means the frame was WRITTEN: the device answers with the
// packet id and a routing result, and that answer is what settles the change
// (see settleFavorite). Callers must not record the star before it.
func (h *Hub) setFavorite(id uint32, favorite bool) error {
	if id == 0 || id == core.BroadcastNodeID.Uint32() {
		return fmt.Errorf("meshtastic: %08x is not a node that can be favorited", id)
	}
	h.mu.Lock()
	conn := h.conn
	self := h.self
	h.mu.Unlock()
	if conn == nil || !conn.IsConnected() {
		return errDeviceNotConnected
	}
	if self == nil || self.ID == "" {
		return errors.New("meshtastic: our own node number is not known yet")
	}
	selfNum, err := strconv.ParseUint(normalizeMeshID(self.ID), 16, 32)
	if err != nil {
		return fmt.Errorf("meshtastic: our own node number %q is unreadable", self.ID)
	}
	admin := &pb.AdminMessage{}
	if favorite {
		admin.PayloadVariant = &pb.AdminMessage_SetFavoriteNode{SetFavoriteNode: id}
	} else {
		admin.PayloadVariant = &pb.AdminMessage_RemoveFavoriteNode{RemoveFavoriteNode: id}
	}
	payload, err := proto.Marshal(admin)
	if err != nil {
		return fmt.Errorf("meshtastic: encode the favorite request: %w", err)
	}
	// The device answers the frame like any other local packet; the
	// placeholder keeps its answer paired with THIS send, so an unrelated
	// text message can never consume the id (same bookkeeping as the
	// NodeInfo refresh).
	at := time.Now()
	ps := &pendingSend{
		to: uint32(selfNum), at: at, lastTry: at, text: "\x00favorite",
		wantAck: true, probe: true,
		favorite: &favoriteChange{node: id, set: favorite},
	}
	h.sendMu.Lock()
	h.echoQueue = append(h.echoQueue, ps)
	h.sendMu.Unlock()
	err = conn.SendToRadio(&pb.ToRadio{PayloadVariant: &pb.ToRadio_Packet{Packet: &pb.MeshPacket{
		To:      uint32(selfNum),
		WantAck: true,
		PayloadVariant: &pb.MeshPacket_Decoded{
			Decoded: &pb.Data{Portnum: pb.PortNum_ADMIN_APP, Payload: payload},
		},
	}}})
	if err != nil {
		h.dropPending(ps)
		return fmt.Errorf("meshtastic: favorite request write failed: %w", err)
	}
	return nil
}

// queueRemovalLocked records one removal the broker owes. The ledger is
// BOUNDED (maxPendingRemovals): past it the removal is DEFERRED — the node
// stays eligible and a later pass offers it again once the broker has worked
// off what is in flight. Nothing is remembered and nothing is counted here:
// the work simply waits, which is what an obsolete document on a broker that
// is not answering deserves (the ledger cannot grow past its bound, because
// what waits is the node directory itself, pruned by nodeDirRetention).
func (h *Hub) queueRemovalLocked(id string, quietSince time.Time) bool {
	if h.tombstones[id] != nil {
		return false
	}
	if h.pendingRemovalsLocked() >= h.maxRemovals() {
		return false
	}
	h.tombstones[id] = &nodeTombstone{quietSince: quietSince}
	return true
}

// maxRemovals is the removal-ledger bound (a field so tests can shrink it).
func (h *Hub) maxRemovals() int {
	if h.pendingRemovalsMax > 0 {
		return h.pendingRemovalsMax
	}
	return maxPendingStationRemovals
}

// pendingRemovalsLocked counts the removals waiting for the broker (a
// confirmed entry is bookkeeping, not work).
func (h *Hub) pendingRemovalsLocked() int {
	n := 0
	for _, tb := range h.tombstones {
		if !tb.confirmed {
			n++
		}
	}
	return n
}

// StationRemovals reports the removals in flight at the broker and how many
// are waiting for room in that ledger. "Waiting" is not a failure: it is the
// station's own backlog of quiet nodes, worked off as the broker confirms the
// removals ahead of them. A non-zero "dropped" is the genuine overflow — rows
// restored from a database whose backlog is longer than the ledger bound.
func (h *Hub) StationRemovals() (pending, deferred, dropped int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.pendingRemovalsLocked(), h.removalsWaiting, h.removalsDropped
}

// expireNodes cleans the MQTT station feed: nodes unheard for longer than
// NodeTTL have their retained document removed, and nodes unheard for longer
// than nodeDirRetention are dropped from the directory entirely, so memory and
// the persisted table stay bounded.
//
// The removals live in their own ledger (h.tombstones), NOT in the directory:
// the directory drops a node long before the broker necessarily took the
// removal, and a dropped node would silently lose the pending work. A removal
// is queued once, retried (paced by tombstoneRetry) until the broker CONFIRMED
// it, and voided when the node is heard again — that very packet published its
// document again.
func (h *Hub) expireNodes() {
	h.mu.Lock()
	now := time.Now()
	if h.tombstones == nil {
		h.tombstones = make(map[string]*nodeTombstone)
	}
	// A node heard after its removal was queued is active again: the removal
	// is void, and the next quiet period queues a fresh one.
	for id, tb := range h.tombstones {
		if n := h.nodes[id]; n != nil && n.LastSeen.After(tb.quietSince) {
			delete(h.tombstones, id)
			h.nodesDirty = true
		}
	}
	// Every quiet node owes the broker a removal — but only when this
	// installation HAS a station feed: a document nobody ever published cannot
	// be owed to the broker, and queueing it would be pure growth.
	//
	// A node whose document the broker already confirmed removing stays out of
	// it until the node is heard again (`RemovedSince`): that is what keeps a
	// restart from re-queueing removals that are already done.
	queued := 0
	var waiting []string
	if h.stationSink != nil {
		for id, n := range h.nodes {
			if h.tombstones[id] != nil || now.Sub(n.LastSeen) <= h.cfg.NodeTTL {
				continue
			}
			if !n.LastSeen.After(n.RemovedSince) {
				continue // its document is already gone (nothing heard since)
			}
			if h.queueRemovalLocked(id, n.LastSeen) {
				queued++
				continue
			}
			waiting = append(waiting, id)
		}
		if queued > 0 {
			h.nodesDirty = true
		}
	}
	h.removalsWaiting = len(waiting)
	// The directory forgets a node long before the broker necessarily did.
	forced := make(map[string]bool, len(waiting))
	for _, id := range waiting {
		forced[id] = true
	}
	for id, n := range h.nodes {
		if now.Sub(n.LastSeen) > nodeDirRetention {
			// A node leaving the directory while its removal was still WAITING
			// for room has nothing left to defer to: the directory was the
			// queue. That is the one genuine loss, so it is counted instead of
			// disappearing silently.
			if forced[id] {
				h.removalsDropped++
			}
			delete(h.nodes, id)
			h.nodesDirty = true
		}
	}
	// A key mark is panel evidence for a row this station holds, so it goes
	// with the node the directory just dropped: without this the map keeps one
	// entry per node EVER heard while the directory itself stays bounded.
	//
	// The refresh cooldown is NOT touched here. It rate-limits a question the
	// station already asked — and the peer that never answered is exactly the
	// one missing from the directory: dropping its entry would let every later
	// PKI refusal ask again on the next maintenance pass. It expires by its own
	// clock instead (see sweepPeerState).
	for pid := range h.peerKeys {
		if h.nodes[meshIDString(pid)] == nil {
			delete(h.peerKeys, pid)
		}
	}
	// A CONFIRMED removal of a node the directory no longer holds has nothing
	// left to protect: the next appearance of that node publishes a fresh
	// document. An unconfirmed one keeps its entry (and its persisted row), so
	// the work survives both the directory prune and a restart.
	for id, tb := range h.tombstones {
		if tb.confirmed && h.nodes[id] == nil {
			delete(h.tombstones, id)
			h.nodesDirty = true
		}
	}
	var due []string
	for id, tb := range h.tombstones {
		if tb.confirmed {
			continue // the node stays quiet, the broker already took it
		}
		if !tb.tried.IsZero() && now.Sub(tb.tried) < h.tombstoneRetryDelay() {
			continue // the broker has not taken the last attempt yet
		}
		tb.tried = now
		due = append(due, id)
	}
	// The numbers the log lines below report are taken HERE, under the lock:
	// they describe the ledger this pass just pruned, and reading it from the
	// caller (as the log used to) raced with the confirmations the feed worker
	// writes when the broker takes a removal — the race detector caught a
	// pendingRemovalsLocked pass against a concurrent `confirmed = true`.
	inFlight, ledger := h.pendingRemovalsLocked(), h.maxRemovals()
	h.mu.Unlock()
	if len(waiting) > 0 && h.logger != nil {
		// Not a failure and not a loss: the documents wait their turn while the
		// broker works off the removals already in flight.
		h.logger.Debug("meshtastic: station removals waiting for room in the ledger",
			"waiting", len(waiting), "in_flight", inFlight, "ledger", ledger)
	}
	if dropped := h.removalsDropped; dropped > 0 && h.logger != nil {
		// Only a node that left the directory while its removal was still
		// waiting lands here: the directory WAS its queue.
		h.logger.Warn("meshtastic: station removals left the directory before the broker took them",
			"dropped", dropped, "ledger", ledger)
	}
	// The removals go to the feed worker, NEVER out from here: a broker that
	// keeps the connection open but does not confirm publications would
	// otherwise park the session loop (heartbeat, watchdog, retries, the
	// shutdown path) for a whole batch. A queue that is full is not an error —
	// the entry stays pending and the retry pace offers it again.
	for _, id := range due {
		h.enqueueStationFeed(feedJob{id: id})
	}
}

// enqueueStationFeed hands one operation to the worker. It NEVER waits: the
// caller may be the packet path or the maintenance loop, and a broker that
// stopped answering must not stall either of them.
func (h *Hub) enqueueStationFeed(job feedJob) {
	if h.feedQueue == nil {
		// No queue at all: this hub was assembled without the constructor (an
		// embedding, or a test literal). Publishing inline keeps the feed
		// working instead of dropping every document on the floor.
		h.mu.Lock()
		sink := h.stationSink
		h.mu.Unlock()
		if sink == nil {
			return
		}
		if job.doc == nil {
			h.publishStationRemoval(context.Background(), sink, job.id)
			return
		}
		h.publishStationDoc(context.Background(), sink, job)
		return
	}
	select {
	case h.feedQueue <- job:
	default:
		if h.logger != nil {
			h.logger.Debug("meshtastic: station feed queue is full, dropping the operation", "id", job.id)
		}
	}
}

// runStationFeed drains the station feed on its own goroutine, one operation at
// a time (which is also what orders a document publish against the removal of
// the same node). The context is the hub's: cancelling it (station shutdown)
// cancels the publication in flight, and the worker never delays the session
// loop that enqueued the work.
func (h *Hub) runStationFeed(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case job := <-h.feedQueue:
			h.mu.Lock()
			sink := h.stationSink
			h.mu.Unlock()
			if sink == nil {
				continue
			}
			if job.doc == nil {
				h.publishStationRemoval(ctx, sink, job.id)
				continue
			}
			h.publishStationDoc(ctx, sink, job)
		}
	}
}

// tombstoneRetryDelay is the pause between removal attempts the broker has not
// accepted yet.
func (h *Hub) tombstoneRetryDelay() time.Duration {
	if h.tombstoneRetry > 0 {
		return h.tombstoneRetry
	}
	return defaultTombstoneRetry
}

// openRemovalLocked returns the removal the broker still owes for one node, or
// nil when there is none (a confirmed removal is not owed any more).
func (h *Hub) openRemovalLocked(id string) *nodeTombstone {
	tb := h.tombstones[id]
	if tb == nil || tb.confirmed {
		return nil
	}
	return tb
}

// publishStationRemoval removes one retained station document and records the
// removal only when the broker CONFIRMED it: a refused publish stays queued (a
// later pass retries it), and a node that published again while this removal
// was waiting keeps its document.
//
// The confirmation is written for the ACTIVITY VERSION this step removed (the
// entry's quietSince) — never for whatever the node's LastSeen happens to be by
// then, and never for a replacement entry a later quiet period queued. Anything
// else would mark a document the broker still holds as gone, and that document
// would then never be removed.
//
// The whole step runs under stationMu, which the position publish takes too:
// the two operations for one node can never interleave, so the last one to run
// decides what the broker holds — and a node that just spoke always leaves its
// document behind.
func (h *Hub) publishStationRemoval(parent context.Context, sink func(ctx context.Context, topic string, retained bool, payload []byte) error, id string) {
	h.stationMu.Lock()
	defer h.stationMu.Unlock()
	h.mu.Lock()
	tb := h.openRemovalLocked(id)
	// The node may have spoken (or been restored) since this removal was
	// queued — the removal is only ever about a node that is still quiet, and
	// that is checked here and not only where the entry was queued.
	if tb != nil {
		if n := h.nodes[id]; n != nil {
			if n.LastSeen.After(tb.quietSince) || time.Since(n.LastSeen) <= h.cfg.NodeTTL {
				tb = nil
			}
		}
	}
	h.mu.Unlock()
	if tb == nil {
		return
	}
	// One budget per publication, and the hub's cancellation reaches it: a
	// broker that ignores the deadline cannot hold this worker past the
	// station's own lifetime.
	ctx, cancel := context.WithTimeout(parent, stationPublishTimeout)
	defer cancel()
	if err := sink(ctx, "meshtastic/stations/"+id, true, nil); err != nil {
		if h.logger != nil {
			h.logger.Warn("meshtastic: station removal failed, will retry",
				"id", id, "retry_in", h.tombstoneRetryDelay(), "error", err)
		}
		return
	}
	h.mu.Lock()
	// The broker took the removal of the document that was on the topic when
	// this step started: that is the ACTIVITY VERSION the entry names
	// (tb.quietSince) — never the node's current LastSeen. A node that spoke
	// while the publish was in flight has just published a NEW document;
	// marking that newer version as removed would leave it on the broker
	// forever, because the next TTL crossing would skip the node.
	//
	// A replacement entry (the node spoke, this removal was voided, a later
	// quiet period queued fresh work) is not confirmed by this publish either:
	// it names a different activity version, whose document the broker still
	// holds. Only the entry this step actually published gets confirmed.
	if cur := h.openRemovalLocked(id); cur != nil && cur == tb {
		cur.confirmed = true
	}
	if n := h.nodes[id]; n != nil && tb.quietSince.After(n.RemovedSince) {
		n.RemovedSince = tb.quietSince
		h.nodesDirty = true
	}
	h.mu.Unlock()
}

// handlePacket consumes one mesh packet delivered from the radio.
func (h *Hub) handlePacket(pkt *pb.MeshPacket) {
	if pkt == nil {
		return
	}
	h.lastRadio.Store(time.Now().UnixNano())
	decoded := pkt.GetDecoded()
	if decoded == nil {
		return
	}
	switch decoded.GetPortnum() {
	case pb.PortNum_TEXT_MESSAGE_APP:
		if h.selfID() != "" && fmt.Sprintf("%08x", pkt.GetFrom()) == h.selfID() {
			h.handleOwnEcho(pkt, decoded)
			return
		}
		h.receiveText(pkt, decoded)
	case pb.PortNum_POSITION_APP:
		h.receivePosition(pkt, decoded)
	case pb.PortNum_NODEINFO_APP:
		h.receiveNodeInfo(pkt, decoded)
	case pb.PortNum_TELEMETRY_APP:
		h.receiveTelemetry(pkt, decoded)
	case pb.PortNum_ROUTING_APP:
		h.handleRoutingAck(pkt, decoded)
	case pb.PortNum_TRACEROUTE_APP:
		// Older firmware answers the probe on the TRACEROUTE port with a
		// raw RouteDiscovery instead of a ROUTING_APP route_reply.
		h.handleTracerouteReply(pkt, decoded)
	}
	// Low-priority banner traffic drains AFTER this packet's direct
	// replies: alarm and command answers always go out first.
	h.drainBanner()
}

// selfID returns our node id without holding the lock longer than needed.
func (h *Hub) selfID() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.self == nil {
		return ""
	}
	return h.self.ID
}

// handleOwnEcho pairs the device's echo of our own transmission with the
// pending send: the echo carries the device-assigned packet id (needed to
// match the recipient's acknowledgment) and proves the radio transmitted
// the frame.
func (h *Hub) handleOwnEcho(pkt *pb.MeshPacket, decoded *pb.Data) {
	text := string(decoded.GetPayload())
	h.sendMu.Lock()
	var ps *pendingSend
	for i, q := range h.echoQueue {
		if !q.echoed && q.text == text {
			ps = q
			h.echoQueue = append(h.echoQueue[:i], h.echoQueue[i+1:]...)
			break
		}
	}
	if ps == nil {
		h.sendMu.Unlock()
		return
	}
	ps.echoed = true
	ps.id = pkt.GetId()
	if ps.id != 0 {
		h.pending[ps.id] = ps
	}
	h.sendMu.Unlock()
	h.markStatus(ps, TxSent)
}

// pairQueueStatus learns the device-assigned packet id from a queueStatus
// answer: the device replies to every client send with the mesh packet id
// that send was given. The oldest unpaired send within the pairing window
// wins (the queue is FIFO, so every send must be represented in the echo
// queue — that is what keeps the positional pairing correct).
//
// A non-zero res is the device REFUSING the frame: it never entered the
// air, so no acknowledgment will ever arrive. Such a send settles failed
// AT ONCE with the refusal reason instead of marking the frame "sent"
// and waiting out the acknowledgment window for a message that was never
// transmitted.
func (h *Hub) pairQueueStatus(id uint32, res pb.Routing_Error) {
	if id == 0 {
		return
	}
	now := time.Now()
	h.sendMu.Lock()
	for i, q := range h.echoQueue {
		if q.echoed || q.id != 0 || now.Sub(q.lastTry) > 2*dmAckTimeout {
			continue
		}
		h.echoQueue = append(h.echoQueue[:i], h.echoQueue[i+1:]...)
		q.echoed = true
		q.id = id
		if res != pb.Routing_NONE {
			h.sendMu.Unlock()
			h.rejectSend(q, res)
			return
		}
		if !q.finalOnModemAck() {
			// A favorite change is answered by the modem itself: it never
			// enters the delivery window that waits for a recipient ack.
			h.pending[id] = q
		}
		h.sendMu.Unlock()
		h.markStatus(q, TxSent)
		return
	}
	h.sendMu.Unlock()
}

// rejectSend settles one send the device refused outright: the frame
// never reached the mesh, the failure is final and it carries the
// device's reason. A refusal caused by the recipient's unknown public key
// additionally asks that peer to re-announce its NodeInfo — the radio
// encrypts direct messages with the peer key it learned from NodeInfo and
// refuses to fall back to channel encryption, so the refresh is the only
// thing that can unblock a later attempt.
func (h *Hub) rejectSend(ps *pendingSend, res pb.Routing_Error) {
	reason := sendFailureReason(res)
	h.sendMu.Lock()
	ps.failReason = reason
	h.sendMu.Unlock()
	if h.logger != nil {
		h.logger.Warn("meshtastic: device rejected a send",
			"res", int32(res), "reason", reason, "id", ps.id, "to", fmt.Sprintf("%08x", ps.to))
	}
	h.markStatus(ps, TxFailed)
	if !ps.probe && needsNodeInfoRefresh(res) {
		h.recoverFromKeyRefusal(ps)
	}
}

// awaitPeerKey remembers one message the radio refused because the peer's
// public key was missing, so the NodeInfo that lands next can finish what the
// operator started. Only a message the operator asked for qualifies, and only
// its FIRST such refusal: the retry is refused with the same reason when the
// key was not the real problem, and re-arming the wait then would keep the
// station re-sending the same text at every NodeInfo.
//
// A message the DURABLE action queue owns is left out of it: recording the
// failure already re-arms that job (markStatus → RecordMeshFailureAndRequeue),
// so the worker re-sends the alert on its own. Remembering it here as well puts
// the same text on the air twice — the worker's retry AND the earlier copy the
// key releases — which is exactly the reported 3 transmissions instead of 2.
// The key refresh itself is shared by both kinds of message; only the RETRY
// belongs to the durable queue alone.
//
// One peer can owe SEVERAL messages (the operator sent two texts before the
// key landed) and every one of them is kept — the queue is per peer, oldest
// first, and each entry carries its own deadline. What is bounded is the
// queue itself: past keyRetryPerPeer for one peer or keyRetryTotal for the
// hub, the message is NOT remembered, and that overflow is logged instead of
// being silent — the message keeps its failed row and its reason, so the
// operator sees exactly which text stayed behind.
func (h *Hub) awaitPeerKey(ps *pendingSend) {
	if ps.keyTried || !ps.wantAck || ps.text == "" || ps.prog.active() {
		return
	}
	h.mu.Lock()
	if h.keyRetries == nil {
		h.keyRetries = make(map[uint32][]*keyRetry)
	}
	queue := h.keyRetries[ps.to]
	queued := false
	for _, kr := range queue {
		if kr.ps == ps {
			queued = true
			break
		}
	}
	perPeer, total := h.keyRetryPerPeer(), h.keyRetryTotal()
	overflow := len(queue) >= perPeer || h.keyWaitsLocked() >= total
	if !queued && !overflow {
		h.keyRetries[ps.to] = append(queue, &keyRetry{ps: ps, when: h.clock()})
	}
	waiting, peers := len(queue), len(h.keyRetries)
	h.mu.Unlock()
	if queued || !overflow {
		return
	}
	if h.logger != nil {
		h.logger.Warn("meshtastic: too many messages wait for a peer key, this one stays failed",
			"to", fmt.Sprintf("%08x", ps.to), "peer_waiting", waiting, "peer_limit", perPeer,
			"peers_waiting", peers, "total_limit", total)
	}
}

// keyWaitsLocked counts every message waiting for a key, across peers.
func (h *Hub) keyWaitsLocked() int {
	n := 0
	for _, queue := range h.keyRetries {
		n += len(queue)
	}
	return n
}

// keyRetryPerPeer and keyRetryTotal are the queue bounds (fields so tests can
// shrink them).
func (h *Hub) keyRetryPerPeer() int {
	if h.keyRetryPerPeerMax > 0 {
		return h.keyRetryPerPeerMax
	}
	return keyRetryPerPeer
}

func (h *Hub) keyRetryTotal() int {
	if h.keyRetryTotalMax > 0 {
		return h.keyRetryTotalMax
	}
	return keyRetryTotal
}

// takeKeyRetry hands over the messages waiting for one peer's key, oldest
// first: every one of them was refused for that same missing key, and the
// NodeInfo that just arrived is the answer for all of them. Entries whose
// window ran out are dropped — a key that arrives too late no longer belongs
// to the refusal that asked for it.
func (h *Hub) takeKeyRetry(to uint32) []*keyRetry {
	h.mu.Lock()
	defer h.mu.Unlock()
	queue := h.keyRetries[to]
	if len(queue) == 0 {
		return nil
	}
	delete(h.keyRetries, to)
	now := h.clock()
	out := make([]*keyRetry, 0, len(queue))
	for _, kr := range queue {
		if now.Sub(kr.when) <= keyWaitWindow {
			out = append(out, kr)
		}
	}
	return out
}

// retryAfterKey re-sends one direct message the radio refused for a missing
// peer key: that key just arrived with this NodeInfo, and the text the
// operator already typed must not be left behind failed. The retry keeps the
// original history row (same timestamp, same text), so the panel flips THE
// MESSAGE to sent/delivered instead of growing a duplicate nobody typed.
func (h *Hub) retryAfterKey(kr *keyRetry) {
	if kr == nil || kr.ps == nil {
		return
	}
	ps := kr.ps
	h.sendMu.Lock()
	ps.keyTried = true
	ps.failReason = ""
	ps.status = ""
	ps.id = 0
	ps.echoed = false
	ps.retries = 0
	h.sendMu.Unlock()
	if h.logger != nil {
		h.logger.Info("meshtastic: peer key received, retrying the direct message",
			"to", fmt.Sprintf("%08x", ps.to))
	}
	h.resend(ps)
}

// expireKeyRetries drops the waits whose window ran out: the refusal stands
// and the operator re-sends by hand (the panel shows why it failed). Each
// entry has its own deadline, so an old refusal expires even when a newer
// message to the same peer is still inside its window.
func (h *Hub) expireKeyRetries(now time.Time) {
	h.mu.Lock()
	var expired []uint32
	for to, queue := range h.keyRetries {
		kept := queue[:0]
		dropped := 0
		for _, kr := range queue {
			if now.Sub(kr.when) > keyWaitWindow {
				dropped++
				continue
			}
			kept = append(kept, kr)
		}
		if dropped == 0 {
			continue
		}
		if len(kept) == 0 {
			delete(h.keyRetries, to)
		} else {
			h.keyRetries[to] = kept
		}
		expired = append(expired, to)
	}
	h.mu.Unlock()
	if len(expired) > 0 && h.logger != nil {
		for _, to := range expired {
			h.logger.Warn("meshtastic: the peer key never arrived, the message stays failed",
				"to", fmt.Sprintf("%08x", to))
		}
	}
}

// sendFailureReason maps the device's refusal code (queueStatus.res, the
// ROUTING_APP error_reason) onto a stable key shared by the log line, the
// stored history row and the UI translation. An unknown code keeps its
// numeric form so a newer firmware enum is never silently swallowed.
func sendFailureReason(res pb.Routing_Error) string {
	switch res {
	case pb.Routing_NO_ROUTE:
		return "no_route"
	case pb.Routing_GOT_NAK:
		return "got_nak"
	case pb.Routing_TIMEOUT:
		return "timeout"
	case pb.Routing_NO_INTERFACE:
		return "no_interface"
	case pb.Routing_MAX_RETRANSMIT:
		return "max_retransmit"
	case pb.Routing_NO_CHANNEL:
		return "no_channel"
	case pb.Routing_TOO_LARGE:
		return "too_large"
	case pb.Routing_NO_RESPONSE:
		return "no_response"
	case pb.Routing_DUTY_CYCLE_LIMIT:
		return "duty_cycle"
	case pb.Routing_BAD_REQUEST:
		return "bad_request"
	case pb.Routing_NOT_AUTHORIZED:
		return "not_authorized"
	case pb.Routing_PKI_FAILED:
		return "pki_failed"
	case pb.Routing_PKI_UNKNOWN_PUBKEY:
		return "pki_unknown_key"
	case pb.Routing_ADMIN_BAD_SESSION_KEY:
		return "admin_bad_session_key"
	case pb.Routing_ADMIN_PUBLIC_KEY_UNAUTHORIZED:
		return "admin_unauthorized"
	case pb.Routing_RATE_LIMIT_EXCEEDED:
		return "rate_limit"
	case pb.Routing_PKI_SEND_FAIL_PUBLIC_KEY:
		return "pki_no_key"
	default:
		return fmt.Sprintf("res_%d", int32(res))
	}
}

// needsNodeInfoRefresh reports whether a refusal is a missing or unusable
// PEER KEY — the one refusal a NodeInfo refresh can cure.
func needsNodeInfoRefresh(res pb.Routing_Error) bool {
	switch res {
	case pb.Routing_PKI_FAILED, pb.Routing_PKI_UNKNOWN_PUBKEY, pb.Routing_PKI_SEND_FAIL_PUBLIC_KEY:
		return true
	}
	return false
}

// nodeInfoRefreshGap rate-limits the NodeInfo refresh per peer: the
// peer's firmware suppresses a reply to the same requester for hours, so
// asking more often only spends airtime.
const nodeInfoRefreshGap = 6 * time.Hour

// maxPeerKeyMarks caps the key marks one session keeps (see rememberPeerKeyLocked):
// a mark is a hint rebuilt at every handshake, so a cap costs an operator
// nothing but a hint, while the map otherwise grows with every node ever heard.
const maxPeerKeyMarks = 4096

// maxNodeInfoRefreshEntries caps the refresh cooldown map: one small entry per
// peer asked within the gap, so this bound only matters on a mesh that answers
// none of them.
const maxNodeInfoRefreshEntries = 4096

// requestNodeInfo asks one node's radio to re-announce its NodeInfo (and
// with it its public key), which is what makes a later direct message to
// that node possible when the key is missing from the device database.
// The request rides the send bookkeeping as a placeholder so its own
// answer pairs with IT — the positional FIFO pairing of the echo queue
// stays intact and an unrelated text message never consumes its id.
func (h *Hub) requestNodeInfo(to uint32) {
	if to == core.BroadcastNodeID.Uint32() {
		return
	}
	now := time.Now()
	h.mu.Lock()
	if h.nodeInfoAsked == nil {
		h.nodeInfoAsked = make(map[uint32]time.Time)
	}
	if last, ok := h.nodeInfoAsked[to]; ok && now.Sub(last) < nodeInfoRefreshGap {
		h.mu.Unlock()
		return
	}
	h.nodeInfoAsked[to] = now
	conn := h.conn
	h.mu.Unlock()
	if conn == nil || !conn.IsConnected() {
		return
	}
	p := &pendingSend{to: to, at: now, lastTry: now, text: "\x00nodeinfo", wantAck: true, probe: true}
	h.sendMu.Lock()
	h.echoQueue = append(h.echoQueue, p)
	h.sendMu.Unlock()
	err := conn.SendToRadio(&pb.ToRadio{PayloadVariant: &pb.ToRadio_Packet{Packet: &pb.MeshPacket{
		To:      to,
		WantAck: true,
		PayloadVariant: &pb.MeshPacket_Decoded{Decoded: &pb.Data{
			Portnum:      pb.PortNum_NODEINFO_APP,
			WantResponse: true,
		}},
	}}})
	if err != nil {
		h.dropPending(p)
		if h.logger != nil {
			h.logger.Warn("meshtastic: node info request failed", "to", fmt.Sprintf("%08x", to), "error", err)
		}
		return
	}
	if h.logger != nil {
		h.logger.Info("meshtastic: requested node info to refresh the peer key", "to", fmt.Sprintf("%08x", to))
	}
}

// pairByRoutingLocked recovers the send behind a ROUTING_APP frame whose
// request id we could not match (firmwares that never announce the id):
// a self-addressed frame confirms a device-originated transmit (the
// broadcast "sent" confirmation), an external frame acknowledges the
// oldest direct message to that sender.
func (h *Hub) pairByRoutingLocked(rid, from uint32, fromSelf bool) *pendingSend {
	for i, q := range h.echoQueue {
		if q.echoed || q.id != 0 {
			continue
		}
		if !fromSelf && !(q.wantAck && q.to == from) {
			continue
		}
		h.echoQueue = append(h.echoQueue[:i], h.echoQueue[i+1:]...)
		q.echoed = true
		q.id = rid
		h.pending[rid] = q
		return q
	}
	return nil
}

// handleRoutingAck processes a ROUTING_APP frame: the recipient of a
// direct message answers with a routing packet whose request_id matches
// our packet id and whose error reason tells whether it was delivered;
// the device itself answers broadcasts with a self-addressed frame (the
// "sent" confirmation). The ack is bound to BOTH sides of the exchange:
// the request id (our packet id) and the expected recipient — a foreign
// node's routing frame never settles our wait. Modem acceptance and
// final delivery stay distinct: a self frame with an error reason means
// the modem accepted the message but could NOT deliver it.
//
// A self-addressed frame WITHOUT an error is the modem's acceptance of
// a DIRECT message, NOT its final result: the recipient's acknowledgment
// may still arrive and lift the status to delivered. The wait therefore
// STAYS registered (the settle window resolves it to failed when the
// recipient keeps silent), so a synthetic "own ack, then recipient ack"
// sequence still ends delivered. Broadcast confirmations and every
// error frame are final and consume the wait.
func (h *Hub) handleRoutingAck(pkt *pb.MeshPacket, decoded *pb.Data) {
	var routing pb.Routing
	if err := proto.Unmarshal(decoded.GetPayload(), &routing); err != nil {
		return
	}
	// Traceroute replies (route_reply / route_request) are not delivery
	// acks: fan them out to the waiting probes BEFORE the ack logic — a
	// route frame must never settle (or consume) a pending send.
	if rd := routing.GetRouteReply(); rd != nil {
		h.deliverRouteReply(pkt.GetFrom(), rd)
		return
	}
	if rd := routing.GetRouteRequest(); rd != nil {
		h.deliverRouteReply(pkt.GetFrom(), rd)
		return
	}
	rid := decoded.GetRequestId()
	if rid == 0 {
		return
	}
	fromSelf := fmt.Sprintf("%08x", pkt.GetFrom()) == h.selfID()
	h.sendMu.Lock()
	ps := h.pending[rid]
	if ps != nil && !fromSelf && ps.to != pkt.GetFrom() {
		// A foreign node's routing frame (same request id, wrong
		// sender): never settles our exchange, never consumes it.
		h.sendMu.Unlock()
		return
	}
	if ps == nil {
		ps = h.pairByRoutingLocked(rid, pkt.GetFrom(), fromSelf)
	}
	if ps == nil {
		h.sendMu.Unlock()
		return
	}
	status := TxDelivered
	switch {
	case routing.GetErrorReason() != pb.Routing_NONE:
		status = TxFailed // the modem (self) or an intermediate node reports failed delivery
		ps.failReason = sendFailureReason(routing.GetErrorReason())
	case fromSelf:
		status = TxSent // modem accepted the frame (broadcast "sent" confirmation)
	}
	// A modem-accepted DIRECT message keeps its wait for the recipient's
	// ack — the settle window is the final result when none comes.
	// (Logging makes the occurrence of this sequence observable on the
	// live firmware: it is expected on some device builds and absent on
	// others — the keep is correct either way.)
	final := true
	if fromSelf && routing.GetErrorReason() == pb.Routing_NONE && ps.wantAck && !ps.finalOnModemAck() {
		final = false
		if h.logger != nil {
			h.logger.Info("meshtastic: modem ack for direct message, recipient ack still expected",
				"request_id", rid, "to", fmt.Sprintf("%08x", ps.to))
		}
	}
	if final {
		delete(h.pending, rid)
	}
	h.sendMu.Unlock()
	h.markStatus(ps, status)
	if status == TxFailed && !ps.probe && needsNodeInfoRefresh(routing.GetErrorReason()) {
		h.recoverFromKeyRefusal(ps)
	}
}

// recoverFromKeyRefusal handles the one refusal a NodeInfo refresh can cure —
// the peer's public key is missing or unusable, so the frame never entered the
// air. BOTH paths that report a refusal (the device's queueStatus answer and a
// ROUTING_APP frame carrying the error) come through here, so neither can end
// up asking for the key without also remembering the message that needs it:
// without the wait the key would arrive and the operator's text would stay
// failed for good.
//
// The retry limit lives in awaitPeerKey (a message already re-sent once never
// arms another wait), so the recovery stays "one NodeInfo ask, one re-send".
func (h *Hub) recoverFromKeyRefusal(ps *pendingSend) {
	h.awaitPeerKey(ps)
	h.requestNodeInfo(ps.to)
}

// tracerouteHopLimit is the hop budget of one route probe: far above
// the device default (3) so the probe survives long relay chains.
const tracerouteHopLimit = 7

// tracerouteProbeGap spaces the probe re-sends (one every gap while
// nothing comes back) and tracerouteExtraProbes is how many re-sends
// follow the first probe: silence is loss, not a dead route, and a
// busy mesh drops a lot of single probes.
const (
	tracerouteProbeGap    = 12 * time.Second
	tracerouteExtraProbes = 2
)

// Traceroute probes the radio path to addr (8-hex node id): the device
// sends a TRACEROUTE_APP frame and the nodes along the way answer with
// route-discovery frames. The call waits up to timeout for the best
// answer — the destination's own reply wins and is returned immediately;
// otherwise the longest partial route heard is returned with
// Reached=false. No reply at all yields an empty result, not an error.
func (h *Hub) Traceroute(ctx context.Context, addr string, timeout time.Duration) (TracerouteResult, error) {
	idStr := normalizeMeshID(addr)
	if len(idStr) != 8 {
		return TracerouteResult{}, fmt.Errorf("meshtastic: node id must be 8 hex characters, got %q", addr)
	}
	dest, err := strconv.ParseUint(idStr, 16, 32)
	if err != nil {
		return TracerouteResult{}, fmt.Errorf("meshtastic: node id must be 8 hex characters, got %q", addr)
	}
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	h.mu.Lock()
	conn := h.conn
	h.mu.Unlock()
	if conn == nil || !conn.IsConnected() {
		return TracerouteResult{}, errDeviceNotConnected
	}

	// The probe payload is an EMPTY RouteDiscovery protobuf (the firmware
	// 2.x format): every relay appends its id + SNR and the destination
	// answers with the accumulated route. The legacy 4-byte destination
	// payload only ever produced an empty route from direct neighbours.
	payload, err := proto.Marshal(&pb.RouteDiscovery{})
	if err != nil {
		return TracerouteResult{}, fmt.Errorf("meshtastic: traceroute payload: %w", err)
	}

	ch := h.registerRouteWaiter(uint32(dest))
	defer h.unregisterRouteWaiter(uint32(dest), ch)
	h.beginTrace(uint32(dest), idStr, timeout)
	defer h.endTrace(uint32(dest))

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	probes := make([]*pendingSend, 0, 2)
	defer func() {
		h.sendMu.Lock()
		for _, p := range probes {
			h.dropPendingLocked(p)
		}
		h.sendMu.Unlock()
	}()
	send := func() error {
		// The session goroutine swaps h.conn on reconnect: refetch it
		// for every attempt so a retry rides the fresh TCP session.
		h.mu.Lock()
		c := h.conn
		h.mu.Unlock()
		if c == nil || !c.IsConnected() {
			return errDeviceNotConnected
		}
		// Each probe rides the send path as a placeholder so its ROUTING
		// ack pairs up like any other send.
		p := &pendingSend{
			to:      uint32(dest),
			at:      time.Now(),
			lastTry: time.Now(),
			text:    "\x00traceroute", // never matches a history row
			wantAck: true,
			probe:   true,
		}
		h.sendMu.Lock()
		h.echoQueue = append(h.echoQueue, p)
		h.sendMu.Unlock()
		if err := c.SendToRadio(&pb.ToRadio{PayloadVariant: &pb.ToRadio_Packet{Packet: &pb.MeshPacket{
			To:      uint32(dest),
			WantAck: true,
			// Reach multi-hop nodes: the device default (3) dies en route to
			// nodes 4+ hops away and nobody ever answers the probe.
			HopLimit: tracerouteHopLimit,
			PayloadVariant: &pb.MeshPacket_Decoded{
				Decoded: &pb.Data{Portnum: pb.PortNum_TRACEROUTE_APP, Payload: payload, WantResponse: true},
			},
		}}}); err != nil {
			return fmt.Errorf("meshtastic: traceroute send failed: %w", err)
		}
		probes = append(probes, p)
		h.traceSent(uint32(dest))
		if h.logger != nil {
			h.logger.Info("meshtastic: traceroute probe sent", "to", idStr, "timeout", timeout)
		}
		return nil
	}
	// The TCP session to the device can die between the connectivity
	// check and the write (broken pipe) — the session watchdog then
	// reconnects within seconds. Retry briefly instead of failing the
	// probe on a stale transport.
	sendErr := send()
	for i := 0; sendErr != nil && i < 3 && ctx.Err() == nil; i++ {
		if h.logger != nil {
			h.logger.Warn("meshtastic: traceroute send failed, retrying", "to", idStr, "error", sendErr)
		}
		sel := time.NewTimer(2 * time.Second)
		select {
		case <-ctx.Done():
			sel.Stop()
			return TracerouteResult{}, sendErr
		case <-sel.C:
		}
		sendErr = send()
	}
	if sendErr != nil {
		return TracerouteResult{}, sendErr
	}

	reprobe := time.NewTimer(tracerouteProbeGap)
	defer reprobe.Stop()
	reprobesLeft := tracerouteExtraProbes
	started := time.Now()
	best := TracerouteResult{}
	for {
		select {
		case <-ctx.Done():
			best.ElapsedMS = time.Since(started).Milliseconds()
			return best, nil
		case <-reprobe.C:
			// Re-probe while silence persists (lossy mesh, busy
			// channel): each re-send restarts the firmware's own
			// want_ack retransmission cycle.
			if reprobesLeft > 0 {
				reprobesLeft--
				if err := send(); err != nil && h.logger != nil {
					h.logger.Warn("meshtastic: traceroute re-probe failed", "to", idStr, "error", err)
				}
				reprobe.Reset(tracerouteProbeGap)
			}
		case rep := <-ch:
			res := h.routeResult(rep)
			if rep.from == uint32(dest) {
				res.Reached = true
				res.ElapsedMS = time.Since(started).Milliseconds()
				h.traceHeard(uint32(dest), res, true)
				return res, nil
			}
			if len(res.Hops) > len(best.Hops) {
				best = res
			}
			h.traceHeard(uint32(dest), res, false)
		}
	}
}

// beginTrace records a probe that is going on the air, so the panel can
// follow it. The previous record for the same destination (a probe that
// timed out a moment ago) is replaced.
func (h *Hub) beginTrace(dest uint32, id string, timeout time.Duration) {
	h.traceMu.Lock()
	defer h.traceMu.Unlock()
	if h.traces == nil {
		h.traces = make(map[uint32]*TraceProgress)
	}
	h.traces[dest] = &TraceProgress{
		Destination: id,
		Started:     time.Now(),
		Timeout:     timeout,
		MaxProbes:   1 + tracerouteExtraProbes,
	}
}

// traceSent counts one frame that went out for the running probe.
func (h *Hub) traceSent(dest uint32) {
	h.traceMu.Lock()
	defer h.traceMu.Unlock()
	if p := h.traces[dest]; p != nil {
		p.Probes++
	}
}

// traceHeard records what came back: the hops of the answering node's
// route (kept when it is longer than what is already there) and whether
// the destination itself answered.
func (h *Hub) traceHeard(dest uint32, res TracerouteResult, reached bool) {
	h.traceMu.Lock()
	defer h.traceMu.Unlock()
	p := h.traces[dest]
	if p == nil {
		return
	}
	if reached || len(res.Hops) > len(p.Hops) {
		p.Hops = res.Hops
	}
	if reached {
		p.Reached = true
	}
}

// endTrace drops the record once the probe settled.
func (h *Hub) endTrace(dest uint32) {
	h.traceMu.Lock()
	defer h.traceMu.Unlock()
	delete(h.traces, dest)
}

// TracerouteProgress reports the route probe currently on the air for
// addr, if any: elapsed time, frames sent so far and the hops heard up to
// this moment. ok is false when no probe to that node is running.
func (h *Hub) TracerouteProgress(addr string) (TraceProgress, bool) {
	id := normalizeMeshID(addr)
	if len(id) != 8 {
		return TraceProgress{}, false
	}
	dest, err := strconv.ParseUint(id, 16, 32)
	if err != nil {
		return TraceProgress{}, false
	}
	h.traceMu.Lock()
	defer h.traceMu.Unlock()
	p := h.traces[uint32(dest)]
	if p == nil {
		return TraceProgress{}, false
	}
	out := *p
	out.Hops = append([]TracerouteHop(nil), p.Hops...)
	return out, true
}

// registerRouteWaiter subscribes one probe to route replies from dest.
func (h *Hub) registerRouteWaiter(dest uint32) chan routeReply {
	ch := make(chan routeReply, 4)
	h.sendMu.Lock()
	h.routeWaiters[dest] = append(h.routeWaiters[dest], ch)
	h.sendMu.Unlock()
	return ch
}

// unregisterRouteWaiter removes the probe's subscription (idempotent).
func (h *Hub) unregisterRouteWaiter(dest uint32, ch chan routeReply) {
	h.sendMu.Lock()
	list := h.routeWaiters[dest]
	for i, c := range list {
		if c == ch {
			h.routeWaiters[dest] = append(list[:i], list[i+1:]...)
			break
		}
	}
	h.sendMu.Unlock()
}

// deliverRouteReply fans one route-discovery frame out to every probe
// waiting on the replying node (buffered, non-blocking).
func (h *Hub) deliverRouteReply(from uint32, rd *pb.RouteDiscovery) {
	h.sendMu.Lock()
	chans := h.routeWaiters[from]
	if len(chans) > 0 {
		delete(h.routeWaiters, from)
	}
	h.sendMu.Unlock()
	if len(chans) == 0 {
		// Nobody waits on this node: log the orphan so route frames
		// that arrive after the probe window stay observable.
		if h.logger != nil {
			h.logger.Debug("meshtastic: traceroute reply without a waiter", "from", fmt.Sprintf("%08x", from))
		}
		return
	}
	if h.logger != nil {
		h.logger.Info("meshtastic: traceroute reply", "from", fmt.Sprintf("%08x", from), "hops", len(rd.GetRoute()))
	}
	for _, ch := range chans {
		select {
		case ch <- routeReply{from: from, route: rd}:
		default:
		}
	}
}

// handleTracerouteReply processes a route discovery delivered on the
// TRACEROUTE_APP port (see the handlePacket switch).
func (h *Hub) handleTracerouteReply(pkt *pb.MeshPacket, decoded *pb.Data) {
	var rd pb.RouteDiscovery
	if err := proto.Unmarshal(decoded.GetPayload(), &rd); err != nil {
		return
	}
	h.deliverRouteReply(pkt.GetFrom(), &rd)
}

// routeResult converts one route-discovery answer into the hop list from
// us to the answering node. The protocol stores SNR values scaled 4x;
// hop names resolve through the heard-node directory when known.
func (h *Hub) routeResult(rep routeReply) TracerouteResult {
	res := TracerouteResult{}
	route := rep.route.GetRoute()
	snrs := rep.route.GetSnrBack()
	if len(snrs) == 0 {
		snrs = rep.route.GetSnrTowards()
	}
	// Name lookups happen under the lock: the node directory is mutated
	// by the session goroutine while probes read it here.
	h.mu.Lock()
	for i, id := range route {
		hop := TracerouteHop{ID: fmt.Sprintf("%08x", id)}
		if n := h.nodes[hop.ID]; n != nil && n.Name != "" {
			hop.Name = n.Name
		}
		if i < len(snrs) {
			hop.SNR = float32(snrs[i]) / 4
		}
		res.Hops = append(res.Hops, hop)
	}
	// The answering node closes the route (mirrors ParseRoute's
	// sender → intermediates → receiver shape) — resolve its name too.
	last := fmt.Sprintf("%08x", rep.from)
	if len(res.Hops) == 0 || res.Hops[len(res.Hops)-1].ID != last {
		hop := TracerouteHop{ID: last}
		if n := h.nodes[last]; n != nil && n.Name != "" {
			hop.Name = n.Name
		}
		res.Hops = append(res.Hops, hop)
	}
	h.mu.Unlock()
	return res
}

// retryPending settles or re-transmits sends that got no answer:
//
//   - Once the device has CONFIRMED the send (queueStatus/echo paired the
//     packet id), the firmware owns the transmission — the mesh layer
//     retransmits want_ack frames itself, so a client-side resend would
//     only duplicate the message. Such sends just wait dmAckSettle for
//     the recipient's acknowledgment and settle failed without it.
//   - Sends the device never confirmed (no id known) were possibly never
//     queued, so they are re-transmitted up to dmMaxRetries, but only
//     while the device is demonstrably alive: retrying into radio
//     silence multiplies duplicates when the device was in fact
//     transmitting (the session watchdog reconnects it instead).
func (h *Hub) retryPending(now time.Time) {
	if now.Sub(time.Unix(0, h.lastRadio.Load())) > 30*time.Second {
		return // device silent: the session watchdog will reconnect
	}
	h.sendMu.Lock()
	var resend []*pendingSend
	for id, ps := range h.pending {
		if !ps.wantAck || ps.status != TxSent || now.Sub(ps.lastTry) < h.cfg.AckSettleTimeout {
			continue
		}
		delete(h.pending, id)
		// The device transmitted the frame and the firmware's own
		// retransmissions are spent: the RECIPIENT never acknowledged it.
		// Record that verdict — a failed row without a reason tells the
		// operator nothing about where the message stopped.
		ps.failReason = "no_response"
		if h.logger != nil {
			h.logger.Warn("meshtastic: no acknowledgment from the recipient, giving up the message",
				"id", ps.id, "to", fmt.Sprintf("%08x", ps.to), "waited", now.Sub(ps.lastTry).Round(time.Second))
		}
		h.markStatusLocked(ps, TxFailed)
	}
	// Unpaired direct messages whose id never arrived (the device may
	// have been busy) are retransmitted.
	for i := 0; i < len(h.echoQueue); {
		q := h.echoQueue[i]
		if q.probe {
			// A favorite change needs the answer that settles it: keep the
			// placeholder for the pairing window, then read silence as a
			// refusal (the star is not confirmed and the pass tries again).
			if q.favorite != nil && now.Sub(q.lastTry) < dmAckTimeout {
				i++
				continue
			}
			// Traceroute placeholders are never retransmitted: the
			// probe wait already timed out (the device may still
			// answer; the reply is dropped as an orphan).
			h.echoQueue = append(h.echoQueue[:i], h.echoQueue[i+1:]...)
			h.settleFavorite(q, TxFailed)
			continue
		}
		if !q.echoed && !q.wantAck && now.Sub(q.lastTry) > 5*time.Minute {
			h.echoQueue = append(h.echoQueue[:i], h.echoQueue[i+1:]...)
			continue
		}
		if !q.wantAck || q.echoed || now.Sub(q.lastTry) < dmAckTimeout {
			i++
			continue
		}
		h.echoQueue = append(h.echoQueue[:i], h.echoQueue[i+1:]...)
		if q.retries >= dmMaxRetries {
			// The device never assigned the frame an id even after the
			// retries: it may never have left the radio at all. That is a
			// DIFFERENT verdict from an unacknowledged delivery, so it
			// carries its own reason.
			q.failReason = "not_confirmed"
			if h.logger != nil {
				h.logger.Warn("meshtastic: the device never confirmed the send, giving up the message",
					"to", fmt.Sprintf("%08x", q.to), "retries", q.retries)
			}
			h.markStatusLocked(q, TxFailed)
			continue
		}
		q.retries++
		resend = append(resend, q)
	}
	h.sendMu.Unlock()
	for _, ps := range resend {
		h.resend(ps)
	}
}

// resend transmits one retry of a pending direct message.
func (h *Hub) resend(ps *pendingSend) {
	h.mu.Lock()
	conn := h.conn
	h.mu.Unlock()
	if conn == nil || !conn.IsConnected() {
		h.dropPending(ps)
		return
	}
	pkt := &pb.MeshPacket{
		To:      ps.to,
		WantAck: true,
		PayloadVariant: &pb.MeshPacket_Decoded{
			Decoded: &pb.Data{
				Portnum: pb.PortNum_TEXT_MESSAGE_APP,
				Payload: []byte(ps.text),
			},
		},
	}
	h.sendMu.Lock()
	ps.lastTry = time.Now()
	h.echoQueue = append(h.echoQueue, ps)
	h.sendMu.Unlock()
	if err := conn.SendToRadio(&pb.ToRadio{PayloadVariant: &pb.ToRadio_Packet{Packet: pkt}}); err != nil {
		h.dropPending(ps)
	}
}

// dropPending removes a never-transmitted send from the queues.
func (h *Hub) dropPending(ps *pendingSend) {
	h.sendMu.Lock()
	h.dropPendingLocked(ps)
	h.sendMu.Unlock()
}

// dropPendingLocked assumes sendMu is already held.
func (h *Hub) dropPendingLocked(ps *pendingSend) {
	if ps.id != 0 {
		delete(h.pending, ps.id)
	}
	for i, q := range h.echoQueue {
		if q == ps {
			h.echoQueue = append(h.echoQueue[:i], h.echoQueue[i+1:]...)
			break
		}
	}
}

// markStatus stamps one pending send's delivery state into the durable
// history — and mirrors it into the action-progress ledger: the progress
// entry is recorded once the radio actually transmitted (sent, or
// delivered on the recipient's ack) and REVOKED when the transmission
// ultimately failed, so a later group retry sends the recipient again.
// The failure additionally sets the durable failure marker and fires the
// re-arm sink: the delivery job (already settled as accepted by the
// worker) is revived for a retry within the attempt budget.
func (h *Hub) markStatus(ps *pendingSend, status string) {
	h.sendMu.Lock()
	defer h.sendMu.Unlock()
	h.markStatusLocked(ps, status)
}

// markStatusLocked assumes sendMu is already held. It exists because the
// send bookkeeping fields (status, failReason) are touched from two
// goroutines: the packet path settles an acknowledgment while the
// maintenance loop settles a timeout, and both read the OTHER's field.
func (h *Hub) markStatusLocked(ps *pendingSend, status string) {
	ps.status = status
	// The verdict on a favorite frame is hub bookkeeping and must land even
	// when no message history is attached (which is how the tests run).
	h.settleFavorite(ps, status)
	h.mu.Lock()
	rec := h.recorder
	h.mu.Unlock()
	if rec == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rec.UpdateMeshtasticMessageStatus(ctx, status, ps.failReason, ps.at, ps.text); err != nil && h.logger != nil {
		h.logger.Warn("meshtastic: message status update failed", "status", status, "error", err)
	}
	if !ps.prog.active() {
		return
	}
	switch status {
	case TxSent, TxDelivered:
		if err := rec.RecordMeshActionProgress(ctx, ps.prog.Publisher, ps.prog.EventKey, ps.prog.ChangeID, ps.prog.recipient, ps.prog.channel, time.Now()); err != nil && h.logger != nil {
			h.logger.Warn("meshtastic: action progress record failed", "error", err)
		}
		if err := rec.SetMeshActionFailed(ctx, ps.prog.ActionID, ps.prog.GroupID, ps.prog.DedupKey, ps.prog.Publisher, ps.prog.EventKey, ps.prog.ChangeID, ps.prog.recipient, ps.prog.channel, false); err != nil && h.logger != nil {
			h.logger.Warn("meshtastic: action failure marker clear failed", "error", err)
		}
	case TxFailed:
		// ONE atomic recorder call: progress revoked + marker recorded +
		// the settled job re-armed in a single transaction, so a crash
		// can never leave an accepted job with a durable marker and no
		// retry (reported P1) — and the concrete job identity keeps
		// other groups' jobs of the same action untouched (reported P2).
		if _, err := rec.RecordMeshFailureAndRequeue(ctx, ps.prog.ActionID, ps.prog.GroupID, ps.prog.DedupKey, ps.prog.Publisher, ps.prog.EventKey, ps.prog.ChangeID, ps.prog.recipient, ps.prog.channel, time.Now()); err != nil && h.logger != nil {
			h.logger.Warn("meshtastic: action failure record + re-arm failed", "error", err)
		}
	}
}

// packetHops reports how many mesh hops the packet already travelled:
// the sender's hop_start minus the remaining hop_limit. Direct packets
// arrive with hop_start == hop_limit (0 hops).
func packetHops(pkt *pb.MeshPacket) int {
	h := int(pkt.GetHopStart()) - int(pkt.GetHopLimit())
	if h < 0 {
		return 0
	}
	return h
}

// recordSignal marks one observed packet kind on the sender's node and
// refreshes its last-seen time (any packet means the node is alive).
// The node is created when it is not in the directory yet.
func (h *Hub) recordSignal(pkt *pb.MeshPacket, signal string) {
	id := fmt.Sprintf("%08x", pkt.GetFrom())
	h.mu.Lock()
	if h.self != nil && id == h.self.ID {
		h.mu.Unlock()
		return
	}
	n := h.nodes[id]
	if n == nil {
		n = &Node{ID: id}
		h.nodes[id] = n
		h.nodesDirty = true
	}
	if !slices.Contains(n.Sends, signal) {
		n.Sends = append(n.Sends, signal)
		h.nodesDirty = true
	}
	n.Hops = packetHops(pkt)
	n.LastSeen = time.Now()
	h.mu.Unlock()
}

// cloneTelemetry deep-copies one telemetry snapshot. Publishers and
// snapshots marshal it without the hub lock, so later telemetry updates
// must never race them through the shared pointer.
func cloneTelemetry(t *Telemetry) *Telemetry {
	if t == nil {
		return nil
	}
	c := *t
	c.PowerVoltage = append([]float32(nil), t.PowerVoltage...)
	c.PowerCurrent = append([]float32(nil), t.PowerCurrent...)
	return &c
}

// receiveTelemetry applies one TELEMETRY_APP broadcast to the sender's
// node: the signal badge, the full metric set (device, environment,
// air-quality, power) and the refreshed last-seen time. The node is
// created when it is not in the directory yet.
func (h *Hub) receiveTelemetry(pkt *pb.MeshPacket, decoded *pb.Data) {
	tele := &pb.Telemetry{}
	if err := proto.Unmarshal(decoded.GetPayload(), tele); err != nil {
		// Undecodable telemetry still proves the node broadcasts it.
		h.recordSignal(pkt, SignalTelemetry)
		return
	}
	id := fmt.Sprintf("%08x", pkt.GetFrom())
	h.mu.Lock()
	if h.self != nil && id == h.self.ID {
		h.mu.Unlock()
		return
	}
	n := h.nodes[id]
	if n == nil {
		n = &Node{ID: id}
		h.nodes[id] = n
		h.nodesDirty = true
	}
	if !slices.Contains(n.Sends, SignalTelemetry) {
		n.Sends = append(n.Sends, SignalTelemetry)
		h.nodesDirty = true
	}
	if n.Telemetry == nil {
		n.Telemetry = &Telemetry{}
	}
	t := n.Telemetry
	if m := tele.GetDeviceMetrics(); m != nil {
		t.BatteryLevel = m.GetBatteryLevel()
		t.Voltage = m.GetVoltage()
		t.ChannelUtil = m.GetChannelUtilization()
		t.AirUtilTx = m.GetAirUtilTx()
		t.UptimeSecs = m.GetUptimeSeconds()
	}
	if m := tele.GetEnvironmentMetrics(); m != nil {
		t.Temperature = m.GetTemperature()
		t.Humidity = m.GetRelativeHumidity()
		t.Pressure = m.GetBarometricPressure()
		t.GasResist = m.GetGasResistance()
		t.EnvVoltage = m.GetVoltage()
		t.EnvCurrent = m.GetCurrent()
		t.IAQ = m.GetIaq()
		t.Lux = m.GetLux()
		t.WhiteLux = m.GetWhiteLux()
		t.IRLux = m.GetIrLux()
		t.UVLux = m.GetUvLux()
		t.WindDir = m.GetWindDirection()
		t.WindSpeed = m.GetWindSpeed()
		t.WindGust = m.GetWindGust()
		t.WindLull = m.GetWindLull()
		t.Weight = m.GetWeight()
		t.Radiation = m.GetRadiation()
		t.Rainfall1H = m.GetRainfall_1H()
		t.Rainfall24H = m.GetRainfall_24H()
		t.SoilMoist = m.GetSoilMoisture()
		t.SoilTemp = m.GetSoilTemperature()
	}
	if m := tele.GetAirQualityMetrics(); m != nil {
		t.PM10Std = m.GetPm10Standard()
		t.PM25Std = m.GetPm25Standard()
		t.PM100Std = m.GetPm100Standard()
		t.PM40Std = m.GetPm40Standard()
		t.PM10Env = m.GetPm10Environmental()
		t.PM25Env = m.GetPm25Environmental()
		t.PM100Env = m.GetPm100Environmental()
		t.Particles03 = m.GetParticles_03Um()
		t.Particles05 = m.GetParticles_05Um()
		t.Particles10 = m.GetParticles_10Um()
		t.Particles25 = m.GetParticles_25Um()
		t.Particles40 = m.GetParticles_40Um()
		t.Particles50 = m.GetParticles_50Um()
		t.Particles100 = m.GetParticles_100Um()
		t.ParticlesTPS = m.GetParticlesTps()
		t.CO2 = m.GetCo2()
		t.CO2Temp = m.GetCo2Temperature()
		t.CO2Hum = m.GetCo2Humidity()
		t.FormForm = m.GetFormFormaldehyde()
		t.FormHum = m.GetFormHumidity()
		t.FormTemp = m.GetFormTemperature()
		t.PMTemp = m.GetPmTemperature()
		t.PMHum = m.GetPmHumidity()
		t.PMVOCIdx = m.GetPmVocIdx()
		t.PMNOxIdx = m.GetPmNoxIdx()
	}
	if m := tele.GetPowerMetrics(); m != nil {
		t.PowerVoltage = []float32{
			m.GetCh1Voltage(), m.GetCh2Voltage(), m.GetCh3Voltage(), m.GetCh4Voltage(),
			m.GetCh5Voltage(), m.GetCh6Voltage(), m.GetCh7Voltage(), m.GetCh8Voltage(),
		}
		t.PowerCurrent = []float32{
			m.GetCh1Current(), m.GetCh2Current(), m.GetCh3Current(), m.GetCh4Current(),
			m.GetCh5Current(), m.GetCh6Current(), m.GetCh7Current(), m.GetCh8Current(),
		}
	}
	t.At = time.Now()
	n.LastSeen = time.Now()
	n.Hops = packetHops(pkt)
	copyN := *n
	copyN.Telemetry = cloneTelemetry(n.Telemetry)
	h.mu.Unlock()
	h.publishStation(&copyN)
}

// receiveNodeInfo applies a nodeinfo broadcast (name only; positions
// arrive through POSITION_APP).
func (h *Hub) receiveNodeInfo(pkt *pb.MeshPacket, decoded *pb.Data) {
	user := &pb.User{}
	if err := proto.Unmarshal(decoded.GetPayload(), user); err != nil {
		return
	}
	id := fmt.Sprintf("%08x", pkt.GetFrom())
	name := strings.TrimSpace(user.GetLongName())
	if name == "" {
		name = strings.TrimSpace(user.GetShortName())
	}
	h.mu.Lock()
	if h.self != nil && id == h.self.ID {
		if name != "" {
			h.self.LongName = name
		}
		h.mu.Unlock()
		return
	}
	// Remember whether this announcement carried a public key: it is what
	// decides whether the radio can encrypt a direct message to that node,
	// and the panel shows it next to the name.
	asked := h.askedForNodeInfoLocked(pkt.GetFrom())
	if h.peerKeys == nil {
		h.peerKeys = make(map[uint32]bool)
	}
	if len(user.GetPublicKey()) > 0 {
		h.rememberPeerKeyLocked(pkt.GetFrom())
	} else {
		delete(h.peerKeys, pkt.GetFrom())
	}
	n := h.nodes[id]
	if n == nil {
		n = &Node{ID: id}
		h.nodes[id] = n
		h.nodesDirty = true
	}
	if name != "" {
		n.Name = name
	}
	if sn := strings.TrimSpace(user.GetShortName()); sn != "" {
		n.Short = sn
	}
	n.LastSeen = time.Now()
	n.Hops = packetHops(pkt)
	copyN := *n
	copyN.Telemetry = cloneTelemetry(n.Telemetry)
	h.mu.Unlock()
	// A NodeInfo is also how the radio learns the peer key it needs to
	// encrypt direct messages: when one was refused for a missing key, this
	// is the answer that refusal was waiting for.
	if len(user.GetPublicKey()) > 0 {
		// Every message this peer owes an answer to goes out again: the key
		// just arrived, and each of them was refused for missing exactly it.
		for _, kr := range h.takeKeyRetry(pkt.GetFrom()) {
			h.retryAfterKey(kr)
		}
	} else if asked && h.logger != nil {
		// The station asked THIS peer for its NodeInfo and the answer
		// carried no key: a direct message to it stays impossible until
		// its firmware starts announcing one, so do not send the operator
		// hunting for a route problem.
		h.logger.Warn("meshtastic: the peer answered its NodeInfo without a public key, direct messages stay impossible",
			"node", id)
	}
	h.publishStation(&copyN)
}

// askedForNodeInfoLocked reports whether the station asked this peer for its
// NodeInfo recently (a refresh only lands once per nodeInfoRefreshGap, so a
// timestamp inside that gap marks the answer to OUR request).
func (h *Hub) askedForNodeInfoLocked(to uint32) bool {
	asked, ok := h.nodeInfoAsked[to]
	if !ok {
		return false
	}
	return h.clock().Sub(asked) <= nodeInfoRefreshGap
}

// rememberPeerKeyLocked records that one node announced a public key (h.mu is
// held). The mark is UI evidence, not a decision: the radio keeps its own node
// table, and the whole map is rebuilt from that table at every handshake — so
// under pressure the hub prefers dropping what it cannot show (the marks of
// nodes the directory no longer holds) and then stops remembering new ones,
// counting them instead of growing without a bound.
func (h *Hub) rememberPeerKeyLocked(id uint32) {
	if h.peerKeys == nil {
		h.peerKeys = make(map[uint32]bool)
	}
	if h.peerKeys[id] {
		return
	}
	if cap := h.maxPeerKeys(); len(h.peerKeys) >= cap {
		for other := range h.peerKeys {
			if h.nodes[meshIDString(other)] == nil {
				delete(h.peerKeys, other)
			}
		}
		if len(h.peerKeys) >= cap {
			h.peerKeysSkipped++
			return
		}
	}
	h.peerKeys[id] = true
}

// maxPeerKeys is the cap on the key marks (a field so tests can shrink it).
func (h *Hub) maxPeerKeys() int {
	if h.peerKeysMax > 0 {
		return h.peerKeysMax
	}
	return maxPeerKeyMarks
}

// sweepPeerState drops the per-peer bookkeeping that has outlived what it
// describes, and reports the pressure the key-mark cap caused:
//
//   - nodeInfoAsked: the refresh gap has passed, so the entry rate-limits
//     nothing (the next refusal may ask again). That expiry is what bounds the
//     map — it holds at most the peers asked within one gap window.
//   - peerKeys is bounded by the node directory (a mark is dropped with its
//     node, see expireNodes) and by the cap; what the cap refused to remember
//     is counted and reported HERE, so the log carries it once per pass
//     instead of one line per announcement.
func (h *Hub) sweepPeerState(now time.Time) {
	h.mu.Lock()
	for id, at := range h.nodeInfoAsked {
		if now.Sub(at) >= nodeInfoRefreshGap {
			delete(h.nodeInfoAsked, id)
		}
	}
	// The cap is a safety net, not the usual bound: entries live six hours, so
	// the map is already bounded by the peers asked within one gap. Under real
	// pressure the OLDEST questions go first — they are the closest to their
	// own expiry — because the alternative (refusing to remember a question)
	// would let that peer be asked again on every refusal.
	expired := 0
	for len(h.nodeInfoAsked) > h.maxNodeInfoAsked() {
		var oldest uint32
		var oldestAt time.Time
		for id, at := range h.nodeInfoAsked {
			if oldestAt.IsZero() || at.Before(oldestAt) {
				oldest, oldestAt = id, at
			}
		}
		delete(h.nodeInfoAsked, oldest)
		expired++
	}
	skipped, marks, asked := h.peerKeysSkipped, len(h.peerKeys), len(h.nodeInfoAsked)
	h.peerKeysSkipped = 0
	h.mu.Unlock()
	if skipped > 0 && h.logger != nil {
		h.logger.Warn("meshtastic: peer key marks are at the cap, new ones are not remembered",
			"skipped", skipped, "cap", h.maxPeerKeys(), "marks", marks, "refreshes_waiting", asked)
	}
	if expired > 0 && h.logger != nil {
		h.logger.Warn("meshtastic: too many peers wait for their node info, the oldest questions are forgotten",
			"forgotten", expired, "cap", h.maxNodeInfoAsked(), "waiting", asked)
	}
}

// maxNodeInfoAsked caps the refresh cooldown map (a field so tests can shrink
// it).
func (h *Hub) maxNodeInfoAsked() int {
	if h.nodeInfoAskedMax > 0 {
		return h.nodeInfoAskedMax
	}
	return maxNodeInfoRefreshEntries
}

// receivePosition updates a node's position (and our own).
func (h *Hub) receivePosition(pkt *pb.MeshPacket, decoded *pb.Data) {
	pos := &pb.Position{}
	if err := proto.Unmarshal(decoded.GetPayload(), pos); err != nil {
		return
	}
	lat, lon := positionDeg(pos)
	if lat == 0 && lon == 0 {
		return
	}
	id := fmt.Sprintf("%08x", pkt.GetFrom())
	h.mu.Lock()
	if h.self != nil && id == h.self.ID {
		h.self.Lat, h.self.Lon = lat, lon
		h.mu.Unlock()
		return
	}
	n := h.nodes[id]
	if n == nil {
		n = &Node{ID: id}
		h.nodes[id] = n
		h.nodesDirty = true
	}
	n.Lat, n.Lon = lat, lon
	n.LastSeen = time.Now()
	n.Hops = packetHops(pkt)
	if !slices.Contains(n.Sends, SignalPosition) {
		n.Sends = append(n.Sends, SignalPosition)
		h.nodesDirty = true
	}
	copyN := *n
	copyN.Telemetry = cloneTelemetry(n.Telemetry)
	h.mu.Unlock()
	h.publishStation(&copyN)
}

// receiveText records an incoming text message and bridges direct
// messages from directory-known senders into the alarm pipeline.
func (h *Hub) receiveText(pkt *pb.MeshPacket, decoded *pb.Data) {
	id := fmt.Sprintf("%08x", pkt.GetFrom())
	h.mu.Lock()
	selfID := ""
	if h.self != nil {
		selfID = h.self.ID
	}
	h.mu.Unlock()
	if selfID != "" && id == selfID {
		return // our own transmission echoed by the radio
	}
	h.recordSignal(pkt, SignalText)
	channel := h.channelNameFor(pkt)
	// Hops travelled (hop_start - hop_limit), matching the node
	// directory and the map pins; hop_start alone overstated direct
	// packets (the device echoes them with the full budget left).
	h.recordMessage("rx", id, "", channel, string(decoded.GetPayload()), "", packetHops(pkt), time.Now())

	// Only messages addressed EXACTLY to our node route (broadcasts,
	// To=0 packets and third-party traffic stay on the feed): the gate
	// resolves the sender's registered directory user; unknown senders
	// stay off the alarm pipeline. The address match is exact on
	// purpose — a To=0 packet must never reach the command interpreter.
	h.mu.Lock()
	selfID = ""
	if h.self != nil {
		selfID = h.self.ID
	}
	gate := h.senderGate
	eventSink := h.eventSink
	cli := h.cli
	h.mu.Unlock()
	if gate == nil || eventSink == nil || selfID == "" {
		return
	}
	if fmt.Sprintf("%08x", pkt.GetTo()) != selfID {
		return // not addressed exactly to us
	}
	owner := gate(id)
	text := string(decoded.GetPayload())

	// Radio CLI: slash-prefixed messages are command attempts and never
	// become alarms on their own. Public commands (/help) run for
	// everyone; restricted commands (/alert) run only for
	// directory-registered senders — the shared interpreter decides.
	// The same packet delivered again by the mesh (rebroadcast, router
	// retry) re-sends the previous reply instead of executing the
	// command again.
	if cli != nil && strings.HasPrefix(strings.TrimSpace(text), "/") {
		mid := commandIdentity(pkt, text)
		cid := id + ":" + mid
		now := time.Now()
		h.mu.Lock()
		rec := h.cmds[cid]
		fresh := rec != nil && now.Sub(rec.at) < cmdDedupWindow
		reply := ""
		retry := false
		if fresh {
			reply = rec.reply
			// A transiently failed command admits controlled retries:
			// after the cooldown a redelivery executes again instead of
			// replaying the stale failure.
			retry = rec.retryable && now.Sub(rec.at) >= h.cfg.CmdRetryCooldown
		}
		h.mu.Unlock()
		if fresh && !retry {
			if reply != "" && h.replyAllowed(id) {
				h.sendCLIReply(id, reply)
			}
			return
		}
		res := cli.Handle(text, radiocli.Sender{Authorized: owner != "", Name: owner})
		// The stored reply is the FINAL confirmation (post-acceptance),
		// so a redelivery replays exactly the previous result. Both
		// commands share the same confirmation path. A rejection is
		// transient: remembered as retryable instead of final. A durable
		// registry hit (stored result) proves the job executed before —
		// the redelivery replays the result and never re-executes it,
		// even across a restart.
		reply = res.Reply
		retryable := false
		if res.Alert != nil {
			ttl := res.Alert.TTL
			if ttl <= 0 {
				ttl = 4 * time.Hour
			}
			key := "meshtastic:" + id + ":alert:" + mid
			eff, exp, prev := h.resolveCommand(key, ttl)
			if prev != "" {
				reply = prev
			} else {
				acc := h.publishAlertEvent(id, owner, res.Alert, mid, eff, exp, res.Reply)
				reply = confirmation(res.Reply, "FAILED: alert rejected", acc)
				retryable = retryable || acc == dispatch.Rejected
			}
		}
		h.mu.Lock()
		h.pruneCmds(now)
		h.cmds[cid] = &cmdRecord{at: now, reply: reply, retryable: retryable}
		h.mu.Unlock()
		if res.Handled {
			if reply != "" && h.replyAllowed(id) {
				h.sendCLIReply(id, reply)
			}
		}
		return
	}
	// Plain (non-command) direct messages never become alarms: the
	// standard installation banner answers instead (rate-limited,
	// low-priority), and the message stays in the history and the
	// message feed.
	if cli != nil {
		h.queueBanner(id, cli.Banner())
	}
}

// replyAllowed reports whether ONE automatic reply may go out to the
// sender now. Every automatic answer — command confirmations,
// redelivery replays, /help, denials and banners — shares one per-sender
// burst window, so a flooding sender (or a stuck device repeating
// /unknown) cannot make the station chatter.
func (h *Hub) replyAllowed(id string) bool {
	now := time.Now()
	h.mu.Lock()
	defer h.mu.Unlock()
	w := h.replyLast[id]
	if now.Sub(w.start) >= h.cfg.ReplyBurstWindow {
		w = replyWindow{}
	}
	if w.count >= h.cfg.ReplyBurst {
		return false
	}
	if w.start.IsZero() {
		w.start = now
	}
	w.count++
	h.replyLast[id] = w
	if len(h.replyLast) > 256 {
		cutoff := now.Add(-h.cfg.ReplyBurstWindow)
		for k, rw := range h.replyLast {
			if rw.start.Before(cutoff) {
				delete(h.replyLast, k)
			}
		}
	}
	return true
}

// queueBanner enqueues one banner answer as low-priority traffic. The
// per-sender window stops answering bots from looping each other; the
// global pacing and the one-at-a-time drain keep the automatic-answer
// path behind direct alarm and command replies. The sender's window
// starts when the banner is queued, so dropped (coalesced) replies
// never reset it.
func (h *Hub) queueBanner(id, text string) {
	if !h.replyAllowed(id) {
		return
	}
	now := time.Now()
	h.mu.Lock()
	defer h.mu.Unlock()
	if last, ok := h.bannerLast[id]; ok && now.Sub(last) < h.cfg.BannerMinInterval {
		return // already answered recently
	}
	for _, r := range h.banners {
		if r.id == id {
			return // one queued banner per sender
		}
	}
	if len(h.banners) >= bannerQueueCap {
		return // bounded: drop under pressure
	}
	h.bannerLast[id] = now
	if len(h.bannerLast) > 4*bannerQueueCap {
		cutoff := now.Add(-h.cfg.BannerMinInterval)
		for k, at := range h.bannerLast {
			if at.Before(cutoff) {
				delete(h.bannerLast, k)
			}
		}
	}
	h.banners = append(h.banners, bannerReply{id: id, text: text})
}

// drainBanner sends at most one queued banner reply, paced by the global
// interval. It runs after inbound traffic and on the session tick, so
// banners strictly yield to direct replies sent while a packet is
// handled.
func (h *Hub) drainBanner() {
	now := time.Now()
	h.mu.Lock()
	if len(h.banners) == 0 || now.Before(h.bannerNext) {
		h.mu.Unlock()
		return
	}
	r := h.banners[0]
	h.banners = h.banners[1:]
	h.bannerNext = now.Add(h.cfg.BannerGlobalInterval)
	h.mu.Unlock()
	h.sendCLIReply(r.id, r.text)
}

// msgIdentity is the stable per-packet identity used for command
// deduplication: the device-assigned packet id when present, otherwise
// a short content digest — a redelivered copy of the same packet
// produces the same identity.
func msgIdentity(pkt *pb.MeshPacket, text string) string {
	if pid := pkt.GetId(); pid != 0 {
		return strconv.FormatUint(uint64(pid), 10)
	}
	return "h" + radiocli.ContentID(fmt.Sprintf("%08x", pkt.GetFrom()), text)
}

// commandIdentity is the dedup identity of one command packet: the
// packet identity ALWAYS followed by the content fingerprint. A reused
// packet id with different text is therefore a different command (a new
// alarm runs instead of replaying the old result), while a byte-identical
// redelivery maps to the same identity. The durable registry and the
// event key share this identity.
func commandIdentity(pkt *pb.MeshPacket, text string) string {
	return msgIdentity(pkt, text) + ":h" + radiocli.ContentID(fmt.Sprintf("%08x", pkt.GetFrom()), text)
}

// pruneCmds drops expired command records and bounds the map. The
// caller holds mu.
func (h *Hub) pruneCmds(now time.Time) {
	cutoff := now.Add(-cmdDedupWindow)
	for k, rec := range h.cmds {
		if rec.at.Before(cutoff) {
			delete(h.cmds, k)
		}
	}
	for len(h.cmds) > cmdDedupCap {
		var oldestK string
		var oldestAt time.Time
		for k, rec := range h.cmds {
			if oldestK == "" || rec.at.Before(oldestAt) {
				oldestK, oldestAt = k, rec.at
			}
		}
		delete(h.cmds, oldestK)
	}
}

// sendCLIReply answers one radio command with a direct message back to
// the sender (best-effort; the reply lands in the durable TX history
// with its delivery tracking). The reply is fitted to the mesh text
// limit through the shared CLI mechanism (identity shortens
// progressively, payload truncates last); multi-line replies (the
// /hazard list) send one message per line.
func (h *Hub) sendCLIReply(id, text string) {
	h.mu.Lock()
	cli := h.cli
	h.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	for _, line := range radiocli.ReplyLines(text, radiocli.MaxReplyLines) {
		if cli != nil {
			line = cli.Fit(line, meshTextMaxRunes)
		}
		if err := h.SendContactMessage(ctx, id, line, "system"); err != nil && h.logger != nil {
			h.logger.Warn("meshtastic: cli reply failed", "to", id, "error", err)
		}
	}
}

// channelNameFor resolves the display channel for a packet: the channel
// name by index, "dm" for direct packets.
func (h *Hub) channelNameFor(pkt *pb.MeshPacket) string {
	if pkt.GetTo() != core.BroadcastNodeID.Uint32() {
		return "dm"
	}
	return h.ChannelLabel(int(pkt.GetChannel()))
}

// ChannelLabel resolves a channel index onto its display name.
func (h *Hub) ChannelLabel(idx int) string {
	h.mu.Lock()
	defer h.mu.Unlock()
	if idx >= 0 && idx < len(h.channels) && h.channels[idx] != "" {
		return h.channels[idx]
	}
	return fmt.Sprintf("ch%d", idx)
}

// OwnChannel reports the installation's OWN channel — the configured
// announcement channel as the device holds it: its index, name and key.
// ok is false when the beacon channel is disabled, the device has not
// reported the table yet, or the channel carries no key; the Help page
// then renders no join card at all.
func (h *Hub) OwnChannel() (idx int, name string, key []byte, ok bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	idx = h.cfg.EmcomChannel
	if idx <= 0 || idx >= len(h.channels) {
		return 0, "", nil, false
	}
	name = h.channels[idx]
	if name == "" || idx >= len(h.channelKeys) || len(h.channelKeys[idx]) == 0 {
		return 0, "", nil, false
	}
	return idx, name, append([]byte(nil), h.channelKeys[idx]...), true
}

// ChannelShareURL renders one channel as the Meshtastic share link: the
// payload is a ChannelSet carrying just this channel (name + key), which
// the apps accept after the fragment — the same form the phone clients
// show when sharing a channel. Users scan it (or paste it) to join.
func ChannelShareURL(name string, key []byte) string {
	if strings.TrimSpace(name) == "" || len(key) == 0 {
		return ""
	}
	set := &pb.ChannelSet{
		Settings: []*pb.ChannelSettings{{Name: name, Psk: key}},
	}
	raw, err := proto.Marshal(set)
	if err != nil {
		return ""
	}
	frag := base64.RawURLEncoding.EncodeToString(raw)
	return "https://meshtastic.org/e/#" + frag
}

// resolveCommand returns the durable command-registry entry for one
// command key: the lifecycle anchor (effective / expires) and the
// recorded result of a previous durable acceptance. The resolver is
// the inbox-transaction half of the registry; on a miss (or without a
// resolver) the command starts its lifecycle fresh at now / now+ttl
// with no stored result.
func (h *Hub) resolveCommand(key string, ttl time.Duration) (eff, exp time.Time, result string) {
	h.mu.Lock()
	resolver := h.eventTimes
	h.mu.Unlock()
	if resolver != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		eff, exp, result, ok := resolver(ctx, key)
		cancel()
		if ok {
			return eff, exp, result
		}
	}
	eff = time.Now().UTC()
	return eff, eff.Add(ttl), ""
}

// confirmation maps the local acceptance of a command event onto the
// in-band confirmation: durable acceptance answers with the handler
// confirmation, the emergency fallback says so explicitly, and a
// rejection answers with the given failure text — never a false "OK".
func confirmation(base, failure string, acc dispatch.Acceptance) string {
	switch acc {
	case dispatch.Rejected:
		return failure
	case dispatch.AcceptedEmergency:
		if base != "" {
			return base + " (failover mode)"
		}
	}
	return base
}

// publishAlertEvent raises one operator-requested hazard (/alert) into
// the /events stream: severe, bounded by the requested TTL (4 hours by
// default). mid keeps the event identity stable across redeliveries
// (see publishRoutedEvent); lifecycle times come from the durable
// registry (resolveCommand) and the ChangeID derives from the content,
// so a retransmitted copy is byte-identical and deduplicates across
// restarts. The result travels on the wire document for the
// transactional inbox anchor. The returned acceptance reports how the
// LOCAL pipeline took the alert (durable, emergency or rejected); the
// confirmation reply must follow it. A broker failure alone never
// downgrades the local acceptance.
func (h *Hub) publishAlertEvent(senderID, owner string, spec *radiocli.AlertSpec, mid string, eff, exp time.Time, commandResult string) dispatch.Acceptance {
	if spec == nil {
		return dispatch.Rejected
	}
	h.mu.Lock()
	name := ""
	if n := h.nodes[senderID]; n != nil && n.Name != "" {
		name = n.Name
	}
	sink := h.eventSink
	acceptor := h.eventAcceptor
	h.mu.Unlock()
	sourceID := senderID + ":alert:" + mid
	nowS := eff.Format(time.RFC3339)
	expS := exp.Format(time.RFC3339)
	from := name
	if from == "" {
		from = "!" + senderID
	}
	we := MessageEventWire{
		SchemaVersion: meshMessageEventSchemaVersion,
		ChangeID:      radiocli.StableID("meshtastic:alert", sourceID, spec.Headline),
		ChangeType:    "new",
		EventKey:      "meshtastic:" + sourceID,
		CommandResult: commandResult,
		Event: MessageEventHazard{
			Source:      "meshtastic",
			SourceID:    sourceID,
			Category:    "meshtastic",
			Event:       "Meshtastic alert",
			Severity:    "severe",
			Urgency:     "immediate",
			Certainty:   "observed",
			Headline:    spec.Headline,
			Description: fmt.Sprintf("Alert raised by %s (%s) over the mesh.", owner, from),
			EffectiveAt: &nowS,
			ExpiresAt:   &expS,
			Areas:       []string{},
			Status:      "active",
			ReceivedAt:  nowS,
			UpdatedAt:   nowS,
		},
	}
	payload, err := json.Marshal(we)
	if err != nil {
		return dispatch.Rejected
	}
	if acceptor != nil {
		return acceptor(payload)
	}
	// Legacy path (no acceptor installed): best-effort through the
	// event sink; the hub cannot distinguish local acceptance here.
	if sink == nil {
		return dispatch.AcceptedDurable
	}
	if err := sink(context.Background(), "events", false, payload); err != nil && h.logger != nil {
		h.logger.Warn("meshtastic: alert event publish failed", "error", err)
	}
	return dispatch.AcceptedDurable
}

// ErrPrimaryChannelBlocked reports an attempt to broadcast on the
// default PRIMARY channel (channel 0): broadcasts there are disabled by
// policy — the primary channel is receive-only for this station.
var ErrPrimaryChannelBlocked = errors.New("meshtastic: broadcasts on the primary channel (0) are disabled")

// SendChannelMessage broadcasts one text message on the primary channel
// (channel 0) — blocked by policy: the station never transmits on the
// default public channel.
func (h *Hub) SendChannelMessage(ctx context.Context, text, operator string) error {
	return ErrPrimaryChannelBlocked
}

// SendChannelText broadcasts one text message on the given channel index
// (1-7) of the device channel table. Channel 0 (the default PRIMARY
// channel) is blocked by policy.
func (h *Hub) SendChannelText(ctx context.Context, idx int, text, operator string) error {
	if idx < 1 || idx > 7 {
		return ErrPrimaryChannelBlocked
	}
	return h.sendText(ctx, core.BroadcastNodeID.Uint32(), idx, h.ChannelLabel(idx), text, operator, ProgressRef{})
}

// SendChannelTextVersioned is SendChannelText with the durable
// action-progress identity attached: the ledger follows the transmission
// outcome (recorded on sent/delivered, revoked on failed).
func (h *Hub) SendChannelTextVersioned(ctx context.Context, idx int, text, operator string, prog ProgressRef) error {
	if idx < 1 || idx > 7 {
		return ErrPrimaryChannelBlocked
	}
	prog.recipient = ""
	prog.channel = idx
	return h.sendText(ctx, core.BroadcastNodeID.Uint32(), idx, h.ChannelLabel(idx), text, operator, prog)
}

// SendContactMessage sends one direct text message to a node id (8 hex
// chars, with or without the leading '!').
func (h *Hub) SendContactMessage(ctx context.Context, addr, text, operator string) error {
	return h.sendContact(ctx, addr, text, operator, ProgressRef{})
}

// SendContactMessageVersioned is SendContactMessage with the durable
// action-progress identity attached: the ledger follows the transmission
// outcome (recorded on sent/delivered, revoked on failed).
func (h *Hub) SendContactMessageVersioned(ctx context.Context, addr, text, operator string, prog ProgressRef) error {
	prog.recipient = normalizeMeshID(addr)
	prog.channel = 0
	return h.sendContact(ctx, addr, text, operator, prog)
}

func (h *Hub) sendContact(ctx context.Context, addr, text, operator string, prog ProgressRef) error {
	idStr := normalizeMeshID(addr)
	if len(idStr) != 8 {
		return fmt.Errorf("meshtastic: node id must be 8 hex characters, got %q", addr)
	}
	n, err := strconv.ParseUint(idStr, 16, 32)
	if err != nil {
		return fmt.Errorf("meshtastic: node id must be 8 hex characters, got %q", addr)
	}
	return h.sendText(ctx, uint32(n), 0, "dm", text, operator, prog)
}

// MeshActionProgressDone reports whether the exact versioned
// transmission (publisher + event key + change id + recipient + channel)
// already has a durable progress row: the resume ledger of the
// meshtastic action. It is INDEPENDENT of the message history — a
// historical delivered message with the same text never suppresses a
// NEW alert version (reported P1). With no recorder attached nothing
// can be proven and the query reports false (fail-open: a re-send is
// safer than a lost alert).
func (h *Hub) MeshActionProgressDone(ctx context.Context, publisher, eventKey string, changeID int64, recipient string, channel int) (bool, error) {
	h.mu.Lock()
	rec := h.recorder
	h.mu.Unlock()
	if rec == nil {
		return false, nil
	}
	return rec.MeshActionProgressDone(ctx, publisher, eventKey, changeID, normalizeMeshID(recipient), channel)
}

// RecordMeshActionProgress durably marks one successful transmission of
// the meshtastic action for the exact message version and recipient
// (empty recipient + channel index = the group broadcast). Idempotent;
// a missing recorder is a no-op (the ledger is best-effort, a failed
// write only costs a duplicate on retry — at-least-once).
func (h *Hub) RecordMeshActionProgress(ctx context.Context, publisher, eventKey string, changeID int64, recipient string, channel int) error {
	h.mu.Lock()
	rec := h.recorder
	h.mu.Unlock()
	if rec == nil {
		return nil
	}
	return rec.RecordMeshActionProgress(ctx, publisher, eventKey, changeID, normalizeMeshID(recipient), channel, time.Now())
}

// normalizeMeshID canonicalizes a node id the way SendContactMessage
// does (the progress ledger and the DM addressing must agree).
func normalizeMeshID(addr string) string {
	return strings.ToLower(strings.TrimPrefix(strings.TrimSpace(addr), "!"))
}

// requestReconnect asks the session goroutine to rebuild the device link
// at once: the current one is known dead (a send or a heartbeat could not
// write to it). Safe to call with no session running.
func (h *Hub) requestReconnect() {
	if h.reconnect == nil {
		return
	}
	select {
	case h.reconnect <- struct{}{}:
	default: // a rebuild is already pending
	}
}

// awaitConn returns the live device session, waiting up to wait for one
// to come back. The session goroutine rebuilds the link on its own after
// a drop, so a send that lands inside that window only has to wait.
// When stale is not nil the caller needs a REPLACEMENT session (the one
// it holds is dead-but-not-yet-noticed), so the stale transport never
// satisfies the wait.
func (h *Hub) awaitConn(ctx context.Context, wait time.Duration, stale transportConn) (transportConn, error) {
	deadline := time.Now().Add(wait)
	if dl, ok := ctx.Deadline(); ok && dl.Before(deadline) {
		deadline = dl
	}
	for {
		h.mu.Lock()
		conn := h.conn
		h.mu.Unlock()
		if conn != nil && conn != stale && conn.IsConnected() {
			return conn, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if !time.Now().Before(deadline) {
			return nil, errDeviceNotConnected
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(connPollInterval):
		}
	}
}

// heartbeatFrame is the protocol's keepalive: the device reads it as "the
// client is still here", and the write itself exercises the socket.
func heartbeatFrame() *pb.ToRadio {
	return &pb.ToRadio{PayloadVariant: &pb.ToRadio_Heartbeat{Heartbeat: &pb.Heartbeat{}}}
}

// retrySendOnAFreshLink rebuilds the session and transmits the frame
// again on the replacement. cause is the write error that lost the old
// link: it stays the reported error when the link does not come back,
// because "broken pipe" names the cause better than a follow-up timeout.
func (h *Hub) retrySendOnAFreshLink(ctx context.Context, ps *pendingSend, msg *pb.ToRadio, stale transportConn, cause error) error {
	if ctx.Err() != nil {
		return cause
	}
	h.requestReconnect()
	// Wait for the REPLACEMENT session: the live-but-dead link would only
	// fail the write again (the socket is closed, the session just has not
	// noticed yet).
	conn, err := h.awaitConn(ctx, linkSendWait, stale)
	if err != nil {
		return cause
	}
	// The echo (the device assigns the packet id) pairs by FIFO order and
	// by age: refresh the stamp so a slow rebuild cannot age the entry out
	// of the pairing window.
	ps.lastTry = time.Now()
	if err := conn.SendToRadio(msg); err != nil {
		return err
	}
	return nil
}

// sendText transmits one text message and records it as TX. Broadcasts
// carry the target channel index; direct messages leave the channel to
// the device. prog attaches the durable action-progress identity when
// the send comes from the meshtastic action.
func (h *Hub) sendText(ctx context.Context, to uint32, channelIdx int, channelLabel, text, operator string, prog ProgressRef) error {
	h.sendMu.Lock()
	defer h.sendMu.Unlock()
	conn, err := h.awaitConn(ctx, linkSendWait, nil)
	if err != nil {
		return err
	}
	wantAck := to != core.BroadcastNodeID.Uint32()
	pkt := &pb.MeshPacket{
		To:      to,
		WantAck: wantAck,
		PayloadVariant: &pb.MeshPacket_Decoded{
			Decoded: &pb.Data{
				Portnum: pb.PortNum_TEXT_MESSAGE_APP,
				Payload: []byte(text),
			},
		},
	}
	if to == core.BroadcastNodeID.Uint32() {
		pkt.Channel = uint32(channelIdx)
	}
	msg := &pb.ToRadio{
		PayloadVariant: &pb.ToRadio_Packet{Packet: pkt},
	}
	// Track the delivery state: the device answers with a queueStatus (or,
	// on older firmware, echoes the frame) carrying the assigned packet id,
	// and direct messages additionally carry a ROUTING_APP acknowledgment
	// from the recipient. The timestamp is shared with the history row so
	// status updates can pin the exact row.
	at := time.Now()
	ps := &pendingSend{
		to:      to,
		at:      at,
		lastTry: at,
		text:    text,
		channel: channelLabel,
		wantAck: wantAck,
		prog:    prog,
	}
	// sendMu is held by the defer above; the echo must be queued before
	// the frame goes out so a fast device echo can never miss its entry.
	h.echoQueue = append(h.echoQueue, ps)
	// The history row is written first so the device echo (which can race
	// back immediately) always finds its row to stamp. Direct messages
	// carry the recipient node id so the DM tab can pair the conversation.
	recipient := ""
	if wantAck {
		recipient = fmt.Sprintf("%08x", to)
	}
	h.recordMessage("tx", "", recipient, channelLabel, text, operator, 0, at)

	if err := conn.SendToRadio(msg); err != nil {
		// The link died under the frame: a socket the peer closed fails
		// every write with "broken pipe", and without a rebuild this send
		// would stay broken until the silence watchdog noticed (up to two
		// minutes). Rebuild the session and give the frame one more chance
		// on the fresh link — a write that fails this early never reached
		// the device, so the retry cannot duplicate it on the air.
		if retryErr := h.retrySendOnAFreshLink(ctx, ps, msg, conn, err); retryErr != nil {
			h.dropPendingLocked(ps)
			h.markStatusLocked(ps, TxFailed)
			return fmt.Errorf("meshtastic: send failed: %w", retryErr)
		}
		return nil
	}
	return nil
}

// recordMessage appends one message to the recent list, the durable
// history and the MQTT feed.
func (h *Hub) recordMessage(direction, sender, recipient, channel, text, operator string, hops int, at time.Time) {
	msg := Message{Direction: direction, Sender: sender, Recipient: recipient, Channel: channel, Hops: hops, Operator: operator, Text: text, At: at}
	h.mu.Lock()
	h.recent = append(h.recent, msg)
	if len(h.recent) > 64 {
		h.recent = h.recent[len(h.recent)-64:]
	}
	rec := h.recorder
	mSink := h.messageSink
	h.mu.Unlock()
	if rec != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := rec.RecordMeshtasticMessage(ctx, direction, sender, recipient, channel, text, operator, hops, at)
		cancel()
		if err != nil && h.logger != nil {
			h.logger.Warn("meshtastic: message history record failed", "error", err)
		}
	}
	if mSink != nil {
		payload, _ := json.Marshal(map[string]any{
			"direction": direction,
			"sender":    sender,
			"recipient": recipient,
			"channel":   channel,
			"hops":      hops,
			"operator":  operator,
			"text":      text,
			"at":        at.Format(time.RFC3339),
		})
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := mSink(ctx, "meshtastic/messages", false, payload)
		cancel()
		if err != nil && h.logger != nil {
			h.logger.Warn("meshtastic: message feed publish failed", "error", err)
		}
	}
}

// publishStation publishes (or refreshes) one retained station document. The
// document is handed to the feed worker: this call may come from the packet
// path, which must never wait for a broker.
func (h *Hub) publishStation(n *Node) {
	h.mu.Lock()
	sink := h.stationSink
	h.mu.Unlock()
	if sink == nil {
		return
	}
	doc := map[string]any{
		"id":        n.ID,
		"name":      n.Name,
		"short":     n.Short,
		"sends":     n.Sends,
		"hops":      n.Hops,
		"lat":       n.Lat,
		"lon":       n.Lon,
		"last_seen": n.LastSeen.UTC().Format(time.RFC3339),
	}
	if n.Telemetry != nil {
		doc["telemetry"] = n.Telemetry
	}
	payload, err := json.Marshal(doc)
	if err != nil {
		return
	}
	h.enqueueStationFeed(feedJob{id: n.ID, doc: payload})
}

// publishStationDoc writes one document to the broker (the worker side of
// publishStation). The station feed is ordered against the removal of a
// document for the same node: the worker publishes one operation at a time and
// the removal re-checks that the node is still quiet before it acts.
func (h *Hub) publishStationDoc(parent context.Context, sink func(ctx context.Context, topic string, retained bool, payload []byte) error, job feedJob) {
	h.stationMu.Lock()
	defer h.stationMu.Unlock()
	ctx, cancel := context.WithTimeout(parent, stationPublishTimeout)
	defer cancel()
	if err := sink(ctx, "meshtastic/stations/"+job.id, true, job.doc); err != nil && h.logger != nil {
		h.logger.Warn("meshtastic: station publish failed", "id", job.id, "error", err)
	}
}

// SeedNode merges one retained station document restored from the broker
// (the startup seeding path) into the node directory. Stale documents
// also restore: the directory is a history, the MQTT feed freshness is
// governed by NodeTTL tombstones alone.
func (h *Hub) SeedNode(id, name, short string, lat, lon float64, lastSeen time.Time, sends []string) {
	id = strings.ToLower(strings.TrimPrefix(strings.TrimSpace(id), "!"))
	if len(id) != 8 {
		return
	}
	h.mu.Lock()
	n := h.nodes[id]
	if n == nil {
		n = &Node{ID: id}
		h.nodes[id] = n
	}
	if name != "" {
		n.Name = name
	}
	if short != "" {
		n.Short = short
	}
	if len(sends) > 0 && n.Sends == nil {
		n.Sends = append([]string(nil), sends...)
	}
	if lat != 0 || lon != 0 {
		n.Lat, n.Lon = lat, lon
	}
	if lastSeen.After(n.LastSeen) {
		n.LastSeen = lastSeen
	}
	h.nodesDirty = true
	h.mu.Unlock()
}

// Snapshot builds the current state for the admin page and the public
// map.
func (h *Hub) Snapshot() Snapshot {
	h.mu.Lock()
	defer h.mu.Unlock()
	snap := Snapshot{
		Connected: h.connected,
		NodeTTL:   h.cfg.NodeTTL,
		// The feed's backlog is part of the station's state: an operator has
		// to see a broker that has not taken its removals for a while.
		PendingRemovals: h.pendingRemovalsLocked(),
		RemovalsWaiting: h.removalsWaiting,
		RemovalsDropped: h.removalsDropped,
	}
	if h.self != nil {
		snap.Self = *h.self
	}
	snap.Channels = append([]string(nil), h.channels...)
	snap.Nodes = make([]Node, 0, len(h.nodes))
	for _, n := range h.nodes {
		c := *n
		c.Sends = append([]string(nil), n.Sends...)
		// Deep-copy the telemetry: the live object keeps mutating under
		// the hub lock after this snapshot is released, and readers
		// (the public map API, the admin page) must never race it.
		c.Telemetry = cloneTelemetry(n.Telemetry)
		if h.self != nil && h.self.Lat != 0 || h.self != nil && h.self.Lon != 0 {
			if c.Lat != 0 || c.Lon != 0 {
				c.DistKM = DistanceKM(h.self.Lat, h.self.Lon, c.Lat, c.Lon)
				c.BearingDeg = BearingDeg(h.self.Lat, h.self.Lon, c.Lat, c.Lon)
			}
		}
		snap.Nodes = append(snap.Nodes, c)
	}
	sort.Slice(snap.Nodes, func(i, j int) bool { return snap.Nodes[i].ID < snap.Nodes[j].ID })
	// The link facts ride along with the state: the panel shows the device
	// path and the last error next to the connection pill.
	snap.Device = h.cfg.Device
	if h.lastErr != nil {
		snap.LastError = h.lastErr.Error()
	}
	return snap
}

// DistanceKM computes the great-circle distance between two coordinates
// (haversine).
func DistanceKM(lat1, lon1, lat2, lon2 float64) float64 {
	const r = 6371.0
	dLat := (lat2 - lat1) * math.Pi / 180
	dLon := (lon2 - lon1) * math.Pi / 180
	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(lat1*math.Pi/180)*math.Cos(lat2*math.Pi/180)*
			math.Sin(dLon/2)*math.Sin(dLon/2)
	return 2 * r * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
}

// BearingDeg computes the initial bearing from (lat1,lon1) to
// (lat2,lon2) in degrees [0,360).
func BearingDeg(lat1, lon1, lat2, lon2 float64) float64 {
	φ1 := lat1 * math.Pi / 180
	φ2 := lat2 * math.Pi / 180
	dLon := (lon2 - lon1) * math.Pi / 180
	y := math.Sin(dLon) * math.Cos(φ2)
	x := math.Cos(φ1)*math.Sin(φ2) - math.Sin(φ1)*math.Cos(φ2)*math.Cos(dLon)
	return math.Mod(math.Atan2(y, x)*180/math.Pi+360, 360)
}
