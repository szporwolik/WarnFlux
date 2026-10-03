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
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
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
	"google.golang.org/protobuf/proto"

	"github.com/szporwolik/WarnFlux/internal/dispatch"
	"github.com/szporwolik/WarnFlux/internal/radiocli"
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
	// Device is the serial device path (e.g. /dev/ttyACM0 or /dev/ttyUSB0)
	// of the Meshtastic node.
	Device string
	Baud   int
	// RouteMessages re-publishes direct messages from directory-known
	// senders as canonical /events documents, so they enter the normal
	// routing matrix (the alarm pipeline) like APRS messages do.
	RouteMessages bool
	// NodeTTL bounds how long an unheard neighbour stays in the node list.
	NodeTTL time.Duration
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
	// EmcomInterval is the beacon spacing (default 4 hours). The first
	// beacon fires as soon as the first session comes up (server
	// start/restart).
	EmcomInterval time.Duration
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
}

// Recorder persists the meshtastic message history (implemented by
// storage stores). Best-effort: the hub logs recording failures and
// keeps running.
type Recorder interface {
	RecordMeshtasticMessage(ctx context.Context, direction, sender, channel, text, operator string, hops int, at time.Time) error
	// UpdateMeshtasticMessageStatus marks the tx delivery state of the
	// matching row (created at + text).
	UpdateMeshtasticMessageStatus(ctx context.Context, status string, at time.Time, text string) error
}

// NodeStore persists the heard-node directory (implemented by storage
// stores). Optional: a hub without a store keeps the directory in RAM.
type NodeStore interface {
	LoadMeshtasticNodes(ctx context.Context) ([]storage.MeshtasticNode, error)
	SaveMeshtasticNodes(ctx context.Context, nodes []storage.MeshtasticNode) error
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

// defaultEmcomInterval is the spacing between presence beacons on the
// emcom channel.
const defaultEmcomInterval = 4 * time.Hour

// meshTextMaxRunes bounds one outbound channel text message (133 chars
// per the Meshtastic spec).
const meshTextMaxRunes = 133

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
	Lat   float64
	Lon   float64
	// Telemetry is the latest telemetry the node broadcast (nil when
	// the node never sent any).
	Telemetry *Telemetry
	// DistKM and BearingDeg are computed relative to our station's
	// position in Snapshot (0 when either position is unknown).
	DistKM     float64
	BearingDeg float64
	LastSeen   time.Time
	// tombstoned records that the node's retained station document was
	// already removed (the node itself stays in the persistent directory).
	tombstoned bool
}

// cmdRecord is the remembered outcome of one radio command, keyed by
// sender + packet identity: a redelivered packet (rebroadcast, router
// retry) re-sends the previous reply and never re-executes the command.
type cmdRecord struct {
	at    time.Time
	reply string
}

// bannerReply is one queued low-priority banner answer.
type bannerReply struct {
	id   string
	text string
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
	// Channels are the device channel names by index.
	Channels []string
	// Nodes is the full node directory (recent and long-gone alike);
	// LastSeen distinguishes them. The MQTT station feed only carries
	// nodes heard within NodeTTL.
	Nodes []Node
	// NodeTTL is the freshness bound used to filter the live feed.
	NodeTTL time.Duration
}

// Message is one received or sent text message.
type Message struct {
	Direction string // rx | tx
	Sender    string // node id (8 hex) for rx; "" for tx
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

// Dial opens the device transport (the seam for tests). Production dials
// through serial.Connect, which performs the client handshake itself and
// returns a transport that is already connected.
var Dial = func(ctx context.Context, cfg Config) (transportConn, error) {
	t, err := serial.Connect(ctx, serial.Config{Port: cfg.Device, BaudRate: cfg.Baud})
	if err != nil {
		return nil, err
	}
	return &clientAdapter{t: t}, nil
}

// Hub owns the serial connection to the Meshtastic node and the shared
// state: self info, neighbours and the message history. The source
// plugin runs Run; the outbound action sends through SendChannelMessage
// or SendContactMessage; the web admin page reads Snapshot.
type Hub struct {
	cfg    Config
	logger *slog.Logger

	mu        sync.Mutex
	conn      transportConn
	connected bool
	lastErr   error
	self      *SelfInfo
	nodes     map[string]*Node
	channels  []string
	recent    []Message

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

	// lastRadio is the unix-nano time of the most recent FromRadio
	// message; the session is declared dead after cfg.SilenceTimeout
	// (defaultSilenceTimeout) of total radio silence.
	lastRadio atomic.Int64

	// nodeStore persists the heard-node directory across restarts
	// (optional; nil = directory lives in RAM only).
	nodeStore   NodeStore
	nodesDirty  bool
	lastPersist time.Time

	// startedAt anchors the beacon uptime; emcomNext schedules the next
	// presence beacon on the emcom channel.
	startedAt time.Time
	emcomNext time.Time

	// cmds remembers executed radio commands by sender + packet
	// identity; guarded by mu. A redelivered packet is answered with
	// the previous result instead of re-executing the command.
	cmds map[string]*cmdRecord

	// alertAcceptor accepts a marshalled /alert event into the LOCAL
	// pipeline and reports how it was accepted (durable inbox,
	// emergency RAM fallback or rejection); guarded by mu. nil falls
	// back to the event sink (tests, legacy wiring).
	alertAcceptor func(payload []byte) dispatch.Acceptance

	// banners queues low-priority automatic banner replies; guarded by
	// mu. They drain one per drain call (inbound packet / session
	// tick), so direct alarm and command replies always go first.
	banners    []bannerReply
	bannerLast map[string]time.Time // sender -> last banner answer
	bannerNext time.Time            // global pacing: next allowed banner
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
	if cfg.EmcomInterval <= 0 {
		cfg.EmcomInterval = defaultEmcomInterval
	}
	if cfg.EmcomChannel < 0 || cfg.EmcomChannel > 7 {
		return nil, errors.New("meshtastic: emcom channel must be 0-7")
	}
	if cfg.BannerMinInterval <= 0 {
		cfg.BannerMinInterval = bannerMinInterval
	}
	if cfg.BannerGlobalInterval <= 0 {
		cfg.BannerGlobalInterval = bannerGlobalInterval
	}
	if !cfg.Enabled {
		// Disabled hub: no serial device needed; the source plugin skips
		// Run and the admin page shows the device as disconnected.
		return &Hub{cfg: cfg, logger: logger, nodes: make(map[string]*Node),
			pending: make(map[uint32]*pendingSend), cmds: make(map[string]*cmdRecord),
			bannerLast: make(map[string]time.Time), startedAt: time.Now()}, nil
	}
	if cfg.Device == "" {
		return nil, errors.New("meshtastic: device path is required")
	}
	return &Hub{
		cfg:        cfg,
		logger:     logger,
		nodes:      make(map[string]*Node),
		pending:    make(map[uint32]*pendingSend),
		cmds:       make(map[string]*cmdRecord),
		bannerLast: make(map[string]time.Time),
		startedAt:  time.Now(),
	}, nil
}

// Enabled reports whether the meshtastic integration is configured.
func (h *Hub) Enabled() bool { return h.cfg.Enabled }

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
// heard node becomes a retained document under meshtastic/stations/<id>
// and is tombstoned when its NodeTTL expires.
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

// SetAlertAcceptor installs the LOCAL acceptance path for /alert
// events: the callback enqueues a marshalled alert into the local
// pipeline and reports how it was accepted (durable inbox, emergency
// RAM fallback or rejection). Without it the hub falls back to the
// event sink (tests, legacy wiring).
func (h *Hub) SetAlertAcceptor(fn func(payload []byte) dispatch.Acceptance) {
	h.mu.Lock()
	h.alertAcceptor = fn
	h.mu.Unlock()
}

// SetCLI attaches the shared radio-command interpreter (optional): a
// direct message that parses as a command is answered in-band and
// (except /debug) stays off the alarm pipeline.
func (h *Hub) SetCLI(b *radiocli.Bot) { h.mu.Lock(); h.cli = b; h.mu.Unlock() }

// Run dials the device, performs the client handshake and pumps incoming
// messages until ctx is cancelled. It reconnects with bounded backoff so
// transient USB hiccups never kill the plugin permanently.
func (h *Hub) Run(ctx context.Context) error {
	h.restoreNodes(ctx)
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
		h.logger.Warn("meshtastic: device session ended, reconnecting", "error", err)
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
			h.pairQueueStatus(qs.GetMeshPacketId())
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
	// The presence beacon fires immediately at server start: the first
	// session schedules it for now, so the first maintenance tick sends
	// it. Later sessions are device reconnects and must not reschedule
	// the cadence — it belongs to the process, not the session.
	if h.cfg.EmcomChannel > 0 && h.cfg.EmcomIdentity != "" {
		h.mu.Lock()
		if h.emcomNext.IsZero() {
			h.emcomNext = time.Now()
		}
		h.mu.Unlock()
	}
	h.populateFromState(conn.State())

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

	// Maintenance: expire unheard neighbours and watch the transport.
	ticker := time.NewTicker(dmRetryInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if !conn.IsConnected() {
				return errors.New("device disconnected")
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
			h.retryPending(time.Now())
			h.persistNodes()
			h.beaconTick()
			h.drainBanner()
		}
	}
}

// beaconTick sends the periodic presence beacon on the emcom channel
// (identity + uptime + node count). The PRIMARY channel is never used.
func (h *Hub) beaconTick() {
	h.mu.Lock()
	next := h.emcomNext
	h.mu.Unlock()
	if next.IsZero() || time.Now().Before(next) {
		return
	}
	h.mu.Lock()
	h.emcomNext = time.Now().Add(h.cfg.EmcomInterval)
	nodes := len(h.nodes)
	up := time.Since(h.startedAt).Round(time.Minute)
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
	for _, n := range nodes {
		if cur := h.nodes[n.ID]; cur != nil {
			continue // live knowledge wins
		}
		h.nodes[n.ID] = &Node{
			ID:       n.ID,
			Name:     n.Name,
			Short:    n.Short,
			Lat:      n.Lat,
			Lon:      n.Lon,
			LastSeen: n.LastSeen,
			Sends:    append([]string(nil), n.Sends...),
		}
	}
	h.mu.Unlock()
	if len(nodes) > 0 {
		h.logger.Info("meshtastic: restored node directory", "nodes", len(nodes))
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
	nodes := make([]storage.MeshtasticNode, 0, len(h.nodes))
	for _, n := range h.nodes {
		nodes = append(nodes, storage.MeshtasticNode{
			ID:       n.ID,
			Name:     n.Name,
			Short:    n.Short,
			Lat:      n.Lat,
			Lon:      n.Lon,
			LastSeen: n.LastSeen,
			Sends:    append([]string(nil), n.Sends...),
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
			h.channels = append(h.channels, make([]string, idx+1-len(h.channels))...)
		}
		if name := strings.TrimSpace(ch.GetSettings().GetName()); name != "" {
			h.channels[idx] = name
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

// expireNodes cleans the MQTT station feed: nodes unheard for longer
// than NodeTTL get their retained document tombstoned (once). The nodes
// themselves stay in the persistent directory — the list only ever
// grows, like the device's own node database.
func (h *Hub) expireNodes() {
	h.mu.Lock()
	now := time.Now()
	var stale []*Node
	for _, n := range h.nodes {
		if !n.tombstoned && now.Sub(n.LastSeen) > h.cfg.NodeTTL {
			n.tombstoned = true
			stale = append(stale, n)
		}
	}
	sink := h.stationSink
	h.mu.Unlock()
	for _, n := range stale {
		if sink != nil {
			if err := sink(context.Background(), "meshtastic/stations/"+n.ID, true, nil); err != nil && h.logger != nil {
				h.logger.Warn("meshtastic: station tombstone failed", "id", n.ID, "error", err)
			}
		}
	}
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
// wins (the queue is FIFO).
func (h *Hub) pairQueueStatus(id uint32) {
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
		h.pending[id] = q
		h.sendMu.Unlock()
		h.markStatus(q, TxSent)
		return
	}
	h.sendMu.Unlock()
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
// "sent" confirmation).
func (h *Hub) handleRoutingAck(pkt *pb.MeshPacket, decoded *pb.Data) {
	rid := decoded.GetRequestId()
	if rid == 0 {
		return
	}
	var routing pb.Routing
	if err := proto.Unmarshal(decoded.GetPayload(), &routing); err != nil {
		return
	}
	fromSelf := fmt.Sprintf("%08x", pkt.GetFrom()) == h.selfID()
	h.sendMu.Lock()
	ps := h.pending[rid]
	if ps == nil {
		ps = h.pairByRoutingLocked(rid, pkt.GetFrom(), fromSelf)
	}
	if ps == nil {
		h.sendMu.Unlock()
		return
	}
	delete(h.pending, rid)
	h.sendMu.Unlock()
	status := TxDelivered
	if fromSelf {
		status = TxSent
	} else if er := routing.GetErrorReason(); er != pb.Routing_NONE {
		status = TxFailed
	}
	h.markStatus(ps, status)
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
		h.markStatus(ps, TxFailed)
	}
	// Unpaired direct messages whose id never arrived (the device may
	// have been busy) are retransmitted.
	for i := 0; i < len(h.echoQueue); {
		q := h.echoQueue[i]
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
			h.markStatus(q, TxFailed)
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
	ps.lastTry = time.Now()
	h.sendMu.Lock()
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
// history.
func (h *Hub) markStatus(ps *pendingSend, status string) {
	ps.status = status
	h.mu.Lock()
	rec := h.recorder
	h.mu.Unlock()
	if rec == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rec.UpdateMeshtasticMessageStatus(ctx, status, ps.at, ps.text); err != nil && h.logger != nil {
		h.logger.Warn("meshtastic: message status update failed", "status", status, "error", err)
	}
}

// recordSignal marks one observed packet kind on the sender's node and
// refreshes its last-seen time (any packet means the node is alive).
// The node is created when it is not in the directory yet.
func (h *Hub) recordSignal(from uint32, signal string) {
	id := fmt.Sprintf("%08x", from)
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
		h.recordSignal(pkt.GetFrom(), SignalTelemetry)
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
	copyN := *n
	copyN.Telemetry = cloneTelemetry(n.Telemetry)
	h.mu.Unlock()
	h.publishStation(&copyN)
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
	h.recordSignal(pkt.GetFrom(), SignalText)
	channel := h.channelNameFor(pkt)
	h.recordMessage("rx", id, channel, string(decoded.GetPayload()), "", int(pkt.GetHopStart()), time.Now())

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
	// everyone; restricted commands (/debug) run only for
	// directory-registered senders — the shared interpreter decides.
	// The same packet delivered again by the mesh (rebroadcast, router
	// retry) re-sends the previous reply instead of executing the
	// command again.
	if cli != nil && strings.HasPrefix(strings.TrimSpace(text), "/") {
		mid := msgIdentity(pkt, text)
		cid := id + ":" + mid
		now := time.Now()
		h.mu.Lock()
		rec := h.cmds[cid]
		fresh := rec != nil && now.Sub(rec.at) < cmdDedupWindow
		reply := ""
		if fresh {
			reply = rec.reply
		}
		h.mu.Unlock()
		if fresh {
			if reply != "" {
				h.sendCLIReply(id, reply)
			}
			return
		}
		res := cli.Handle(text, owner != "")
		// The stored reply is the FINAL confirmation (post-acceptance),
		// so a redelivery replays exactly the previous result.
		reply = res.Reply
		if res.Alert != nil {
			reply = alertReply(res.Reply, h.publishAlertEvent(id, owner, res.Alert, mid))
		}
		h.mu.Lock()
		h.pruneCmds(now)
		h.cmds[cid] = &cmdRecord{at: now, reply: reply}
		h.mu.Unlock()
		if res.Handled {
			if res.Debug {
				h.publishRoutedEvent(id, owner, text, mid)
			}
			if reply != "" {
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

// queueBanner enqueues one banner answer as low-priority traffic. The
// per-sender window stops answering bots from looping each other; the
// global pacing and the one-at-a-time drain keep the automatic-answer
// path behind direct alarm and command replies. The sender's window
// starts when the banner is queued, so dropped (coalesced) replies
// never reset it.
func (h *Hub) queueBanner(id, text string) {
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
// progressively, payload truncates last).
func (h *Hub) sendCLIReply(id, text string) {
	h.mu.Lock()
	cli := h.cli
	h.mu.Unlock()
	if cli != nil {
		text = cli.Fit(text, meshTextMaxRunes)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := h.SendContactMessage(ctx, id, text, "system"); err != nil && h.logger != nil {
		h.logger.Warn("meshtastic: cli reply failed", "to", id, "error", err)
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

// publishRoutedEvent bridges one approved direct message into the /events
// stream as a canonical hazard transition. The directory owner travels
// in the headline and description so operators see WHO is speaking. mid
// is the per-packet identity (msgIdentity) that keeps the event key
// stable across redeliveries, so the storage deduplication collapses
// rebroadcasts into one alarm.
func (h *Hub) publishRoutedEvent(senderID, owner, text, mid string) {
	h.mu.Lock()
	name := ""
	if n := h.nodes[senderID]; n != nil && n.Name != "" {
		name = n.Name
	}
	sink := h.eventSink
	h.mu.Unlock()
	if sink == nil {
		return
	}
	now := time.Now().UTC()
	from := name
	if from == "" {
		from = "!" + senderID
	}
	sourceID := senderID + ":msg:" + mid
	we := MessageEventWire{
		SchemaVersion: meshMessageEventSchemaVersion,
		ChangeID:      now.UnixMilli(),
		ChangeType:    "new",
		EventKey:      "meshtastic:" + sourceID,
		Event: MessageEventHazard{
			Source:      "meshtastic",
			SourceID:    sourceID,
			Category:    "meshtastic",
			Event:       "Meshtastic message",
			Severity:    "severe",
			Urgency:     "immediate",
			Certainty:   "observed",
			Headline:    fmt.Sprintf("Message from: %s (%s): %s", owner, from, text),
			Description: fmt.Sprintf("Direct message routed from the Meshtastic network (sender: %s).", owner),
			Status:      "active",
			ReceivedAt:  now.Format(time.RFC3339),
			UpdatedAt:   now.Format(time.RFC3339),
		},
	}
	payload, err := json.Marshal(we)
	if err != nil {
		return
	}
	if err := sink(context.Background(), "events", false, payload); err != nil && h.logger != nil {
		h.logger.Warn("meshtastic: routed message event publish failed", "error", err)
	}
}

// alertReply maps the local acceptance of a /alert event onto the
// in-band confirmation: durable acceptance answers with the handler
// confirmation, the emergency fallback says so explicitly, and a
// rejection never claims success.
func alertReply(base string, acc dispatch.Acceptance) string {
	switch acc {
	case dispatch.Rejected:
		return "FAILED: alert rejected"
	case dispatch.AcceptedEmergency:
		if base == "" {
			return "OK: alert raised (failover mode)"
		}
		return base + " (failover mode)"
	default:
		if base == "" {
			return "OK: alert raised"
		}
		return base
	}
}

// publishAlertEvent raises one operator-requested hazard (/alert) into
// the /events stream: severe, bounded by the requested TTL (4 hours by
// default). mid keeps the event identity stable across redeliveries
// (see publishRoutedEvent). The returned acceptance reports how the
// LOCAL pipeline took the alert (durable, emergency or rejected); the
// confirmation reply must follow it. A broker failure alone never
// downgrades the local acceptance.
func (h *Hub) publishAlertEvent(senderID, owner string, spec *radiocli.AlertSpec, mid string) dispatch.Acceptance {
	if spec == nil {
		return dispatch.Rejected
	}
	h.mu.Lock()
	name := ""
	if n := h.nodes[senderID]; n != nil && n.Name != "" {
		name = n.Name
	}
	sink := h.eventSink
	acceptor := h.alertAcceptor
	h.mu.Unlock()
	now := time.Now().UTC()
	ttl := spec.TTL
	if ttl <= 0 {
		ttl = 4 * time.Hour
	}
	nowS := now.Format(time.RFC3339)
	expires := now.Add(ttl).Format(time.RFC3339)
	from := name
	if from == "" {
		from = "!" + senderID
	}
	sourceID := senderID + ":alert:" + mid
	we := MessageEventWire{
		SchemaVersion: meshMessageEventSchemaVersion,
		ChangeID:      now.UnixMilli(),
		ChangeType:    "new",
		EventKey:      "meshtastic:" + sourceID,
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
			ExpiresAt:   &expires,
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
	return h.sendText(ctx, core.BroadcastNodeID.Uint32(), idx, h.ChannelLabel(idx), text, operator)
}

// SendContactMessage sends one direct text message to a node id (8 hex
// chars, with or without the leading '!').
func (h *Hub) SendContactMessage(ctx context.Context, addr, text, operator string) error {
	idStr := strings.ToLower(strings.TrimPrefix(strings.TrimSpace(addr), "!"))
	if len(idStr) != 8 {
		return fmt.Errorf("meshtastic: node id must be 8 hex characters, got %q", addr)
	}
	n, err := strconv.ParseUint(idStr, 16, 32)
	if err != nil {
		return fmt.Errorf("meshtastic: node id must be 8 hex characters, got %q", addr)
	}
	return h.sendText(ctx, uint32(n), 0, "dm", text, operator)
}

// sendText transmits one text message and records it as TX. Broadcasts
// carry the target channel index; direct messages leave the channel to
// the device.
func (h *Hub) sendText(ctx context.Context, to uint32, channelIdx int, channelLabel, text, operator string) error {
	h.sendMu.Lock()
	defer h.sendMu.Unlock()
	h.mu.Lock()
	conn := h.conn
	h.mu.Unlock()
	if conn == nil || !conn.IsConnected() {
		return errors.New("meshtastic: device not connected")
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
	}
	// sendMu is held by the defer above; the echo must be queued before
	// the frame goes out so a fast device echo can never miss its entry.
	h.echoQueue = append(h.echoQueue, ps)
	// The history row is written first so the device echo (which can race
	// back immediately) always finds its row to stamp.
	h.recordMessage("tx", "", channelLabel, text, operator, 0, at)

	if err := conn.SendToRadio(msg); err != nil {
		h.dropPendingLocked(ps)
		h.markStatus(ps, TxFailed)
		return fmt.Errorf("meshtastic: send failed: %w", err)
	}
	return nil
}

// recordMessage appends one message to the recent list, the durable
// history and the MQTT feed.
func (h *Hub) recordMessage(direction, sender, channel, text, operator string, hops int, at time.Time) {
	msg := Message{Direction: direction, Sender: sender, Channel: channel, Hops: hops, Operator: operator, Text: text, At: at}
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
		err := rec.RecordMeshtasticMessage(ctx, direction, sender, channel, text, operator, hops, at)
		cancel()
		if err != nil && h.logger != nil {
			h.logger.Warn("meshtastic: message history record failed", "error", err)
		}
	}
	if mSink != nil {
		payload, _ := json.Marshal(map[string]any{
			"direction": direction,
			"sender":    sender,
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

// publishStation publishes (or refreshes) one retained station document.
// The sink is called without the hub lock held: sinks perform network
// I/O and must never block the hub state.
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
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := sink(ctx, "meshtastic/stations/"+n.ID, true, payload); err != nil && h.logger != nil {
		h.logger.Warn("meshtastic: station publish failed", "id", n.ID, "error", err)
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
	snap := Snapshot{Connected: h.connected, NodeTTL: h.cfg.NodeTTL}
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
