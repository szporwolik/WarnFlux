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
	"time"

	"github.com/kabili207/meshtastic-go/core"
	pb "github.com/kabili207/meshtastic-go/core/proto"
	"github.com/kabili207/meshtastic-go/transport"
	"github.com/kabili207/meshtastic-go/transport/client"
	"github.com/kabili207/meshtastic-go/transport/serial"
	"google.golang.org/protobuf/proto"
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
}

// Recorder persists the meshtastic message history (implemented by
// storage stores). Best-effort: the hub logs recording failures and
// keeps running.
type Recorder interface {
	RecordMeshtasticMessage(ctx context.Context, direction, sender, channel, text, operator string, hops int, at time.Time) error
}

// Node signal kinds observed in packets from the node (shown as badges
// on the admin page and the home map).
const (
	SignalTelemetry = "telemetry"
	SignalPosition  = "position"
	SignalText      = "text"
)

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
	// DistKM and BearingDeg are computed relative to our station's
	// position in Snapshot (0 when either position is unknown).
	DistKM     float64
	BearingDeg float64
	LastSeen   time.Time
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
	Nodes    []Node
}

// Message is one received or sent text message.
type Message struct {
	Direction string // rx | tx
	Sender    string // node id (8 hex) for rx; "" for tx
	Channel   string // channel name or "dm"
	Hops      int    // radio path length from the packet (0 = unknown/ours)
	Operator  string // admin username behind a tx (rx rows are empty)
	Text      string
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

	sendMu sync.Mutex
}

// NewHub validates the config and builds the hub.
func NewHub(cfg Config, logger *slog.Logger) (*Hub, error) {
	if cfg.Baud == 0 {
		cfg.Baud = DefaultBaud
	}
	if cfg.NodeTTL <= 0 {
		cfg.NodeTTL = DefaultNodeTTL
	}
	if !cfg.Enabled {
		// Disabled hub: no serial device needed; the source plugin skips
		// Run and the admin page shows the device as disconnected.
		return &Hub{cfg: cfg, logger: logger, nodes: make(map[string]*Node)}, nil
	}
	if cfg.Device == "" {
		return nil, errors.New("meshtastic: device path is required")
	}
	return &Hub{
		cfg:    cfg,
		logger: logger,
		nodes:  make(map[string]*Node),
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

// Run dials the device, performs the client handshake and pumps incoming
// messages until ctx is cancelled. It reconnects with bounded backoff so
// transient USB hiccups never kill the plugin permanently.
func (h *Hub) Run(ctx context.Context) error {
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
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if !conn.IsConnected() {
				return errors.New("device disconnected")
			}
			h.expireNodes()
		}
	}
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
		publish = append(publish, *n)
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

// expireNodes tombstones neighbours unheard for longer than NodeTTL.
func (h *Hub) expireNodes() {
	h.mu.Lock()
	now := time.Now()
	var expired []*Node
	for id, n := range h.nodes {
		if now.Sub(n.LastSeen) > h.cfg.NodeTTL {
			expired = append(expired, n)
			delete(h.nodes, id)
		}
	}
	sink := h.stationSink
	h.mu.Unlock()
	for _, n := range expired {
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
	decoded := pkt.GetDecoded()
	if decoded == nil {
		return
	}
	switch decoded.GetPortnum() {
	case pb.PortNum_TEXT_MESSAGE_APP:
		h.receiveText(pkt, decoded)
	case pb.PortNum_POSITION_APP:
		h.receivePosition(pkt, decoded)
	case pb.PortNum_NODEINFO_APP:
		h.receiveNodeInfo(pkt, decoded)
	case pb.PortNum_TELEMETRY_APP:
		// No content we surface, but the sender is now known to
		// broadcast telemetry — recorded for the badges.
		h.recordSignal(pkt.GetFrom(), SignalTelemetry)
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
	}
	if !slices.Contains(n.Sends, signal) {
		n.Sends = append(n.Sends, signal)
	}
	n.LastSeen = time.Now()
	h.mu.Unlock()
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
	}
	if name != "" {
		n.Name = name
	}
	if sn := strings.TrimSpace(user.GetShortName()); sn != "" {
		n.Short = sn
	}
	n.LastSeen = time.Now()
	copyN := *n
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
	}
	n.Lat, n.Lon = lat, lon
	n.LastSeen = time.Now()
	if !slices.Contains(n.Sends, SignalPosition) {
		n.Sends = append(n.Sends, SignalPosition)
	}
	copyN := *n
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

	// Only DIRECT messages to our node route (broadcasts stay on the
	// feed): the gate resolves the sender's registered directory user;
	// unknown senders stay off the alarm pipeline.
	h.mu.Lock()
	selfID = ""
	if h.self != nil {
		selfID = h.self.ID
	}
	gate := h.senderGate
	eventSink := h.eventSink
	h.mu.Unlock()
	if gate == nil || eventSink == nil || selfID == "" {
		return
	}
	if pkt.GetTo() != 0 && fmt.Sprintf("%08x", pkt.GetTo()) != selfID {
		return // not addressed to us
	}
	owner := gate(id)
	if owner == "" {
		return
	}
	h.publishRoutedEvent(id, owner, string(decoded.GetPayload()))
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
// in the headline and description so operators see WHO is speaking.
func (h *Hub) publishRoutedEvent(senderID, owner, text string) {
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
	we := MessageEventWire{
		SchemaVersion: meshMessageEventSchemaVersion,
		ChangeID:      now.UnixMilli(),
		ChangeType:    "new",
		EventKey:      fmt.Sprintf("meshtastic:%s-%d", senderID, now.UnixMilli()),
		Event: MessageEventHazard{
			Source:      "meshtastic",
			SourceID:    senderID,
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

// SendChannelMessage broadcasts one text message on the primary channel.
func (h *Hub) SendChannelMessage(ctx context.Context, text, operator string) error {
	return h.sendText(ctx, core.BroadcastNodeID.Uint32(), "", text, operator)
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
	return h.sendText(ctx, uint32(n), "dm", text, operator)
}

// sendText transmits one text message and records it as TX.
func (h *Hub) sendText(ctx context.Context, to uint32, channel, text, operator string) error {
	h.sendMu.Lock()
	defer h.sendMu.Unlock()
	h.mu.Lock()
	conn := h.conn
	h.mu.Unlock()
	if conn == nil || !conn.IsConnected() {
		return errors.New("meshtastic: device not connected")
	}
	wantAck := to != core.BroadcastNodeID.Uint32()
	msg := &pb.ToRadio{
		PayloadVariant: &pb.ToRadio_Packet{
			Packet: &pb.MeshPacket{
				To:      to,
				WantAck: wantAck,
				PayloadVariant: &pb.MeshPacket_Decoded{
					Decoded: &pb.Data{
						Portnum: pb.PortNum_TEXT_MESSAGE_APP,
						Payload: []byte(text),
					},
				},
			},
		},
	}
	if err := conn.SendToRadio(msg); err != nil {
		return fmt.Errorf("meshtastic: send failed: %w", err)
	}
	h.recordMessage("tx", "", channel, text, operator, 0, time.Now())
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
	doc, err := json.Marshal(map[string]any{
		"id":        n.ID,
		"name":      n.Name,
		"short":     n.Short,
		"sends":     n.Sends,
		"lat":       n.Lat,
		"lon":       n.Lon,
		"last_seen": n.LastSeen.UTC().Format(time.RFC3339),
	})
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := sink(ctx, "meshtastic/stations/"+n.ID, true, doc); err != nil && h.logger != nil {
		h.logger.Warn("meshtastic: station publish failed", "id", n.ID, "error", err)
	}
}

// SeedNode merges one retained station document restored from the broker
// (the startup seeding path): fresh documents restore the node; expired
// ones are tombstoned.
func (h *Hub) SeedNode(id, name, short string, lat, lon float64, lastSeen time.Time, sends []string) {
	id = strings.ToLower(strings.TrimPrefix(strings.TrimSpace(id), "!"))
	if len(id) != 8 {
		return
	}
	h.mu.Lock()
	if time.Since(lastSeen) > h.cfg.NodeTTL {
		h.mu.Unlock()
		h.publishStation(&Node{ID: id})
		return
	}
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
	h.mu.Unlock()
}

// Snapshot builds the current state for the admin page and the public
// map.
func (h *Hub) Snapshot() Snapshot {
	h.mu.Lock()
	defer h.mu.Unlock()
	snap := Snapshot{Connected: h.connected}
	if h.self != nil {
		snap.Self = *h.self
	}
	snap.Channels = append([]string(nil), h.channels...)
	snap.Nodes = make([]Node, 0, len(h.nodes))
	for _, n := range h.nodes {
		c := *n
		c.Sends = append([]string(nil), n.Sends...)
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
