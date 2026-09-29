package meshcore

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"sort"
	"sync"
	"time"
)

// Recorder persists mesh message history (implemented by storage stores).
// Best-effort: the hub logs recording failures and keeps running.
type Recorder interface {
	RecordMeshMessage(ctx context.Context, direction, sender, channel, text string, at time.Time) error
}

// Node is one neighbour heard through adverts.
type Node struct {
	PubKey   string
	Name     string
	Type     byte
	Lat      float64
	Lon      float64
	// DistKM and BearingDeg are computed relative to our station's
	// position in Snapshot (0 when either position is unknown).
	DistKM     float64
	BearingDeg float64
	LastSeen   time.Time
}

// Message is one received or sent channel/contact message.
type Message struct {
	Direction string // rx | tx
	Sender    string
	Channel   string
	Text      string
	At        time.Time
}

// Config is the hub configuration (top-level "meshcore:").
type Config struct {
	Enabled    bool
	Device     string
	Baud       int
	ChannelIdx int
	// NodeTTL bounds how long an unheard neighbour stays in the node list.
	NodeTTL time.Duration
}

// Defaults applied when the config omits values.
const (
	DefaultBaud    = 115200
	DefaultNodeTTL = 30 * time.Minute
)

// Hub owns the serial connection to the MeshCore Companion device and the
// shared state: self info, neighbours and the message history. The source
// plugin runs Run; the outbound action sends through SendChannelMessage;
// the web admin page reads Snapshot.
type Hub struct {
	cfg    Config
	logger *slog.Logger

	mu        sync.Mutex
	client    conn
	self      *SelfInfo
	device    *DeviceInfo
	battery   int
	nodes     map[string]*Node
	recent    []Message
	recorder  Recorder
	lastErr   error
	connected bool
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
	if cfg.ChannelIdx < 0 || cfg.ChannelIdx > 7 {
		return nil, fmt.Errorf("meshcore: channel_idx %d out of range 0-7", cfg.ChannelIdx)
	}
	if cfg.Device == "" {
		return nil, errors.New("meshcore: device path is required")
	}
	return &Hub{
		cfg:    cfg,
		logger: logger,
		nodes:  make(map[string]*Node),
	}, nil
}

// SetRecorder attaches the durable message history store (optional).
func (h *Hub) SetRecorder(r Recorder) { h.mu.Lock(); h.recorder = r; h.mu.Unlock() }

// Enabled reports whether the mesh is configured.
func (h *Hub) Enabled() bool { return h.cfg.Enabled }

// Connected reports whether the serial session is currently up.
func (h *Hub) Connected() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.connected
}

// Run opens the serial device, performs the companion handshake and pumps
// incoming frames until ctx is cancelled. It reconnects with bounded
// backoff so transient USB hiccups never kill the plugin permanently.
func (h *Hub) Run(ctx context.Context) error {
	backoff := time.Second
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		err := h.runSession(ctx)
		if ctx.Err() != nil {
			return nil
		}
		h.mu.Lock()
		h.connected = false
		h.lastErr = err
		h.client = nil
		h.mu.Unlock()
		h.logger.Warn("meshcore: session ended, reconnecting", "error", err)
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

// Dial is the seam for tests (production: open the serial device).
var Dial = func(cfg Config) (conn, error) {
	return openSerial(cfg.Device, cfg.Baud)
}

func (h *Hub) runSession(ctx context.Context) error {
	conn, err := Dial(h.cfg)
	if err != nil {
		return err
	}
	h.mu.Lock()
	h.client = conn
	h.connected = true
	h.mu.Unlock()
	defer conn.Close()

	if err := h.handshake(conn); err != nil {
		return err
	}

	buf := make([]byte, 512)
	dec := &decoder{}
	for {
		if ctx.Err() != nil {
			return nil
		}
		conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		n, err := conn.Read(buf)
		if n > 0 {
			for _, frame := range dec.feed(buf[:n]) {
				h.handleFrame(frame)
			}
		}
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if isTimeout(err) {
				continue
			}
			return fmt.Errorf("read: %w", err)
		}
	}
}

func (h *Hub) handshake(conn conn) error {
	var payload []byte
	payload = append(payload, buildAppStart("warnflux")...)
	payload = append(payload, buildDeviceQuery(3)...)
	payload = append(payload, buildGetBattery()...)
	if _, err := conn.Write(encodeFrame(payload)); err != nil {
		return fmt.Errorf("handshake write: %w", err)
	}
	return nil
}

// writeFrame encodes and writes one outgoing command.
func (h *Hub) writeFrame(payload []byte) error {
	h.mu.Lock()
	conn := h.client
	h.mu.Unlock()
	if conn == nil {
		return errors.New("meshcore: device not connected")
	}
	if _, err := conn.Write(encodeFrame(payload)); err != nil {
		return fmt.Errorf("meshcore: write: %w", err)
	}
	return nil
}

// SendChannelMessage sends one text message on the configured channel.
func (h *Hub) SendChannelMessage(text string) error {
	if err := h.writeFrame(buildSendChannelTxtMsg(byte(h.cfg.ChannelIdx), text)); err != nil {
		return err
	}
	h.recordMessage("tx", "SOSNA", fmt.Sprintf("ch%d", h.cfg.ChannelIdx), text)
	return nil
}

// SendContactMessage sends one direct text message to a contact's public
// key prefix (12 hex chars). The contact must already exist on the device.
func (h *Hub) SendContactMessage(prefixHex, text string) error {
	b, err := hex.DecodeString(prefixHex)
	if err != nil || len(b) != 6 {
		return fmt.Errorf("meshcore: contact prefix must be 12 hex chars")
	}
	payload := []byte{cmdSendTxtMsg, 0, 0} // txtType plain, attempt 0
	payload = binary.LittleEndian.AppendUint32(payload, uint32(nowUnix()))
	payload = append(payload, b...)
	payload = append(payload, []byte(text)...)
	if err := h.writeFrame(payload); err != nil {
		return err
	}
	h.recordMessage("tx", prefixHex, "direct", text)
	return nil
}

// SendAdvert triggers a manual advert (0 = zero-hop, 1 = flood).
func (h *Hub) SendAdvert(kind int) error {
	if kind != AdvertZeroHop && kind != AdvertFlood {
		return fmt.Errorf("meshcore: invalid advert kind %d", kind)
	}
	return h.writeFrame(buildSendSelfAdvert(byte(kind)))
}

func (h *Hub) handleFrame(frame []byte) {
	if len(frame) == 0 {
		return
	}
	switch frame[0] {
	case respSelfInfo:
		if s, err := parseSelfInfo(frame[1:]); err == nil {
			h.mu.Lock()
			h.self = &s
			h.mu.Unlock()
			if h.logger != nil {
				h.logger.Info("meshcore: self info", "name", s.Name,
					"freq_mhz", float64(s.RadioFreqKHz)/1e3, "bw_khz", float64(s.RadioBwHz)/1e3)
			}
		}
	case respDeviceInfo:
		if d, err := parseDeviceInfo(frame[1:]); err == nil {
			h.mu.Lock()
			h.device = &d
			h.mu.Unlock()
			if h.logger != nil {
				h.logger.Info("meshcore: device info", "model", d.Model, "build", d.Build)
			}
		}
	case respBattery:
		if len(frame) >= 3 {
			h.mu.Lock()
			h.battery = int(binaryLE16(frame[1:3]))
			h.mu.Unlock()
		}
	case pushAdvert:
		if len(frame) >= 33 {
			h.touchNode(frame[1:33], "", 0, 0, 0)
		}
	case pushNewAdvert:
		if a, err := parseNewAdvert(frame[1:]); err == nil {
			h.touchNode(a.PublicKey, a.AdvName, a.Type, a.Lat(), a.Lon())
			if h.logger != nil {
				h.logger.Debug("meshcore: advert", "name", a.AdvName, "type", a.Type)
			}
		}
	case respChannelMsg:
		if m, err := parseChannelMsg(frame[1:], false); err == nil {
			h.receiveChannel(m)
		}
	case respChannelMsgV3:
		if m, err := parseChannelMsg(frame[1:], true); err == nil {
			h.receiveChannel(m)
		}
	case respContactMsg:
		if m, err := parseContactMsg(frame[1:], false); err == nil {
			h.recordMessage("rx", pubKeyHex(m.PubKeyPrefix), "direct", m.Text)
		}
	case respContactMsgV3:
		if m, err := parseContactMsg(frame[1:], true); err == nil {
			h.recordMessage("rx", pubKeyHex(m.PubKeyPrefix), "direct", m.Text)
		}
	case pushMsgWaiting:
		_ = h.writeFrame(buildSyncNextMessage())
	case pushLogRxData:
		// RF log stream: informational only (ignored).
	case respNoMoreMessages:
		// End of queued messages.
	default:
		// OK/Err/unknown: ignore silently.
	}
}

func (h *Hub) receiveChannel(m ChannelMessage) {
	h.recordMessage("rx", "", fmt.Sprintf("ch%d", m.ChannelIdx), m.Text)
	if h.logger != nil {
		h.logger.Info("meshcore: channel message", "channel", m.ChannelIdx, "text", m.Text)
	}
}

func (h *Hub) touchNode(pubKey []byte, name string, typ byte, lat, lon float64) {
	key := pubKeyHex(pubKey)
	now := time.Now()
	h.mu.Lock()
	defer h.mu.Unlock()
	n := h.nodes[key]
	if n == nil {
		n = &Node{PubKey: key}
		h.nodes[key] = n
	}
	n.LastSeen = now
	if name != "" {
		n.Name = name
	}
	n.Type = typ
	if lat != 0 || lon != 0 {
		n.Lat = lat
		n.Lon = lon
	}
	// Drop expired neighbours opportunistically.
	for k, v := range h.nodes {
		if now.Sub(v.LastSeen) > h.cfg.NodeTTL {
			delete(h.nodes, k)
		}
	}
}

func (h *Hub) recordMessage(direction, sender, channel, text string) {
	now := time.Now()
	h.mu.Lock()
	h.recent = append(h.recent, Message{Direction: direction, Sender: sender, Channel: channel, Text: text, At: now})
	if len(h.recent) > 64 {
		h.recent = h.recent[len(h.recent)-64:]
	}
	rec := h.recorder
	h.mu.Unlock()
	if rec != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := rec.RecordMeshMessage(ctx, direction, sender, channel, text, now); err != nil && h.logger != nil {
			h.logger.Debug("meshcore: message history record failed", "error", err)
		}
	}
}

// Snapshot is the live view for the admin page.
type Snapshot struct {
	Connected bool
	Name      string
	Model     string
	Firmware  string
	BatteryMV int
	FreqMHz   float64
	BwKHz     float64
	SF        byte
	CR        byte
	Lat       float64
	Lon       float64
	Nodes     []Node
	Recent    []Message
	LastError string
}

// Snapshot returns a copy of the hub state.
func (h *Hub) Snapshot() Snapshot {
	h.mu.Lock()
	defer h.mu.Unlock()
	s := Snapshot{
		Connected: h.connected,
		BatteryMV: h.battery,
		Nodes:     make([]Node, 0, len(h.nodes)),
		Recent:    append([]Message(nil), h.recent...),
	}
	if h.self != nil {
		s.Name = h.self.Name
		s.FreqMHz = float64(h.self.RadioFreqKHz) / 1e3
		s.BwKHz = float64(h.self.RadioBwHz) / 1e3
		s.SF = h.self.RadioSF
		s.CR = h.self.RadioCR
		s.Lat = h.self.Lat()
		s.Lon = h.self.Lon()
	}
	if h.device != nil {
		s.Model = h.device.Model
		s.Firmware = h.device.Build
	}
	for _, n := range h.nodes {
		if h.self != nil && (h.self.AdvLatRaw != 0 || h.self.AdvLonRaw != 0) && (n.Lat != 0 || n.Lon != 0) {
			n.DistKM = DistanceKM(h.self.Lat(), h.self.Lon(), n.Lat, n.Lon)
			n.BearingDeg = BearingDeg(h.self.Lat(), h.self.Lon(), n.Lat, n.Lon)
		}
		s.Nodes = append(s.Nodes, *n)
	}
	sort.Slice(s.Nodes, func(i, j int) bool { return s.Nodes[i].LastSeen.After(s.Nodes[j].LastSeen) })
	if h.lastErr != nil {
		s.LastError = h.lastErr.Error()
	}
	return s
}

func binaryLE16(b []byte) uint16 {
	return uint16(b[0]) | uint16(b[1])<<8
}

// isTimeout reports a benign read deadline exceeded.
func isTimeout(err error) bool {
	return errors.Is(err, os.ErrDeadlineExceeded)
}

// DistanceKM returns the haversine great-circle distance in kilometres
// between two WGS84 coordinates.
func DistanceKM(lat1, lon1, lat2, lon2 float64) float64 {
	const deg2rad = math.Pi / 180
	dLat := (lat2 - lat1) * deg2rad
	dLon := (lon2 - lon1) * deg2rad
	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(lat1*deg2rad)*math.Cos(lat2*deg2rad)*math.Sin(dLon/2)*math.Sin(dLon/2)
	return 6371.0088 * 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
}

// BearingDeg returns the initial bearing in degrees in [0, 360) from the
// first WGS84 coordinate to the second.
func BearingDeg(lat1, lon1, lat2, lon2 float64) float64 {
	const deg2rad = math.Pi / 180
	p1 := lat1 * deg2rad
	p2 := lat2 * deg2rad
	dLon := (lon2 - lon1) * deg2rad
	y := math.Sin(dLon) * math.Cos(p2)
	x := math.Cos(p1)*math.Sin(p2) - math.Sin(p1)*math.Cos(p2)*math.Cos(dLon)
	return math.Mod(math.Atan2(y, x)*180/math.Pi+360, 360)
}
