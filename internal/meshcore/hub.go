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
	"strings"
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
	PubKey string
	Name   string
	Type   byte
	Lat    float64
	Lon    float64
	// Hops is the contact's stored outbound path length (0 when unknown).
	Hops int
	// AdvAt is when the neighbour last sent an advert (zero when unknown).
	AdvAt time.Time
	// DistKM and BearingDeg are computed relative to our station's
	// position in Snapshot (0 when either position is unknown).
	DistKM     float64
	BearingDeg float64
	LastSeen   time.Time

	lastQuery time.Time // device contact lookup throttling (internal)
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
	// ChannelName optionally pins the device slot's name (e.g. "#sp9moa"):
	// the hub reads the slot at connect time and issues SET_CHANNEL when
	// the name differs, preserving the channel secret.
	ChannelName string
	// AutoAddContacts makes the device auto-add unknown heard nodes
	// (chat/repeater/room/sensor, up to 8 hops) to its contact list, so
	// their adverts reach WarnFlux and show up as heard nodes.
	AutoAddContacts bool
	// NodeTTL bounds how long an unheard neighbour stays in the node list.
	NodeTTL time.Duration
}

// Defaults applied when the config omits values.
const (
	DefaultBaud    = 115200
	DefaultNodeTTL = 30 * time.Minute
)

// PublicChannelIdx is the device's Public channel. WarnFlux never
// transmits on it: messages sent there would reach the whole mesh
// uninvited. Receiving is still allowed.
const PublicChannelIdx = 0

// contactQueryInterval throttles CMD_GET_CONTACT_BY_KEY lookups per node.
const contactQueryInterval = 30 * time.Second

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

	// pendingAcks queues send-command acknowledgements: the device
	// answers each host command in order with OK / SENT / ERR.
	pendingAcks []chan error
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
	if cfg.ChannelIdx == PublicChannelIdx && logger != nil {
		logger.Warn("meshcore: channel 0 is Public — transmissions are disabled")
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
	defer h.failPendingAcks(errors.New("meshcore: session ended"))

	if err := h.handshake(conn); err != nil {
		return err
	}
	if err := h.syncChannel(conn); err != nil {
		return err
	}
	if err := h.syncAutoadd(conn); err != nil {
		return err
	}

	// The device answers APP_START/DEVICE_QUERY/BATTERY once. If the
	// reply is lost (busy device, USB glitch) the station info stays
	// empty forever, which also breaks distance/bearing for nodes — so
	// re-request it while it is missing.
	lastHandshake := time.Now()
	buf := make([]byte, 512)
	dec := &decoder{}
	for {
		if ctx.Err() != nil {
			return nil
		}
		if time.Since(lastHandshake) > 10*time.Second {
			h.mu.Lock()
			missing := h.self == nil
			h.mu.Unlock()
			if missing {
				if err := h.handshake(conn); err != nil {
					return err
				}
				lastHandshake = time.Now()
			}
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

// syncAutoadd enables the device's auto-add of unknown heard nodes so
// their adverts reach WarnFlux (a persistent device preference). It reads
// the device reply itself because the main pump has not started yet.
func (h *Hub) syncAutoadd(conn conn) error {
	if !h.cfg.AutoAddContacts {
		return nil
	}
	const mask = 0x02 | 0x04 | 0x08 | 0x10 // chat, repeater, room, sensor
	if _, err := conn.Write(encodeFrame(buildSetAutoaddConfig(mask, 8))); err != nil {
		return fmt.Errorf("meshcore: set autoadd write: %w", err)
	}
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	dec := &decoder{}
	buf := make([]byte, 512)
	for {
		n, err := conn.Read(buf)
		if n > 0 {
			for _, frame := range dec.feed(buf[:n]) {
				if len(frame) == 0 {
					continue
				}
				if frame[0] == respOK {
					if h.logger != nil {
						h.logger.Info("meshcore: auto-add enabled", "mask", mask, "max_hops", 8)
					}
					return nil
				}
				if frame[0] == respErr {
					code := byte(0)
					if len(frame) > 1 {
						code = frame[1]
					}
					if h.logger != nil {
						h.logger.Warn("meshcore: autoadd config rejected", "code", code)
					}
					return nil // non-fatal
				}
				h.handleFrame(frame)
			}
		}
		if err != nil {
			if isTimeout(err) {
				if h.logger != nil {
					h.logger.Warn("meshcore: autoadd config timed out")
				}
				return nil // non-fatal: the link still works
			}
			return fmt.Errorf("meshcore: autoadd read: %w", err)
		}
	}
}

// syncChannel aligns the configured channel slot on the device with
// cfg.ChannelName (e.g. "#sp9moa"): it reads the current name via
// GET_CHANNEL and issues SET_CHANNEL only when the name differs, reusing
// the slot's existing secret. A missing answer only logs a warning — the
// link still works.
func (h *Hub) syncChannel(conn conn) error {
	if h.cfg.ChannelName == "" {
		return nil
	}
	idx := byte(h.cfg.ChannelIdx)
	if _, err := conn.Write(encodeFrame(buildGetChannel(idx))); err != nil {
		return fmt.Errorf("meshcore: get channel write: %w", err)
	}
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	dec := &decoder{}
	buf := make([]byte, 512)
	for {
		n, err := conn.Read(buf)
		if n > 0 {
			for _, frame := range dec.feed(buf[:n]) {
				if len(frame) == 0 {
					continue
				}
				if frame[0] == respChannelInfo {
					gotIdx, name, secret, perr := parseChannelInfo(frame[1:])
					if perr == nil && gotIdx == idx {
						if name != h.cfg.ChannelName {
							payload := buildSetChannel(idx, h.cfg.ChannelName, secret)
							if payload == nil {
								return errors.New("meshcore: bad channel secret length")
							}
							if _, werr := conn.Write(encodeFrame(payload)); werr != nil {
								return fmt.Errorf("meshcore: set channel write: %w", werr)
							}
							if h.logger != nil {
								h.logger.Info("meshcore: channel name synced", "channel", idx, "name", h.cfg.ChannelName)
							}
						}
						return nil
					}
				} else {
					h.handleFrame(frame)
				}
			}
		}
		if err != nil {
			if isTimeout(err) {
				if h.logger != nil {
					h.logger.Warn("meshcore: channel name sync timed out", "channel", idx)
				}
				return nil
			}
			return fmt.Errorf("meshcore: channel sync read: %w", err)
		}
	}
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

// registerAck enqueues a waiter for the next device OK/SENT/ERR reply.
func (h *Hub) registerAck() chan error {
	ch := make(chan error, 1)
	h.mu.Lock()
	h.pendingAcks = append(h.pendingAcks, ch)
	h.mu.Unlock()
	return ch
}

// popAck returns the oldest ack waiter, if any.
func (h *Hub) popAck() chan error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.pendingAcks) == 0 {
		return nil
	}
	ch := h.pendingAcks[0]
	h.pendingAcks = h.pendingAcks[1:]
	return ch
}

// failPendingAcks fails every outstanding waiter (session teardown).
func (h *Hub) failPendingAcks(err error) {
	h.mu.Lock()
	waiters := h.pendingAcks
	h.pendingAcks = nil
	h.mu.Unlock()
	for _, ch := range waiters {
		ch <- err
	}
}

// waitAck waits for the device to accept (nil) or reject (error) a
// command. OK, SENT and ERR frames all satisfy the wait.
func (h *Hub) waitAck(ack chan error) error {
	select {
	case err := <-ack:
		return err
	case <-time.After(5 * time.Second):
		return errors.New("meshcore: device did not acknowledge the command")
	}
}

// deviceErrText names the device's PACKET_ERR codes.
func deviceErrText(code byte) string {
	switch code {
	case 1:
		return "unsupported command"
	case 2:
		return "not found"
	case 3:
		return "table full"
	case 4:
		return "bad state"
	case 5:
		return "file io error"
	default:
		return fmt.Sprintf("code %d", code)
	}
}

// DeviceErr is a PACKET_ERR reply from the device.
type DeviceErr struct {
	Code byte
}

func (e *DeviceErr) Error() string {
	return fmt.Sprintf("meshcore: device rejected the command: %s", deviceErrText(e.Code))
}

// SendChannelMessage sends one text message on the configured channel.
// The Public channel (0) is refused: never transmit there.
func (h *Hub) SendChannelMessage(text string) error {
	if h.cfg.ChannelIdx == PublicChannelIdx {
		return errors.New("meshcore: refusing to transmit on public channel 0")
	}
	ack := h.registerAck()
	if err := h.writeFrame(buildSendChannelTxtMsg(byte(h.cfg.ChannelIdx), text)); err != nil {
		return err
	}
	if err := h.waitAck(ack); err != nil {
		return err
	}
	h.recordMessage("tx", "SOSNA", fmt.Sprintf("ch%d", h.cfg.ChannelIdx), text)
	return nil
}

// SendContactMessage sends one direct text message to a contact's public
// key prefix (12 hex chars) or full key (64 hex chars). When the full key
// is given and the device does not know the contact yet, the contact is
// added on the device and the send retried once.
func (h *Hub) SendContactMessage(addr, text string) error {
	b, err := hex.DecodeString(strings.TrimPrefix(addr, "0x"))
	if err != nil || (len(b) != 6 && len(b) != 32) {
		return fmt.Errorf("meshcore: contact address must be 12 or 64 hex chars")
	}
	var fullKey []byte
	if len(b) == 32 {
		fullKey = b
	}
	prefix := b[:6]
	send := func() error {
		payload := []byte{cmdSendTxtMsg, 0, 0} // txtType plain, attempt 0
		payload = binary.LittleEndian.AppendUint32(payload, uint32(nowUnix()))
		payload = append(payload, prefix...)
		payload = append(payload, []byte(text)...)
		ack := h.registerAck()
		if err := h.writeFrame(payload); err != nil {
			return err
		}
		return h.waitAck(ack)
	}
	if err := send(); err != nil {
		var derr *DeviceErr
		if !errors.As(err, &derr) || derr.Code != 2 || fullKey == nil {
			return err
		}
		// The device does not know this contact yet: add it and retry.
		ack := h.registerAck()
		if err := h.writeFrame(buildAddUpdateContact(fullKey)); err != nil {
			return err
		}
		if err := h.waitAck(ack); err != nil {
			return err
		}
		if err := send(); err != nil {
			return err
		}
	}
	h.recordMessage("tx", hex.EncodeToString(prefix), "direct", text)
	return nil
}

// SendAdvert triggers a manual advert (0 = zero-hop, 1 = flood).
func (h *Hub) SendAdvert(kind int) error {
	if kind != AdvertZeroHop && kind != AdvertFlood {
		return fmt.Errorf("meshcore: invalid advert kind %d", kind)
	}
	ack := h.registerAck()
	if err := h.writeFrame(buildSendSelfAdvert(byte(kind))); err != nil {
		return err
	}
	return h.waitAck(ack)
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
			h.touchNode(frame[1:33], "", 0, 0, 0, -1, time.Time{})
			// Known contacts only push their pubkey: ask the device for
			// the full record so the node list shows real names.
			h.maybeQueryContact(frame[1:33])
		}
	case pushNewAdvert:
		if a, err := parseNewAdvert(frame[1:]); err == nil {
			var advAt time.Time
			if a.LastAdvert != 0 {
				advAt = time.Unix(int64(a.LastAdvert), 0)
			}
			h.touchNode(a.PublicKey, a.AdvName, a.Type, a.Lat(), a.Lon(), int(a.OutPathLen), advAt)
			if h.logger != nil {
				h.logger.Debug("meshcore: advert", "name", a.AdvName, "type", a.Type)
			}
		}
	case respContact:
		// Full contact record (name, type, path, position, last advert)
		// answering CMD_GET_CONTACT_BY_KEY. Same layout as NEW_ADVERT.
		if a, err := parseNewAdvert(frame[1:]); err == nil {
			var advAt time.Time
			if a.LastAdvert != 0 {
				advAt = time.Unix(int64(a.LastAdvert), 0)
			}
			h.touchNode(a.PublicKey, a.AdvName, a.Type, a.Lat(), a.Lon(), int(a.OutPathLen), advAt)
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
	case respErr:
		code := byte(0)
		if len(frame) > 1 {
			code = frame[1]
		}
		if ch := h.popAck(); ch != nil {
			ch <- &DeviceErr{Code: code}
		} else if h.logger != nil {
			h.logger.Warn("meshcore: device error", "code", code)
		}
	case respOK, respSent:
		if ch := h.popAck(); ch != nil {
			ch <- nil
		}
	default:
		// unknown: ignore silently.
	}
}

func (h *Hub) receiveChannel(m ChannelMessage) {
	h.recordMessage("rx", "", fmt.Sprintf("ch%d", m.ChannelIdx), m.Text)
	if h.logger != nil {
		h.logger.Info("meshcore: channel message", "channel", m.ChannelIdx, "text", m.Text)
	}
}

func (h *Hub) touchNode(pubKey []byte, name string, typ byte, lat, lon float64, hops int, advAt time.Time) {
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
	if hops >= 0 {
		n.Hops = hops
	}
	if !advAt.IsZero() {
		n.AdvAt = advAt
	}
	// Drop expired neighbours opportunistically.
	for k, v := range h.nodes {
		if now.Sub(v.LastSeen) > h.cfg.NodeTTL {
			delete(h.nodes, k)
		}
	}
}

// maybeQueryContact asks the device for a known contact's full record
// when the node has no name yet (throttled per key).
func (h *Hub) maybeQueryContact(pubKey []byte) {
	key := pubKeyHex(pubKey)
	now := time.Now()
	h.mu.Lock()
	n := h.nodes[key]
	if n == nil || n.Name != "" || now.Sub(n.lastQuery) < contactQueryInterval {
		h.mu.Unlock()
		return
	}
	n.lastQuery = now
	h.mu.Unlock()
	if err := h.writeFrame(buildGetContactByKey(pubKey)); err != nil && h.logger != nil {
		h.logger.Debug("meshcore: contact query failed", "error", err)
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
	Connected   bool
	Name        string
	Model       string
	Firmware    string
	BatteryMV   int
	FreqMHz     float64
	BwKHz       float64
	SF          byte
	CR          byte
	Lat         float64
	Lon         float64
	ChannelIdx  int
	ChannelName string
	Nodes       []Node
	Recent      []Message
	LastError   string
}

// Snapshot returns a copy of the hub state.
func (h *Hub) Snapshot() Snapshot {
	h.mu.Lock()
	defer h.mu.Unlock()
	s := Snapshot{
		Connected:   h.connected,
		BatteryMV:   h.battery,
		ChannelIdx:  h.cfg.ChannelIdx,
		ChannelName: h.cfg.ChannelName,
		Nodes:       make([]Node, 0, len(h.nodes)),
		Recent:      append([]Message(nil), h.recent...),
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
