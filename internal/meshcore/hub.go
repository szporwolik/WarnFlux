package meshcore

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Recorder persists mesh message history (implemented by storage stores).
// Best-effort: the hub logs recording failures and keeps running.
type Recorder interface {
	RecordMeshMessage(ctx context.Context, direction, sender, channel, text, operator string, hops int, at time.Time) error
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
	lastPub   time.Time // MQTT station publish throttling (internal)
}

// Message is one received or sent channel/contact message.
type Message struct {
	Direction string // rx | tx
	Sender    string
	Channel   string
	Hops      int    // radio path length from the frame (0 = unknown/ours)
	Operator  string // admin username behind a tx (rx rows are empty)
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
	// ChannelNames optionally maps channel indices onto friendly display
	// names (e.g. 0: "Public"): recorded messages carry the friendly
	// name instead of "ch0"; the TX slot falls back to ChannelName.
	ChannelNames map[int]string
	// AutoAddContacts makes the device auto-add unknown heard nodes
	// (chat/repeater/room/sensor, up to 8 hops) to its contact list, so
	// their adverts reach WarnFlux and show up as heard nodes.
	AutoAddContacts bool
	// RouteMessages re-publishes direct messages from directory-known
	// senders as canonical /events documents, so they enter the normal
	// MQTT routing matrix (the alarm pipeline) like APRS messages do.
	RouteMessages bool
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

// ChannelLabel resolves a channel index onto its friendly display name:
// the configured channel_names map first, then the pinned ChannelName
// for the TX slot, then "ch<N>" as a fallback for unnamed slots.
func (h *Hub) ChannelLabel(idx int) string {
	if h.cfg.ChannelNames != nil {
		if name := strings.TrimSpace(h.cfg.ChannelNames[idx]); name != "" {
			return name
		}
	}
	if idx == h.cfg.ChannelIdx && h.cfg.ChannelName != "" {
		return h.cfg.ChannelName
	}
	return fmt.Sprintf("ch%d", idx)
}

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

	// stationSink optionally publishes heard stations to MQTT as retained
	// documents (nil payload + retained = topic delete).
	stationSink func(ctx context.Context, topic string, retained bool, payload []byte) error
	// messageSink optionally publishes rx/tx messages to MQTT
	// (non-retained, one JSON document per message).
	messageSink func(ctx context.Context, topic string, retained bool, payload []byte) error
	// senderGate approves the 12-hex key prefix of a direct-message
	// sender for the routing bridge (registered operators only).
	senderGate func(key string) bool
	// contactProtect optionally re-adds a contact the device just
	// overwrote when the key belongs to a registered operator.
	contactProtect func(key string) bool
	// eventSink publishes routed message events on the /events stream.
	eventSink func(ctx context.Context, topic string, retained bool, payload []byte) error

	// cmdCh serializes every host command that expects a device reply:
	// ONE command worker writes a command, waits for its matching reply
	// (the expected type is part of the request) and only then takes the
	// next one. The frame pump never blocks here — commands discovered
	// while reading (contact queries, protect re-adds) are enqueued
	// best-effort, so a reply can always reach the worker.
	cmdCh chan cmdReq
	// curCmd is the command whose reply the frame pump is currently
	// routing (guarded by mu); exactly one command is in flight.
	curCmd *cmdSlot
	// sessionCtx is cancelled when the current session ends: command
	// submission fails against it and the worker stops with it
	// (guarded by mu).
	sessionCtx    context.Context
	sessionCancel context.CancelFunc
	// ready is closed once a session finished its startup handshake and
	// queue drain; sends wait on it so they are written after the
	// startup command stream.
	ready chan struct{}
}

// cmdExpect selects which device reply satisfies a command.
type cmdExpect byte

const (
	// expectAck: the command is satisfied by OK / SENT / ERR.
	expectAck cmdExpect = iota + 1
	// expectContact: the command is satisfied by a contact record
	// (GET_CONTACT_BY_KEY answer) or ERR.
	expectContact
)

// cmdReplyTimeout bounds the wait for a send/ack reply; contact queries
// get a shorter budget (their record is only informational).
const (
	cmdReplyTimeout   = 5 * time.Second
	queryReplyTimeout = 2 * time.Second

	// abandonedWriteGrace bounds how long the command worker waits for a
	// caller-cancelled write to settle before it recreates the session:
	// a healthy serial write completes in milliseconds, and a stuck one
	// must end the session instead of letting the next command race it.
	abandonedWriteGrace = 500 * time.Millisecond
)

// cmdReq is one device command the worker executes.
type cmdReq struct {
	payload []byte
	expect  cmdExpect
	timeout time.Duration // 0 = cmdReplyTimeout
	// ctx is the CALLER's cancellation: the worker aborts the write and
	// the ACK wait the moment it is done, so an unavailable device is a
	// transient error the caller retries — never a hung plugin. nil for
	// background commands (frame-pump paths).
	ctx context.Context
	// reply receives the result (buffered, size 1); background commands
	// may leave it unread — the worker never blocks delivering to it.
	reply chan error
}

// cmdSlot is the in-flight command the frame pump routes replies to.
type cmdSlot struct {
	expect cmdExpect
	// done carries the reply from the frame pump to the worker
	// (buffered, size 1); the worker forwards it to the caller.
	done chan error
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
		return &Hub{cfg: cfg, logger: logger, nodes: make(map[string]*Node), ready: make(chan struct{}), cmdCh: make(chan cmdReq, 16)}, nil
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
		ready:  make(chan struct{}),
		cmdCh:  make(chan cmdReq, 16),
	}, nil
}

// SetRecorder attaches the durable message history store (optional).
func (h *Hub) SetRecorder(r Recorder) { h.mu.Lock(); h.recorder = r; h.mu.Unlock() }

// SetStationSink attaches the MQTT station publisher (optional): every
// heard node becomes a retained document under meshcore/stations/<key12>
// and is tombstoned when its NodeTTL expires.
func (h *Hub) SetStationSink(fn func(ctx context.Context, topic string, retained bool, payload []byte) error) {
	h.mu.Lock()
	h.stationSink = fn
	h.mu.Unlock()
}

// SetMessageSink attaches the MQTT message publisher (optional): every
// received or sent message becomes one non-retained JSON document on
// meshcore/messages.
func (h *Hub) SetMessageSink(fn func(ctx context.Context, topic string, retained bool, payload []byte) error) {
	h.mu.Lock()
	h.messageSink = fn
	h.mu.Unlock()
}

// SetSenderGate installs the direct-message sender allow-list check. The
// gate receives the sender's lowercase 12-hex key prefix and reports
// whether it belongs to a registered WarnFlux user. Without the gate no
// mesh message becomes a hazard event.
func (h *Hub) SetSenderGate(fn func(key string) bool) {
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

// SetContactProtector installs the directory check for overwritten
// contacts: when the device evicts a contact (auto-add recycling) whose
// full key the callback approves, the hub immediately re-adds it so
// registered operators stay addressable and decryptable.
func (h *Hub) SetContactProtector(fn func(key string) bool) {
	h.mu.Lock()
	h.contactProtect = fn
	h.mu.Unlock()
}

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
	h.ready = make(chan struct{})
	h.mu.Unlock()
	defer conn.Close()
	// Session cancellation closes the transport: a blocked Read or Write
	// (silent device, full line buffer) unblocks immediately instead of
	// waiting out its read deadline.
	stopWatch := make(chan struct{})
	defer close(stopWatch)
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-stopWatch:
		}
	}()

	if err := h.handshake(conn); err != nil {
		return err
	}
	if err := h.syncChannel(conn); err != nil {
		return err
	}
	if err := h.syncAutoadd(conn); err != nil {
		return err
	}
	if err := h.drainMessages(conn); err != nil {
		return err
	}

	// Startup stream done: install the session context, unblock the
	// sends and start the single command worker. The context is stored
	// BEFORE ready closes so a send that passes waitReady always finds
	// a live session.
	cmdCtx, cmdCancel := context.WithCancel(ctx)
	h.mu.Lock()
	h.sessionCtx = cmdCtx
	h.sessionCancel = cmdCancel
	h.curCmd = nil
	close(h.ready)
	h.mu.Unlock()
	cmdDone := make(chan struct{})
	go func() {
		defer close(cmdDone)
		h.commandWorker(cmdCtx, conn)
	}()
	defer func() {
		// Every exit path: stop the worker first (an in-flight command
		// gets "session ended"), close the connection so a stuck write
		// can never hold the worker hostage, wait for it, then forget
		// the session.
		cmdCancel()
		conn.Close()
		<-cmdDone
		h.mu.Lock()
		h.sessionCtx = nil
		h.sessionCancel = nil
		h.curCmd = nil
		h.mu.Unlock()
	}()

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

// drainMessages pulls messages the device queued while the app was away:
// it loops SYNC_NEXT until the device answers NO_MORE, so messages never
// sit in the device queue for hours waiting for the next tickle. It reads
// its own replies because the main pump has not started yet; unrelated
// pushes are handled normally.
func (h *Hub) drainMessages(conn conn) error {
	dec := &decoder{}
	buf := make([]byte, 512)
	deadline := time.Now().Add(8 * time.Second)
syncNext:
	for time.Now().Before(deadline) {
		// The device always reads; a write failure here (stalled peer,
		// closed test pipe) is non-fatal — the queue drains on the next
		// tickle or reconnect.
		if _, err := conn.Write(encodeFrame(buildSyncNextMessage())); err != nil {
			return nil
		}
		conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		for {
			if time.Now().After(deadline) {
				return nil
			}
			n, err := conn.Read(buf)
			if n > 0 {
				gotMsg := false
				noMore := false
				for _, frame := range dec.feed(buf[:n]) {
					if len(frame) == 0 {
						continue
					}
					switch frame[0] {
					case respContactMsg, respContactMsgV3:
						if m, err := parseContactMsg(frame[1:], frame[0] == respContactMsgV3); err == nil {
							h.receiveContact(m)
						}
						gotMsg = true
					case respChannelMsg, respChannelMsgV3:
						if m, err := parseChannelMsg(frame[1:], frame[0] == respChannelMsgV3); err == nil {
							h.receiveChannel(m)
						}
						gotMsg = true
					case respNoMoreMessages:
						noMore = true
					default:
						// Unrelated push (advert, path update…) — normal
						// handling, keep draining.
						h.handleFrame(frame)
					}
				}
				// A chunk may carry NO_MORE followed by further pushes
				// (self info, adverts): handle them all, then stop.
				if noMore {
					return nil
				}
				if gotMsg {
					continue syncNext
				}
			}
			if err != nil {
				if isTimeout(err) {
					return nil // nothing queued
				}
				return fmt.Errorf("meshcore: drain read: %w", err)
			}
		}
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
	// Mask 0x01 (overwrite oldest non-favourite when the contact table is
	// full) keeps auto-add working forever; without it the device stops
	// accepting new contacts and fires CONTACTS_FULL (0x90) pushes while
	// direct messages from unknown keys are silently dropped.
	const mask = 0x01 | 0x02 | 0x04 | 0x08 | 0x10 // overwrite + chat, repeater, room, sensor
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

// writeFrameOn encodes and writes one outgoing command on the given
// connection (the command worker owns its session connection).
func (h *Hub) writeFrameOn(c conn, payload []byte) error {
	if _, err := c.Write(encodeFrame(payload)); err != nil {
		return fmt.Errorf("meshcore: write: %w", err)
	}
	return nil
}

// commandWorker is the single goroutine executing every reply-expecting
// host command, strictly one at a time.
func (h *Hub) commandWorker(ctx context.Context, c conn) {
	for {
		select {
		case <-ctx.Done():
			return
		case req := <-h.cmdCh:
			h.runCommand(ctx, c, req)
		}
	}
}

// writeResult reports one completed writeFrameOn attempt.
type writeResult struct{ err error }

// runCommand writes one command and waits for its MATCHING reply. The
// expectation slot is published before the write so the frame pump can
// route the reply the instant it arrives; every exit path (write
// failure, session end, timeout) clears the slot — no stale waiter can
// ever consume a later command's reply.
//
// Cancellation contract (P1): a command whose caller is ALREADY
// cancelled is rejected BEFORE any I/O — nothing reaches the transport.
// A write that is in flight when the cancellation lands is never
// abandoned mid-frame on the live stream: the worker waits a bounded
// grace for it to settle and then recreates the session (two concurrent
// writes would interleave frames), and a write that stalls past the
// command timeout does the same at once.
func (h *Hub) runCommand(ctx context.Context, c conn, req cmdReq) {
	timeout := req.timeout
	if timeout <= 0 {
		timeout = cmdReplyTimeout
	}
	// The caller's cancellation competes with the session context and
	// the command timeout on every wait — an action whose context is
	// done fails fast with a transient error instead of being mistaken
	// for a hung plugin.
	var callerDone <-chan struct{}
	if req.ctx != nil {
		callerDone = req.ctx.Done()
	}

	// Cancel-check BEFORE any I/O: an already-cancelled command must
	// never reach the transport.
	if req.ctx != nil {
		select {
		case <-callerDone:
			deliverErr(req.reply, req.ctx.Err())
			return
		default:
		}
	}
	select {
	case <-ctx.Done():
		deliverErr(req.reply, errors.New("meshcore: session ended"))
		return
	default:
	}

	slot := &cmdSlot{expect: req.expect, done: make(chan error, 1)}
	h.mu.Lock()
	h.curCmd = slot
	h.mu.Unlock()
	clear := func() {
		h.mu.Lock()
		if h.curCmd == slot {
			h.curCmd = nil
		}
		h.mu.Unlock()
	}

	// The write itself is bounded too: a stalled transport must never
	// hold the worker (and every queued command) hostage. Once STARTED,
	// a write is never abandoned on the live stream (see the contract
	// above); the session teardown closes the connection, which unblocks
	// a stray write.
	wdone := make(chan writeResult, 1)
	go func() { wdone <- writeResult{h.writeFrameOn(c, req.payload)} }()
	select {
	case wr := <-wdone:
		if wr.err != nil {
			clear()
			deliverErr(req.reply, wr.err)
			return
		}
	case <-ctx.Done():
		// Session ending: the teardown closes the connection and the
		// next session is a fresh one — no stream corruption possible.
		clear()
		deliverErr(req.reply, errors.New("meshcore: session ended"))
		return
	case <-callerDone:
		clear()
		deliverErr(req.reply, req.ctx.Err())
		h.settleOrRecreateSession(c, wdone)
		return
	case <-time.After(timeout):
		// The WRITE itself stalled (full line buffer): the stream is
		// unusable — recreate the session instead of letting the next
		// command race the stuck write.
		clear()
		deliverErr(req.reply, errors.New("meshcore: device did not acknowledge the command"))
		h.logger.Warn("meshcore: command write stalled; recreating the session")
		_ = c.Close()
		<-wdone // bounded: Close unblocks the write
		return
	}

	select {
	case err := <-slot.done:
		// The frame pump routed this command's matching reply here.
		clear()
		deliverErr(req.reply, err)
	case <-callerDone:
		clear()
		deliverErr(req.reply, req.ctx.Err())
	case <-ctx.Done():
		clear()
		deliverErr(req.reply, errors.New("meshcore: session ended"))
	case <-time.After(timeout):
		clear()
		deliverErr(req.reply, errors.New("meshcore: device did not acknowledge the command"))
	}
}

// settleOrRecreateSession keeps the session stream frame-consistent after
// a caller cancelled a command mid-write: it waits a bounded grace for
// the in-flight write to complete (a healthy serial write settles in
// milliseconds), and closes the connection when the write is still stuck
// — ending the session, so the next command runs on a freshly dialed one
// and can never interleave with the abandoned frame.
func (h *Hub) settleOrRecreateSession(c conn, wdone <-chan writeResult) {
	select {
	case <-wdone:
		// The frame completed (or failed cleanly): the stream is
		// consistent and the next command is safe.
	case <-time.After(abandonedWriteGrace):
		// Still mid-write: recreate the session.
		h.logger.Warn("meshcore: command write abandoned; recreating the session")
		_ = c.Close()
		<-wdone // bounded: Close unblocks the write
	}
}

// deliverErr hands a command result to its waiter without ever blocking
// (background commands may leave the channel unread).
func deliverErr(ch chan error, err error) {
	if ch == nil {
		return
	}
	select {
	case ch <- err:
	default:
	}
}

// submitCmd enqueues one command for the worker and waits until the
// worker takes it (commands are strictly serialized). It fails fast
// when no session is running and aborts on the caller's cancellation.
func (h *Hub) submitCmd(ctx context.Context, req cmdReq) error {
	h.mu.Lock()
	sc := h.sessionCtx
	h.mu.Unlock()
	if sc == nil {
		return errors.New("meshcore: device not connected")
	}
	select {
	case h.cmdCh <- req:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-sc.Done():
		return errors.New("meshcore: session ended")
	}
}

// enqueueCmdBackground offers a command to the worker without blocking
// (frame-pump paths: contact queries, protect re-adds). The worker
// still waits for the matching reply to keep the command stream
// ordered; an unanswered background command can only delay later
// commands by its own timeout.
func (h *Hub) enqueueCmdBackground(req cmdReq) {
	h.mu.Lock()
	sc := h.sessionCtx
	h.mu.Unlock()
	if sc == nil {
		return
	}
	select {
	case h.cmdCh <- req:
	default:
		if h.logger != nil {
			h.logger.Debug("meshcore: command queue full, dropping background command")
		}
	}
}

// takeSlot consumes the in-flight command when the frame matches its
// expected reply type; anything else is a late/mismatched reply and is
// ignored by the command stream. The returned channel feeds the worker.
func (h *Hub) takeSlot(expect cmdExpect) chan error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.curCmd == nil || h.curCmd.expect != expect {
		return nil
	}
	ch := h.curCmd.done
	h.curCmd = nil
	return ch
}

// takeSlotAny consumes the in-flight command regardless of type
// (respErr satisfies any command).
func (h *Hub) takeSlotAny() chan error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.curCmd == nil {
		return nil
	}
	ch := h.curCmd.done
	h.curCmd = nil
	return ch
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

// waitReady blocks until the current session finished its startup
// handshake and queue drain, so sends are written after the startup
// command stream instead of interleaving with SYNC_NEXT frames. The
// caller's cancellation aborts the wait immediately — an unavailable
// device is a transient error, never a hung call.
func (h *Hub) waitReady(ctx context.Context) error {
	h.mu.Lock()
	ready := h.ready
	h.mu.Unlock()
	select {
	case <-ready:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(12 * time.Second):
		return errors.New("meshcore: device session is not ready")
	}
}

// SendChannelMessage sends one text message on the configured channel.
// The Public channel (0) is refused: never transmit there. operator is
// the username behind the send (admin panel) or "" for automation.
func (h *Hub) SendChannelMessage(ctx context.Context, text, operator string) error {
	if h.cfg.ChannelIdx == PublicChannelIdx {
		return errors.New("meshcore: refusing to transmit on public channel 0")
	}
	if err := h.waitReady(ctx); err != nil {
		return err
	}
	// One command through the serialized worker: only THIS command's
	// reply (OK/SENT/ERR) can satisfy it.
	req := cmdReq{
		payload: buildSendChannelTxtMsg(byte(h.cfg.ChannelIdx), text),
		expect:  expectAck,
		reply:   make(chan error, 1),
		ctx:     ctx,
	}
	if err := h.submitCmd(ctx, req); err != nil {
		return err
	}
	if err := <-req.reply; err != nil {
		return err
	}
	h.recordMessage("tx", "SOSNA", h.ChannelLabel(h.cfg.ChannelIdx), text, operator, 0)
	return nil
}

// SendContactMessage sends one direct text message to a contact's public
// key prefix (12 hex chars) or full key (64 hex chars). When the full key
// is given and the device does not know the contact yet, the contact is
// added on the device and the send retried once.
func (h *Hub) SendContactMessage(ctx context.Context, addr, text, operator string) error {
	b, err := hex.DecodeString(strings.TrimPrefix(addr, "0x"))
	if err != nil || (len(b) != 6 && len(b) != 32) {
		return fmt.Errorf("meshcore: contact address must be 12 or 64 hex chars")
	}
	var fullKey []byte
	if len(b) == 32 {
		fullKey = b
	}
	prefix := b[:6]
	if err := h.waitReady(ctx); err != nil {
		return err
	}
	// The contact add/retry sequence and the send share one in-order
	// command stream (the worker serializes it); each step waits for its
	// own reply.
	send := func() error {
		payload := []byte{cmdSendTxtMsg, 0, 0} // txtType plain, attempt 0
		payload = binary.LittleEndian.AppendUint32(payload, uint32(nowUnix()))
		payload = append(payload, prefix...)
		payload = append(payload, []byte(text)...)
		req := cmdReq{payload: payload, expect: expectAck, reply: make(chan error, 1), ctx: ctx}
		if err := h.submitCmd(ctx, req); err != nil {
			return err
		}
		return <-req.reply
	}
	if err := send(); err != nil {
		var derr *DeviceErr
		if !errors.As(err, &derr) || derr.Code != 2 || fullKey == nil {
			return err
		}
		// The device does not know this contact yet: add it and retry.
		req := cmdReq{payload: buildAddUpdateContact(fullKey), expect: expectAck, reply: make(chan error, 1), ctx: ctx}
		if err := h.submitCmd(ctx, req); err != nil {
			return err
		}
		if err := <-req.reply; err != nil {
			return err
		}
		if err := send(); err != nil {
			return err
		}
	}
	h.recordMessage("tx", hex.EncodeToString(prefix), "direct", text, operator, 0)
	return nil
}

// SendAdvert triggers a manual advert (0 = zero-hop, 1 = flood).
func (h *Hub) SendAdvert(ctx context.Context, kind int) error {
	if kind != AdvertZeroHop && kind != AdvertFlood {
		return fmt.Errorf("meshcore: invalid advert kind %d", kind)
	}
	if err := h.waitReady(ctx); err != nil {
		return err
	}
	req := cmdReq{
		payload: buildSendSelfAdvert(byte(kind)),
		expect:  expectAck,
		reply:   make(chan error, 1),
		ctx:     ctx,
	}
	if err := h.submitCmd(ctx, req); err != nil {
		return err
	}
	return <-req.reply
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
		// A pending contact query is satisfied by exactly this record
		// (never by an ack frame, and never the other way around).
		if ch := h.takeSlot(expectContact); ch != nil {
			deliverErr(ch, nil)
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
			h.receiveContact(m)
		}
	case respContactMsgV3:
		if m, err := parseContactMsg(frame[1:], true); err == nil {
			h.receiveContact(m)
		}
	case pushMsgWaiting:
		if h.logger != nil {
			h.logger.Debug("meshcore: messages waiting, pulling queue")
		}
		_ = h.writeFrame(buildSyncNextMessage())
	case pushSendConfirmed:
		// The device heard the destination's delivery ACK for one of our
		// direct messages: the protocol's proof of delivery (channel
		// floods carry no acks, so only direct sends can confirm).
		if len(frame) >= 9 {
			trip := binary.LittleEndian.Uint32(frame[5:9])
			if h.logger != nil {
				h.logger.Info("meshcore: direct message confirmed",
					"ack", hex.EncodeToString(frame[1:5]), "trip_ms", trip)
			}
		} else if h.logger != nil {
			h.logger.Debug("meshcore: send confirmed push too short", "len", len(frame))
		}
	case pushContactDeleted:
		// The device overwrote a contact (auto-add recycling). When the
		// key belongs to a registered operator, re-add it immediately so
		// direct messages stay decryptable — the device fills in name and
		// details from the next advert.
		if len(frame) >= 33 {
			key := pubKeyHex(frame[1:33])
			h.mu.Lock()
			protect := h.contactProtect
			h.mu.Unlock()
			if protect != nil && protect(key) {
				if h.logger != nil {
					h.logger.Info("meshcore: re-adding protected contact", "key", key[:12])
				}
				req := cmdReq{
					payload: buildAddUpdateContact(frame[1:33]),
					expect:  expectAck,
					reply:   make(chan error, 1),
				}
				h.enqueueCmdBackground(req)
				return
			}
		}
		if h.logger != nil {
			key := ""
			if len(frame) >= 33 {
				key = pubKeyHex(frame[1:33])[:12]
			}
			h.logger.Debug("meshcore: contact overwritten on device", "key", key)
		}
	case pushContactsFull:
		// Without the overwrite-oldest bit the device stops adding new
		// contacts — direct messages from unknown keys are then dropped.
		if h.logger != nil {
			h.logger.Warn("meshcore: device contact storage full", "hint", "auto-add overwrite bit is enabled, table will recycle")
		}
	case pushLogRxData:
		// RF log stream: informational only (ignored).
	case respNoMoreMessages:
		// End of queued messages.
		if h.logger != nil {
			h.logger.Debug("meshcore: queued messages drained")
		}
	case respErr:
		code := byte(0)
		if len(frame) > 1 {
			code = frame[1]
		}
		if ch := h.takeSlotAny(); ch != nil {
			deliverErr(ch, &DeviceErr{Code: code})
		} else if h.logger != nil {
			h.logger.Warn("meshcore: device error", "code", code)
		}
	case respOK, respSent:
		// Satisfy an in-flight ack-expecting command. A late reply for a
		// timed-out command (or a reply to a command that never expected
		// an ack) matches no slot and is dropped — it can never satisfy
		// a different command.
		if ch := h.takeSlot(expectAck); ch != nil {
			deliverErr(ch, nil)
		} else if h.logger != nil {
			h.logger.Debug("meshcore: ack without an in-flight command", "type", frame[0])
		}
	default:
		// unknown: ignore silently, but log the type at debug level so
		// unexpected device frames are visible in the log.
		if h.logger != nil {
			h.logger.Debug("meshcore: unhandled frame", "type", frame[0], "len", len(frame))
		}
	}
}

func (h *Hub) receiveChannel(m ChannelMessage) {
	h.recordMessage("rx", "", h.ChannelLabel(int(m.ChannelIdx)), m.Text, "", int(m.PathLen))
	if h.logger != nil {
		h.logger.Info("meshcore: channel message", "channel", m.ChannelIdx, "text", m.Text)
	}
}

// receiveContact handles one direct message: it records the rx and, when
// routing is enabled and the sender's 12-hex key prefix sits on the
// registered-user allow-list, re-publishes the message as a canonical
// /events document (the "meshcore" source in the routing matrix).
func (h *Hub) receiveContact(m ContactMessage) {
	prefix := pubKeyHex(m.PubKeyPrefix)
	if h.logger != nil {
		h.logger.Info("meshcore: contact message", "from", prefix, "text", m.Text)
	}
	h.recordMessage("rx", prefix, "direct", m.Text, "", int(m.PathLen))
	text := strings.TrimSpace(m.Text)
	if !h.cfg.RouteMessages || text == "" || ackText(text) {
		return
	}
	h.mu.Lock()
	gate := h.senderGate
	name := h.nodeNameForPrefixLocked(prefix)
	selfName := ""
	if h.self != nil {
		selfName = h.self.Name
	}
	h.mu.Unlock()
	if gate == nil || !gate(prefix) {
		return
	}
	if h.logger != nil {
		h.logger.Info("meshcore: direct message routed", "from", prefix, "text", text)
	}
	h.publishMessageEvent(prefix, name, selfName, text)
}

// nodeNameForPrefixLocked returns the known name of the node whose key
// starts with the given 12-hex prefix (callers hold h.mu).
func (h *Hub) nodeNameForPrefixLocked(prefix string) string {
	for key, n := range h.nodes {
		if strings.HasPrefix(key, prefix) && n.Name != "" {
			return n.Name
		}
	}
	return ""
}

// ackText reports whether a received direct-message text is an ack/rej
// protocol reply.
func ackText(text string) bool {
	return strings.HasPrefix(text, "ack") || strings.HasPrefix(text, "rej")
}

// publishMessageEvent re-publishes one routed direct message as a
// canonical /events payload. The forwarded content starts with
// "Message from: <node name or key prefix>", the text follows, and our
// node name is carried as context. Messages from trusted operators are
// alerts by nature: the default severity is severe.
//
// The event identity (ChangeID + event key) is derived from the receipt
// timestamp, not from a per-process counter: the routing engine persists
// delivery claims keyed by source/key/ChangeID, and a counter that
// restarts with the process would let a message collide with a past
// claim and be silently dropped as a duplicate.
func (h *Hub) publishMessageEvent(prefix, name, selfName, text string) {
	now := time.Now().UTC()
	id := now.UnixNano()
	nowS := now.Format(time.RFC3339)
	expires := now.Add(time.Hour).Format(time.RFC3339)
	label := prefix
	if name != "" {
		label = name
	}
	desc := "Received by "
	if selfName != "" {
		desc += selfName
	} else {
		desc += "the MeshCore node"
	}
	desc += " via MeshCore"

	doc := MessageEventWire{
		SchemaVersion: meshMessageEventSchemaVersion,
		ChangeID:      id,
		ChangeType:    "new",
		EventKey:      "meshcore:" + prefix + ":" + strconv.FormatInt(id, 10),
		Event: MessageEventHazard{
			Source:      "meshcore",
			SourceID:    prefix,
			Event:       "MeshCore message",
			Severity:    "severe",
			Urgency:     "unknown",
			Certainty:   "unknown",
			Headline:    "Message from: " + label + ": " + text,
			Description: desc,
			EffectiveAt: &nowS,
			ExpiresAt:   &expires,
			Areas:       []string{},
			Status:      "active",
			ReceivedAt:  nowS,
			UpdatedAt:   nowS,
		},
	}
	payload, err := json.Marshal(doc)
	if err != nil {
		if h.logger != nil {
			h.logger.Warn("meshcore: routed message marshal failed", "error", err)
		}
		return
	}
	h.mu.Lock()
	sink := h.eventSink
	h.mu.Unlock()
	if sink == nil {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := sink(ctx, "events", false, payload); err != nil && h.logger != nil {
			h.logger.Warn("meshcore: routed message publish failed", "from", prefix, "error", err)
		}
	}()
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
		if n.Name == "" {
			// Fresh name: publish to MQTT right away.
			n.lastPub = time.Time{}
		}
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
	// Drop expired neighbours opportunistically and tombstone them.
	for k, v := range h.nodes {
		if now.Sub(v.LastSeen) > h.cfg.NodeTTL {
			delete(h.nodes, k)
			h.publishStationLocked(k, v, now, true)
		}
	}
	h.publishStationLocked(key, n, now, false)
}

// stationPublishInterval throttles MQTT updates per heard node: station
// docs are informational, not a packet feed.
const stationPublishInterval = 60 * time.Second

// publishStationLocked schedules one retained MQTT publish (or tombstone)
// for a node. Callers hold h.mu. Publish work happens on its own
// goroutine so the serial read loop never blocks on the broker.
func (h *Hub) publishStationLocked(key string, n *Node, now time.Time, removed bool) {
	if h.stationSink == nil {
		return
	}
	topic := "meshcore/stations/" + key
	if len(key) > 12 {
		topic = "meshcore/stations/" + key[:12]
	}
	var payload []byte
	if !removed {
		if now.Sub(n.lastPub) < stationPublishInterval {
			return
		}
		n.lastPub = now
		var err error
		payload, err = json.Marshal(struct {
			Key      string  `json:"key"`
			Name     string  `json:"name"`
			Type     byte    `json:"type"`
			Lat      float64 `json:"lat"`
			Lon      float64 `json:"lon"`
			Hops     int     `json:"hops"`
			LastSeen string  `json:"last_seen"`
		}{
			Key:      n.PubKey,
			Name:     n.Name,
			Type:     n.Type,
			Lat:      n.Lat,
			Lon:      n.Lon,
			Hops:     n.Hops,
			LastSeen: n.LastSeen.UTC().Format(time.RFC3339),
		})
		if err != nil {
			return
		}
	}
	sink := h.stationSink
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := sink(ctx, topic, true, payload); err != nil && h.logger != nil {
			h.logger.Debug("meshcore: station publish failed", "topic", topic, "error", err)
		}
	}()
}

// SeedNode merges one retained station document (restored from the
// broker at startup) into the node registry, so heard stations survive
// restarts. Seeding never publishes for fresh documents — the broker
// already holds them — and never rewinds fresher live state. Documents
// older than NodeTTL are tombstoned as expired instead of restored:
// retention stays bounded exactly like the live publish path.
func (h *Hub) SeedNode(key, name string, typ byte, lat, lon float64, hops int, lastSeen time.Time) {
	if key == "" {
		return
	}
	now := time.Now()
	h.mu.Lock()
	defer h.mu.Unlock()
	if !lastSeen.IsZero() && now.Sub(lastSeen) > h.cfg.NodeTTL {
		// Expired document: clear it from the broker.
		h.publishStationLocked(key, &Node{PubKey: key, LastSeen: lastSeen}, now, true)
		return
	}
	n := h.nodes[key]
	if n == nil {
		n = &Node{PubKey: key, LastSeen: lastSeen}
		h.nodes[key] = n
	} else if lastSeen.After(n.LastSeen) {
		n.LastSeen = lastSeen
	}
	if n.Name == "" {
		n.Name = name
	}
	if n.Type == 0 {
		n.Type = typ
	}
	if (n.Lat == 0 || n.Lon == 0) && (lat != 0 || lon != 0) {
		n.Lat = lat
		n.Lon = lon
	}
	if n.Hops == 0 {
		n.Hops = hops
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
	// The query goes through the command worker with an EXPLICIT
	// expected reply type (the contact record): the frame pump never
	// blocks on the command stream, and the query reply can never be
	// confused with — or steal — a send's ack.
	req := cmdReq{
		payload: buildGetContactByKey(pubKey),
		expect:  expectContact,
		timeout: queryReplyTimeout,
		reply:   make(chan error, 1),
	}
	h.enqueueCmdBackground(req)
}

func (h *Hub) recordMessage(direction, sender, channel, text, operator string, hops int) {
	// The device uses 0xFF as the "no path info" sentinel on direct
	// frames; normalize it here so neither the history nor the MQTT feed
	// ever claims 255 hops.
	if hops >= 255 {
		hops = 0
	}
	now := time.Now()
	h.mu.Lock()
	h.recent = append(h.recent, Message{Direction: direction, Sender: sender, Channel: channel, Hops: hops, Operator: operator, Text: text, At: now})
	if len(h.recent) > 64 {
		h.recent = h.recent[len(h.recent)-64:]
	}
	rec := h.recorder
	mSink := h.messageSink
	h.mu.Unlock()
	if rec != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := rec.RecordMeshMessage(ctx, direction, sender, channel, text, operator, hops, now); err != nil && h.logger != nil {
			h.logger.Debug("meshcore: message history record failed", "error", err)
		}
	}
	if mSink != nil {
		payload, err := json.Marshal(struct {
			Direction string `json:"direction"`
			Sender    string `json:"sender"`
			Channel   string `json:"channel"`
			Hops      int    `json:"hops"`
			Operator  string `json:"operator,omitempty"`
			Text      string `json:"text"`
			At        string `json:"at"`
		}{direction, sender, channel, hops, operator, text, now.UTC().Format(time.RFC3339)})
		if err == nil {
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if err := mSink(ctx, "meshcore/messages", false, payload); err != nil && h.logger != nil {
					h.logger.Debug("meshcore: message publish failed", "error", err)
				}
			}()
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
