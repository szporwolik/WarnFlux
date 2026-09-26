package aprs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/szporwolik/WarnFlux/internal/sanity"
)

// Hub runtime bounds.
const (
	opsQueueSize   = 1024
	publishTimeout = 5 * time.Second
	tickInterval   = 30 * time.Second
	// startupPublishDelay postpones the first self-station publish so the
	// MQTT sink has time to connect (see run).
	startupPublishDelay = 2 * time.Second
	packetDigestWindow  = 10 * time.Minute
	recentMessagesCap   = 64
	// maxTrackPoints bounds the movement tail kept per station: the map
	// draws the last three previous positions behind the marker.
	maxTrackPoints = 3
	// trackMoveMinM is the minimum displacement (meters) before a new
	// position earns a track point, so digipeat duplicates and GPS noise
	// cannot pad the tail.
	trackMoveMinM = 30.0
	// ackPendingCap bounds concurrent outbound messages awaiting an ack.
	ackPendingCap = 64
	// ackWaitDefault is the fallback ack wait when the caller does not
	// ask for a specific one.
	ackWaitDefault = 30 * time.Second
)

// BackendRadio is the via label of the KISS radio backend; frames coming
// from it count as rf-received (no q-construct in KISS).
const BackendRadio = "aprs-radio"

// BackendInternet is the via label of the APRS-IS backend.
const BackendInternet = "aprs-inet"

// ErrNoAck reports that the addressee did not acknowledge a message
// within the wait window.
var ErrNoAck = errors.New("aprs: no ack received")

// hubOp is one queued observation from a backend.
type hubOp struct {
	p   Packet
	via string
}

// stationRecord is the merged runtime state of one nearby station. It is
// owned by the single hub worker goroutine (no locking needed).
type stationRecord struct {
	callsign     string
	self         bool
	lastDigest   string
	lastPacketAt int64
	dirty        bool
	state        stationState
}

// stationState is the merged last-known state of a station.
type stationState struct {
	position       *Position
	positionAt     int64
	symbolTable    byte
	symbol         byte
	courseDeg      int
	speedKMH       float64
	altitudeM      *float64
	name           string
	comment        string
	status         string
	messageCapable bool
	origin         Origin
	lastHeard      int64
	via            map[string]bool
	packets        int
	weather        *WeatherReport

	// track is the movement tail: up to maxTrackPoints earlier positions,
	// oldest first. The current position lives in position, so the map
	// can draw the full polyline [track..., position].
	track []trackPoint
}

// trackPoint is one recorded position of a station's movement tail.
type trackPoint struct {
	lat, lon float64
	at       int64
}

// pushTrack records one position into the movement tail (bounded,
// oldest-first). Sub-trackMoveMinM displacement is treated as noise.
func (st *stationState) pushTrack(lat, lon float64, at int64) {
	if n := len(st.track); n > 0 {
		last := &st.track[n-1]
		if DistanceKM(last.lat, last.lon, lat, lon)*1000 < trackMoveMinM {
			return
		}
	}
	st.track = append(st.track, trackPoint{lat: lat, lon: lon, at: at})
	if len(st.track) > maxTrackPoints {
		st.track = st.track[len(st.track)-maxTrackPoints:]
	}
}

// Hub is the shared APRS merge point. Every backend (aprs-inet today,
// aprs-radio later) feeds parsed packets via Observe and may register as a
// Transmitter. The hub owns:
//
//   - the merged per-station state (one retained MQTT document per
//     station, no duplicate topics between backends),
//   - the non-retained packet and message feeds,
//   - station expiry (empty retained payload = topic deletion),
//   - outbound APRS message routing to the first ready transmitter.
type Hub struct {
	cfg    HubConfig
	logger *slog.Logger
	now    func() time.Time

	mu           sync.Mutex
	sink         Sink
	weatherSink  func(context.Context, WeatherReport) error
	stations     map[string]*stationRecord
	transmitters map[string]Transmitter
	seenDigests  map[string]int64 // digest -> last seen unix
	recent       []MessageDocument

	ops  chan hubOp
	tick time.Duration
	// selfDelay postpones the first self-station publish after Start (the
	// MQTT sink needs a moment to connect). Overridable in tests.
	selfDelay time.Duration
	accepted  atomic.Int64
	dropped   atomic.Int64
	filtered  atomic.Int64
	expired   atomic.Int64
	// msgSeq numbers outbound message ids (the {id} ack suffix).
	msgSeq atomic.Int64
	// pending holds one result channel per in-flight message id; guarded
	// by mu.
	pending map[string]chan string

	// senderGate approves the base callsign of a message sender for the
	// notification routing bridge; guarded by mu. nil disables message
	// routing entirely — a message is only routed when the operator is on
	// the configured allow-list (SSIDs may differ).
	senderGate func(base string) bool
}

// NewHub validates the hub identity and returns the hub. The hub is
// constructed even when disabled (plugins then fail fast on startup).
func NewHub(cfg HubConfig, logger *slog.Logger) (*Hub, error) {
	if cfg.RadiusKM == 0 {
		cfg.RadiusKM = DefaultRadiusKM
	}
	if cfg.StationTTL == 0 {
		cfg.StationTTL = DefaultStationTTL
	}
	cfg.Callsign = NormalizeCallsign(cfg.Callsign)

	if cfg.Enabled {
		if !ValidCallsign(cfg.Callsign) {
			return nil, fmt.Errorf("aprs: callsign %q is not a valid APRS callsign", cfg.Callsign)
		}
		lat, lon, ok := ParseGridSquare(cfg.GridSquare)
		if !ok {
			return nil, fmt.Errorf("aprs: gridsquare %q is not a valid Maidenhead locator", cfg.GridSquare)
		}
		cfg.CenterLat, cfg.CenterLon = lat, lon
		if cfg.Latitude != nil || cfg.Longitude != nil {
			if cfg.Latitude == nil || cfg.Longitude == nil {
				return nil, fmt.Errorf("aprs: latitude and longitude must be set together")
			}
			cfg.CenterLat, cfg.CenterLon = *cfg.Latitude, *cfg.Longitude
		}
		if cfg.RadiusKM < 1 || cfg.RadiusKM > 1000 {
			return nil, fmt.Errorf("aprs: radius_km must be between 1 and 1000, got %v", cfg.RadiusKM)
		}
		// Operational area: defaults to the station position/radius; an
		// explicit territory center overrides the map circle and the
		// geo-scoped sources without moving the station itself.
		cfg.AreaLat, cfg.AreaLon = cfg.CenterLat, cfg.CenterLon
		if cfg.AreaLatitude != nil || cfg.AreaLongitude != nil {
			if cfg.AreaLatitude == nil || cfg.AreaLongitude == nil {
				return nil, fmt.Errorf("aprs: area_latitude and area_longitude must be set together")
			}
			cfg.AreaLat, cfg.AreaLon = *cfg.AreaLatitude, *cfg.AreaLongitude
		}
		if cfg.AreaRadiusKM == 0 {
			cfg.AreaRadiusKM = cfg.RadiusKM
		}
		if cfg.AreaRadiusKM < 1 || cfg.AreaRadiusKM > 1000 {
			return nil, fmt.Errorf("aprs: area_radius_km must be between 1 and 1000, got %v", cfg.AreaRadiusKM)
		}
		if cfg.StationTTL < time.Minute || cfg.StationTTL > 24*time.Hour {
			return nil, fmt.Errorf("aprs: station_ttl must be between 1m and 24h, got %s", cfg.StationTTL)
		}
		switch len(cfg.Icon) {
		case 0:
		case 1:
			cfg.SymbolTable, cfg.Symbol = '/', cfg.Icon[0]
		case 2:
			cfg.SymbolTable, cfg.Symbol = cfg.Icon[0], cfg.Icon[1]
		default:
			return nil, fmt.Errorf("aprs: icon %q must be one or two characters (<code> or <table><code>)", cfg.Icon)
		}
	}

	return &Hub{
		cfg:          cfg,
		logger:       logger,
		now:          time.Now,
		stations:     make(map[string]*stationRecord),
		transmitters: make(map[string]Transmitter),
		seenDigests:  make(map[string]int64),
		ops:          make(chan hubOp, opsQueueSize),
		tick:         tickInterval,
		selfDelay:    startupPublishDelay,
		pending:      make(map[string]chan string),
	}, nil
}

// Enabled reports whether the hub is switched on.
func (h *Hub) Enabled() bool { return h.cfg.Enabled }

// Callsign returns our normalized APRS identity.
func (h *Hub) Callsign() string { return h.cfg.Callsign }

// GridSquare returns our configured Maidenhead locator.
func (h *Hub) GridSquare() string { return h.cfg.GridSquare }

// CenterLat/CenterLon return the configured position of our station:
// the explicit aprs.latitude/longitude when set, otherwise the center of
// the configured gridsquare.
func (h *Hub) CenterLat() float64 { return h.cfg.CenterLat }
func (h *Hub) CenterLon() float64 { return h.cfg.CenterLon }

// OwnLat/OwnLon return the best-known position of our station: the
// position learned from our own position packets (e.g. the Direwolf
// beacon), falling back to the configured center until one is heard.
func (h *Hub) OwnLat() float64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	if rec := h.stations[h.cfg.Callsign]; rec != nil && rec.state.position != nil {
		return rec.state.position.Latitude
	}
	return h.cfg.CenterLat
}

func (h *Hub) OwnLon() float64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	if rec := h.stations[h.cfg.Callsign]; rec != nil && rec.state.position != nil {
		return rec.state.position.Longitude
	}
	return h.cfg.CenterLon
}

// RadiusKM returns the configured nearby radius (the default for the
// operational area when area_radius_km is unset).
func (h *Hub) RadiusKM() float64 { return h.cfg.RadiusKM }

// AreaLat/AreaLon return the operational-area center: the explicit
// area_latitude/area_longitude when set, otherwise the station position.
func (h *Hub) AreaLat() float64 { return h.cfg.AreaLat }
func (h *Hub) AreaLon() float64 { return h.cfg.AreaLon }

// AreaRadius returns the operational-area radius (falls back to the
// station radius when not configured).
func (h *Hub) AreaRadius() float64 { return h.cfg.AreaRadiusKM }

// SetSenderGate installs the message-sender allow-list check. The
// callback receives the BASE callsign (no SSID) of a message sender and
// reports whether it may feed the notification routing bridge. A nil
// gate disables message routing entirely (fail closed).
func (h *Hub) SetSenderGate(fn func(base string) bool) {
	h.mu.Lock()
	h.senderGate = fn
	h.mu.Unlock()
}

// Name returns the display name of our station (falls back to the
// callsign).
func (h *Hub) Name() string {
	if h.cfg.Name != "" {
		return h.cfg.Name
	}
	return h.cfg.Callsign
}

// Version returns the WarnFlux version for APRS-IS login strings.
func (h *Hub) Version() string { return h.cfg.Version }

// SetSink attaches the MQTT sink. It is set after construction (the
// receiver manager is built later in startup); publications before that
// simply stay dirty and are retried.
func (h *Hub) SetSink(s Sink) {
	h.mu.Lock()
	h.sink = s
	h.mu.Unlock()
}

// SetWeatherSink attaches the weather bridge: every decoded APRS weather
// report is handed to the sink so it can feed the canonical weather
// information pipeline (dashboard cards + retained MQTT info topics).
func (h *Hub) SetWeatherSink(fn func(context.Context, WeatherReport) error) {
	h.mu.Lock()
	h.weatherSink = fn
	h.mu.Unlock()
}

// Observe queues one parsed packet from a backend. The call never blocks:
// when the queue is full the packet is dropped and counted.
func (h *Hub) Observe(p Packet, via string) {
	select {
	case h.ops <- hubOp{p: p, via: via}:
	default:
		h.dropped.Add(1)
	}
}

// AddTransmitter registers (or replaces) an outbound backend.
func (h *Hub) AddTransmitter(name string, t Transmitter) {
	h.mu.Lock()
	h.transmitters[name] = t
	h.mu.Unlock()
}

// RemoveTransmitter unregisters an outbound backend.
func (h *Hub) RemoveTransmitter(name string) {
	h.mu.Lock()
	delete(h.transmitters, name)
	h.mu.Unlock()
}

// Stats is a snapshot of hub counters for the web health page.
type Stats struct {
	Stations int
	Accepted int64
	Dropped  int64
	Filtered int64
	Expired  int64
}

// Stats returns a consistent counter snapshot.
func (h *Hub) Stats() Stats {
	h.mu.Lock()
	stations := len(h.stations)
	h.mu.Unlock()
	return Stats{
		Stations: stations,
		Accepted: h.accepted.Load(),
		Dropped:  h.dropped.Load(),
		Filtered: h.filtered.Load(),
		Expired:  h.expired.Load(),
	}
}

// RecentMessages returns a copy of the latest received APRS messages
// (newest last).
func (h *Hub) RecentMessages() []MessageDocument {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]MessageDocument, len(h.recent))
	copy(out, h.recent)
	return out
}

// Stations returns the current merged state of every nearby station (the
// hub's own station document excluded), sorted by callsign. It backs the
// public /api/aprs/stations endpoint that feeds the home-page map.
func (h *Hub) Stations() []StationDocument {
	h.mu.Lock()
	recs := make([]*stationRecord, 0, len(h.stations))
	for _, rec := range h.stations {
		if !rec.self {
			recs = append(recs, rec)
		}
	}
	// The worker mutates station state under the same mutex, so building
	// the documents here yields a consistent snapshot.
	out := make([]StationDocument, 0, len(recs))
	for _, rec := range recs {
		out = append(out, h.buildStationDoc(rec))
	}
	h.mu.Unlock()
	sort.Slice(out, func(i, j int) bool { return out[i].Callsign < out[j].Callsign })
	return out
}

// Start launches the hub worker: it publishes our own station document and
// then applies observations and maintenance until ctx is cancelled.
func (h *Hub) Start(ctx context.Context) {
	go h.run(ctx)
}

func (h *Hub) run(ctx context.Context) {
	// The MQTT sink needs a moment after startup: publishing while the
	// receiver's connection is still coming up stalls for the whole
	// publish timeout and logs a misleading warning. The first self
	// publish therefore goes out shortly after start; anything still
	// failing is retried by the maintenance tick.
	selfTimer := time.NewTimer(h.selfDelay)
	defer selfTimer.Stop()
	ticker := time.NewTicker(h.tick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-selfTimer.C:
			h.publishSelf()
		case op := <-h.ops:
			h.apply(op)
		case <-ticker.C:
			h.maintenance()
		}
	}
}

// apply merges one packet into the station registry and publishes the
// resulting documents.
func (h *Hub) apply(op hubOp) {
	p := op.p
	if p.Src == "" || !ValidCallsign(p.Src) {
		return
	}
	h.accepted.Add(1)

	// Infrastructure filter: objects, digipeaters, gateways and similar
	// never become station state (they are not ham operators).
	if h.cfg.ExcludeInfrastructure && IsInfrastructure(p) {
		h.filtered.Add(1)
		return
	}

	// Defense in depth: the APRS-IS filter already limits the feed to the
	// operational area (virtual center of the served towns); position-bearing
	// packets outside the area are dropped.
	if p.Position != nil {
		d := DistanceKM(h.cfg.AreaLat, h.cfg.AreaLon, p.Position.Latitude, p.Position.Longitude)
		if d > h.cfg.AreaRadiusKM {
			h.filtered.Add(1)
			return
		}
	}

	now := h.now().Unix()
	h.mu.Lock()
	rec := h.stations[p.Src]
	if rec == nil {
		rec = &stationRecord{callsign: p.Src, state: stationState{via: make(map[string]bool)}}
		h.stations[p.Src] = rec
	}
	// Duplicate observation (same content, same receiver): skip.
	digest := packetDigest(p)
	if digest == rec.lastDigest && rec.state.via[op.via] {
		h.mu.Unlock()
		return
	}
	rec.lastDigest = digest
	if p.Timestamp != nil && *p.Timestamp > rec.lastPacketAt {
		rec.lastPacketAt = *p.Timestamp
	}
	rec.merge(&p, op.via, now)
	firstSeen := h.seenDigests[digest] == 0
	h.seenDigests[digest] = now
	h.mu.Unlock()

	// Weather reports feed the canonical weather pipeline in addition to
	// the station document: positionless reports fall back to the merged
	// station position.
	if w := ParseWeather(&p); w != nil {
		w.Callsign = p.Src
		h.mu.Lock()
		if w.Latitude == 0 && w.Longitude == 0 && rec.state.position != nil {
			w.Latitude = rec.state.position.Latitude
			w.Longitude = rec.state.position.Longitude
		}
		rec.state.weather = w
		sink := h.weatherSink
		h.mu.Unlock()
		if sink != nil {
			if err := sink(context.Background(), *w); err != nil {
				h.logger.Debug("aprs: weather sink failed", "callsign", p.Src, "error", err)
			}
		}
	}

	h.publishStation(rec)
	if firstSeen {
		h.publishPacket(p, op.via)
	}
	if p.Message != nil && p.Message.To == h.cfg.Callsign {
		h.receiveMessage(p, op.via)
	}
}

// merge folds one packet into the station state. The caller has already
// excluded exact duplicates.
func (r *stationRecord) merge(p *Packet, via string, now int64) {
	st := &r.state
	if p.Position != nil && (st.position == nil || st.position.Latitude != p.Position.Latitude || st.position.Longitude != p.Position.Longitude) {
		// The previous position joins the movement tail; the new one
		// becomes the current position (track holds only EARLIER
		// positions, so the map draws [track..., position]).
		if st.position != nil {
			st.pushTrack(st.position.Latitude, st.position.Longitude, st.positionAt)
		}
		st.position = p.Position
		st.positionAt = now
	}
	if p.Symbol != 0 {
		st.symbol, st.symbolTable = p.Symbol, p.SymbolTable
	}
	if p.Kind == KindPosition {
		st.courseDeg, st.speedKMH = p.CourseDeg, p.SpeedKMH
		if p.AltitudeM != nil {
			st.altitudeM = p.AltitudeM
		}
	}
	if p.Name != "" {
		st.name = p.Name
	}
	if p.Comment != "" {
		st.comment = p.Comment
	}
	if p.Status != "" {
		st.status = p.Status
	}
	if o := OriginFromPath(p.Path); o != OriginUnknown {
		st.origin = o
	} else if via == BackendRadio {
		// KISS frames have no q-construct: radio reception is rf by
		// definition.
		st.origin = OriginRF
	}
	if p.MessageCapable {
		st.messageCapable = true
	}
	st.via[via] = true
	st.packets++
	st.lastHeard = now
}

// publishStation marshals and publishes one retained station document.
func (h *Hub) publishStation(rec *stationRecord) {
	payload, err := json.Marshal(h.buildStationDoc(rec))
	if err != nil {
		return
	}
	if err := h.publishWithTimeout(StationsTopicPrefix+rec.callsign, true, payload); err != nil {
		rec.dirty = true
		h.logger.Warn("aprs: station state publish failed", "callsign", rec.callsign, "error", err)
		return
	}
	rec.dirty = false
}

// buildStationDoc renders the retained wire document of one station.
func (h *Hub) buildStationDoc(rec *stationRecord) StationDocument {
	st := &rec.state
	doc := StationDocument{
		SchemaVersion:  SchemaVersion,
		Callsign:       rec.callsign,
		Self:           rec.self,
		Name:           st.name,
		MessageCapable: st.messageCapable,
		LastHeardAt:    formatTime(st.lastHeard),
		PacketCount:    st.packets,
	}
	if st.position != nil {
		doc.Position = &PositionWire{Latitude: st.position.Latitude, Longitude: st.position.Longitude}
		if !rec.self {
			doc.DistanceKM = DistanceKM(h.cfg.CenterLat, h.cfg.CenterLon, st.position.Latitude, st.position.Longitude)
		}
	}
	for _, tp := range st.track {
		doc.Track = append(doc.Track, TrackWire{Latitude: tp.lat, Longitude: tp.lon, At: formatTime(tp.at)})
	}
	if st.symbol != 0 {
		doc.SymbolTable = string(st.symbolTable)
		doc.Symbol = string(st.symbol)
	}
	doc.CourseDeg = st.courseDeg
	doc.SpeedKMH = st.speedKMH
	doc.AltitudeM = st.altitudeM
	doc.Comment = st.comment
	doc.Status = st.status
	if st.weather != nil {
		doc.Weather = st.weather
	}
	doc.Origin = string(st.origin)
	if rec.lastPacketAt != 0 {
		doc.LastPacketAt = formatTime(rec.lastPacketAt)
	}
	via := make([]string, 0, len(st.via))
	for name := range st.via {
		via = append(via, name)
	}
	sort.Strings(via)
	doc.ReceivedVia = via
	return doc
}

// publishPacket publishes one non-retained packet document. Failures are
// best-effort (logged at debug level; the feed has no retention).
func (h *Hub) publishPacket(p Packet, via string) {
	doc := PacketDocument{
		SchemaVersion:  SchemaVersion,
		Kind:           string(p.Kind),
		Src:            p.Src,
		Dst:            p.Dst,
		Path:           p.Path,
		Timestamp:      formatTime(timeValue(p.Timestamp)),
		SymbolTable:    byteStr(p.SymbolTable),
		Symbol:         byteStr(p.Symbol),
		CourseDeg:      p.CourseDeg,
		SpeedKMH:       p.SpeedKMH,
		AltitudeM:      p.AltitudeM,
		Name:           p.Name,
		Comment:        p.Comment,
		Status:         p.Status,
		MessageCapable: p.MessageCapable,
		ReceivedAt:     formatTime(p.ReceivedAt),
		Via:            via,
	}
	if p.Position != nil {
		doc.Position = &PositionWire{Latitude: p.Position.Latitude, Longitude: p.Position.Longitude}
	}
	if p.Message != nil {
		doc.Message = &MessageWire{To: p.Message.To, Text: p.Message.Text, ID: p.Message.ID}
	}
	payload, err := json.Marshal(doc)
	if err != nil {
		return
	}
	if err := h.publishWithTimeout(PacketsTopic, false, payload); err != nil {
		h.logger.Debug("aprs: packet feed publish failed", "error", err)
	}
}

// receiveMessage publishes one received APRS message, keeps it in the
// recent-message ring and signals a pending ack waiter when it is an
// ack/rej for one of our outbound messages.
func (h *Hub) receiveMessage(p Packet, via string) {
	doc := MessageDocument{
		SchemaVersion: SchemaVersion,
		Direction:     "rx",
		From:          p.Src,
		To:            p.Message.To,
		Text:          p.Message.Text,
		ID:            p.Message.ID,
		ReceivedAt:    formatTime(p.ReceivedAt),
		Via:           via,
	}
	h.mu.Lock()
	h.recent = append(h.recent, doc)
	if len(h.recent) > recentMessagesCap {
		h.recent = h.recent[len(h.recent)-recentMessagesCap:]
	}
	h.mu.Unlock()

	payload, err := json.Marshal(doc)
	if err != nil {
		return
	}
	if err := h.publishWithTimeout(MessagesTopic, false, payload); err != nil {
		h.logger.Warn("aprs: message feed publish failed", "error", err)
	}

	// Routing: APRS messages addressed to us become routable events on
	// the /events stream (the "aprs" source in the routing matrix) when
	// the sender's base callsign is on the registered-user allow-list —
	// SSIDs may differ, and both radio and APRS-IS delivery qualify.
	routed := h.cfg.RouteMessages && h.routableMessage(p) && h.senderApproved(p.Src)
	if h.logger != nil {
		h.logger.Info("aprs: message received",
			"from", p.Src, "to", p.Message.To, "text", p.Message.Text, "routed", routed)
	}
	if routed {
		h.publishMessageEvent(p)
	}

	// Signal the ack waiter only after the rx document is on the message
	// feed: callers that learn about the ack must also see its document.
	if p.Message.ID != "" && len(p.Message.Text) >= 3 {
		if kind := ackKind(p.Message.Text); kind != "" {
			h.signalAck(p.Message.ID, kind)
		}
	}
}

// routableMessage reports whether a message addressed to us is real
// traffic worth routing: not our own transmission and not an ack/rej
// protocol frame.
func (h *Hub) routableMessage(p Packet) bool {
	if p.Src == h.cfg.Callsign || p.Message == nil {
		return false
	}
	text := strings.TrimSpace(p.Message.Text)
	if text == "" || len(text) >= 3 && ackKind(text) != "" {
		return false
	}
	return true
}

// senderApproved reports whether the message sender may feed the routing
// bridge: the gate must be installed and approve the base callsign.
func (h *Hub) senderApproved(callsign string) bool {
	h.mu.Lock()
	gate := h.senderGate
	h.mu.Unlock()
	return gate != nil && gate(BaseCallsign(callsign))
}

// publishMessageEvent re-publishes one APRS message as a canonical
// /events payload. The forwarded content starts with
// "Message from: <callsign with SSID>", the text follows, and our
// station name is carried as context. Messages from trusted operators
// are alerts by nature: the default severity is severe.
//
// The event identity (ChangeID + event key) is derived from the receipt
// timestamp, not from a per-process counter: the routing engine persists
// delivery claims keyed by source/key/ChangeID, and a counter that
// restarts with the process would let a message collide with a past
// claim and be silently dropped as a duplicate.
func (h *Hub) publishMessageEvent(p Packet) {
	now := time.Now().UTC()
	id := now.UnixNano()
	nowS := now.Format(time.RFC3339)
	expires := now.Add(time.Hour).Format(time.RFC3339)
	from := p.Src
	text := strings.TrimSpace(p.Message.Text)

	doc := MessageEventWire{
		SchemaVersion: messageEventSchemaVersion,
		ChangeID:      id,
		ChangeType:    "new",
		EventKey:      "aprs:" + from + ":" + strconv.FormatInt(id, 10),
		Event: MessageEventHazard{
			Source:      "aprs",
			SourceID:    from,
			Event:       "APRS message",
			Severity:    "severe",
			Urgency:     "unknown",
			Certainty:   "unknown",
			Headline:    "Message from: " + from + ": " + text,
			Description: "Received by " + h.Name() + " (" + h.cfg.Callsign + ")",
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
		h.logger.Warn("aprs: routed message marshal failed", "error", err)
		return
	}
	if err := h.publishWithTimeout("events", false, payload); err != nil {
		h.logger.Warn("aprs: routed message publish failed", "callsign", from, "error", err)
	}
}

// ackKind reports whether a received message text is an ack/rej reply.
func ackKind(text string) string {
	switch {
	case strings.HasPrefix(text, "ack"):
		return "ack"
	case strings.HasPrefix(text, "rej"):
		return "rej"
	}
	return ""
}

// signalAck wakes the waiter registered for msgid, if any.
func (h *Hub) signalAck(msgid, kind string) {
	h.mu.Lock()
	ch, ok := h.pending[msgid]
	if ok {
		delete(h.pending, msgid)
	}
	h.mu.Unlock()
	if ok {
		select {
		case ch <- kind:
		default:
		}
	}
}

// SendMessage routes one outbound APRS message to the first ready
// transmitter and publishes the tx confirmation on the message feed.
func (h *Hub) SendMessage(ctx context.Context, to, text string) error {
	to = NormalizeCallsign(to)
	if !ValidCallsign(to) {
		return fmt.Errorf("aprs: invalid addressee callsign %q", to)
	}
	// Sanity stage: every outbound message passes through the shared
	// normalizer (control chars, whitespace, 7-bit transliteration);
	// TODO(llm) will review here in the future. Protocol limits below
	// still own the final length.
	text = sanity.NormalizeText(ctx, sanity.ChannelAPRS, text)
	text = TrimMessageText(text)
	if text == "" {
		return fmt.Errorf("aprs: message text must not be empty")
	}

	tx := h.readyTransmitterFor(to)
	if tx == nil {
		return ErrNoTransmitter
	}
	if err := tx.Send(ctx, to, text); err != nil {
		return fmt.Errorf("aprs: transmitter %s: %w", tx.Name(), err)
	}
	h.publishTxMessage(to, text, "", tx.Name())
	return nil
}

// SendMessageWaitAck sends one message with a hub-generated {id} and waits
// up to timeout for the addressee's ack (or rej) before returning. The
// bool reports whether an ack arrived; ErrNoAck means the timeout elapsed.
// The text is shortened to leave room for the {id} suffix, so the whole
// APRS message field never exceeds the protocol limit.
func (h *Hub) SendMessageWaitAck(ctx context.Context, to, text string, timeout time.Duration) (bool, error) {
	to = NormalizeCallsign(to)
	if !ValidCallsign(to) {
		return false, fmt.Errorf("aprs: invalid addressee callsign %q", to)
	}
	// Sanity stage before the protocol pass (see SendMessage).
	text = sanity.NormalizeText(ctx, sanity.ChannelAPRS, text)
	text = LimitMessageText(text, AckSuffixLen)
	if text == "" {
		return false, fmt.Errorf("aprs: message text must not be empty")
	}

	msgid := fmt.Sprintf("%05d", h.msgSeq.Add(1)%100000)
	ch := make(chan string, 1)
	h.mu.Lock()
	if len(h.pending) >= ackPendingCap {
		h.mu.Unlock()
		return false, fmt.Errorf("aprs: %d messages already awaiting acks", ackPendingCap)
	}
	h.pending[msgid] = ch
	h.mu.Unlock()
	defer func() {
		h.mu.Lock()
		delete(h.pending, msgid)
		h.mu.Unlock()
	}()

	tx := h.readyTransmitterFor(to)
	if tx == nil {
		return false, ErrNoTransmitter
	}
	full := text + "{" + msgid + "}"
	if err := tx.Send(ctx, to, full); err != nil {
		return false, fmt.Errorf("aprs: transmitter %s: %w", tx.Name(), err)
	}
	h.publishTxMessage(to, text, msgid, tx.Name())

	if timeout <= 0 {
		timeout = ackWaitDefault
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case kind := <-ch:
		if kind == "rej" {
			return false, fmt.Errorf("aprs: message rejected by %s", to)
		}
		return true, nil
	case <-ctx.Done():
		return false, ctx.Err()
	case <-timer.C:
		return false, ErrNoAck
	}
}

// readyTransmitterFor picks the outbound backend for one addressee. The
// radio reaches stations heard over RF, the internet backend reaches
// internet-injected stations; when the addressee's origin is unknown the
// radio is preferred (it keeps working without internet). Ready backends
// of the other kind are the fallback, so both paths work in parallel and
// complement each other.
func (h *Hub) readyTransmitterFor(to string) Transmitter {
	h.mu.Lock()
	defer h.mu.Unlock()
	var radio, internet, other Transmitter
	names := make([]string, 0, len(h.transmitters))
	for name := range h.transmitters {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		t := h.transmitters[name]
		if !t.Ready() {
			continue
		}
		switch name {
		case BackendRadio:
			radio = t
		case BackendInternet:
			internet = t
		default:
			if other == nil {
				other = t
			}
		}
	}
	origin := OriginUnknown
	if rec := h.stations[to]; rec != nil {
		origin = rec.state.origin
	}
	order := []Transmitter{radio, internet, other}
	if origin == OriginInternet {
		order = []Transmitter{internet, radio, other}
	}
	for _, t := range order {
		if t != nil {
			return t
		}
	}
	return nil
}

// publishTxMessage publishes the outbound confirmation on the message
// feed.
func (h *Hub) publishTxMessage(to, text, id, via string) {
	doc := MessageDocument{
		SchemaVersion: SchemaVersion,
		Direction:     "tx",
		From:          h.cfg.Callsign,
		To:            to,
		Text:          text,
		ID:            id,
		ReceivedAt:    formatTime(h.now().Unix()),
		Via:           via,
	}
	payload, err := json.Marshal(doc)
	if err != nil {
		return
	}
	if err := h.publishWithTimeout(MessagesTopic, false, payload); err != nil {
		h.logger.Debug("aprs: tx message feed publish failed", "error", err)
	}
}

// publishSelf publishes our own station document once at startup.
func (h *Hub) publishSelf() {
	if h.cfg.Callsign == "" {
		return
	}
	h.mu.Lock()
	rec := h.stations[h.cfg.Callsign]
	if rec == nil {
		rec = &stationRecord{
			callsign: h.cfg.Callsign,
			self:     true,
			state: stationState{
				via:         make(map[string]bool),
				position:    &Position{Latitude: h.cfg.CenterLat, Longitude: h.cfg.CenterLon},
				symbolTable: h.cfg.SymbolTable,
				symbol:      h.cfg.Symbol,
				lastHeard:   h.now().Unix(),
			},
		}
		h.stations[h.cfg.Callsign] = rec
	}
	// A packet from our own callsign may have arrived before the delayed
	// first publish: mark the record as ours either way and never clobber
	// a position it already learned.
	rec.self = true
	if rec.state.position == nil {
		rec.state.position = &Position{Latitude: h.cfg.CenterLat, Longitude: h.cfg.CenterLon}
	}
	h.mu.Unlock()
	h.publishStation(rec)
}

// maintenance expires stale stations (empty retained payload deletes the
// topic), retries dirty publishes and prunes the packet digest window.
func (h *Hub) maintenance() {
	now := h.now()
	staleCutoff := now.Add(-h.cfg.StationTTL).Unix()
	windowCutoff := now.Add(-packetDigestWindow).Unix()

	h.mu.Lock()
	var stale []*stationRecord
	for callsign, rec := range h.stations {
		if rec.self {
			continue
		}
		if rec.state.lastHeard < staleCutoff {
			stale = append(stale, rec)
			delete(h.stations, callsign)
		}
	}
	for digest, at := range h.seenDigests {
		if at < windowCutoff {
			delete(h.seenDigests, digest)
		}
	}
	dirty := make([]*stationRecord, 0, len(h.stations))
	for _, rec := range h.stations {
		if rec.dirty {
			dirty = append(dirty, rec)
		}
	}
	h.mu.Unlock()

	for _, rec := range stale {
		if err := h.publishWithTimeout(StationsTopicPrefix+rec.callsign, true, nil); err != nil {
			h.logger.Warn("aprs: station state delete failed", "callsign", rec.callsign, "error", err)
			continue
		}
		h.expired.Add(1)
	}
	for _, rec := range dirty {
		h.publishStation(rec)
	}
}

// publishWithTimeout publishes one document through the sink with a bounded
// timeout. A nil sink is silently ignored (tests without a sink).
func (h *Hub) publishWithTimeout(suffix string, retained bool, payload []byte) error {
	h.mu.Lock()
	sink := h.sink
	h.mu.Unlock()
	if sink == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), publishTimeout)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- sink.PublishRaw(suffix, retained, payload) }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// packetDigest is the stable content fingerprint used to deduplicate the
// same packet heard by two backends (aprs-inet + aprs-radio).
func packetDigest(p Packet) string {
	h := sha256.New()
	fmt.Fprintf(h, "%s|%s|", p.Kind, p.Src)
	if p.Timestamp != nil {
		fmt.Fprintf(h, "t%d|", *p.Timestamp)
	}
	if p.Position != nil {
		fmt.Fprintf(h, "%.4f,%.4f|", p.Position.Latitude, p.Position.Longitude)
	}
	fmt.Fprintf(h, "%c%c|%d|%.1f|", p.SymbolTable, p.Symbol, p.CourseDeg, p.SpeedKMH)
	if p.AltitudeM != nil {
		fmt.Fprintf(h, "a%.0f|", *p.AltitudeM)
	}
	fmt.Fprintf(h, "%s|%s|%s|", p.Name, p.Comment, p.Status)
	if p.Message != nil {
		fmt.Fprintf(h, "m%s:%s:%s|", p.Message.To, p.Message.ID, p.Message.Text)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// timeValue converts an optional unix timestamp to a time.Time.
func timeValue(unix *int64) int64 {
	if unix == nil {
		return 0
	}
	return *unix
}

// byteStr renders an optional APRS symbol byte.
func byteStr(b byte) string {
	if b == 0 {
		return ""
	}
	return string(b)
}
