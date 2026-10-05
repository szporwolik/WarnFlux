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
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/szporwolik/WarnFlux/internal/dispatch"
	"github.com/szporwolik/WarnFlux/internal/radiocli"
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
	// ackWaitMaxAge is the hard validity window of one pending ack wait:
	// maintenance drops older registrations so a late ack can never
	// confirm an exchange whose validity already lapsed.
	ackWaitMaxAge = 10 * time.Minute
	// cmdDedupWindow is how long a command identity stays remembered:
	// a retransmission of the same packet (RF copy + APRS-IS copy)
	// within this window re-sends the previous reply instead of
	// executing the command again.
	cmdDedupWindow = 10 * time.Minute
	// cmdDedupCap bounds the remembered command outcomes.
	cmdDedupCap = 256
	// cmdRetryCooldownDefault paces retries of transiently failed
	// commands: within the cooldown a retransmission replays the
	// failure; after it the command executes again.
	cmdRetryCooldownDefault = 15 * time.Second
	// bannerMinInterval is the per-sender spacing of automatic banner
	// replies: a sender gets at most one banner per window, so
	// answering bots cannot loop each other.
	bannerMinInterval = 5 * time.Minute
	// bannerGlobalInterval is the global pacing of banner replies: the
	// automatic-answer path never hogs the channel.
	bannerGlobalInterval = 10 * time.Second
	// weatherCacheAge bounds the local weather cache: /weather reads it
	// directly (off-grid operation), matching the aggregation freshness
	// window.
	weatherCacheAge = 3 * time.Hour
	// replyBurstDefault / replyBurstWindowDefault bound EVERY automatic
	// reply per sender (command answers, retransmission replays, /help,
	// denials, banners): a flooding sender cannot make the station
	// chatter.
	replyBurstDefault       = 5
	replyBurstWindowDefault = time.Minute
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

// cmdRecord is the remembered outcome of one radio command, keyed by
// sender + message identity: a retransmitted packet (the RF copy and
// the APRS-IS copy of the same message) re-sends the previous reply and
// never re-executes the command. A transient failure (retryable) admits
// controlled retries after the cooldown instead of replaying the stale
// failure forever.
type cmdRecord struct {
	at        time.Time
	reply     string
	retryable bool
}

// replyWindow is one sender's sliding burst window for automatic
// replies.
type replyWindow struct {
	start time.Time
	count int
}

// weatherEntry is one cached station weather observation.
type weatherEntry struct {
	at     time.Time
	report WeatherReport
}

// ackWait is one in-flight outbound message awaiting its ack/rej. The
// ack must carry the registered message id AND come from the addressee
// the message was sent to (addressed back to us): a foreign station's
// ack never confirms someone else's wait.
type ackWait struct {
	from string // our callsign (the ack addressee)
	to   string // the addressee (the expected ack sender)
	at   time.Time
	ch   chan string
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
	// by mu. Each entry remembers whom the ack must come from.
	pending map[string]*ackWait

	// senderGate approves the base callsign of a message sender for the
	// notification routing bridge; guarded by mu. nil disables message
	// routing entirely — a message is only routed when the operator is on
	// the configured allow-list (SSIDs may differ).
	senderGate func(base string) bool

	// cli is the shared radio-command interpreter (optional): a message
	// that parses as a command is answered in-band and (except /debug)
	// stays off the alarm pipeline; guarded by mu.
	cli *radiocli.Bot

	// bulletins maps retained bulletin topic ids onto their receipt
	// unix time; maintenance deletes them after BulletinTTL.
	bulletins map[string]int64

	// cmds remembers executed radio commands by sender + message
	// identity; guarded by mu. A retransmitted packet is answered with
	// the previous result instead of re-executing the command.
	cmds map[string]*cmdRecord

	// alertAcceptor accepts a marshalled COMMAND event (/alert and
	// /debug) into the LOCAL pipeline and reports how it was accepted
	// (durable inbox, emergency RAM fallback or rejection); guarded by
	// mu. nil falls back to the generic sink (tests, legacy wiring).
	eventAcceptor func(payload []byte) dispatch.Acceptance

	// banner rate limiting: automatic banner answers to plain messages
	// are spaced per sender and globally, so answering bots cannot loop
	// each other or hog the channel; guarded by mu.
	bannerLast map[string]time.Time // sender -> last banner answer
	bannerNext time.Time            // global pacing: next allowed banner

	// replyLast bounds ALL automatic replies per sender (command
	// answers, replays, /help, denials, banners); guarded by mu.
	replyLast map[string]replyWindow

	// weatherCache keeps the latest observed weather report per station
	// (RF or APRS-IS), updated BEFORE the infrastructure filter and any
	// broker publication: /weather stays honest even when the broker is
	// down; guarded by mu.
	weatherCache map[string]weatherEntry

	// eventTimes resolves the durable command registry for one command
	// key: the lifecycle anchor (effective / expires) AND the recorded
	// result of a previously accepted command; guarded by mu. It is the
	// restart-proof half of the registry: the durable anchor row is
	// written transactionally with the inbox acceptance, so a re-issued
	// command recovers the original TTL and — when a result is stored —
	// replays it instead of executing the job again.
	eventTimes func(ctx context.Context, key string) (eff, exp time.Time, result string, ok bool)
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
	if cfg.BulletinTTL == 0 {
		cfg.BulletinTTL = DefaultBulletinTTL
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
		if cfg.BulletinTTL < time.Minute || cfg.BulletinTTL > 24*time.Hour {
			return nil, fmt.Errorf("aprs: bulletin_ttl must be between 1m and 24h, got %s", cfg.BulletinTTL)
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
		bulletins:    make(map[string]int64),
		cmds:         make(map[string]*cmdRecord),
		bannerLast:   make(map[string]time.Time),
		replyLast:    make(map[string]replyWindow),
		weatherCache: make(map[string]weatherEntry),
		transmitters: make(map[string]Transmitter),
		seenDigests:  make(map[string]int64),
		ops:          make(chan hubOp, opsQueueSize),
		tick:         tickInterval,
		selfDelay:    startupPublishDelay,
		pending:      make(map[string]*ackWait),
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

// SetCLI attaches the shared radio-command interpreter (optional): a
// routed message that parses as a command is answered in-band and
// (except /debug) stays off the alarm pipeline.
func (h *Hub) SetCLI(b *radiocli.Bot) { h.mu.Lock(); h.cli = b; h.mu.Unlock() }

// SetEventAcceptor installs the LOCAL acceptance path for command
// events (/alert and /debug): the callback enqueues a marshalled event
// into the local pipeline and reports how it was accepted (durable
// inbox, emergency RAM fallback or rejection). Without it the hub falls
// back to the generic sink (tests, legacy wiring).
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

// Name returns the display name of our station (falls back to the
// callsign).
func (h *Hub) Name() string {
	if h.cfg.Name != "" {
		return h.cfg.Name
	}
	return h.cfg.Callsign
}

// Icon returns the configured APRS symbol of our station (e.g. "/j").
func (h *Hub) Icon() string { return h.cfg.Icon }

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

// WeatherSnapshot returns every weather observation the hub cached
// within the freshness window — REGARDLESS of the infrastructure filter
// and the broker state. /weather reads it first, so off-grid operation
// (RF reception with a dead broker) never loses a measurement. Reports
// are unsorted; the aggregation layer does not care about order.
func (h *Hub) WeatherSnapshot(now time.Time) []WeatherReport {
	cutoff := now.Add(-weatherCacheAge)
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]WeatherReport, 0, len(h.weatherCache))
	for _, e := range h.weatherCache {
		if e.at.Before(cutoff) {
			continue
		}
		out = append(out, e.report)
	}
	return out
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

	// Weather extraction comes FIRST: APRS weather stations are
	// infrastructure and are filtered below when the option is on, but
	// their readings are the /weather data source and must never starve.
	var weather *WeatherReport
	if w := ParseWeather(&p); w != nil {
		w.Callsign = p.Src
		h.mu.Lock()
		if w.Latitude == 0 && w.Longitude == 0 {
			if rec := h.stations[p.Src]; rec != nil && rec.state.position != nil {
				w.Latitude = rec.state.position.Latitude
				w.Longitude = rec.state.position.Longitude
			}
		}
		h.mu.Unlock()
		// Operational-area gate FIRST (reported P2: the cache stored the
		// measurement before the distance check): a reading we can place
		// outside the area never reaches the cache nor the pipeline.
		// Unplaceable reports (no coordinates, unknown station) pass
		// through — fail-open.
		if (w.Latitude != 0 || w.Longitude != 0) &&
			DistanceKM(h.cfg.AreaLat, h.cfg.AreaLon, w.Latitude, w.Longitude) > h.cfg.AreaRadiusKM {
			h.filtered.Add(1)
		} else {
			// Local cache first: /weather reads it directly, so a broker
			// outage (off-grid) never hides a measurement the radio heard.
			at := w.Time
			if at.IsZero() {
				at = h.now()
			}
			h.mu.Lock()
			h.weatherCache[p.Src] = weatherEntry{at: at, report: *w}
			h.mu.Unlock()
			weather = w
		}
	}

	// Infrastructure filter: objects, digipeaters, gateways, weather
	// stations and similar never become station state (they are not ham
	// operators). Their weather reports still reach the pipeline.
	if h.cfg.ExcludeInfrastructure && IsInfrastructure(p) {
		h.filtered.Add(1)
		if weather != nil {
			h.deliverWeather(weather)
		}
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
	// Duplicate observation (same content, same receiver): the telemetry
	// merge, station document and packet feed are skipped.
	digest := packetDigest(p)
	if digest == rec.lastDigest && rec.state.via[op.via] {
		h.mu.Unlock()
		// Command handling is NOT deduplicated here: a retransmitted
		// slash command must reach the command machinery, which
		// remembers the previous result and applies the retry policy
		// (a transient rejection retries after the cooldown). Without
		// this a rejected /alert could never be retried over the same
		// receiver. Only RF-heard messages react: an APRS-IS copy
		// (direct internet injection or an i-gated duplicate) is
		// display-only.
		if op.via == BackendRadio && p.Message != nil && (p.Message.To == h.cfg.Callsign || IsBulletin(p.Message.To)) &&
			strings.HasPrefix(strings.TrimSpace(p.Message.Text), "/") {
			h.mu.Lock()
			cli := h.cli
			h.mu.Unlock()
			if cli != nil && h.routableMessage(p) {
				h.routeOrCLI(p)
			}
		}
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

	// Weather reports feed the station document too (positionless
	// reports fall back to the merged station position).
	if weather != nil {
		h.mu.Lock()
		rec.state.weather = weather
		h.mu.Unlock()
		h.deliverWeather(weather)
	}

	h.publishStation(rec)
	if firstSeen {
		h.publishPacket(p, op.via)
	}
	if p.Message != nil && (p.Message.To == h.cfg.Callsign || IsBulletin(p.Message.To)) {
		h.receiveMessage(p, op.via)
	}
}

// deliverWeather hands one weather report to the canonical weather
// pipeline (best-effort; the aggregation survives lost reports).
func (h *Hub) deliverWeather(w *WeatherReport) {
	h.mu.Lock()
	sink := h.weatherSink
	h.mu.Unlock()
	if sink == nil {
		return
	}
	if err := sink(context.Background(), *w); err != nil {
		h.logger.Debug("aprs: weather sink failed", "callsign", w.Callsign, "error", err)
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

	// Durable history (admin /messages page): best-effort, never blocks
	// message handling on a storage hiccup.
	if h.cfg.MessageRecorder != nil {
		recCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := h.cfg.MessageRecorder.RecordAPRSMessage(recCtx, "rx", p.Src,
			p.Message.To, p.Message.Text, p.Message.ID, via, time.Unix(p.ReceivedAt, 0))
		cancel()
		if err != nil && h.logger != nil {
			h.logger.Debug("aprs: message history record failed", "direction", "rx", "error", err)
		}
	}

	payload, err := json.Marshal(doc)
	if err != nil {
		return
	}
	if err := h.publishWithTimeout(MessagesTopic, false, payload); err != nil {
		h.logger.Warn("aprs: message feed publish failed", "error", err)
	}

	// Bulletins also get a bounded retained presence: one document per
	// bulletin on aprs/bulletins/*, deleted by maintenance after the
	// bulletin TTL. A repeated bulletin updates the same topic.
	if IsBulletin(p.Message.To) {
		h.publishBulletin(p, via)
	}

	// The radio CLI mirrors the Meshtastic hub: every message addressed
	// to us is answered, commands run only for allow-listed senders and
	// alarms fire only from explicit commands (/debug). Plain messages
	// never enter the alarm pipeline. ONLY RF-heard messages react —
	// APRS-IS copies are recorded and displayed, never answered, so an
	// internet-injected message can't raise an alarm or burn reply
	// budget (reported policy: RF only).
	approved := h.routableMessage(p) && h.senderApproved(p.Src)
	if h.logger != nil {
		h.logger.Info("aprs: message received",
			"from", p.Src, "to", p.Message.To, "text", p.Message.Text, "bulletin", IsBulletin(p.Message.To), "approved", approved, "via", via)
	}
	if via == BackendRadio {
		h.mu.Lock()
		cli := h.cli
		h.mu.Unlock()
		// ack/rej frames are protocol confirmations, never answered:
		// a banner reply would be acked again and loop forever.
		isAck := p.Message.ID != "" && ackKind(strings.TrimSpace(p.Message.Text)) != ""
		if cli != nil && h.routableMessage(p) && !isAck {
			text := strings.TrimSpace(p.Message.Text)
			if strings.HasPrefix(text, "/") {
				// The shared interpreter decides: public commands (/help)
				// run for everyone, restricted ones (/debug) only for
				// allow-listed senders.
				h.routeOrCLI(p)
			} else if h.bannerAllowed(p.Src) && h.replyAllowed(p.Src) {
				// Plain messages never become alarms — the standard
				// installation banner answers instead, rate-limited per
				// sender and globally so answering bots cannot loop.
				h.sendCLIReply(p.Src, cli.Banner())
			}
		}
	}

	// Signal the ack waiter only after the rx document is on the message
	// feed: callers that learn about the ack must also see its document.
	if p.Message.ID != "" && len(p.Message.Text) >= 3 {
		if kind := ackKind(p.Message.Text); kind != "" {
			h.signalAck(p, kind)
		}
	}
}

// routeOrCLI handles one slash message: the shared interpreter runs
// public commands for everyone and restricted commands for allow-listed
// senders; /debug additionally fires the alarm for authorized senders.
// Plain messages never become alarms — they stay in the history and the
// message feed. A retransmitted packet (the same message heard through
// RF and APRS-IS) re-sends the previous reply instead of executing the
// command again.
func (h *Hub) routeOrCLI(p Packet) {
	h.mu.Lock()
	cli := h.cli
	h.mu.Unlock()
	if cli == nil {
		return
	}
	cid := p.Src + ":" + commandIdentity(p)
	now := h.now()
	h.mu.Lock()
	rec := h.cmds[cid]
	fresh := rec != nil && now.Sub(rec.at) < cmdDedupWindow
	recReply := ""
	retry := false
	if fresh {
		recReply = rec.reply
		// A transiently failed command admits controlled retries: after
		// the cooldown a retransmission executes again instead of
		// replaying the stale failure.
		retry = rec.retryable && now.Sub(rec.at) >= h.cfg.CmdRetryCooldown
	}
	h.mu.Unlock()
	if fresh && !retry {
		if recReply != "" && h.replyAllowed(p.Src) {
			h.sendCLIReply(p.Src, recReply)
		}
		return
	}
	res := cli.Handle(strings.TrimSpace(p.Message.Text), h.senderApproved(p.Src))
	// The stored reply is the FINAL confirmation (post-acceptance), so a
	// retransmission replays exactly the previous result. Both commands
	// share the same confirmation path. A rejection is transient:
	// remembered as retryable instead of final. A durable registry hit
	// (stored result) proves the job executed before — the
	// retransmission replays the result and never re-executes it, even
	// across a restart.
	reply := res.Reply
	retryable := false
	if res.Debug {
		key := "aprs:" + p.Src + ":msg:" + commandIdentity(p)
		eff, exp, prev := h.resolveCommand(key, time.Hour)
		if prev != "" {
			reply = prev
		} else {
			acc := h.publishMessageEvent(p, eff, exp, res.Reply)
			reply = confirmation(res.Reply, "FAILED: debug alarm rejected", acc)
			retryable = acc == dispatch.Rejected
		}
	}
	if res.Alert != nil {
		ttl := res.Alert.TTL
		if ttl <= 0 {
			ttl = 4 * time.Hour
		}
		key := "aprs:" + p.Src + ":alert:" + commandIdentity(p)
		eff, exp, prev := h.resolveCommand(key, ttl)
		if prev != "" {
			reply = prev
		} else {
			acc := h.publishAlertEvent(p, res.Alert, eff, exp, res.Reply)
			reply = confirmation(res.Reply, "FAILED: alert rejected", acc)
			retryable = retryable || acc == dispatch.Rejected
		}
	}
	h.mu.Lock()
	h.pruneCmds(now)
	h.cmds[cid] = &cmdRecord{at: now, reply: reply, retryable: retryable}
	h.mu.Unlock()
	if !res.Handled {
		return
	}
	if reply != "" && h.replyAllowed(p.Src) {
		h.sendCLIReply(p.Src, reply)
	}
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

// msgIdentity is the stable per-message identity used for command
// deduplication: the message number when the sender requested an ack
// ({id} suffix), otherwise a short content digest — a retransmitted
// copy of the same packet produces the same identity.
func msgIdentity(p Packet) string {
	if p.Message != nil && p.Message.ID != "" {
		return p.Message.ID
	}
	return "h" + radiocli.ContentID(p.Src, p.Message.To, p.Message.Text)
}

// commandIdentity is the dedup identity of one command message: the
// message identity ALWAYS followed by the content fingerprint. A reused
// message number with different text is therefore a different command
// (a new alarm runs instead of replaying the old result), while a
// byte-identical retransmission maps to the same identity. The durable
// registry and the event key share this identity.
func commandIdentity(p Packet) string {
	return msgIdentity(p) + ":h" + radiocli.ContentID(p.Src, p.Message.To, p.Message.Text)
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

// sendCLIReply answers one radio command over the first ready APRS
// transmitter (best-effort; the reply lands in the durable TX history).
// The reply is fitted to the APRS message limit through the shared CLI
// mechanism (identity shortens progressively, payload truncates last);
// multi-line replies (the /hazard list) send one message per line.
//
// Every reply carries a hub-generated {id} ack suffix: the recipient's
// radio acknowledges by protocol (ackNNN) and the durable history marks
// the row delivered/failed — without the id the receiving radio has
// nothing to ACK and keeps retransmitting the message.
func (h *Hub) sendCLIReply(to, text string) {
	h.mu.Lock()
	cli := h.cli
	h.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	for _, line := range radiocli.ReplyLines(text, radiocli.MaxReplyLines) {
		if cli != nil {
			// Fit with the ack budget so the {id} suffix never
			// eats into the fitted payload.
			line = cli.Fit(line, MaxMessageText-AckSuffixLen)
		}
		if _, _, err := h.sendTracked(ctx, to, line); err != nil && h.logger != nil {
			h.logger.Warn("aprs: cli reply failed", "to", to, "error", err)
		}
	}
}

// publishBulletin keeps one heard bulletin in the retained MQTT state:
// the topic id derives from the sender and receipt time, so a repeated
// bulletin overwrites in place. Maintenance tombstones it after the
// bulletin TTL (empty retained payload = topic deletion).
func (h *Hub) publishBulletin(p Packet, via string) {
	id := fmt.Sprintf("%s-%d", p.Src, p.ReceivedAt)
	payload, err := json.Marshal(BulletinDocument{
		SchemaVersion: SchemaVersion,
		From:          p.Src,
		To:            p.Message.To,
		Text:          p.Message.Text,
		ReceivedAt:    formatTime(p.ReceivedAt),
		Via:           via,
	})
	if err != nil {
		h.logger.Warn("aprs: bulletin marshal failed", "error", err)
		return
	}
	h.mu.Lock()
	h.bulletins[id] = p.ReceivedAt
	h.mu.Unlock()
	if err := h.publishWithTimeout(BulletinsTopicPrefix+id, true, payload); err != nil {
		h.logger.Warn("aprs: bulletin publish failed", "id", id, "error", err)
	}
}

// routableMessage reports whether a message addressed to us is real
// traffic worth routing: not our own transmission, not an ack/rej
// protocol frame, and not a broadcast bulletin (those are announcements
// for everyone, not personal alert traffic).
func (h *Hub) routableMessage(p Packet) bool {
	if p.Src == h.cfg.Callsign || p.Message == nil {
		return false
	}
	if IsBulletin(p.Message.To) {
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

// bannerAllowed reports whether an automatic banner answer may go out
// now: at most one banner per sender per window and one banner per
// global interval. The answer path for plain messages therefore stays
// strictly rate-limited — answering bots cannot loop each other or hog
// the channel.
func (h *Hub) bannerAllowed(from string) bool {
	now := h.now()
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.bannerNext.IsZero() && now.Before(h.bannerNext) {
		return false
	}
	if last, ok := h.bannerLast[from]; ok && now.Sub(last) < bannerMinInterval {
		return false
	}
	h.bannerLast[from] = now
	h.bannerNext = now.Add(bannerGlobalInterval)
	if len(h.bannerLast) > 256 {
		cutoff := now.Add(-bannerMinInterval)
		for k, at := range h.bannerLast {
			if at.Before(cutoff) {
				delete(h.bannerLast, k)
			}
		}
	}
	return true
}

// replyAllowed reports whether ONE automatic reply may go out to the
// sender now. Every automatic answer — command confirmations,
// retransmission replays, /help, denials and banners — shares one
// per-sender burst window, so a flooding sender (or a stuck device
// repeating /unknown) cannot make the station chatter.
func (h *Hub) replyAllowed(from string) bool {
	now := h.now()
	h.mu.Lock()
	defer h.mu.Unlock()
	w := h.replyLast[from]
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
	h.replyLast[from] = w
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

// publishMessageEvent re-publishes one APRS message as a canonical
// /events payload. The forwarded content starts with
// "Message from: <callsign with SSID>", the text follows, and our
// station name is carried as context. Messages from trusted operators
// are alerts by nature: the default severity is severe.
//
// The event identity (event key + source id) is stable per message:
// sender + message identity (ack id or content digest). Lifecycle times
// come from the durable registry (resolveCommand) when it exists and
// the ChangeID derives from the content, so a retransmitted copy maps
// to byte-identical content: the storage deduplication collapses it
// into one alarm and one delivery even after a restart. The result
// (commandResult) travels on the wire document so the durable inbox
// acceptance records it transactionally with the lifecycle anchor —
// that anchor is the restart-proof command registry; the in-RAM cache
// in routeOrCLI additionally stops re-execution in-process.
//
// The returned acceptance reports how the LOCAL pipeline took the event
// (durable, emergency or rejected); the /debug confirmation follows it.
func (h *Hub) publishMessageEvent(p Packet, eff, exp time.Time, commandResult string) dispatch.Acceptance {
	nowS := eff.Format(time.RFC3339)
	expS := exp.Format(time.RFC3339)
	from := p.Src
	// Stable identity: sender + message identity. The RF copy and the
	// APRS-IS copy of the same packet map to the same event, so the
	// storage deduplication collapses them into one alarm; the command
	// cache in routeOrCLI already stops re-execution in-process.
	sourceID := from + ":msg:" + commandIdentity(p)
	text := strings.TrimSpace(p.Message.Text)

	doc := MessageEventWire{
		SchemaVersion: messageEventSchemaVersion,
		ChangeID:      radiocli.StableID("aprs:msg", sourceID, text),
		ChangeType:    "new",
		EventKey:      "aprs:" + sourceID,
		CommandResult: commandResult,
		Event: MessageEventHazard{
			Source:      "aprs",
			SourceID:    sourceID,
			Event:       "APRS message",
			Severity:    "severe",
			Urgency:     "unknown",
			Certainty:   "unknown",
			Headline:    "Message from: " + from + ": " + text,
			Description: "Received by " + h.Name() + " (" + h.cfg.Callsign + ")",
			EffectiveAt: &nowS,
			ExpiresAt:   &expS,
			Areas:       []string{},
			Status:      "active",
			ReceivedAt:  nowS,
			UpdatedAt:   nowS,
		},
	}
	payload, err := json.Marshal(doc)
	if err != nil {
		h.logger.Warn("aprs: routed message marshal failed", "error", err)
		return dispatch.Rejected
	}
	h.mu.Lock()
	acceptor := h.eventAcceptor
	h.mu.Unlock()
	if acceptor != nil {
		return acceptor(payload)
	}
	// Legacy path (no acceptor installed): best-effort through the
	// generic sink; the hub cannot distinguish local acceptance here.
	if err := h.publishWithTimeout("events", false, payload); err != nil {
		h.logger.Warn("aprs: routed message publish failed", "callsign", from, "error", err)
	}
	return dispatch.AcceptedDurable
}

// publishAlertEvent raises one operator-requested hazard (/alert) into
// the /events stream: severe, bounded by the requested TTL (4 hours by
// default). The event identity follows the same stable per-message
// scheme as publishMessageEvent; lifecycle times come from the durable
// registry (resolveCommand) and the result travels on the wire document
// for the transactional inbox anchor. The returned acceptance reports
// how the LOCAL pipeline took the alert (durable, emergency or
// rejected); the confirmation reply must follow it. A broker failure
// alone never downgrades the local acceptance.
func (h *Hub) publishAlertEvent(p Packet, spec *radiocli.AlertSpec, eff, exp time.Time, commandResult string) dispatch.Acceptance {
	if spec == nil {
		return dispatch.Rejected
	}
	from := p.Src
	// Stable identity: sender + message identity (see publishMessageEvent).
	sourceID := from + ":alert:" + commandIdentity(p)
	nowS := eff.Format(time.RFC3339)
	expS := exp.Format(time.RFC3339)
	doc := MessageEventWire{
		SchemaVersion: messageEventSchemaVersion,
		ChangeID:      radiocli.StableID("aprs:alert", sourceID, spec.Headline),
		ChangeType:    "new",
		EventKey:      "aprs:" + sourceID,
		CommandResult: commandResult,
		Event: MessageEventHazard{
			Source:      "aprs",
			SourceID:    sourceID,
			Event:       "APRS alert",
			Severity:    "severe",
			Urgency:     "immediate",
			Certainty:   "observed",
			Headline:    spec.Headline,
			Description: "Alert raised by " + from + " over APRS",
			EffectiveAt: &nowS,
			ExpiresAt:   &expS,
			Areas:       []string{},
			Status:      "active",
			ReceivedAt:  nowS,
			UpdatedAt:   nowS,
		},
	}
	payload, err := json.Marshal(doc)
	if err != nil {
		h.logger.Warn("aprs: alert marshal failed", "error", err)
		return dispatch.Rejected
	}
	h.mu.Lock()
	acceptor := h.eventAcceptor
	h.mu.Unlock()
	if acceptor != nil {
		return acceptor(payload)
	}
	// Legacy path (no acceptor installed): best-effort through the
	// generic sink; the hub cannot distinguish local acceptance here.
	if err := h.publishWithTimeout("events", false, payload); err != nil {
		h.logger.Warn("aprs: alert publish failed", "callsign", from, "error", err)
	}
	return dispatch.AcceptedDurable
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

// signalAck wakes the waiter registered for this ack frame — but only
// when the ack really belongs to the pending exchange: the message id
// must be in flight AND the ack must come from the addressee the
// message was sent to, addressed back to our callsign. A foreign or
// stale ack is ignored and leaves the wait intact, so it can never
// confirm someone else's exchange (or a recycled id). The matching
// durable tx row is marked delivered (ack) or failed (rej) — this is
// how the admin history shows which messages got acknowledged.
func (h *Hub) signalAck(p Packet, kind string) {
	h.mu.Lock()
	w, ok := h.pending[p.Message.ID]
	if !ok || w.to != p.Src || w.from != p.Message.To {
		h.mu.Unlock()
		return
	}
	delete(h.pending, p.Message.ID)
	ch := w.ch
	status := "delivered"
	if kind == "rej" {
		status = "failed"
	}
	if recorder := h.cfg.MessageRecorder; recorder != nil {
		recCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := recorder.UpdateAPRSMessageStatus(recCtx, p.Message.ID, status, h.now())
		cancel()
		if err != nil && h.logger != nil {
			h.logger.Debug("aprs: ack history update failed", "id", p.Message.ID, "status", status, "error", err)
		}
	}
	h.mu.Unlock()
	select {
	case ch <- kind:
	default:
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

// SendBeacon forces an immediate position beacon through a
// beacon-capable transmitter, preferring the radio backend.
func (h *Hub) SendBeacon(ctx context.Context) error {
	h.mu.Lock()
	var radio, fallback BeaconTransmitter
	for name, t := range h.transmitters {
		bt, ok := t.(BeaconTransmitter)
		if !ok || !bt.Ready() {
			continue
		}
		if name == BackendRadio {
			radio = bt
		} else if fallback == nil {
			fallback = bt
		}
	}
	h.mu.Unlock()
	bt := radio
	if bt == nil {
		bt = fallback
	}
	if bt == nil {
		return ErrNoBeacon
	}
	return bt.Beacon(ctx)
}

// SendMessageWaitAck sends one message with a hub-generated {id} and waits
// up to timeout for the addressee's ack (or rej) before returning. The
// bool reports whether an ack arrived; ErrNoAck means the timeout elapsed.
// The text is shortened to leave room for the {id} suffix, so the whole
// APRS message field never exceeds the protocol limit.
func (h *Hub) SendMessageWaitAck(ctx context.Context, to, text string, timeout time.Duration) (bool, error) {
	msgid, ch, err := h.sendTracked(ctx, to, text)
	if err != nil {
		return false, err
	}
	defer func() {
		h.mu.Lock()
		delete(h.pending, msgid)
		h.mu.Unlock()
	}()

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

// sendTracked sends one message with a hub-generated {id} ack suffix,
// registers the pending ack wait and returns the id plus the result
// channel. Waiters select on the channel; fire-and-forget callers (the
// radio CLI replies) ignore it — the durable history status is updated
// from signalAck either way. The text is shortened to leave room for
// the {id} suffix.
func (h *Hub) sendTracked(ctx context.Context, to, text string) (string, <-chan string, error) {
	to = NormalizeCallsign(to)
	if !ValidCallsign(to) {
		return "", nil, fmt.Errorf("aprs: invalid addressee callsign %q", to)
	}
	// Sanity stage before the protocol pass (see SendMessage).
	text = sanity.NormalizeText(ctx, sanity.ChannelAPRS, text)
	text = LimitMessageText(text, AckSuffixLen)
	if text == "" {
		return "", nil, fmt.Errorf("aprs: message text must not be empty")
	}

	msgid := fmt.Sprintf("%05d", h.msgSeq.Add(1)%100000)
	ch := make(chan string, 1)
	h.mu.Lock()
	if len(h.pending) >= ackPendingCap {
		h.mu.Unlock()
		return "", nil, fmt.Errorf("aprs: %d messages already awaiting acks", ackPendingCap)
	}
	// ID reuse guard: the sequence wraps after 100000 messages. An id
	// still in flight is never re-registered, so a late ack of a
	// previous cycle cannot collide with a live wait.
	for {
		if _, busy := h.pending[msgid]; !busy {
			break
		}
		msgid = fmt.Sprintf("%05d", h.msgSeq.Add(1)%100000)
	}
	h.pending[msgid] = &ackWait{from: h.cfg.Callsign, to: to, at: h.now(), ch: ch}
	h.mu.Unlock()

	tx := h.readyTransmitterFor(to)
	if tx == nil {
		h.mu.Lock()
		delete(h.pending, msgid)
		h.mu.Unlock()
		return "", nil, ErrNoTransmitter
	}
	full := text + "{" + msgid + "}"
	if err := tx.Send(ctx, to, full); err != nil {
		h.mu.Lock()
		delete(h.pending, msgid)
		h.mu.Unlock()
		return "", nil, fmt.Errorf("aprs: transmitter %s: %w", tx.Name(), err)
	}
	h.publishTxMessage(to, text, msgid, tx.Name())
	return msgid, ch, nil
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

	// Durable history (admin /messages page): best-effort.
	if h.cfg.MessageRecorder != nil {
		recCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := h.cfg.MessageRecorder.RecordAPRSMessage(recCtx, "tx", h.cfg.Callsign, to, text, id, via, time.Now())
		cancel()
		if err != nil && h.logger != nil {
			h.logger.Debug("aprs: message history record failed", "direction", "tx", "error", err)
		}
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
	bulletCutoff := now.Add(-h.cfg.BulletinTTL).Unix()
	var staleBulletins []string
	for id, at := range h.bulletins {
		if at < bulletCutoff {
			staleBulletins = append(staleBulletins, id)
			delete(h.bulletins, id)
		}
	}
	// Ack waits beyond the hard validity window are dropped: their acks
	// can never confirm an exchange whose validity already lapsed.
	ackCutoff := now.Add(-ackWaitMaxAge)
	for id, w := range h.pending {
		if w.at.Before(ackCutoff) {
			delete(h.pending, id)
		}
	}
	// The weather cache ages out with the /weather freshness window.
	wCutoff := now.Add(-weatherCacheAge)
	for call, e := range h.weatherCache {
		if e.at.Before(wCutoff) {
			delete(h.weatherCache, call)
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
	for _, id := range staleBulletins {
		if err := h.publishWithTimeout(BulletinsTopicPrefix+id, true, nil); err != nil {
			h.logger.Warn("aprs: bulletin delete failed", "id", id, "error", err)
		}
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
