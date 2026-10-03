package aprs

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/dispatch"
	"github.com/szporwolik/WarnFlux/internal/radiocli"
)

// fakeSink records publications per suffix.
type fakeSink struct {
	mu   sync.Mutex
	pubs map[string][][]byte // suffix -> payloads (nil payload = delete)
}

func newFakeSink() *fakeSink {
	return &fakeSink{pubs: make(map[string][][]byte)}
}

func (f *fakeSink) PublishRaw(suffix string, retained bool, payload []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pubs[suffix] = append(f.pubs[suffix], payload)
	return nil
}

func (f *fakeSink) payloads(suffix string) [][]byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([][]byte(nil), f.pubs[suffix]...)
}

// byPrefix returns the first topic (and its payloads) starting with
// prefix — used for topics whose full id is not known up front.
func (f *fakeSink) byPrefix(prefix string) (string, [][]byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for k, v := range f.pubs {
		if strings.HasPrefix(k, prefix) {
			return k, append([][]byte(nil), v...)
		}
	}
	return "", nil
}

// fakeTransmitter records sends.
type fakeTransmitter struct {
	name  string
	ready bool
	mu    sync.Mutex
	sent  [][2]string
}

func (f *fakeTransmitter) Name() string { return f.name }
func (f *fakeTransmitter) Ready() bool  { return f.ready }
func (f *fakeTransmitter) Send(_ context.Context, to, text string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sent = append(f.sent, [2]string{to, text})
	return nil
}

func (f *fakeTransmitter) sends() [][2]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([][2]string(nil), f.sent...)
}

func testHub(t *testing.T, cfg HubConfig) (*Hub, *fakeSink) {
	t.Helper()
	hub, err := NewHub(cfg, nil)
	if err != nil {
		t.Fatalf("NewHub: %v", err)
	}
	sink := newFakeSink()
	hub.SetSink(sink)
	return hub, sink
}

func TestHubSelfPublishAfterStartupDelay(t *testing.T) {
	hub, sink := testHub(t, HubConfig{
		Enabled:    true,
		Callsign:   "SP9MOA-10",
		Icon:       "/j",
		GridSquare: "JO90WW",
		RadiusKM:   DefaultRadiusKM,
		StationTTL: 30 * time.Minute,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hub.selfDelay = 250 * time.Millisecond
	hub.Start(ctx)

	// The self document must not race the (still connecting) MQTT sink at
	// startup; it arrives after selfDelay.
	if got := len(sink.payloads(StationsTopicPrefix + "SP9MOA-10")); got != 0 {
		t.Fatalf("self station published immediately (%d), want after selfDelay", got)
	}
	waitFor(t, func() bool {
		return len(sink.payloads(StationsTopicPrefix+"SP9MOA-10")) >= 1
	})
	var doc StationDocument
	if err := json.Unmarshal(sink.payloads(StationsTopicPrefix + "SP9MOA-10")[0], &doc); err != nil {
		t.Fatalf("unmarshal self doc: %v", err)
	}
	if !doc.Self {
		t.Error("self document has self=false")
	}
}

func testPacket(line string) Packet {
	return ParseFeedLine(line, time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC))
}

// TestHubOperationalArea pins the territory/area resolution: the area
// defaults to the station position and radius, an explicit territory
// center moves the map circle and geo sources without moving the station,
// and a lone coordinate is rejected.
func TestHubOperationalArea(t *testing.T) {
	lat, lon := 50.05, 20.1
	hub, err := NewHub(HubConfig{
		Enabled: true, Callsign: "SP9MOA-10", GridSquare: "JO90WW",
		RadiusKM: 30, Latitude: &lat, Longitude: &lon,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if hub.AreaLat() != 50.05 || hub.AreaLon() != 20.1 || hub.AreaRadius() != 30 {
		t.Fatalf("area defaults = (%v,%v) r=%v, want station position with radius", hub.AreaLat(), hub.AreaLon(), hub.AreaRadius())
	}

	alat, alon := 50.2, 19.9
	hub, err = NewHub(HubConfig{
		Enabled: true, Callsign: "SP9MOA-10", GridSquare: "JO90WW",
		RadiusKM: 30, Latitude: &lat, Longitude: &lon,
		AreaLatitude: &alat, AreaLongitude: &alon, AreaRadiusKM: 55,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if hub.AreaLat() != 50.2 || hub.AreaLon() != 19.9 || hub.AreaRadius() != 55 {
		t.Fatalf("area override = (%v,%v) r=%v, want the territory values", hub.AreaLat(), hub.AreaLon(), hub.AreaRadius())
	}
	if hub.CenterLat() != 50.05 || hub.CenterLon() != 20.1 || hub.RadiusKM() != 30 {
		t.Fatalf("station must keep its own position: (%v,%v) r=%v", hub.CenterLat(), hub.CenterLon(), hub.RadiusKM())
	}

	// A lone area coordinate is a configuration error.
	if _, err := NewHub(HubConfig{
		Enabled: true, Callsign: "SP9MOA-10", GridSquare: "JO90WW",
		RadiusKM: 30, AreaLatitude: &alat,
	}, nil); err == nil {
		t.Fatal("area_latitude without area_longitude accepted")
	}
}

// TestHubOwnPositionLearnedFromBeacon verifies that a position packet from
// our own callsign (e.g. the Direwolf PBEACON) moves the own-position
// locator away from the gridsquare center.
func TestHubOwnPositionLearnedFromBeacon(t *testing.T) {
	hub, sink := testHub(t, HubConfig{
		Enabled:    true,
		Callsign:   "SP9MOA-10",
		Icon:       "/j",
		GridSquare: "JO90WW",
		RadiusKM:   DefaultRadiusKM,
		StationTTL: 30 * time.Minute,
	})
	ctx, cancel := context.WithCancel(context.Background())
	hub.Start(ctx)
	defer cancel()

	// Before any own packet the locator falls back to the gridsquare center.
	if lat, lon := hub.OwnLat(), hub.OwnLon(); lat != hub.CenterLat() || lon != hub.CenterLon() {
		t.Fatalf("own position before beacon = (%v, %v), want center (%v, %v)", lat, lon, hub.CenterLat(), hub.CenterLon())
	}

	line := "SP9MOA-10>APRS,TCPIP*:!5100.00N/02010.00E-"
	hub.Observe(testPacket(line), "aprs-inet")

	waitFor(t, func() bool {
		lat, lon := hub.OwnLat(), hub.OwnLon()
		return lat != hub.CenterLat() && lon != hub.CenterLon()
	})

	// The learned position rides along in the self station document.
	waitFor(t, func() bool {
		return len(sink.payloads(StationsTopicPrefix+"SP9MOA-10")) >= 1
	})
	pubs := sink.payloads(StationsTopicPrefix + "SP9MOA-10")
	var doc StationDocument
	if err := json.Unmarshal(pubs[len(pubs)-1], &doc); err != nil {
		t.Fatalf("unmarshal self doc: %v", err)
	}
	if doc.Position == nil || doc.Position.Latitude == hub.CenterLat() {
		t.Errorf("self doc position = %+v, want the beacon position", doc.Position)
	}
}

// TestHubConfiguredPositionOverridesGridSquare verifies the explicit
// latitude/longitude option pins the center (and the locator) at the
// configured coordinates.
func TestHubConfiguredPositionOverridesGridSquare(t *testing.T) {
	lat, lon := 50.0212, 20.2075
	hub, _ := testHub(t, HubConfig{
		Enabled:    true,
		Callsign:   "SP9MOA-10",
		GridSquare: "JO90WW",
		Latitude:   &lat,
		Longitude:  &lon,
		RadiusKM:   DefaultRadiusKM,
		StationTTL: 30 * time.Minute,
	})
	if hub.CenterLat() != lat || hub.CenterLon() != lon {
		t.Fatalf("center = (%v, %v), want configured (%v, %v)", hub.CenterLat(), hub.CenterLon(), lat, lon)
	}
	if hub.OwnLat() != lat || hub.OwnLon() != lon {
		t.Fatalf("own = (%v, %v), want configured position", hub.OwnLat(), hub.OwnLon())
	}
}

// TestHubPositionRequiresBothCoordinates: half a coordinate pair is a
// construction error even when the hub is built directly.
func TestHubPositionRequiresBothCoordinates(t *testing.T) {
	lat := 50.0
	if _, err := NewHub(HubConfig{
		Enabled:    true,
		Callsign:   "SP9MOA-10",
		GridSquare: "JO90WW",
		Latitude:   &lat,
		RadiusKM:   DefaultRadiusKM,
		StationTTL: 30 * time.Minute,
	}, nil); err == nil {
		t.Fatal("latitude without longitude accepted")
	}
}

func TestHubStationStateAndDedupe(t *testing.T) {
	hub, sink := testHub(t, HubConfig{
		Enabled:    true,
		Callsign:   "SP9MOA-10",
		Icon:       "/j",
		GridSquare: "JO90WW",
		RadiusKM:   DefaultRadiusKM,
		StationTTL: 30 * time.Minute,
	})
	ctx, cancel := context.WithCancel(context.Background())
	hub.Start(ctx)
	defer cancel()

	line := "SP9XYZ-7>APRS,TCPIP*:!5056.25N/01952.50E-"
	hub.Observe(testPacket(line), "aprs-inet")
	hub.Observe(testPacket(line), "aprs-inet")  // duplicate
	hub.Observe(testPacket(line), "aprs-radio") // same content, new backend

	waitFor(t, func() bool {
		return len(sink.payloads(StationsTopicPrefix+"SP9XYZ-7")) >= 2
	})

	pubs := sink.payloads(StationsTopicPrefix + "SP9XYZ-7")
	if len(pubs) != 2 {
		t.Fatalf("station publications = %d, want 2 (first + new backend; the exact duplicate must not republish)", len(pubs))
	}
	var doc StationDocument
	if err := json.Unmarshal(pubs[len(pubs)-1], &doc); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(doc.ReceivedVia) != 2 {
		t.Errorf("received_via = %v, want both backends", doc.ReceivedVia)
	}
	if doc.Position == nil {
		t.Fatal("position missing")
	}

	// The packet feed publishes the content only once.
	if got := len(sink.payloads(PacketsTopic)); got != 1 {
		t.Errorf("packet feed publications = %d, want 1", got)
	}
}

func TestHubRadiusFilter(t *testing.T) {
	hub, sink := testHub(t, HubConfig{
		Enabled:    true,
		Callsign:   "SP9MOA-10",
		GridSquare: "JO90WW",
		RadiusKM:   10,
		StationTTL: 30 * time.Minute,
	})
	ctx, cancel := context.WithCancel(context.Background())
	hub.Start(ctx)
	defer cancel()

	// At the hub center (JO90WW): inside.
	hub.Observe(testPacket("SP9NR1>APRS:!5056.25N/01952.50E-"), "aprs-inet")
	// ~43 km south: outside.
	hub.Observe(testPacket("SP9FAR>APRS:!5033.08N/01956.44E-"), "aprs-inet")

	waitFor(t, func() bool {
		return len(sink.payloads(StationsTopicPrefix+"SP9NR1")) >= 1
	})
	if got := len(sink.payloads(StationsTopicPrefix + "SP9FAR")); got != 0 {
		t.Errorf("far station published %d times, want 0", got)
	}
	if got := hub.Stats().Filtered; got != 1 {
		t.Errorf("filtered = %d, want 1", got)
	}
}

func TestHubMessageRXAndTX(t *testing.T) {
	hub, sink := testHub(t, HubConfig{
		Enabled:    true,
		Callsign:   "SP9MOA-10",
		GridSquare: "JO90WW",
		RadiusKM:   DefaultRadiusKM,
		StationTTL: 30 * time.Minute,
	})
	ctx, cancel := context.WithCancel(context.Background())
	hub.Start(ctx)
	defer cancel()

	tx := &fakeTransmitter{name: "aprs-inet", ready: true}
	hub.AddTransmitter("aprs-inet", tx)

	// RX: message addressed to us.
	hub.Observe(testPacket("SP9XYZ>APRS,TCPIP*::SP9MOA-10:hello there"), "aprs-inet")
	waitFor(t, func() bool { return len(sink.payloads(MessagesTopic)) >= 1 })

	// TX through the hub.
	if err := hub.SendMessage(context.Background(), "SP9XYZ", "test reply"); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	waitFor(t, func() bool { return len(tx.sends()) == 1 })

	sends := tx.sends()
	if sends[0][0] != "SP9XYZ" || sends[0][1] != "test reply" {
		t.Errorf("send = %v", sends[0])
	}

	var doc MessageDocument
	pubs := sink.payloads(MessagesTopic)
	if err := json.Unmarshal(pubs[len(pubs)-1], &doc); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if doc.Direction != "tx" || doc.To != "SP9XYZ" || doc.Via != "aprs-inet" {
		t.Errorf("tx doc = %+v", doc)
	}
	if got := hub.RecentMessages(); len(got) != 1 || got[0].Direction != "rx" {
		t.Errorf("recent messages = %+v", got)
	}

	// No transmitter → clear error.
	hub.RemoveTransmitter("aprs-inet")
	if err := hub.SendMessage(context.Background(), "SP9XYZ", "nope"); err != ErrNoTransmitter {
		t.Errorf("SendMessage with no transmitter = %v, want ErrNoTransmitter", err)
	}
}

// TestHubSendMessageSanity verifies every outbound message passes through
// the shared sanity normalizer before the protocol pass: control
// characters and doubled whitespace collapse, Polish diacritics
// transliterate, and non-ASCII drops — the transmitter never sees raw
// operator text.
func TestHubSendMessageSanity(t *testing.T) {
	hub, _ := testHub(t, HubConfig{
		Enabled:    true,
		Callsign:   "SP9MOA-10",
		Icon:       "/j",
		GridSquare: "JO90WW",
		RadiusKM:   DefaultRadiusKM,
		StationTTL: 30 * time.Minute,
	})
	tx := &fakeTransmitter{name: "aprs-inet", ready: true}
	hub.AddTransmitter("aprs-inet", tx)

	if err := hub.SendMessage(context.Background(), "SP9XYZ", "  Uwaga  \n śnieg   i lód 🚨  "); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	sends := tx.sends()
	if len(sends) != 1 {
		t.Fatalf("sends = %d, want 1", len(sends))
	}
	if sends[0][1] != "Uwaga snieg i lod" {
		t.Errorf("transmitted = %q, want %q", sends[0][1], "Uwaga snieg i lod")
	}

	// Same funnel for the ack-tracked path.
	if _, err := hub.SendMessageWaitAck(context.Background(), "SP9XYZ", "  test \n  ", 100*time.Millisecond); err == nil {
		// Timeout/ErrNoAck expected without a receiver; only the text matters.
		t.Fatalf("SendMessageWaitAck unexpectedly succeeded")
	}
	if got := tx.sends()[1][1]; got != "test{00001}" {
		t.Errorf("ack send = %q, want normalized text with ack suffix", got)
	}
}

func TestHubStationExpiry(t *testing.T) {
	hub, sink := testHub(t, HubConfig{
		Enabled:    true,
		Callsign:   "SP9MOA-10",
		GridSquare: "JO90WW",
		RadiusKM:   DefaultRadiusKM,
		StationTTL: time.Minute,
	})
	hub.tick = 20 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	hub.Start(ctx)
	defer cancel()

	hub.Observe(testPacket("SP9OLD>APRS:!5056.25N/01952.50E-"), "aprs-inet")
	waitFor(t, func() bool {
		return len(sink.payloads(StationsTopicPrefix+"SP9OLD")) >= 1
	})

	// Age the station beyond the TTL.
	hub.mu.Lock()
	hub.stations["SP9OLD"].state.lastHeard = time.Now().Add(-2 * time.Minute).Unix()
	hub.mu.Unlock()

	waitFor(t, func() bool {
		return hub.Stats().Expired == 1
	})
	pubs := sink.payloads(StationsTopicPrefix + "SP9OLD")
	if len(pubs) != 2 || pubs[1] != nil {
		t.Fatalf("expiry publications = %#v, want [doc, nil-delete]", pubs)
	}
}

func TestHubSelfDocument(t *testing.T) {
	hub, sink := testHub(t, HubConfig{
		Enabled:    true,
		Callsign:   "SP9MOA-10",
		Icon:       "/j",
		GridSquare: "JO90WW",
		RadiusKM:   DefaultRadiusKM,
		StationTTL: 30 * time.Minute,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hub.selfDelay = 10 * time.Millisecond
	hub.Start(ctx)

	waitFor(t, func() bool {
		return len(sink.payloads(StationsTopicPrefix+"SP9MOA-10")) >= 1
	})
	var doc StationDocument
	if err := json.Unmarshal(sink.payloads(StationsTopicPrefix + "SP9MOA-10")[0], &doc); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !doc.Self || doc.Position == nil {
		t.Errorf("self doc = %+v", doc)
	}
	if doc.SymbolTable != "/" || doc.Symbol != "j" {
		t.Errorf("icon = %s%s", doc.SymbolTable, doc.Symbol)
	}
}

func TestHubStationsSnapshot(t *testing.T) {
	hub, _ := testHub(t, HubConfig{
		Enabled:    true,
		Callsign:   "SP9MOA-10",
		GridSquare: "JO90WW",
		RadiusKM:   DefaultRadiusKM,
		StationTTL: 30 * time.Minute,
	})
	ctx, cancel := context.WithCancel(context.Background())
	hub.Start(ctx)
	defer cancel()

	hub.Observe(testPacket("SP9AAA>APRS:!5056.25N/01952.50E-"), "aprs-inet")
	hub.Observe(testPacket("SP9BBB>APRS:!5056.25N/01952.50E-"), "aprs-radio")
	waitFor(t, func() bool { return len(hub.Stations()) == 2 })

	got := hub.Stations()
	if len(got) != 2 {
		t.Fatalf("Stations() = %d docs, want 2", len(got))
	}
	// Sorted by callsign; the self document is excluded.
	if got[0].Callsign != "SP9AAA" || got[1].Callsign != "SP9BBB" {
		t.Errorf("Stations() order = %v", got)
	}
	for _, doc := range got {
		if doc.Self || doc.Position == nil {
			t.Errorf("station doc = %+v", doc)
		}
	}
}

func TestHubInfrastructureFilter(t *testing.T) {
	hub, sink := testHub(t, HubConfig{
		Enabled:               true,
		Callsign:              "SP9MOA-10",
		GridSquare:            "JO90WW",
		RadiusKM:              DefaultRadiusKM,
		StationTTL:            30 * time.Minute,
		ExcludeInfrastructure: true,
	})
	ctx, cancel := context.WithCancel(context.Background())
	hub.Start(ctx)
	defer cancel()

	// A real ham (house symbol) is kept.
	hub.Observe(testPacket("SP9OK>APRS:!5056.25N/01952.50E-"), "aprs-inet")
	// A digipeater (primary # symbol) is dropped.
	hub.Observe(testPacket("SR9NR>APRS:!5056.25N/01952.50E#"), "aprs-inet")
	// An igate (primary I) is dropped.
	hub.Observe(testPacket("SR9IG>APRS:!5056.25N/01952.50EI"), "aprs-inet")
	// An object (repeater announcement) is dropped.
	hub.Observe(testPacket("SP9MOA>APRS:;SR9NR  *111111z5056.25N/01952.50ErT145.550"), "aprs-inet")

	waitFor(t, func() bool { return len(sink.payloads(StationsTopicPrefix+"SP9OK")) >= 1 })
	if got := len(sink.payloads(StationsTopicPrefix + "SR9NR")); got != 0 {
		t.Errorf("digipeater published %d times, want 0", got)
	}
	if got := len(sink.payloads(StationsTopicPrefix + "SR9IG")); got != 0 {
		t.Errorf("igate published %d times, want 0", got)
	}
	if got := hub.Stats().Filtered; got != 3 {
		t.Errorf("filtered = %d, want 3", got)
	}
}

func TestHubInfrastructureFilterDisabled(t *testing.T) {
	hub, sink := testHub(t, HubConfig{
		Enabled:    true,
		Callsign:   "SP9MOA-10",
		GridSquare: "JO90WW",
		RadiusKM:   DefaultRadiusKM,
		StationTTL: 30 * time.Minute,
	})
	ctx, cancel := context.WithCancel(context.Background())
	hub.Start(ctx)
	defer cancel()

	// Without the filter the digipeater stays on the map.
	hub.Observe(testPacket("SR9NR>APRS:!5056.25N/01952.50E#"), "aprs-inet")
	waitFor(t, func() bool { return len(sink.payloads(StationsTopicPrefix+"SR9NR")) >= 1 })
}

// TestStationTrackTail pins the movement-tail wire: up to three earlier
// positions, oldest first, deduplicated against sub-30m noise.
func TestStationTrackTail(t *testing.T) {
	hub, sink := testHub(t, HubConfig{
		Enabled:    true,
		Callsign:   "SP9MOA-10",
		GridSquare: "JO90WW",
		RadiusKM:   DefaultRadiusKM,
		StationTTL: 30 * time.Minute,
	})
	ctx, cancel := context.WithCancel(context.Background())
	hub.Start(ctx)
	defer cancel()

	// 6 packets near the configured center: 4 distinct positions ~1 km
	// apart, one exact duplicate (digest-skipped) and one sub-30m
	// near-duplicate (position updates, track does not grow).
	hub.Observe(testPacket("SP9MOV>APRS:!5055.00N/01952.00E>"), "aprs-inet")
	hub.Observe(testPacket("SP9MOV>APRS:!5055.50N/01952.50E>"), "aprs-inet")
	hub.Observe(testPacket("SP9MOV>APRS:!5056.00N/01953.00E>"), "aprs-inet")
	hub.Observe(testPacket("SP9MOV>APRS:!5056.50N/01953.50E>"), "aprs-inet")
	hub.Observe(testPacket("SP9MOV>APRS:!5056.50N/01953.50E>"), "aprs-inet")  // exact duplicate
	hub.Observe(testPacket("SP9MOV>APRS:!5056.51N/01953.51E>"), "aprs-radio") // ~20 m: noise

	// The track tail carries positions 2-4 and the current position is
	// the noise-adjusted 4th one — wait for the FULL tail, not just the
	// first position (the worker applies packets one by one).
	waitFor(t, func() bool {
		docs := hub.Stations()
		return len(docs) == 1 && docs[0].Position != nil &&
			mathAbs(docs[0].Position.Latitude-50.941833) < 0.0001 &&
			len(docs[0].Track) == 3
	})
	docs := hub.Stations()
	doc := docs[0]
	if len(doc.Track) != 3 {
		t.Fatalf("track length = %d, want 3 (got %+v)", len(doc.Track), doc.Track)
	}
	// Oldest first: after the noise packet the tail holds positions 2-4.
	want := []float64{50.925, 50.933333, 50.941667}
	for i, tp := range doc.Track {
		if mathAbs(tp.Latitude-want[i]) > 0.0001 {
			t.Errorf("track[%d].latitude = %v, want %v", i, tp.Latitude, want[i])
		}
		if tp.At == "" {
			t.Errorf("track[%d].at empty", i)
		}
	}
	// The current position is the 4th distinct one.
	if doc.Position == nil || mathAbs(doc.Position.Latitude-50.941833) > 0.0001 {
		t.Errorf("position = %+v, want latitude ~50.941833", doc.Position)
	}

	// The retained wire document carries the same track.
	payloads := sink.payloads(StationsTopicPrefix + "SP9MOV")
	if len(payloads) == 0 {
		t.Fatal("no station payload published")
	}
	var wire StationDocument
	if err := json.Unmarshal(payloads[len(payloads)-1], &wire); err != nil {
		t.Fatalf("unmarshal station doc: %v", err)
	}
	if len(wire.Track) != 3 {
		t.Errorf("wire track length = %d, want 3", len(wire.Track))
	}
}

func TestHubStationOrigin(t *testing.T) {
	hub, sink := testHub(t, HubConfig{
		Enabled:    true,
		Callsign:   "SP9MOA-10",
		GridSquare: "JO90WW",
		RadiusKM:   DefaultRadiusKM,
		StationTTL: 30 * time.Minute,
	})
	ctx, cancel := context.WithCancel(context.Background())
	hub.Start(ctx)
	defer cancel()

	// Heard over the radio by an i-gate (qAR path).
	hub.Observe(testPacket("SP9RF>APRS,WIDE1-1,SR9NR*,qAR,SR9NR:!5056.25N/01952.50E-"), "aprs-inet")
	// Injected directly from the internet (TCPIP* path).
	hub.Observe(testPacket("SP9NET>APRS,TCPIP*,qAC,T2POLAND:!5056.25N/01952.50E-"), "aprs-inet")
	waitFor(t, func() bool {
		return len(sink.payloads(StationsTopicPrefix+"SP9RF")) >= 1 && len(sink.payloads(StationsTopicPrefix+"SP9NET")) >= 1
	})

	origin := make(map[string]string)
	for _, doc := range hub.Stations() {
		origin[doc.Callsign] = doc.Origin
	}
	if origin["SP9RF"] != "rf" {
		t.Errorf("SP9RF origin = %q, want rf", origin["SP9RF"])
	}
	if origin["SP9NET"] != "internet" {
		t.Errorf("SP9NET origin = %q, want internet", origin["SP9NET"])
	}
}

func TestNewHubValidation(t *testing.T) {
	if _, err := NewHub(HubConfig{Enabled: true, Callsign: "BAD!CALL", GridSquare: "JO90WW"}, nil); err == nil {
		t.Error("invalid callsign accepted")
	}
	if _, err := NewHub(HubConfig{Enabled: true, Callsign: "SP9MOA-10", GridSquare: "NOPE"}, nil); err == nil {
		t.Error("invalid gridsquare accepted")
	}
	if _, err := NewHub(HubConfig{Enabled: true, Callsign: "SP9MOA-10", GridSquare: "JO90WW", RadiusKM: 5000}, nil); err == nil {
		t.Error("out-of-range radius accepted")
	}
	if _, err := NewHub(HubConfig{Enabled: false, Callsign: "", GridSquare: ""}, nil); err != nil {
		t.Errorf("disabled hub must not validate identity: %v", err)
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met within 2s")
}

// TestBulletinRetainedFeed pins the persistent bulletin presence: a heard
// bulletin publishes one retained document under aprs/bulletins/* and is
// never routed as an event.
func TestBulletinRetainedFeed(t *testing.T) {
	hub, sink := testHub(t, HubConfig{
		Enabled: true, Callsign: "SP9MOA-10", GridSquare: "JO90WW",
		RadiusKM: DefaultRadiusKM, StationTTL: 30 * time.Minute,
		RouteMessages: true,
	})
	ctx, cancel := context.WithCancel(context.Background())
	hub.Start(ctx)
	defer cancel()
	hub.SetSenderGate(func(base string) bool { return base == "SP9XYZ" })
	hub.Observe(ParseFeedLine("SP9XYZ-7>APRS,WIDE1-1*::BLN0     :ops bulletin", time.Now()), BackendRadio)

	var topic string
	var pubs [][]byte
	waitFor(t, func() bool {
		topic, pubs = sink.byPrefix(BulletinsTopicPrefix)
		return topic != "" && len(pubs) >= 1 && pubs[0] != nil
	})
	if !strings.HasPrefix(topic, BulletinsTopicPrefix+"SP9XYZ-7-") {
		t.Fatalf("bulletin topic = %q", topic)
	}
	var doc BulletinDocument
	if err := json.Unmarshal(pubs[0], &doc); err != nil {
		t.Fatalf("bulletin payload: %v", err)
	}
	if doc.SchemaVersion != SchemaVersion || doc.From != "SP9XYZ-7" || doc.To != "BLN0" || doc.Text != "ops bulletin" {
		t.Fatalf("bulletin doc = %+v", doc)
	}
	time.Sleep(150 * time.Millisecond)
	if got := len(sink.payloads("events")); got != 0 {
		t.Fatalf("bulletin produced %d routed events", got)
	}
}

// TestHubRadioCLI pins the radio-command interpreter on the APRS side:
// /help is answered in-band and stays off the alarm pipeline, /debug
// fires the alarm like a plain message plus a confirmation, and plain
// messages still route.
func TestHubRadioCLI(t *testing.T) {
	hub, sink := testHub(t, HubConfig{
		Enabled: true, Callsign: "SP9MOA-10", GridSquare: "JO90WW",
		RadiusKM: DefaultRadiusKM, StationTTL: 30 * time.Minute,
		RouteMessages: true,
	})
	ctx, cancel := context.WithCancel(context.Background())
	hub.Start(ctx)
	defer cancel()
	tx := &fakeTransmitter{name: "aprs-inet", ready: true}
	hub.AddTransmitter("aprs-inet", tx)
	hub.SetSenderGate(func(base string) bool { return base == "SP9XYZ" })
	hub.SetCLI(radiocli.New("WarnFlux v1.0 - SOSNA - sosna.sp9moa.pl"))

	// /help: answered in-band, no alarm event.
	hub.Observe(testPacket("SP9XYZ>APRS,TCPIP*::SP9MOA-10:/help"), "aprs-inet")
	waitFor(t, func() bool { return len(tx.sends()) == 1 })
	sends := tx.sends()
	if sends[0][0] != "SP9XYZ" || !strings.Contains(sends[0][1], "Commands") {
		t.Fatalf("help reply = %v, want the command list to SP9XYZ", sends[0])
	}
	if got := len(sink.payloads("events")); got != 0 {
		t.Fatalf("/help produced %d alarm events", got)
	}

	// /debug: alarm event + confirmation reply.
	hub.Observe(testPacket("SP9XYZ>APRS,TCPIP*::SP9MOA-10:/debug"), "aprs-inet")
	waitFor(t, func() bool { return len(sink.payloads("events")) == 1 })
	waitFor(t, func() bool { return len(tx.sends()) == 2 })
	sends = tx.sends()
	if !strings.Contains(sends[1][1], "debug alarm") {
		t.Fatalf("debug reply = %v, want the confirmation", sends[1])
	}

	// A plain message never routes to an alarm: the standard
	// installation banner (with the /help hint) answers instead.
	hub.Observe(testPacket("SP9XYZ>APRS,TCPIP*::SP9MOA-10:plain alarm"), "aprs-inet")
	waitFor(t, func() bool { return len(tx.sends()) == 3 })
	sends = tx.sends()
	if !strings.Contains(sends[2][1], "WarnFlux v1.0 - SOSNA") {
		t.Fatalf("plain reply = %v, want the standard banner", sends[2])
	}
	if !strings.Contains(sends[2][1], "type /help for help") {
		t.Fatalf("plain reply = %v, missing the /help hint", sends[2])
	}
	if got := len(sink.payloads("events")); got != 1 {
		t.Fatalf("plain message produced %d alarm events, want 1 (debug only)", got)
	}

	// Unauthorized senders: commands never run and never alarm — a
	// restricted command gets an explicit denial.
	hub.Observe(testPacket("SP9ZZZ>APRS,TCPIP*::SP9MOA-10:/debug"), "aprs-inet")
	waitFor(t, func() bool { return len(tx.sends()) == 4 })
	sends = tx.sends()
	if !strings.Contains(sends[3][1], "WarnFlux v1.0 - SOSNA") || !strings.Contains(sends[3][1], "You are not authorized") {
		t.Fatalf("unauthorized /debug reply = %v, want the banner with the denial", sends[3])
	}
	if got := len(sink.payloads("events")); got != 1 {
		t.Fatalf("unauthorized /debug produced alarm events (%d total)", got)
	}

	// /help is public: an unregistered sender still gets the command
	// list — but only of the commands they may run (no /debug).
	hub.Observe(testPacket("SP9ZZZ>APRS,TCPIP*::SP9MOA-10:/help"), "aprs-inet")
	waitFor(t, func() bool { return len(tx.sends()) == 5 })
	sends = tx.sends()
	if !strings.Contains(sends[4][1], "Commands:") || !strings.Contains(sends[4][1], "/help") {
		t.Fatalf("public /help reply = %v, want the public command list", sends[4])
	}
	if strings.Contains(sends[4][1], "/debug") || strings.Contains(sends[4][1], "/alert") {
		t.Fatalf("public /help reply = %v, must not list restricted commands", sends[4])
	}
	if got := len(sink.payloads("events")); got != 1 {
		t.Fatalf("public /help produced alarm events (%d total)", got)
	}
}

// TestHubCommandDedup pins the retransmission guard: the same packet
// heard twice — once over the radio and once through APRS-IS — raises
// ONE alarm, and the retransmission gets the previous reply instead of
// executing the command again. The event identity is stable (sender +
// message identity), so the storage deduplication collapses late
// redeliveries too.
func TestHubCommandDedup(t *testing.T) {
	hub, sink := testHub(t, HubConfig{
		Enabled: true, Callsign: "SP9MOA-10", GridSquare: "JO90WW",
		RadiusKM: DefaultRadiusKM, StationTTL: 30 * time.Minute,
		RouteMessages: true,
	})
	ctx, cancel := context.WithCancel(context.Background())
	hub.Start(ctx)
	defer cancel()
	tx := &fakeTransmitter{name: "aprs-inet", ready: true}
	hub.AddTransmitter("aprs-inet", tx)
	hub.SetSenderGate(func(base string) bool { return base == "SP9XYZ" })
	hub.SetCLI(radiocli.New("WarnFlux v1.0 - SOSNA - sosna.sp9moa.pl"))

	// The same /debug packet (no ack id) arrives through both backends:
	// the content digest identifies it as one command.
	hub.Observe(testPacket("SP9XYZ-7>APRS,WIDE1-1*::SP9MOA-10:/debug"), BackendRadio)
	hub.Observe(testPacket("SP9XYZ-7>APRS,TCPIP*,qAO::SP9MOA-10:/debug"), BackendInternet)

	waitFor(t, func() bool { return len(sink.payloads("events")) == 1 })
	waitFor(t, func() bool { return len(tx.sends()) == 2 })
	time.Sleep(150 * time.Millisecond) // settle: no second event may appear
	if got := len(sink.payloads("events")); got != 1 {
		t.Fatalf("retransmission raised %d events, want 1", got)
	}
	sends := tx.sends()
	if !strings.Contains(sends[0][1], "debug alarm") || !strings.Contains(sends[1][1], "debug alarm") {
		t.Fatalf("retransmission replies = %v, want the previous result on both deliveries", sends)
	}
	var ev MessageEventWire
	if err := json.Unmarshal(sink.payloads("events")[0], &ev); err != nil {
		t.Fatalf("event payload: %v", err)
	}
	if !strings.HasPrefix(ev.EventKey, "aprs:SP9XYZ-7:msg:") {
		t.Fatalf("event key = %q, want the stable sender+message identity", ev.EventKey)
	}

	// A packet with an ack id deduplicates by that id, and the event
	// identity carries it. The observation-level digest guard already
	// swallows a repeated Internet copy of the same packet, so only the
	// radio copy gets the reply.
	hub.Observe(testPacket("SP9XYZ-7>APRS,WIDE1-1*::SP9MOA-10:/debug{0042"), BackendRadio)
	hub.Observe(testPacket("SP9XYZ-7>APRS,TCPIP*,qAO::SP9MOA-10:/debug{0042"), BackendInternet)
	waitFor(t, func() bool { return len(sink.payloads("events")) == 2 })
	waitFor(t, func() bool { return len(tx.sends()) == 3 })
	time.Sleep(150 * time.Millisecond)
	if got := len(sink.payloads("events")); got != 2 {
		t.Fatalf("id-carrying retransmission raised %d events, want 2 total", got)
	}
	if err := json.Unmarshal(sink.payloads("events")[1], &ev); err != nil {
		t.Fatalf("event payload: %v", err)
	}
	if ev.EventKey != "aprs:SP9XYZ-7:msg:0042" {
		t.Fatalf("event key = %q, want aprs:SP9XYZ-7:msg:0042", ev.EventKey)
	}
}

// TestHubAlertConfirmationTracksAcceptance pins the honest /alert
// confirmation: the reply follows the LOCAL acceptance — durable keeps
// the handler confirmation, the emergency fallback is reported as such,
// and a rejection never claims success. A rejection is TRANSIENT: the
// failure is replayed within the retry cooldown, and after it a
// retransmission executes the command again (controlled retry). The
// acceptor replaces the generic sink here, so its calls double as the
// event capture.
func TestHubAlertConfirmationTracksAcceptance(t *testing.T) {
	hub, sink := testHub(t, HubConfig{
		Enabled: true, Callsign: "SP9MOA-10", GridSquare: "JO90WW",
		RadiusKM: DefaultRadiusKM, StationTTL: 30 * time.Minute,
		RouteMessages: true, CmdRetryCooldown: 500 * time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())
	hub.Start(ctx)
	defer cancel()
	tx := &fakeTransmitter{name: "aprs-inet", ready: true}
	hub.AddTransmitter("aprs-inet", tx)
	hub.SetSenderGate(func(base string) bool { return base == "SP9XYZ" })
	cli := radiocli.New("WarnFlux v1.0 - SOSNA - sosna.sp9moa.pl")
	cli.RegisterRestricted("alert", "alert", func(args string) radiocli.Result {
		return radiocli.Result{Handled: true, Alert: &radiocli.AlertSpec{Headline: strings.TrimSpace(args), TTL: 4 * time.Hour}, Reply: "OK: alert raised"}
	})
	hub.SetCLI(cli)

	var (
		mu       sync.Mutex
		accepted []dispatch.Acceptance
		accValue = dispatch.AcceptedDurable
	)
	hub.SetEventAcceptor(func([]byte) dispatch.Acceptance {
		mu.Lock()
		defer mu.Unlock()
		accepted = append(accepted, accValue)
		return accValue
	})

	// Durable acceptance: the plain handler confirmation.
	hub.Observe(testPacket("SP9XYZ>APRS,TCPIP*::SP9MOA-10:/alert pozar lasu"), "aprs-inet")
	waitFor(t, func() bool { return len(tx.sends()) == 1 })
	if got := tx.sends()[0][1]; got != "OK: alert raised" {
		t.Fatalf("durable confirmation = %q, want the plain handler reply", got)
	}

	// Emergency acceptance: the fallback is reported explicitly.
	accValue = dispatch.AcceptedEmergency
	hub.Observe(testPacket("SP9XYZ>APRS,TCPIP*::SP9MOA-10:/alert pozar lasu 2"), "aprs-inet")
	waitFor(t, func() bool { return len(tx.sends()) == 2 })
	if got := tx.sends()[1][1]; !strings.Contains(got, "OK: alert raised") || !strings.Contains(got, "failover") {
		t.Fatalf("emergency confirmation = %q, want the explicit failover note", got)
	}

	// Rejection: never claim success.
	accValue = dispatch.Rejected
	rejPkt := testPacket("SP9XYZ>APRS,TCPIP*::SP9MOA-10:/alert pozar lasu 3")
	hub.Observe(rejPkt, "aprs-inet")
	waitFor(t, func() bool { return len(tx.sends()) == 3 })
	if got := tx.sends()[2][1]; got != "FAILED: alert rejected" {
		t.Fatalf("rejected confirmation = %q, want the explicit failure", got)
	}

	// A retransmission within the cooldown replays the failure WITHOUT
	// re-executing (no new acceptor call).
	hub.Observe(rejPkt, "aprs-radio")
	waitFor(t, func() bool { return len(tx.sends()) == 4 })
	if got := tx.sends()[3][1]; got != "FAILED: alert rejected" {
		t.Fatalf("in-cooldown retransmission = %q, want the replayed failure", got)
	}
	mu.Lock()
	n := len(accepted)
	mu.Unlock()
	if n != 3 {
		t.Fatalf("acceptor called %d times, want 3 (no re-execution within the cooldown)", n)
	}

	// After the cooldown the same packet executes again: the recovered
	// pipeline accepts it durably.
	time.Sleep(600 * time.Millisecond)
	accValue = dispatch.AcceptedDurable
	hub.Observe(rejPkt, "aprs-replay")
	waitFor(t, func() bool { return len(tx.sends()) == 5 })
	if got := tx.sends()[4][1]; got != "OK: alert raised" {
		t.Fatalf("post-recovery retry = %q, want the success confirmation", got)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(accepted) != 4 {
		t.Fatalf("acceptor called %d times, want 4 (the post-cooldown retry)", len(accepted))
	}
	if got := len(sink.payloads("events")); got != 0 {
		t.Fatalf("acceptor path leaked %d events through the generic sink", got)
	}
}

// TestHubBannerRateLimits pins the automatic-answer guards over APRS: a
// sender gets at most one banner per window and the whole answer path
// is globally paced, so answering bots cannot loop each other or hog
// the channel.
func TestHubBannerRateLimits(t *testing.T) {
	hub, _ := testHub(t, HubConfig{
		Enabled: true, Callsign: "SP9MOA-10", GridSquare: "JO90WW",
		RadiusKM: DefaultRadiusKM, StationTTL: 30 * time.Minute,
		RouteMessages: true,
	})
	// No worker started: the limiter is exercised directly against a
	// synthetic clock (single-threaded).
	base := time.Now()
	var offset time.Duration
	hub.now = func() time.Time { return base.Add(offset) }

	if !hub.bannerAllowed("SP9XYZ") {
		t.Fatal("first banner must be allowed")
	}
	if hub.bannerAllowed("SP9XYZ") {
		t.Fatal("second banner from the same sender within the window must be denied")
	}
	if hub.bannerAllowed("SP9QQQ") {
		t.Fatal("banner for another sender within the global window must be denied")
	}
	offset = 30 * time.Second // global pacing passed, per-sender window still active
	if !hub.bannerAllowed("SP9QQQ") {
		t.Fatal("banner for another sender after the global window must be allowed")
	}
	if hub.bannerAllowed("SP9XYZ") {
		t.Fatal("same sender must stay blocked within the per-sender window")
	}
	offset = 6 * time.Minute // both windows passed
	if !hub.bannerAllowed("SP9XYZ") {
		t.Fatal("sender must be answered again after the window")
	}
}

// TestRadioEventContentDeterministic pins the restart-proof dedup: the
// event content (lifecycle anchor from the durable registry resolver,
// content-derived ChangeID) is byte-identical for two copies of the same
// packet, so the store classifies a post-restart replay as a duplicate
// and never issues a second delivery.
func TestRadioEventContentDeterministic(t *testing.T) {
	hub, sink := testHub(t, HubConfig{
		Enabled: true, Callsign: "SP9MOA-10", GridSquare: "JO90WW",
		RadiusKM: DefaultRadiusKM, StationTTL: 30 * time.Minute,
		RouteMessages: true,
	})
	ctx, cancel := context.WithCancel(context.Background())
	hub.Start(ctx)
	defer cancel()
	hub.SetSenderGate(func(base string) bool { return base == "SP9XYZ" })
	cli := radiocli.New("WarnFlux v1.0 - SOSNA - sosna.sp9moa.pl")
	cli.RegisterRestricted("alert", "alert", func(args string) radiocli.Result {
		return radiocli.Result{Handled: true, Alert: &radiocli.AlertSpec{Headline: strings.TrimSpace(args), TTL: 4 * time.Hour}, Reply: "OK: alert raised"}
	})
	hub.SetCLI(cli)
	// The resolver is the durable-registry half: it hands back the
	// stored lifecycle anchor (here a fixed one), so a re-issued command
	// maps to byte-identical content.
	fixed := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	hub.SetEventTimesResolver(func(ctx context.Context, key string) (time.Time, time.Time, bool) {
		return fixed, fixed.Add(time.Hour), true
	})

	// /debug, then a post-restart replay (RAM cache cleared, another
	// backend) publishes byte-identical content.
	pkt := testPacket("SP9XYZ-7>APRS,WIDE1-1*::SP9MOA-10:/debug")
	hub.Observe(pkt, BackendRadio)
	waitFor(t, func() bool { return len(sink.payloads("events")) == 1 })
	hub.mu.Lock()
	hub.cmds = make(map[string]*cmdRecord) // simulated restart
	hub.mu.Unlock()
	hub.Observe(pkt, "aprs-other")
	waitFor(t, func() bool { return len(sink.payloads("events")) == 2 })
	pubs := sink.payloads("events")
	if !bytes.Equal(pubs[0], pubs[1]) {
		t.Fatalf("replayed /debug content differs:\n%q\n%q", pubs[0], pubs[1])
	}

	// Same for /alert with its 4h TTL. A fresh backend label keeps the
	// observation-level digest guard (which remembers receivers, not
	// digests) from swallowing the replay.
	apkt := testPacket("SP9XYZ-7>APRS,TCPIP*::SP9MOA-10:/alert pozar lasu")
	hub.Observe(apkt, BackendRadio)
	waitFor(t, func() bool { return len(sink.payloads("events")) == 3 })
	hub.mu.Lock()
	hub.cmds = make(map[string]*cmdRecord)
	hub.mu.Unlock()
	hub.Observe(apkt, "aprs-replay")
	waitFor(t, func() bool { return len(sink.payloads("events")) == 4 })
	pubs = sink.payloads("events")
	if !bytes.Equal(pubs[2], pubs[3]) {
		t.Fatalf("replayed /alert content differs:\n%q\n%q", pubs[2], pubs[3])
	}
}

// TestHubDebugConfirmationTracksAcceptance pins the SHARED confirmation
// path: /debug follows the local acceptance exactly like /alert —
// durable keeps the handler confirmation, the emergency fallback is
// reported as such, a rejection answers FAILED and admits controlled
// retries after the cooldown.
func TestHubDebugConfirmationTracksAcceptance(t *testing.T) {
	hub, sink := testHub(t, HubConfig{
		Enabled: true, Callsign: "SP9MOA-10", GridSquare: "JO90WW",
		RadiusKM: DefaultRadiusKM, StationTTL: 30 * time.Minute,
		RouteMessages: true, CmdRetryCooldown: 500 * time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())
	hub.Start(ctx)
	defer cancel()
	tx := &fakeTransmitter{name: "aprs-inet", ready: true}
	hub.AddTransmitter("aprs-inet", tx)
	hub.SetSenderGate(func(base string) bool { return base == "SP9XYZ" })
	hub.SetCLI(radiocli.New("WarnFlux v1.0 - SOSNA - sosna.sp9moa.pl"))

	var (
		mu       sync.Mutex
		accepted []dispatch.Acceptance
		accValue = dispatch.AcceptedDurable
	)
	hub.SetEventAcceptor(func([]byte) dispatch.Acceptance {
		mu.Lock()
		defer mu.Unlock()
		accepted = append(accepted, accValue)
		return accValue
	})

	// Durable acceptance: the plain handler confirmation. Distinct
	// senders keep each execution a distinct command.
	hub.Observe(testPacket("SP9XYZ-1>APRS,TCPIP*::SP9MOA-10:/debug"), "aprs-inet")
	waitFor(t, func() bool { return len(tx.sends()) == 1 })
	if got := tx.sends()[0][1]; got != "OK: debug alarm generated" {
		t.Fatalf("durable confirmation = %q, want the plain handler reply", got)
	}

	// Emergency acceptance: the fallback is reported explicitly.
	accValue = dispatch.AcceptedEmergency
	hub.Observe(testPacket("SP9XYZ-2>APRS,TCPIP*::SP9MOA-10:/debug"), "aprs-inet")
	waitFor(t, func() bool { return len(tx.sends()) == 2 })
	if got := tx.sends()[1][1]; !strings.Contains(got, "OK: debug alarm generated") || !strings.Contains(got, "failover") {
		t.Fatalf("emergency confirmation = %q, want the explicit failover note", got)
	}

	// Rejection: never claim success.
	accValue = dispatch.Rejected
	rejPkt := testPacket("SP9XYZ-3>APRS,TCPIP*::SP9MOA-10:/debug")
	hub.Observe(rejPkt, "aprs-inet")
	waitFor(t, func() bool { return len(tx.sends()) == 3 })
	if got := tx.sends()[2][1]; got != "FAILED: debug alarm rejected" {
		t.Fatalf("rejected confirmation = %q, want the explicit failure", got)
	}

	// In-cooldown retransmission: replayed failure, no re-execution.
	hub.Observe(rejPkt, "aprs-radio")
	waitFor(t, func() bool { return len(tx.sends()) == 4 })
	if got := tx.sends()[3][1]; got != "FAILED: debug alarm rejected" {
		t.Fatalf("in-cooldown retransmission = %q, want the replayed failure", got)
	}
	mu.Lock()
	n := len(accepted)
	mu.Unlock()
	if n != 3 {
		t.Fatalf("acceptor called %d times, want 3 (no re-execution within the cooldown)", n)
	}

	// After the cooldown the same packet executes again.
	time.Sleep(600 * time.Millisecond)
	accValue = dispatch.AcceptedDurable
	hub.Observe(rejPkt, "aprs-replay")
	waitFor(t, func() bool { return len(tx.sends()) == 5 })
	if got := tx.sends()[4][1]; got != "OK: debug alarm generated" {
		t.Fatalf("post-recovery retry = %q, want the success confirmation", got)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(accepted) != 4 {
		t.Fatalf("acceptor called %d times, want 4 (the post-cooldown retry)", len(accepted))
	}
	if got := len(sink.payloads("events")); got != 0 {
		t.Fatalf("acceptor path leaked %d events through the generic sink", got)
	}
}

// TestHubReplyBurstLimit pins the shared output limit: a flooding sender
// (ten distinct /unknown commands) gets at most ReplyBurst automatic
// replies per window — slash commands can no longer bypass the banner
// limits.
func TestHubReplyBurstLimit(t *testing.T) {
	hub, _ := testHub(t, HubConfig{
		Enabled: true, Callsign: "SP9MOA-10", GridSquare: "JO90WW",
		RadiusKM: DefaultRadiusKM, StationTTL: 30 * time.Minute,
		RouteMessages: true, ReplyBurst: 3,
	})
	ctx, cancel := context.WithCancel(context.Background())
	hub.Start(ctx)
	defer cancel()
	tx := &fakeTransmitter{name: "aprs-inet", ready: true}
	hub.AddTransmitter("aprs-inet", tx)
	hub.SetSenderGate(func(base string) bool { return base == "SP9XYZ" })
	hub.SetCLI(radiocli.New("WarnFlux v1.0 - SOSNA - sosna.sp9moa.pl"))

	for i := 0; i < 10; i++ {
		hub.Observe(testPacket(fmt.Sprintf("SP9XYZ>APRS,TCPIP*::SP9MOA-10:/unknown %d", i)), "aprs-inet")
	}
	waitFor(t, func() bool { return len(tx.sends()) == 3 })
	time.Sleep(150 * time.Millisecond) // settle: no further replies
	if got := len(tx.sends()); got != 3 {
		t.Fatalf("automatic replies after a 10-command flood = %d, want exactly ReplyBurst (3)", got)
	}
}

// TestHubAlertCommand pins the operator /alert command: an authorized
// sender raises a severe hazard with a ~4-hour expiry through the event
// bridge; a missing parameter answers with the usage; an unauthorized
// sender gets the denial.
func TestHubAlertCommand(t *testing.T) {
	hub, sink := testHub(t, HubConfig{
		Enabled: true, Callsign: "SP9MOA-10", GridSquare: "JO90WW",
		RadiusKM: DefaultRadiusKM, StationTTL: 30 * time.Minute,
		RouteMessages: true,
	})
	ctx, cancel := context.WithCancel(context.Background())
	hub.Start(ctx)
	defer cancel()
	tx := &fakeTransmitter{name: "aprs-inet", ready: true}
	hub.AddTransmitter("aprs-inet", tx)
	hub.SetSenderGate(func(base string) bool { return base == "SP9XYZ" })
	cli := radiocli.New("WarnFlux v1.0 - SOSNA - sosna.sp9moa.pl")
	cli.RegisterRestricted("alert", "alert", func(args string) radiocli.Result {
		headline := strings.TrimSpace(args)
		if headline == "" {
			return radiocli.Result{Handled: true, Reply: "Missing parameter: /alert <text>"}
		}
		return radiocli.Result{Handled: true, Alert: &radiocli.AlertSpec{Headline: headline, TTL: 4 * time.Hour}, Reply: "OK: alert raised"}
	})
	hub.SetCLI(cli)

	// /alert with text: severe event + confirmation reply.
	hub.Observe(testPacket("SP9XYZ>APRS,TCPIP*::SP9MOA-10:/alert pozar lasu"), "aprs-inet")
	waitFor(t, func() bool { return len(sink.payloads("events")) >= 1 })
	waitFor(t, func() bool { return len(tx.sends()) == 1 })
	var we MessageEventWire
	if err := json.Unmarshal(sink.payloads("events")[0], &we); err != nil {
		t.Fatalf("alert payload: %v", err)
	}
	if we.Event.Source != "aprs" || we.Event.Severity != "severe" {
		t.Fatalf("alert event = %+v, want severe from aprs", we.Event)
	}
	if we.Event.Headline != "pozar lasu" {
		t.Fatalf("headline = %q, want the operator text", we.Event.Headline)
	}
	if we.Event.ExpiresAt == nil {
		t.Fatal("alert event has no expiry")
	}
	exp, err := time.Parse(time.RFC3339, *we.Event.ExpiresAt)
	if err != nil {
		t.Fatalf("expiry = %q: %v", *we.Event.ExpiresAt, err)
	}
	if d := exp.Sub(time.Now()); d < 3*time.Hour+45*time.Minute || d > 4*time.Hour+15*time.Minute {
		t.Fatalf("expiry in %s, want ~4h", d)
	}
	sends := tx.sends()
	if !strings.Contains(sends[0][1], "OK: alert raised") {
		t.Fatalf("alert reply = %v, want the confirmation", sends[0])
	}

	// /alert without text: usage reply, no new event.
	hub.Observe(testPacket("SP9XYZ>APRS,TCPIP*::SP9MOA-10:/alert"), "aprs-inet")
	waitFor(t, func() bool { return len(tx.sends()) == 2 })
	sends = tx.sends()
	if !strings.Contains(sends[1][1], "Missing parameter") {
		t.Fatalf("no-arg /alert reply = %v, want the usage", sends[1])
	}
	if got := len(sink.payloads("events")); got != 1 {
		t.Fatalf("events after no-arg /alert = %d, want still 1", got)
	}

	// Unauthorized sender: denial, no event.
	hub.Observe(testPacket("SP9ZZZ>APRS,TCPIP*::SP9MOA-10:/alert x"), "aprs-inet")
	waitFor(t, func() bool { return len(tx.sends()) == 3 })
	sends = tx.sends()
	if !strings.Contains(sends[2][1], "You are not authorized") {
		t.Fatalf("unauthorized /alert reply = %v, want the denial", sends[2])
	}
	if got := len(sink.payloads("events")); got != 1 {
		t.Fatalf("events after unauthorized /alert = %d, want still 1", got)
	}
}

// TestHubReplyFitsChannelLimit pins the shared fitting at the gateway:
// a very long installation identity shortens progressively instead of
// overflowing the 67-character APRS message limit.
func TestHubReplyFitsChannelLimit(t *testing.T) {
	hub, _ := testHub(t, HubConfig{
		Enabled: true, Callsign: "SP9MOA-10", GridSquare: "JO90WW",
		RadiusKM: DefaultRadiusKM, StationTTL: 30 * time.Minute,
		RouteMessages: true,
	})
	ctx, cancel := context.WithCancel(context.Background())
	hub.Start(ctx)
	defer cancel()
	tx := &fakeTransmitter{name: "aprs-inet", ready: true}
	hub.AddTransmitter("aprs-inet", tx)
	hub.SetSenderGate(func(base string) bool { return base == "SP9XYZ" })
	hub.SetCLI(radiocli.New("WarnFlux v1.0 - SOSNA - a.very.long.domain.example.org"))

	hub.Observe(testPacket("SP9XYZ>APRS,TCPIP*::SP9MOA-10:hello"), "aprs-inet")
	waitFor(t, func() bool { return len(tx.sends()) == 1 })
	sends := tx.sends()
	if got := sends[0][1]; len([]rune(got)) > MaxMessageText {
		t.Fatalf("reply = %q, %d runes — over the APRS limit", got, len([]rune(got)))
	}
	if got := sends[0][1]; got != "WarnFlux v1.0 - SOSNA | type /help for help" {
		t.Fatalf("reply = %q, want the domain dropped", got)
	}
}

// TestBulletinExpiry pins the bounded retention: after the bulletin TTL
// the topic is deleted with an empty retained payload.
func TestBulletinExpiry(t *testing.T) {
	hub, sink := testHub(t, HubConfig{
		Enabled: true, Callsign: "SP9MOA-10", GridSquare: "JO90WW",
		RadiusKM: DefaultRadiusKM, StationTTL: 30 * time.Minute,
		BulletinTTL: time.Minute,
	})
	hub.tick = 20 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	hub.Start(ctx)
	defer cancel()

	hub.Observe(ParseFeedLine("SP9XYZ-7>APRS,WIDE1-1*::BLN0     :ops bulletin", time.Now()), BackendRadio)
	var topic string
	waitFor(t, func() bool {
		topic, _ = sink.byPrefix(BulletinsTopicPrefix)
		return topic != ""
	})

	hub.mu.Lock()
	for id := range hub.bulletins {
		hub.bulletins[id] = time.Now().Add(-2 * time.Minute).Unix()
	}
	hub.mu.Unlock()

	waitFor(t, func() bool {
		_, pubs := sink.byPrefix(BulletinsTopicPrefix)
		return len(pubs) == 2 && pubs[1] == nil
	})
}
