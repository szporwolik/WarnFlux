package aprs

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"
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

	waitFor(t, func() bool {
		docs := hub.Stations()
		return len(docs) == 1 && docs[0].Position != nil
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
