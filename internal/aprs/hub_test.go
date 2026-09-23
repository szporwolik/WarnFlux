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

func testPacket(line string) Packet {
	return ParseFeedLine(line, time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC))
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
	hub.Start(ctx)
	defer cancel()

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
