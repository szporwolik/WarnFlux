package state

import (
	"encoding/json"
	"fmt"
	"runtime"
	"testing"
	"time"
)

// readablePayload builds an info entry of the given kind whose payload is n
// bytes of JSON string. The returned slice is the payload the mirror stores, so
// a test can tell a shared payload from a copy.
func readablePayload(kind, key string, n int) (InfoEntry, json.RawMessage) {
	buf := make([]byte, n)
	for i := range buf {
		buf[i] = 'x'
	}
	payload := json.RawMessage(`"` + string(buf) + `"`)
	return InfoEntry{
		Source:     "adsb",
		ProducerID: "area",
		Key:        key,
		Kind:       kind,
		ReceivedAt: time.Now(),
		Payload:    payload,
	}, payload
}

// allocatedBytes reports how many bytes f allocated in total.
func allocatedBytes(f func()) uint64 {
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	f()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

// TestInfoKindScopesAndSharesPayloads is the regression test for public layers
// that copied the whole mirror. A layer asking for air quality must get only
// air-quality entries, and the payload buffers must be shared with the mirror
// instead of copied per request: the mirrored payloads are immutable once
// stored, so copying them only burned memory and time.
func TestInfoKindScopesAndSharesPayloads(t *testing.T) {
	s := New()
	const big = 64 << 10

	for i := 0; i < 4; i++ {
		e, _ := readablePayload("air_quality", fmt.Sprintf("st-%d", i), 256)
		if err := s.AddOrUpdateInfo("local", fmt.Sprintf("warnflux/info/giosaq/producer-%d", i), e); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 40; i++ {
		e, _ := readablePayload("aircraft", fmt.Sprintf("area-%d", i), big)
		if err := s.AddOrUpdateInfo("local", fmt.Sprintf("warnflux/info/adsb/area-%d", i), e); err != nil {
			t.Fatal(err)
		}
	}

	aq := s.InfoKind("air_quality")
	if len(aq) != 4 {
		t.Fatalf("InfoKind(air_quality) = %d entries, want 4", len(aq))
	}
	for _, e := range aq {
		if e.Kind != "air_quality" {
			t.Fatalf("InfoKind(air_quality) leaked a %q entry", e.Kind)
		}
	}
	// Shared, not copied: two reads must hand out the very same buffer. A
	// per-request copy is what turned every visitor into megabytes of
	// allocations.
	first := map[string]*byte{}
	for _, e := range aq {
		first[e.Topic] = &e.Payload[0]
	}
	for _, e := range s.InfoKind("air_quality") {
		if first[e.Topic] != &e.Payload[0] {
			t.Fatalf("payload of %q was copied per read instead of shared with the mirror", e.Key)
		}
	}

	aircraft := s.InfoKind("aircraft")
	if len(aircraft) != 40 {
		t.Fatalf("InfoKind(aircraft) = %d entries, want 40", len(aircraft))
	}
	again := s.InfoKind("aircraft")
	if &again[0].Payload[0] != &aircraft[0].Payload[0] {
		t.Fatal("aircraft payload was copied per read instead of shared with the mirror")
	}

	if all := s.Info(); len(all) != 44 {
		t.Fatalf("Info() = %d entries, want 44", len(all))
	}
}

// TestPublicLayerDoesNotCopyForeignPayloads measures the work a public layer
// does. Before the per-kind accessors, every layer took a Snapshot: that built
// (and deep-copied) all three collections, including payloads the layer never
// looked at. With ~2.5 MiB of foreign aircraft payloads mirrored, one
// air-quality layer call must stay far below that.
func TestPublicLayerDoesNotCopyForeignPayloads(t *testing.T) {
	s := New()
	const big = 64 << 10
	for i := 0; i < 40; i++ {
		e, _ := readablePayload("aircraft", fmt.Sprintf("area-%d", i), big)
		if err := s.AddOrUpdateInfo("local", fmt.Sprintf("warnflux/info/adsb/area-%d", i), e); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 4; i++ {
		e, _ := readablePayload("air_quality", fmt.Sprintf("st-%d", i), 256)
		if err := s.AddOrUpdateInfo("local", fmt.Sprintf("warnflux/info/giosaq/producer-%d", i), e); err != nil {
			t.Fatal(err)
		}
	}

	got := allocatedBytes(func() { _ = s.InfoKind("air_quality") })
	if got > 256<<10 {
		t.Fatalf("InfoKind(air_quality) allocated %d bytes with %d MiB of foreign payloads mirrored; a scoped layer must not copy them",
			got, (40*big)>>20)
	}
	// The full snapshot stays cheap as well: it shares payloads instead of
	// deep-copying them.
	gotSnap := allocatedBytes(func() { _ = s.Snapshot() })
	if gotSnap > 1<<20 {
		t.Fatalf("Snapshot allocated %d bytes for %d MiB of payloads; payloads must be shared, not copied",
			gotSnap, (40*big)>>20)
	}
}

// TestHazardsAccessorPrunesAndSorts documents the dedicated hazard view: it
// prunes expired entries like Snapshot does, returns them most severe first and
// keeps the byte accounting honest after the prune.
func TestHazardsAccessorPrunesAndSorts(t *testing.T) {
	s := New()
	past := time.Now().Add(-time.Minute)
	now := time.Now()

	add := func(key, severity string, expires *time.Time) {
		t.Helper()
		if err := s.AddOrUpdateActive("r1", "warnflux/active/x/"+key, Hazard{
			EventKey:   key,
			Status:     "active",
			Severity:   severity,
			Headline:   "headline " + key,
			ReceivedAt: now,
			ExpiresAt:  expires,
		}); err != nil {
			t.Fatal(err)
		}
	}
	add("expired", "severe", &past)
	add("minor", "minor", nil)
	add("severe", "severe", nil)

	before := s.MirrorCounters().ActiveBytes
	if before <= 0 {
		t.Fatalf("ActiveBytes = %d, want the mirrored hazards accounted", before)
	}
	hazards := s.Hazards()
	if len(hazards) != 2 {
		t.Fatalf("Hazards() = %d entries, want 2 (expired pruned)", len(hazards))
	}
	if hazards[0].Severity != "severe" || hazards[1].Severity != "minor" {
		t.Fatalf("Hazards() order = %q, %q; want severe first", hazards[0].Severity, hazards[1].Severity)
	}
	for _, h := range hazards {
		if h.EventKey == "expired" {
			t.Fatal("expired hazard survived Hazards()")
		}
	}
	after := s.MirrorCounters().ActiveBytes
	if after >= before {
		t.Fatalf("ActiveBytes after prune = %d, want less than %d (pruned bytes must be released)", after, before)
	}
}

// TestStoreDetachesProducerBuffers documents that the mirror owns its payloads:
// reads share the stored buffers, so a producer that reuses or rewrites its own
// buffer after a message was accepted must not change what the panel shows.
func TestStoreDetachesProducerBuffers(t *testing.T) {
	s := New()
	payload := json.RawMessage(`"first"`)
	if err := s.AddOrUpdateInfo("local", "warnflux/info/adsb/area", InfoEntry{
		Source: "adsb", Key: "area", Kind: "aircraft", Payload: payload,
	}); err != nil {
		t.Fatal(err)
	}
	areas := []string{"Kraków"}
	if err := s.AddOrUpdateActive("local", "warnflux/active/x/1", Hazard{
		EventKey: "x:1", Status: "active", Areas: areas,
	}); err != nil {
		t.Fatal(err)
	}

	copy(payload[1:], "second")
	areas[0] = "Warszawa"

	info := s.InfoKind("aircraft")
	if len(info) != 1 || string(info[0].Payload) != `"first"` {
		t.Fatalf("stored payload = %s, want %q (the mirror must own its copy)", info[0].Payload, `"first"`)
	}
	hazards := s.Hazards()
	if len(hazards) != 1 || len(hazards[0].Areas) != 1 || hazards[0].Areas[0] != "Kraków" {
		t.Fatalf("stored areas = %v, want [Kraków] (the mirror must own its copy)", hazards[0].Areas)
	}
}

// TestVersionTracksAcceptedMutations pins the freshness token public layers key
// their caches on: every accepted change bumps it, a rejected one does not.
func TestVersionTracksAcceptedMutations(t *testing.T) {
	s := New()
	start := s.Version()

	h := Hazard{EventKey: "x:1", Status: "active"}
	if err := s.AddOrUpdateActive("r1", "t/1", h); err != nil {
		t.Fatal(err)
	}
	if got := s.Version(); got == start {
		t.Fatal("Version did not change after an accepted hazard")
	}
	afterHazard := s.Version()

	if err := s.AddOrUpdateInfo("r1", "i/1", InfoEntry{Key: "k", Kind: "aircraft"}); err != nil {
		t.Fatal(err)
	}
	if got := s.Version(); got == afterHazard {
		t.Fatal("Version did not change after an accepted info entry")
	}
	afterInfo := s.Version()

	s.DeleteActive("r1", "t/1")
	if got := s.Version(); got == afterInfo {
		t.Fatal("Version did not change after a delete")
	}
	afterDelete := s.Version()

	// A rejected entry (over the byte budget) must not look like a change.
	s.mu.Lock()
	s.infoBytes = MaxInfoBytes
	s.mu.Unlock()
	if err := s.AddOrUpdateInfo("r1", "i/big", InfoEntry{Key: "big", Kind: "aircraft", Payload: make([]byte, 4096)}); err == nil {
		t.Fatal("oversized info entry was accepted")
	}
	if got := s.Version(); got != afterDelete {
		t.Fatalf("Version changed after a rejected entry: %d -> %d", afterDelete, got)
	}
}
