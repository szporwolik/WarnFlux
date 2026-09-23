package mqttreceiver

import "testing"

// TestTrafficBufferRing pins the bounded ring behavior: entries beyond the
// cap drop the oldest first and sequences keep counting up.
func TestTrafficBufferRing(t *testing.T) {
	b := NewTrafficBuffer(3)
	if b.Max() != 3 {
		t.Fatalf("Max() = %d, want 3", b.Max())
	}
	for i := 0; i < 5; i++ {
		b.Add("r1", "generic", "some/topic", 1, false, i)
	}
	got := b.Snapshot(0)
	if len(got) != 3 {
		t.Fatalf("len = %d, want 3", len(got))
	}
	if got[0].Seq != 3 || got[2].Seq != 5 {
		t.Fatalf("seqs = %d..%d, want 3..5", got[0].Seq, got[2].Seq)
	}
}

// TestTrafficBufferSnapshotCursor pins the incremental cursor semantics.
func TestTrafficBufferSnapshotCursor(t *testing.T) {
	b := NewTrafficBuffer(10)
	b.Add("r1", "events", "a", 0, false, 1)
	b.Add("r1", "active", "b", 1, true, 2)
	b.Add("r2", "info", "c", 2, false, 3)

	all := b.Snapshot(0)
	if len(all) != 3 {
		t.Fatalf("len = %d, want 3", len(all))
	}
	if e := all[1]; e.Kind != "active" || e.Topic != "b" || !e.Retained || e.QoS != 1 || e.Size != 2 || e.Receiver != "r1" || e.At == "" {
		t.Errorf("entry fields = %+v", e)
	}
	after := all[1].Seq
	rest := b.Snapshot(after)
	if len(rest) != 1 || rest[0].Topic != "c" {
		t.Fatalf("Snapshot(after %d) = %+v, want only topic c", after, rest)
	}
}

// TestTrafficBufferNilSafe: a nil buffer never panics.
func TestTrafficBufferNilSafe(t *testing.T) {
	var b *TrafficBuffer
	b.Add("r", "events", "t", 0, false, 1)
	if b.Snapshot(0) != nil {
		t.Fatal("nil buffer Snapshot must be nil")
	}
	if b.Max() != 0 {
		t.Fatalf("nil buffer Max = %d, want 0", b.Max())
	}
}

// TestIngestorTrafficRecording pins the HandleMessage hook: every inbound
// frame lands in the buffer, even oversized ones that are rejected, and
// WarnFlux protocol topics are classified by kind.
func TestIngestorTrafficRecording(t *testing.T) {
	env := newIngestorEnv(t, "recv-a", true, nil)
	env.ingestor.traffic = NewTrafficBuffer(10)

	env.ingestor.HandleMessage(nil, &testMessage{topic: "warnflux/events", payload: []byte("{}")})
	env.ingestor.HandleMessage(nil, &testMessage{topic: "warnflux/active/imgw/" + TopicHash("k"), payload: []byte("{}")})
	env.ingestor.HandleMessage(nil, &testMessage{topic: "warnflux/info/src/prod/key/kind", payload: []byte("{}")})
	env.ingestor.HandleMessage(nil, &testMessage{topic: "warnflux/status", payload: []byte("{}")})
	env.ingestor.HandleMessage(nil, &testMessage{topic: "other/bus", payload: []byte("{}")})
	env.ingestor.HandleMessage(nil, &testMessage{topic: "huge", payload: make([]byte, MaxPayload+1)})

	got := env.ingestor.traffic.Snapshot(0)
	if len(got) != 6 {
		t.Fatalf("entries = %d, want 6 (oversized frame counts as traffic)", len(got))
	}
	kinds := map[string]bool{}
	for _, e := range got {
		kinds[e.Kind] = true
		if e.Receiver != "recv-a" {
			t.Errorf("receiver = %q", e.Receiver)
		}
	}
	if !kinds["events"] || !kinds["active"] || !kinds["info"] || !kinds["status"] || !kinds["generic"] {
		t.Errorf("kinds = %v, want events+active+info+status+generic", kinds)
	}
	if got[5].Size != MaxPayload+1 {
		t.Errorf("oversized size = %d", got[5].Size)
	}
}
