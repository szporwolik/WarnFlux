package sqlite

import (
	"context"
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/storage"
)

// TestMeshtasticNodesPersist pins the heard-node directory round trip:
// save, load, restart persistence and the sends slice.
func TestMeshtasticNodesPersist(t *testing.T) {
	s := openTemp(t)
	ctx := context.Background()
	seen := time.Now().Truncate(time.Millisecond)

	nodes := []storage.MeshtasticNode{
		{ID: "a0a85934", Name: "Meshtastic 5934", Short: "SPM", Lat: 50.02, Lon: 20.0, LastSeen: seen, Sends: []string{"telemetry", "text"}},
		{ID: "b0b85934", Name: "Other", LastSeen: seen.Add(-time.Hour)},
	}
	if err := s.SaveMeshtasticNodes(ctx, nodes); err != nil {
		t.Fatalf("save: %v", err)
	}
	got, err := s.LoadMeshtasticNodes(ctx)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(got) != 2 || got[0].ID != "a0a85934" || got[1].ID != "b0b85934" {
		t.Fatalf("nodes = %+v, want both ids", got)
	}
	if got[0].Name != "Meshtastic 5934" || got[0].Short != "SPM" || got[0].Lat != 50.02 {
		t.Fatalf("first node = %+v", got[0])
	}
	if len(got[0].Sends) != 2 || got[0].Sends[1] != "text" {
		t.Fatalf("sends = %v, want [telemetry text]", got[0].Sends)
	}
	if !got[0].LastSeen.Equal(seen) {
		t.Fatalf("last seen = %v, want %v", got[0].LastSeen, seen)
	}

	// Replace semantics: saving an empty list clears the directory.
	if err := s.SaveMeshtasticNodes(ctx, nil); err != nil {
		t.Fatalf("clear: %v", err)
	}
	got, err = s.LoadMeshtasticNodes(ctx)
	if err != nil || len(got) != 0 {
		t.Fatalf("after clear = %+v, %v", got, err)
	}
}

// TestMeshtasticMessageStatus pins the delivery lifecycle: a tx row starts
// empty and the hub stamps sent/delivered/failed onto it, matching only the
// exact (timestamp, text) pair.
func TestMeshtasticMessageStatus(t *testing.T) {
	s := openTemp(t)
	ctx := context.Background()
	at := time.Now().Truncate(time.Millisecond)

	if err := s.RecordMeshtasticMessage(ctx, "tx", "", "", "dm", "hello", "admin", 0, at); err != nil {
		t.Fatalf("record: %v", err)
	}
	rows, err := s.ListMeshtasticMessages(ctx, storage.MeshtasticMessageFilter{Direction: "tx"}, 10, 0)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(rows) != 1 || rows[0].Status != "" {
		t.Fatalf("row = %+v, want status empty", rows)
	}

	if err := s.UpdateMeshtasticMessageStatus(ctx, "sent", at, "hello"); err != nil {
		t.Fatalf("sent: %v", err)
	}
	if err := s.UpdateMeshtasticMessageStatus(ctx, "delivered", at, "hello"); err != nil {
		t.Fatalf("delivered: %v", err)
	}
	rows, err = s.ListMeshtasticMessages(ctx, storage.MeshtasticMessageFilter{Direction: "tx"}, 10, 0)
	if err != nil || len(rows) != 1 {
		t.Fatalf("list: %v rows=%v", err, rows)
	}
	if rows[0].Status != "delivered" {
		t.Fatalf("status = %q, want delivered", rows[0].Status)
	}

	// A second row at the same instant with different text stays untouched.
	if err := s.RecordMeshtasticMessage(ctx, "tx", "", "", "dm", "other", "admin", 0, at); err != nil {
		t.Fatalf("record: %v", err)
	}
	if err := s.UpdateMeshtasticMessageStatus(ctx, "failed", at, "hello"); err != nil {
		t.Fatalf("failed: %v", err)
	}
	rows, err = s.ListMeshtasticMessages(ctx, storage.MeshtasticMessageFilter{Direction: "tx"}, 10, 0)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	for _, r := range rows {
		switch r.Text {
		case "other":
			if r.Status != "" {
				t.Fatalf("other row status = %q, want empty", r.Status)
			}
		case "hello":
			if r.Status != "failed" {
				t.Fatalf("hello row status = %q, want failed", r.Status)
			}
		}
	}
}

// TestMeshtasticMessageChannelFilter pins the channel narrowing of the
// history: exact-label match (the ch0 tab), inverted exclusion (every
// other tab hides ch0) and the combination with the direction filter.
func TestMeshtasticMessageChannelFilter(t *testing.T) {
	s := openTemp(t)
	ctx := context.Background()
	at := time.Now().Truncate(time.Millisecond)

	rows := []storage.MeshMessage{
		{Direction: "rx", Channel: "ch0", Text: "rx-ch0", At: at},
		{Direction: "tx", Channel: "ch0", Text: "tx-ch0", At: at},
		{Direction: "rx", Channel: "SP9MOA", Text: "rx-sp9", At: at},
		{Direction: "rx", Channel: "dm", Text: "rx-dm", At: at},
	}
	for _, m := range rows {
		if err := s.RecordMeshtasticMessage(ctx, m.Direction, m.Sender, m.Recipient, m.Channel, m.Text, m.Operator, m.Hops, m.At); err != nil {
			t.Fatalf("record %s: %v", m.Text, err)
		}
	}

	// The ch0 tab shows both directions of the primary channel only.
	got, err := s.ListMeshtasticMessages(ctx, storage.MeshtasticMessageFilter{Channel: "ch0"}, 10, 0)
	if err != nil || len(got) != 2 {
		t.Fatalf("ch0 filter = %d rows, %v", len(got), err)
	}
	for _, m := range got {
		if m.Channel != "ch0" {
			t.Fatalf("ch0 filter leaked %q (%s)", m.Channel, m.Text)
		}
	}

	// Every other tab excludes ch0.
	got, err = s.ListMeshtasticMessages(ctx, storage.MeshtasticMessageFilter{Channel: "ch0", Exclude: true}, 10, 0)
	if err != nil || len(got) != 2 {
		t.Fatalf("exclude filter = %d rows, %v", len(got), err)
	}
	for _, m := range got {
		if m.Channel == "ch0" {
			t.Fatalf("exclude filter leaked %s", m.Text)
		}
	}

	// Direction + exclusion combine: rx without ch0 leaves the SP9MOA
	// and dm rows (both are rx rows on other channels).
	got, err = s.ListMeshtasticMessages(ctx, storage.MeshtasticMessageFilter{Direction: "rx", Channel: "ch0", Exclude: true}, 10, 0)
	if err != nil || len(got) != 2 {
		t.Fatalf("rx-exclude filter = %+v, %v", got, err)
	}
	seen := map[string]bool{}
	for _, m := range got {
		if m.Channel == "ch0" {
			t.Fatalf("rx-exclude filter leaked %s", m.Text)
		}
		seen[m.Text] = true
	}
	if !seen["rx-sp9"] || !seen["rx-dm"] {
		t.Fatalf("rx-exclude filter = %+v, want rx-sp9 and rx-dm", got)
	}

	// Counts mirror the same filters.
	if n, err := s.CountMeshtasticMessages(ctx, storage.MeshtasticMessageFilter{Channel: "ch0"}); err != nil || n != 2 {
		t.Fatalf("ch0 count = %d, %v", n, err)
	}
	if n, err := s.CountMeshtasticMessages(ctx, storage.MeshtasticMessageFilter{Channel: "ch0", Exclude: true}); err != nil || n != 2 {
		t.Fatalf("exclude count = %d, %v", n, err)
	}
	if n, err := s.CountMeshtasticMessages(ctx, storage.MeshtasticMessageFilter{Direction: "tx"}); err != nil || n != 1 {
		t.Fatalf("tx count = %d, %v", n, err)
	}
}

// TestMeshActionFailures pins the durable failure marker that ties the
// async modem result to the delivery JOB: a failure marks the job, the
// job-scoped query reports it, a successful retransmission clears it,
// and old rows age out.
func TestMeshActionFailures(t *testing.T) {
	s := openTemp(t)
	ctx := context.Background()

	if failed, err := s.MeshActionFailed(ctx, "mesh", 1, "d1", "p1", "k", 1); err != nil || failed {
		t.Fatalf("empty = (%v, %v), want false", failed, err)
	}
	if err := s.SetMeshActionFailed(ctx, "mesh", 1, "d1", "p1", "k", 1, "a0a85934", 0, true); err != nil {
		t.Fatal(err)
	}
	// The query is job-scoped: the same version of another GROUP or
	// another job of the same group never matches — only the concrete
	// job the failure belongs to (reported P2).
	if failed, err := s.MeshActionFailed(ctx, "mesh", 1, "d1", "p1", "k", 1); err != nil || !failed {
		t.Fatalf("marked = (%v, %v), want true", failed, err)
	}
	if failed, err := s.MeshActionFailed(ctx, "mesh", 2, "d2", "p1", "k", 1); err != nil || failed {
		t.Fatalf("other group = (%v, %v), want false", failed, err)
	}
	if failed, err := s.MeshActionFailed(ctx, "mesh", 1, "d2", "p1", "k", 1); err != nil || failed {
		t.Fatalf("other job = (%v, %v), want false", failed, err)
	}
	if failed, err := s.MeshActionFailed(ctx, "smtp", 1, "d1", "p1", "k", 1); err != nil || failed {
		t.Fatalf("other action = (%v, %v), want false", failed, err)
	}
	// Another version is independent.
	if failed, err := s.MeshActionFailed(ctx, "mesh", 1, "d1", "p1", "k", 2); err != nil || failed {
		t.Fatalf("other version = (%v, %v), want false", failed, err)
	}

	// A successful retransmission clears its own marker.
	if err := s.SetMeshActionFailed(ctx, "mesh", 1, "d1", "p1", "k", 1, "a0a85934", 0, false); err != nil {
		t.Fatal(err)
	}
	if failed, err := s.MeshActionFailed(ctx, "mesh", 1, "d1", "p1", "k", 1); err != nil || failed {
		t.Fatalf("cleared = (%v, %v), want false", failed, err)
	}

	// Failure rows age out on the retention bound.
	old := time.Now().Add(-40 * 24 * time.Hour)
	if _, err := s.db.Exec(`INSERT INTO mesh_action_failures (action_id, group_id, dedup_key, publisher, event_key, change_id, recipient, channel, created_at_ms)
		VALUES ('mesh', 1, 'd1', 'p1', 'old', 1, 'a0a85934', 0, ?)`, old.UnixMilli()); err != nil {
		t.Fatal(err)
	}
	// A fresh mark prunes the stale row.
	if err := s.SetMeshActionFailed(ctx, "mesh", 1, "d1", "p1", "new", 1, "a0a85934", 0, true); err != nil {
		t.Fatal(err)
	}
	var stale int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM mesh_action_failures WHERE event_key = 'old'`).Scan(&stale); err != nil || stale != 0 {
		t.Fatalf("stale failure rows = (%d, %v), want 0 (pruned)", stale, err)
	}
}

// TestMeshActionProgress pins the versioned resume ledger of the
// meshtastic action: an entry counts only for the exact (publisher,
// event key, change id, recipient, channel) identity, records are
// idempotent and old rows are pruned.
func TestMeshActionProgress(t *testing.T) {
	s := openTemp(t)
	ctx := context.Background()
	at := time.Now().Truncate(time.Millisecond)

	if done, err := s.MeshActionProgressDone(ctx, "p1", "k", 1, "a0a85934", 0); err != nil || done {
		t.Fatalf("empty ledger = (%v, %v), want false", done, err)
	}
	if err := s.RecordMeshActionProgress(ctx, "p1", "k", 1, "a0a85934", 0, at); err != nil {
		t.Fatal(err)
	}
	if done, err := s.MeshActionProgressDone(ctx, "p1", "k", 1, "a0a85934", 0); err != nil || !done {
		t.Fatalf("recorded entry = (%v, %v), want true", done, err)
	}

	// A new VERSION of the same event and recipient is independent —
	// the whole point of the ledger (a new alarm with the same text
	// must never be suppressed).
	if done, err := s.MeshActionProgressDone(ctx, "p1", "k", 2, "a0a85934", 0); err != nil || done {
		t.Fatalf("new version = (%v, %v), want false", done, err)
	}
	// Same version, different channel: independent.
	if done, err := s.MeshActionProgressDone(ctx, "p1", "k", 1, "a0a85934", 2); err != nil || done {
		t.Fatalf("different channel = (%v, %v), want false", done, err)
	}

	// The group broadcast has its own entry (empty recipient + channel).
	if err := s.RecordMeshActionProgress(ctx, "p1", "k", 1, "", 1, at); err != nil {
		t.Fatal(err)
	}
	if done, err := s.MeshActionProgressDone(ctx, "p1", "k", 1, "", 1); err != nil || !done {
		t.Fatalf("broadcast entry = (%v, %v), want true", done, err)
	}

	// Revocation: a failed transmission deletes the entry (the retry
	// sends the recipient again). Idempotent.
	if err := s.DeleteMeshActionProgress(ctx, "p1", "k", 1, "", 1); err != nil {
		t.Fatal(err)
	}
	if done, err := s.MeshActionProgressDone(ctx, "p1", "k", 1, "", 1); err != nil || done {
		t.Fatalf("revoked entry = (%v, %v), want false", done, err)
	}
	if err := s.DeleteMeshActionProgress(ctx, "p1", "k", 1, "", 1); err != nil {
		t.Fatalf("second revoke: %v", err)
	}

	// Idempotent: re-recording never duplicates.
	if err := s.RecordMeshActionProgress(ctx, "p1", "k", 1, "a0a85934", 0, at); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM mesh_action_progress`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("rows after re-record = (%d, %v), want 1 (broadcast entry revoked earlier)", n, err)
	}

	// Retention: rows older than the bound are pruned on every record.
	old := time.Now().Add(-40 * 24 * time.Hour)
	if err := s.RecordMeshActionProgress(ctx, "p1", "k2", 1, "a0a85934", 0, old); err != nil {
		t.Fatal(err)
	}
	var oldN int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM mesh_action_progress WHERE event_key = 'k2'`).Scan(&oldN); err != nil || oldN != 0 {
		t.Fatalf("old rows retained = (%d, %v), want 0 (pruned)", oldN, err)
	}
}

// TestMeshtasticMessageTextFilter pins the exact-text narrowing used by
// the meshtastic action as its durable delivery-progress ledger: only
// rows with the exact message text match, combined freely with the
// direction, channel and peer conditions.
func TestMeshtasticMessageTextFilter(t *testing.T) {
	s := openTemp(t)
	ctx := context.Background()
	at := time.Now().Truncate(time.Millisecond)

	for i, text := range []string{"alarm-a", "alarm-b", "alarm-a"} {
		if err := s.RecordMeshtasticMessage(ctx, "tx", "", "a0a85934", "dm",
			text, "system", 0, at.Add(time.Duration(i)*time.Second)); err != nil {
			t.Fatalf("record %s: %v", text, err)
		}
	}

	got, err := s.ListMeshtasticMessages(ctx, storage.MeshtasticMessageFilter{Direction: "tx", Text: "alarm-a"}, 10, 0)
	if err != nil {
		t.Fatalf("text filter: %v", err)
	}
	if len(got) != 2 || got[0].Text != "alarm-a" || got[1].Text != "alarm-a" {
		t.Fatalf("text-filtered rows = %+v, want the two alarm-a rows newest first", got)
	}

	// Combined with the peer condition.
	got, err = s.ListMeshtasticMessages(ctx, storage.MeshtasticMessageFilter{
		Direction: "tx", Peer: "a0a85934", Text: "alarm-b",
	}, 10, 0)
	if err != nil || len(got) != 1 || got[0].Text != "alarm-b" {
		t.Fatalf("peer+text rows = %+v, %v; want exactly the alarm-b row", got, err)
	}

	if n, err := s.CountMeshtasticMessages(ctx, storage.MeshtasticMessageFilter{Text: "alarm-a"}); err != nil || n != 2 {
		t.Fatalf("count = (%d, %v), want 2", n, err)
	}
}

// TestMeshtasticMessagePeerFilter pins the DM conversation filter: one
// peer's rx rows plus the tx rows addressed to them, combined with the
// dm-channel base and the direction narrowing.
func TestMeshtasticMessagePeerFilter(t *testing.T) {
	s := openTemp(t)
	ctx := context.Background()
	at := time.Now().Truncate(time.Millisecond)

	rows := []storage.MeshMessage{
		{Direction: "rx", Sender: "a0a85934", Channel: "dm", Text: "from-a", At: at},
		{Direction: "tx", Recipient: "a0a85934", Channel: "dm", Text: "to-a", At: at},
		{Direction: "rx", Sender: "deadbeef", Channel: "dm", Text: "from-b", At: at},
		{Direction: "tx", Recipient: "deadbeef", Channel: "dm", Text: "to-b", At: at},
		{Direction: "tx", Channel: "dm", Text: "legacy-no-recipient", At: at},
		{Direction: "rx", Sender: "a0a85934", Channel: "SP9MOA", Text: "channel-row", At: at},
	}
	for _, m := range rows {
		if err := s.RecordMeshtasticMessage(ctx, m.Direction, m.Sender, m.Recipient, m.Channel, m.Text, m.Operator, m.Hops, m.At); err != nil {
			t.Fatalf("record %s: %v", m.Text, err)
		}
	}

	// The conversation with a0a85934: dm rx from them + dm tx to them.
	got, err := s.ListMeshtasticMessages(ctx, storage.MeshtasticMessageFilter{Channel: "dm", Peer: "a0a85934"}, 10, 0)
	if err != nil || len(got) != 2 {
		t.Fatalf("peer filter = %+v, %v", got, err)
	}
	for _, m := range got {
		if m.Text != "from-a" && m.Text != "to-a" {
			t.Fatalf("peer filter leaked %s", m.Text)
		}
	}

	// Direction narrows the conversation further.
	got, err = s.ListMeshtasticMessages(ctx, storage.MeshtasticMessageFilter{Channel: "dm", Direction: "rx", Peer: "a0a85934"}, 10, 0)
	if err != nil || len(got) != 1 || got[0].Text != "from-a" {
		t.Fatalf("rx peer filter = %+v, %v", got, err)
	}
	got, err = s.ListMeshtasticMessages(ctx, storage.MeshtasticMessageFilter{Channel: "dm", Direction: "tx", Peer: "a0a85934"}, 10, 0)
	if err != nil || len(got) != 1 || got[0].Text != "to-a" {
		t.Fatalf("tx peer filter = %+v, %v", got, err)
	}

	// The peers list carries the distinct dm participants only.
	peers, err := s.MeshtasticPeers(ctx)
	if err != nil || len(peers) != 2 || peers[0] != "a0a85934" || peers[1] != "deadbeef" {
		t.Fatalf("peers = %v, %v", peers, err)
	}

	// Counts mirror the filter.
	if n, err := s.CountMeshtasticMessages(ctx, storage.MeshtasticMessageFilter{Channel: "dm", Peer: "a0a85934"}); err != nil || n != 2 {
		t.Fatalf("peer count = %d, %v", n, err)
	}
	if n, err := s.CountMeshtasticMessages(ctx, storage.MeshtasticMessageFilter{Channel: "dm"}); err != nil || n != 5 {
		t.Fatalf("dm count = %d, %v", n, err)
	}
}
