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

	if err := s.RecordMeshtasticMessage(ctx, "tx", "", "dm", "hello", "admin", 0, at); err != nil {
		t.Fatalf("record: %v", err)
	}
	rows, err := s.ListMeshtasticMessages(ctx, "tx", 10, 0)
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
	rows, err = s.ListMeshtasticMessages(ctx, "tx", 10, 0)
	if err != nil || len(rows) != 1 {
		t.Fatalf("list: %v rows=%v", err, rows)
	}
	if rows[0].Status != "delivered" {
		t.Fatalf("status = %q, want delivered", rows[0].Status)
	}

	// A second row at the same instant with different text stays untouched.
	if err := s.RecordMeshtasticMessage(ctx, "tx", "", "dm", "other", "admin", 0, at); err != nil {
		t.Fatalf("record: %v", err)
	}
	if err := s.UpdateMeshtasticMessageStatus(ctx, "failed", at, "hello"); err != nil {
		t.Fatalf("failed: %v", err)
	}
	rows, err = s.ListMeshtasticMessages(ctx, "tx", 10, 0)
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
