package sqlite

import (
	"context"
	"testing"
	"time"
)

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
