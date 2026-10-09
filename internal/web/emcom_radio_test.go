package web_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/storage"
	"github.com/szporwolik/WarnFlux/internal/storage/sqlite"
)

// TestRadioEmcom pins the shared /emcom radio command: the network list
// with ids, the missing/invalid parameter hints and a level transition
// through the exact same flow the panel uses.
func TestRadioEmcom(t *testing.T) {
	store, _, err := sqlite.Open(filepath.Join(t.TempDir(), "emcom.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()
	if _, err := store.SaveEmcomNetwork(context.Background(), storage.EmcomNetwork{
		Slug: "sp9moa", Name: "SP9MOA EMCOM", Level: 0, UpdatedAt: time.Now(),
	}); err != nil {
		t.Fatalf("seed network: %v", err)
	}
	env := newTestEnvWithUsers(t, store)
	srv := env.server

	// No arguments: usage + the network list with the numeric id.
	res := srv.RadioEmcom("", "radio")
	if !res.Handled || !strings.Contains(res.Reply, "1 = SP9MOA EMCOM (level 0)") {
		t.Fatalf("list reply = %+v", res)
	}

	// Missing level parameter.
	res = srv.RadioEmcom("1", "radio")
	if !strings.Contains(res.Reply, "missing level") {
		t.Fatalf("missing level reply = %+v", res)
	}

	// Invalid level.
	res = srv.RadioEmcom("1 7", "radio")
	if !strings.Contains(res.Reply, "invalid level") {
		t.Fatalf("invalid level reply = %+v", res)
	}

	// Unknown network id.
	res = srv.RadioEmcom("9 2", "radio")
	if !strings.Contains(res.Reply, "unknown network id 9") {
		t.Fatalf("unknown network reply = %+v", res)
	}

	// A valid transition by the numeric id: confirmation with the name.
	res = srv.RadioEmcom("1 2", "radio")
	if !res.Handled || !strings.Contains(res.Reply, "OK: SP9MOA EMCOM -> level 2") {
		t.Fatalf("set reply = %+v", res)
	}
	rows, err := store.EmcomNetworks(context.Background())
	if err != nil || len(rows) != 1 || rows[0].Level != 2 || rows[0].UpdatedBy != "radio" || rows[0].ID != 1 {
		t.Fatalf("rows = %+v, %v; want one network id 1 at level 2 by radio", rows, err)
	}

	// The slug still works as a fallback reference.
	res = srv.RadioEmcom("sp9moa 0", "radio")
	if !res.Handled || !strings.Contains(res.Reply, "OK: SP9MOA EMCOM -> level 0") {
		t.Fatalf("slug fallback reply = %+v", res)
	}
}
