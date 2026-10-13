package web_test

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/meshtastic"
)

// meshFavoriteLedger stands in for the pinned-favorite store the hub reads.
type meshFavoriteLedger struct{ ids []uint32 }

func (m meshFavoriteLedger) LoadMeshtasticFavorites(context.Context) ([]uint32, error) {
	return m.ids, nil
}

func (m meshFavoriteLedger) SaveMeshtasticFavorites(context.Context, []uint32) error { return nil }

// TestMeshtasticPinnedStar pins the star in the heard-nodes table: it marks
// the nodes the station keeps as radio favorites (whose public key the
// firmware therefore never evicts) and nothing else.
func TestMeshtasticPinnedStar(t *testing.T) {
	hub, err := meshtastic.NewHub(
		meshtastic.Config{Enabled: true, Device: "/dev/fake", NodeTTL: time.Hour},
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	hub.SeedNode("a0a85934", "SPM", "SPM", 0, 0, time.Now(), []string{"text"})
	hub.SeedNode("deadbeef", "Bunkier", "", 0, 0, time.Now(), []string{"text"})
	hub.SetFavoriteStore(meshFavoriteLedger{ids: []uint32{0xa0a85934}})

	env := newTestEnvAll(t, nil, nil, nil, hub, nil, &fakeMeshMsgs{})
	env.login()
	_, html := env.get("/meshtastic")

	rows := strings.Split(html, `<tr class="click-send"`)
	var starred, plain string
	for _, row := range rows[1:] {
		switch {
		case strings.Contains(row, "a0a85934"):
			starred = row
		case strings.Contains(row, "deadbeef"):
			plain = row
		}
	}
	if starred == "" || plain == "" {
		t.Fatalf("both node rows expected: %.400s", html)
	}
	if !strings.Contains(starred, `<span class="pin-star" title="`) {
		t.Errorf("the pinned node carries no star: %.400s", starred)
	}
	if strings.Contains(plain, "pin-star") {
		t.Errorf("a node that is not pinned carries a star: %.400s", plain)
	}
}
