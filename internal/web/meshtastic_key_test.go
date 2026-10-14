package web_test

import (
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/meshtastic"
)

// TestMeshtasticNodeKeyMarkers pins the PKI state in the heard-nodes table: a
// node the station has never seen announce a public key is MARKED, because a
// direct message to it will be refused by the radio — the operator learns that
// before typing, not from a failed row afterwards. The DM hint explains the
// same rule next to the send form.
func TestMeshtasticNodeKeyMarkers(t *testing.T) {
	hub, err := meshtastic.NewHub(
		meshtastic.Config{Enabled: true, Device: "/dev/fake", NodeTTL: time.Hour},
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	// No radio ever handed this hub its node table, so no node has a known
	// key: every row must carry the warning marker (the keyed case comes from
	// the handshake and is pinned in the meshtastic package).
	hub.SeedNode("a0a85934", "SPM", "SPM", 0, 0, time.Now(), []string{"text"})
	hub.SeedNode("deadbeef", "Bunkier", "", 0, 0, time.Now(), []string{"text"})

	env := newTestEnvAll(t, nil, nil, nil, hub, nil, &fakeMeshMsgs{})
	env.login()
	_, html := env.get("/meshtastic")

	rows := strings.Split(html, `<tr class="click-send"`)
	if len(rows) < 3 {
		t.Fatalf("both node rows expected: %.400s", html)
	}
	for _, row := range rows[1:] {
		if !strings.Contains(row, `<span class="key-missing" title="`) {
			t.Errorf("a node without a known key is not marked: %.300s", row)
		}
		if strings.Contains(row, "key-known") {
			t.Errorf("a node without a known key carries the ready mark: %.300s", row)
		}
	}
	if !strings.Contains(html, "NodeInfo") {
		t.Errorf("the DM hint about the recipient's key is missing: %.600s", html)
	}
}
