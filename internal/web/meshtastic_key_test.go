package web_test

import (
	"io"
	"log/slog"
	"regexp"
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

// TestMeshtasticNodesPinnedFilterControl pins the filter that switches the
// heard-nodes table between every node and only the pinned ones: the strip has
// to be OUTSIDE the polled fragment (the poll replaces the table every 10 s and
// would wipe a control inside it) and carries the label the JS uses when the
// filtered table is empty.
func TestMeshtasticNodesPinnedFilterControl(t *testing.T) {
	hub, err := meshtastic.NewHub(
		meshtastic.Config{Enabled: true, Device: "/dev/fake", NodeTTL: time.Hour},
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	hub.SeedNode("a0a85934", "SPM", "SPM", 0, 0, time.Now(), []string{"text"})
	// The star is what the filter looks for, so the ledger has to know the pin.
	hub.SetFavoriteStore(meshFavoriteLedger{ids: []uint32{0xa0a85934}})

	env := newTestEnvAll(t, nil, nil, nil, hub, nil, &fakeMeshMsgs{})
	env.login()
	_, html := env.get("/meshtastic")

	strip := regexp.MustCompile(`(?s)<div class="page-tabs"[^>]*id="mesh-nodes-filter".*?</div>`).FindString(html)
	if strip == "" {
		t.Fatalf("the node filter strip is missing: %.600s", html)
	}
	for _, want := range []string{`data-nodes="all"`, `data-nodes="pinned"`, `data-lbl-none="`} {
		if !strings.Contains(strip, want) {
			t.Errorf("the filter strip lacks %s: %s", want, strip)
		}
	}
	// Outside the polled box, or the next poll erases the active choice.
	box := strings.Index(html, `id="mesh-nodes"`)
	if box < 0 || strings.Index(html, `id="mesh-nodes-filter"`) > box {
		t.Fatalf("the filter strip must precede the polled #mesh-nodes box: %.400s", html)
	}
	// The filter needs a marker to filter BY: the pinned row carries the star.
	if !strings.Contains(html, `<span class="pin-star" title="`) {
		t.Errorf("the pinned node carries no star for the filter to find: %.400s", html)
	}
}
