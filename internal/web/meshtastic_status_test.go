package web_test

import (
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/meshtastic"
)

// TestMeshtasticLinkCard pins the Meshtastic page's link pill: it says whether
// the device link is up and prints WHICH device that is — the same shape as the
// GSM modem pill, so every radio page reads the same way.
func TestMeshtasticLinkCard(t *testing.T) {
	hub, err := meshtastic.NewHub(meshtastic.Config{
		Enabled: true, Device: "/dev/ttyUSB0", NodeTTL: time.Hour,
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	env := newTestEnvWithMesh(t, hub)
	env.login()

	resp, html := env.get("/meshtastic")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /meshtastic = %d, want 200", resp.StatusCode)
	}
	if !strings.Contains(html, "Device not connected") {
		t.Errorf("the page does not report the device link is down: %s", html)
	}
	if !strings.Contains(html, "Device: /dev/ttyUSB0") {
		t.Errorf("the page does not name the device: %s", html)
	}
	if strings.Contains(html, "Device connected and ready") {
		t.Errorf("a hub without a session claimed the device is ready: %s", html)
	}
}
