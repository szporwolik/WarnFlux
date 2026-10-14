package web_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/szporwolik/WarnFlux/internal/plugin"
)

// TestAPRSLinkCard pins the link card an operator reads the same way as the
// GSM modem pill: the internet link (APRS-IS) and the radio link (KISS TNC,
// usually Direwolf) reported SEPARATELY — one can be up while the other is
// down, and a backend nobody configured is muted rather than alarming.
func TestAPRSLinkCard(t *testing.T) {
	env := newTestEnv(t)
	env.login()

	// Nothing configured: both links are muted, nothing claims a failure.
	resp, html := env.get("/aprs")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /aprs = %d, want 200", resp.StatusCode)
	}
	for _, want := range []string{"APRS links", "APRS-IS: not configured", "RF (Direwolf): not configured"} {
		if !strings.Contains(html, want) {
			t.Errorf("unconfigured page missing %q: %s", want, html)
		}
	}

	// One link up, one down with a reason: the badges differ, and the plugin's
	// own report rides along (its stats summary, or the error that stopped it).
	env.router.statuses = []plugin.PluginStatus{
		{ID: "aprs-inet-main", Type: "aprs-inet", Kind: plugin.KindSource,
			State: plugin.StateRunning, LastSummary: "12 packets / 3 stations"},
		{ID: "aprs-radio-main", Type: "aprs-radio", Kind: plugin.KindSource,
			State: plugin.StateDegraded, LastError: "dial tcp 127.0.0.1:8001: connect: connection refused"},
	}
	_, html = env.get("/aprs")
	if !strings.Contains(html, "APRS-IS: connected and ready") {
		t.Errorf("running APRS-IS link is not reported as ready: %s", html)
	}
	if !strings.Contains(html, "12 packets / 3 stations") {
		t.Errorf("the APRS-IS report is missing: %s", html)
	}
	if !strings.Contains(html, "RF (Direwolf): not connected") {
		t.Errorf("the down radio link is not reported: %s", html)
	}
	if !strings.Contains(html, `<span class="badge muted">degraded</span>`) {
		t.Errorf("the radio link does not name the state it is in as its own badge: %s", html)
	}
	if !strings.Contains(html, "connection refused") {
		t.Errorf("the radio link does not carry the reason it is down: %s", html)
	}
	if strings.Contains(html, "RF (Direwolf): connected and ready") {
		t.Errorf("a degraded radio link was reported as ready: %s", html)
	}

	// A backend the station switched OFF is muted, not alarming: nothing is
	// broken, and the state badge says which one is off.
	env.router.statuses = []plugin.PluginStatus{
		{ID: "aprs-inet-main", Type: "aprs-inet", Kind: plugin.KindSource,
			State: plugin.StateRunning, LastSummary: "12 packets / 3 stations"},
		{ID: "aprs-radio-main", Type: "aprs-radio", Kind: plugin.KindSource,
			State: plugin.StateDisabled},
	}
	_, html = env.get("/aprs")
	if !strings.Contains(html, `<span class="badge muted">RF (Direwolf): disabled</span>`) {
		t.Errorf("a disabled radio link is not reported as switched off: %s", html)
	}
	if strings.Contains(html, "RF (Direwolf): not connected") {
		t.Errorf("a disabled radio link was reported as a failure: %s", html)
	}
	if strings.Contains(html, "conection refused") || strings.Contains(html, "connection refused") {
		t.Errorf("a disabled radio link carries a stale error: %s", html)
	}

	// The polled fragment serves the same card (that is what keeps it fresh).
	pResp, partial := env.get("/partials/aprs-links")
	if pResp.StatusCode != http.StatusOK {
		t.Fatalf("GET /partials/aprs-links = %d, want 200", pResp.StatusCode)
	}
	if !strings.Contains(partial, "APRS-IS: connected and ready") ||
		!strings.Contains(partial, "RF (Direwolf): disabled") {
		t.Errorf("the polled card differs from the page card: %s", partial)
	}
}
