package web

import (
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/szporwolik/WarnFlux/internal/action"
	"github.com/szporwolik/WarnFlux/internal/actions/discord"
	"github.com/szporwolik/WarnFlux/internal/config"
	"github.com/szporwolik/WarnFlux/internal/metrics"
	"github.com/szporwolik/WarnFlux/internal/trail"
)

// discordHelpServer builds a Server carrying one Discord instance.
func discordHelpServer(t *testing.T, cfg map[string]any) *Server {
	t.Helper()
	data, err := yaml.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var node yaml.Node
	if err := yaml.Unmarshal(data, &node); err != nil {
		t.Fatal(err)
	}
	reg := action.NewRegistry()
	if err := reg.Register(discord.Type, discord.New); err != nil {
		t.Fatal(err)
	}
	mgr, err := action.NewManager([]config.Action{
		{ID: "discord-alerts", Type: discord.Type, Enabled: true, Config: &node},
	}, reg, slog.New(slog.NewTextHandler(io.Discard, nil)), trail.NewRecorder(trail.DefaultMaxTrails), metrics.New())
	if err != nil {
		t.Fatal(err)
	}
	return &Server{actions: mgr}
}

// TestHelpDiscordCard pins the Discord card of the manual: the surfaces
// the station uses there and the cadence of its periodic notices come
// from the running instance, never from a hardcoded description.
func TestHelpDiscordCard(t *testing.T) {
	// No Discord instance at all: no card.
	if view := (&Server{}).helpDiscord("en"); view != nil {
		t.Fatalf("card without an instance = %+v", view)
	}

	// A webhook-only instance: channel alerts, no private messages, and
	// the default cadence (1h / 4h) spelled out.
	webhookOnly := discordHelpServer(t, map[string]any{"url": "https://discord.com/api/webhooks/1/x"})
	view := webhookOnly.helpDiscord("en")
	if view == nil {
		t.Fatal("no card for a webhook-only instance")
	}
	facts := map[string]string{}
	for _, f := range view.Facts {
		facts[f.Label] = f.Value
	}
	if facts["Channel alerts"] != "enabled" {
		t.Errorf("channel alerts = %q", facts["Channel alerts"])
	}
	if facts["Private messages"] != "disabled" {
		t.Errorf("private messages = %q", facts["Private messages"])
	}
	if got := facts["Hazard status"]; got != "every 1h (only for severe or higher alerts)" {
		t.Errorf("hazard status = %q", got)
	}
	// The presence line is anchored to a daily clock time by default — the
	// same rhythm the Meshtastic beacon follows.
	if got := facts["Uptime notice"]; got != "daily at 06:00" {
		t.Errorf("uptime notice = %q", got)
	}
	// Without a bot token the DM-specific instructions stay off the card.
	if len(view.Items) != 1 || view.Items[0] != "help.discord.item.1" {
		t.Errorf("items without a token = %v", view.Items)
	}

	// A token adds the private-message surface and its instructions, and
	// the cadence follows the configuration.
	withToken := discordHelpServer(t, map[string]any{
		"url":              "https://discord.com/api/webhooks/1/x",
		"token":            "bot-token",
		"hazards_interval": "30m",
		"uptime_interval":  "12h",
	})
	view = withToken.helpDiscord("pl")
	if view == nil {
		t.Fatal("no card for a token instance")
	}
	facts = map[string]string{}
	for _, f := range view.Facts {
		facts[f.Label] = f.Value
	}
	if facts["Wiadomości prywatne"] != "włączone" {
		t.Errorf("private messages = %q", facts["Wiadomości prywatne"])
	}
	if got := facts["Status zagrożeń"]; got != "co 30 min (tylko przy alertach severe lub wyżej)" {
		t.Errorf("hazard status = %q", got)
	}
	if got := facts["Powiadomienie uptime"]; got != "co 12h" {
		t.Errorf("uptime notice = %q", got)
	}
	// An anchored line reads as a clock time, not as a spacing.
	anchored := discordHelpServer(t, map[string]any{
		"url":       "https://discord.com/api/webhooks/1/x",
		"uptime_at": "21:30",
	})
	for _, f := range anchored.helpDiscord("en").Facts {
		if f.Label == "Uptime notice" && f.Value != "daily at 21:30" {
			t.Errorf("anchored uptime notice = %q", f.Value)
		}
	}
	if len(view.Items) != 3 {
		t.Errorf("items with a token = %v", view.Items)
	}

	// Both notices can be switched off: the card says so instead of
	// promising a line that never comes.
	off := discordHelpServer(t, map[string]any{
		"url":              "https://discord.com/api/webhooks/1/x",
		"hazards_interval": "0s",
		"uptime_interval":  "0s",
	})
	view = off.helpDiscord("en")
	if view == nil {
		t.Fatal("no card for an instance with the notices off")
	}
	for _, f := range view.Facts {
		if strings.HasPrefix(f.Label, "Hazard") || strings.HasPrefix(f.Label, "Uptime") {
			if f.Value != "disabled" || f.On {
				t.Errorf("%s = %q (on=%v), want disabled", f.Label, f.Value, f.On)
			}
		}
	}
}

// TestHelpDuration pins the human wording of the notice cadence.
func TestHelpDuration(t *testing.T) {
	cases := map[time.Duration]string{
		0:                "",
		30 * time.Minute: "30 min",
		time.Hour:        "1h",
		90 * time.Minute: "1h30m",
		4 * time.Hour:    "4h",
		24 * time.Hour:   "1d",
		36 * time.Hour:   "1d12h",
		720 * time.Hour:  "30d",
		2 * time.Minute:  "2 min",
		45 * time.Second: "0 min",
	}
	for in, want := range cases {
		if got := helpDuration(in); got != want {
			t.Errorf("helpDuration(%s) = %q, want %q", in, got, want)
		}
	}
}
