package web

import (
	"io"
	"log/slog"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/szporwolik/WarnFlux/internal/action"
	"github.com/szporwolik/WarnFlux/internal/actions/discord"
	"github.com/szporwolik/WarnFlux/internal/config"
	"github.com/szporwolik/WarnFlux/internal/metrics"
	"github.com/szporwolik/WarnFlux/internal/trail"
)

// TestMassDiscordNeedsBotToken pins the capability seam the Mass info
// panel keys on: a Discord instance is only a delivery route for a mass
// notice when it holds a bot token — the webhook alone can post on a
// channel, never reach one user, and the panel must not pretend
// otherwise (no silent channel post, a visible warning instead).
func TestMassDiscordNeedsBotToken(t *testing.T) {
	build := func(cfg map[string]any) *Server {
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
		reg.Register(discord.Type, discord.New)
		mgr, err := action.NewManager([]config.Action{
			{ID: "discord-alerts", Type: discord.Type, Enabled: true, Config: &node},
		}, reg, slog.New(slog.NewTextHandler(io.Discard, nil)), trail.NewRecorder(trail.DefaultMaxTrails), metrics.New())
		if err != nil {
			t.Fatal(err)
		}
		return &Server{actions: mgr}
	}

	webhookOnly := build(map[string]any{"url": "https://discord.com/api/webhooks/1/x"})
	if webhookOnly.discordDMAvailable() {
		t.Error("a webhook-only instance must not be offered for direct messages")
	}
	withToken := build(map[string]any{
		"url":   "https://discord.com/api/webhooks/1/x",
		"token": "bot-token",
	})
	if !withToken.discordDMAvailable() {
		t.Error("a token-configured instance must be offered for direct messages")
	}
}
