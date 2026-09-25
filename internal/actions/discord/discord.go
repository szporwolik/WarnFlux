// Package discord implements the Discord webhook action: one POST per
// routed hazard notification, formatted as a human-readable Discord
// message (content plus an optional bot username). It follows the action
// plugin architecture: strict YAML decoding and validation at
// construction, a bounded per-call HTTP request and a no-op Close.
// Delivery retries are owned by the action instance machinery
// (runtime.retries), not by this plugin.
package discord

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/szporwolik/WarnFlux/internal/action"
	"github.com/szporwolik/WarnFlux/internal/dispatch"
)

// Type is the action type name used in the YAML configuration.
const Type = "discord"

const (
	// maxContentRunes bounds the Discord message content (Discord rejects
	// messages longer than 2000 characters).
	maxContentRunes = 2000
	// defaultTimeout bounds one request when the config omits it.
	defaultTimeout = 10 * time.Second
)

// Config is the action-specific configuration.
type Config struct {
	// URL is the Discord webhook endpoint
	// (https://discord.com/api/webhooks/...). Required.
	URL string `yaml:"url"`
	// Username optionally overrides the bot display name of the webhook.
	Username string `yaml:"username"`
	// Timeout bounds one HTTP request; 0 falls back to defaultTimeout.
	Timeout time.Duration `yaml:"timeout"`
}

// payload is the JSON body Discord accepts on a webhook endpoint.
type payload struct {
	Content  string `json:"content"`
	Username string `json:"username,omitempty"`
}

type discordAction struct {
	url      *url.URL
	username string
	client   *http.Client
}

// New decodes and validates the configuration and builds the action. A
// missing or non-http(s) URL is a construction error.
func New(node *yaml.Node) (action.Plugin, error) {
	var cfg Config
	if node != nil {
		if err := node.Decode(&cfg); err != nil {
			return nil, fmt.Errorf("discord: decode config: %w", err)
		}
	}
	if strings.TrimSpace(cfg.URL) == "" {
		return nil, fmt.Errorf("discord: config.url is required")
	}
	u, err := url.Parse(strings.TrimSpace(cfg.URL))
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("discord: config.url must be an absolute http(s) URL")
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	return &discordAction{
		url:      u,
		username: strings.TrimSpace(cfg.Username),
		client:   &http.Client{Timeout: timeout},
	}, nil
}

// Name returns the action type name.
func (a *discordAction) Name() string { return Type }

// Execute delivers one webhook POST. Non-2xx responses and transport
// errors are returned as errors; retries are owned by the action
// instance machinery.
func (a *discordAction) Execute(ctx context.Context, req action.ActionRequest) error {
	body, err := json.Marshal(payload{Content: a.messageText(req), Username: a.username})
	if err != nil {
		return fmt.Errorf("discord: marshal payload: %w", err)
	}
	hreq, err := http.NewRequestWithContext(ctx, http.MethodPost, a.url.String(), bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("discord: build request: %w", err)
	}
	hreq.Header.Set("Content-Type", "application/json")
	if v := strings.TrimSpace(req.App.Version); v != "" {
		hreq.Header.Set("User-Agent", "WarnFlux/"+v)
	} else {
		hreq.Header.Set("User-Agent", "WarnFlux")
	}
	resp, err := a.client.Do(hreq)
	if err != nil {
		if ctx.Err() != nil {
			return fmt.Errorf("discord: request cancelled: %w", ctx.Err())
		}
		return fmt.Errorf("discord: request failed: %w", err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4*1024))
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("discord: webhook answered %s", resp.Status)
	}
	return nil
}

// Close releases nothing (stateless per-call client).
func (a *discordAction) Close(ctx context.Context) error {
	_ = ctx
	a.client.CloseIdleConnections()
	return nil
}

// messageText renders one Discord message from the canonical event
// metadata: severity first, then event, headline and areas. The content
// never exceeds Discord's 2000-character limit.
func (a *discordAction) messageText(req action.ActionRequest) string {
	prefix := strings.TrimSpace(req.App.Header1)
	if prefix == "" {
		prefix = "WarnFlux"
	}
	var text string
	switch req.Event.Kind {
	case dispatch.EventHazardTransition:
		h := req.Event.Hazard
		if h == nil {
			text = fmt.Sprintf("[%s] hazard transition", prefix)
			break
		}
		headline := strings.TrimSpace(h.Hazard.Headline)
		if headline == "" {
			headline = h.Hazard.Event
		}
		text = fmt.Sprintf("[%s] %s: %s — %s", prefix, strings.ToUpper(h.Hazard.Severity), h.Hazard.Event, headline)
		if len(h.Hazard.Areas) > 0 {
			text += "\nAreas: " + strings.Join(h.Hazard.Areas, ", ")
		}
		if h.Hazard.ExpiresAt != nil {
			text += "\nValid until: " + h.Hazard.ExpiresAt.Format(time.RFC3339)
		}
	default:
		text = fmt.Sprintf("[%s] WarnFlux notification", prefix)
	}
	return truncateRunes(text, maxContentRunes)
}

// truncateRunes bounds text to at most max runes (appending an ellipsis),
// so one delivery can never exceed Discord's content limit.
func truncateRunes(text string, max int) string {
	r := []rune(text)
	if len(r) <= max {
		return text
	}
	return string(r[:max-1]) + "…"
}

var _ action.Plugin = (*discordAction)(nil)
