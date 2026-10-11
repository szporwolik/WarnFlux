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
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/szporwolik/WarnFlux/internal/action"
	"github.com/szporwolik/WarnFlux/internal/dispatch"
	"github.com/szporwolik/WarnFlux/internal/i18n"
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
	// Token is the optional Discord bot token (Developer Portal →
	// Bot). The webhook above posts on a CHANNEL; the token lets the
	// station also write a DIRECT MESSAGE to one user id, which the
	// mass-info panel uses — users carry their Discord id in their
	// profile. Empty: the instance can post on the channel only.
	Token string `yaml:"token"`
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
	token    string
	client   *http.Client

	// dmMu guards the direct-message channel cache: opening a DM channel
	// costs one request, and one notice per recipient would otherwise
	// pay it again on every broadcast.
	dmMu       sync.Mutex
	dmChannels map[string]string
}

// apiBase is the Discord REST base. A variable so tests can point an
// instance at their own server.
var apiBase = "https://discord.com/api/v10"

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
	token := strings.TrimSpace(cfg.Token)
	if strings.ContainsAny(token, " \t\r\n") {
		return nil, fmt.Errorf("discord: config.token must be a single token without whitespace")
	}
	return &discordAction{
		url:        u,
		username:   strings.TrimSpace(cfg.Username),
		token:      token,
		client:     &http.Client{Timeout: timeout},
		dmChannels: map[string]string{},
	}, nil
}

// Name returns the action type name.
func (a *discordAction) Name() string { return Type }

// DirectMessenger is the optional capability of an instance holding a bot
// token: panel features (mass info) deliver a plain notice as a DIRECT
// MESSAGE to one user id, so each recipient gets their own private
// message instead of a post the whole channel reads.
type DirectMessenger interface {
	// SendDM delivers one plain content message to one Discord user id
	// (a numeric snowflake, or a mention like "<@123456789>").
	SendDM(ctx context.Context, userID, content string) error
}

// DirectMessengerOf reports the direct-message capability of one plugin
// instance: ok is false when the instance carries no bot token, because a
// webhook alone can only post on a channel — never reach one user.
func DirectMessengerOf(p action.Plugin) (DirectMessenger, bool) {
	a, ok := p.(*discordAction)
	if !ok || a.token == "" {
		return nil, false
	}
	return a, true
}

// dmCacheLimit bounds the direct-message channel cache.
const dmCacheLimit = 512

// normalizeUserID reduces a profile Discord value to the numeric user id
// the API needs: a bare snowflake or a mention ("<@123>", "<@!123>") is
// accepted, anything else (a plain username) yields "".
func normalizeUserID(v string) string {
	v = strings.TrimSpace(v)
	v = strings.TrimPrefix(v, "<@")
	v = strings.TrimPrefix(v, "!")
	v = strings.TrimSuffix(v, ">")
	v = strings.TrimSpace(v)
	if v == "" {
		return ""
	}
	for _, r := range v {
		if r < '0' || r > '9' {
			return ""
		}
	}
	return v
}

// apiError is one non-2xx Discord REST answer; callers inspect the status
// to tell a stale cached DM channel (404) from a real failure.
type apiError struct {
	status int
	msg    string
}

func (e *apiError) Error() string { return e.msg }

// SendDM delivers one plain message to one user's direct-message channel:
// the channel is opened once per user and cached, and a cached channel the
// API no longer knows is rebuilt exactly once.
func (a *discordAction) SendDM(ctx context.Context, userID, content string) error {
	if a.token == "" {
		return errors.New("discord: no bot token configured for direct messages")
	}
	id := normalizeUserID(userID)
	if id == "" {
		return fmt.Errorf("discord: %q is not a Discord user id", strings.TrimSpace(userID))
	}
	body := truncateRunes(strings.TrimSpace(content), maxContentRunes)
	channel, err := a.dmChannel(ctx, id)
	if err != nil {
		return err
	}
	if err := a.postChannelMessage(ctx, channel, body); err != nil {
		var api *apiError
		if errors.As(err, &api) && api.status == http.StatusNotFound {
			a.forgetDM(id)
			if channel, err = a.dmChannel(ctx, id); err != nil {
				return err
			}
			return a.postChannelMessage(ctx, channel, body)
		}
		return err
	}
	return nil
}

// dmChannel returns the direct-message channel id of one user, opening it
// on first use.
func (a *discordAction) dmChannel(ctx context.Context, userID string) (string, error) {
	a.dmMu.Lock()
	channel := a.dmChannels[userID]
	a.dmMu.Unlock()
	if channel != "" {
		return channel, nil
	}
	var created struct {
		ID string `json:"id"`
	}
	if err := a.apiCall(ctx, http.MethodPost, "/users/@me/channels",
		map[string]string{"recipient_id": userID}, &created); err != nil {
		return "", err
	}
	if created.ID == "" {
		return "", errors.New("discord: direct-message channel not returned")
	}
	a.dmMu.Lock()
	if len(a.dmChannels) >= dmCacheLimit {
		a.dmChannels = map[string]string{}
	}
	a.dmChannels[userID] = created.ID
	a.dmMu.Unlock()
	return created.ID, nil
}

// forgetDM drops one cached direct-message channel.
func (a *discordAction) forgetDM(userID string) {
	a.dmMu.Lock()
	delete(a.dmChannels, userID)
	a.dmMu.Unlock()
}

// postChannelMessage posts one content message on a channel id.
func (a *discordAction) postChannelMessage(ctx context.Context, channel, content string) error {
	return a.apiCall(ctx, http.MethodPost, "/channels/"+channel+"/messages",
		map[string]string{"content": content}, nil)
}

// apiCall performs one bot-token REST call. body is marshalled as JSON
// when non-nil, out receives the JSON answer when non-nil.
func (a *discordAction) apiCall(ctx context.Context, method, path string, body any, out any) error {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("discord: marshal request: %w", err)
		}
		reader = bytes.NewReader(encoded)
	}
	req, err := http.NewRequestWithContext(ctx, method, apiBase+path, reader)
	if err != nil {
		return fmt.Errorf("discord: build request: %w", err)
	}
	req.Header.Set("Authorization", "Bot "+a.token)
	req.Header.Set("User-Agent", "WarnFlux")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := a.client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return fmt.Errorf("discord: request cancelled: %w", ctx.Err())
		}
		return fmt.Errorf("discord: request failed: %w", err)
	}
	defer resp.Body.Close()
	payload, _ := io.ReadAll(io.LimitReader(resp.Body, 8*1024))
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		msg := fmt.Sprintf("discord: %s %s answered %s", method, path, resp.Status)
		if detail := apiMessage(payload); detail != "" {
			msg += ": " + detail
		}
		return &apiError{status: resp.StatusCode, msg: msg}
	}
	if out != nil && len(payload) > 0 {
		if err := json.Unmarshal(payload, out); err != nil {
			return fmt.Errorf("discord: decode answer: %w", err)
		}
	}
	return nil
}

// apiMessage extracts Discord's "message" field from an error body.
func apiMessage(body []byte) string {
	var answer struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal(body, &answer); err != nil {
		return ""
	}
	return strings.TrimSpace(answer.Message)
}

// post delivers one webhook POST with the given User-Agent.
func (a *discordAction) post(ctx context.Context, content, userAgent string) error {
	body, err := json.Marshal(payload{Content: content, Username: a.username})
	if err != nil {
		return fmt.Errorf("discord: marshal payload: %w", err)
	}
	hreq, err := http.NewRequestWithContext(ctx, http.MethodPost, a.url.String(), bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("discord: build request: %w", err)
	}
	hreq.Header.Set("Content-Type", "application/json")
	hreq.Header.Set("User-Agent", userAgent)
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

// Execute delivers one webhook POST. Non-2xx responses and transport
// errors are returned as errors; retries are owned by the action
// instance machinery.
func (a *discordAction) Execute(ctx context.Context, req action.ActionRequest) error {
	ua := "WarnFlux"
	if v := strings.TrimSpace(req.App.Version); v != "" {
		ua = "WarnFlux/" + v
	}
	return a.post(ctx, a.messageText(req), ua)
}

// Close releases nothing (stateless per-call client).
func (a *discordAction) Close(ctx context.Context) error {
	_ = ctx
	a.client.CloseIdleConnections()
	return nil
}

// detailsLink builds the absolute deep link to the hazard detail view on
// the public home page, or "" when the event has no key or no public
// domain is configured.
func detailsLink(req action.ActionRequest) string {
	if req.Event.Kind != dispatch.EventHazardTransition || req.Event.Hazard == nil {
		return ""
	}
	key := req.Event.Hazard.Key
	domain := strings.TrimSpace(req.App.Domain)
	if key == "" || domain == "" {
		return ""
	}
	scheme := "https://"
	if i := strings.Index(domain, "://"); i >= 0 {
		scheme = domain[:i+3]
		domain = domain[i+3:]
	}
	domain = strings.TrimSuffix(domain, "/")
	if domain == "" {
		return ""
	}
	return scheme + domain + "/message/" + url.PathEscape(key)
}

// messageText renders one Discord message from the canonical event
// metadata: severity first, then event, headline and areas. The content
// never exceeds Discord's 2000-character limit. The message speaks the
// system notification language (req.Lang), the same one the EMCOM
// channel posts use.
func (a *discordAction) messageText(req action.ActionRequest) string {
	lang := i18n.Effective(req.Lang)
	prefix := strings.TrimSpace(req.App.Header1)
	if prefix == "" {
		prefix = "WarnFlux"
	}
	var text string
	switch req.Event.Kind {
	case dispatch.EventHazardTransition:
		h := req.Event.Hazard
		if h == nil {
			text = fmt.Sprintf("[%s] %s", prefix, i18n.T(lang, "discord.hazard_transition"))
			break
		}
		headline := strings.TrimSpace(h.Hazard.Headline)
		if headline == "" {
			headline = h.Hazard.Event
		}
		text = fmt.Sprintf("[%s] %s: %s — %s", prefix,
			strings.ToUpper(i18n.T(lang, "sev."+strings.ToLower(h.Hazard.Severity))),
			h.Hazard.Event, headline)
		if id := h.Hazard.MessageID(); id != "" {
			text += "\n" + i18n.T(lang, "discord.message_id") + ": " + id
		}
		if len(h.Hazard.Areas) > 0 {
			text += "\n" + i18n.T(lang, "discord.areas") + ": " + strings.Join(h.Hazard.Areas, ", ")
		}
		if h.Hazard.ExpiresAt != nil {
			text += "\n" + i18n.T(lang, "discord.valid_until") + ": " + h.Hazard.ExpiresAt.Format(time.RFC3339)
		}
		if desc := strings.TrimSpace(h.Hazard.Description); desc != "" {
			text += "\n" + desc
		}
	default:
		text = fmt.Sprintf("[%s] %s", prefix, i18n.T(lang, "discord.notification"))
	}
	if len(req.DiscordHandles) > 0 {
		text += "\n" + i18n.T(lang, "discord.for") + ": " + strings.Join(req.DiscordHandles, ", ")
	}
	if link := detailsLink(req); link != "" {
		// The link must survive truncation: bound the body first, then
		// append the details line in full.
		reserve := len(link) + len("\n"+i18n.T(lang, "discord.details")+": ")
		text = truncateRunes(text, maxContentRunes-reserve) + "\n" + i18n.T(lang, "discord.details") + ": " + link
	} else {
		text = truncateRunes(text, maxContentRunes)
	}
	return text
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

var (
	_ action.Plugin   = (*discordAction)(nil)
	_ DirectMessenger = (*discordAction)(nil)
)
