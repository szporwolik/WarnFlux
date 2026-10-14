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
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/szporwolik/WarnFlux/internal/action"
	"github.com/szporwolik/WarnFlux/internal/dispatch"
	"github.com/szporwolik/WarnFlux/internal/i18n"
	"github.com/szporwolik/WarnFlux/internal/schedule"
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
	// Invite is the optional public invite to the station's server
	// (https://discord.gg/...). It is shown on the Help page so users can
	// join with one click instead of hunting for the link; it changes
	// nothing about the delivery itself.
	Invite string `yaml:"invite"`
	// Token is the optional Discord bot token (Developer Portal →
	// Bot). The webhook above posts on a CHANNEL; the token lets the
	// station also write a DIRECT MESSAGE to one user id, which the
	// mass-info panel uses — users carry their Discord id in their
	// profile. Empty: the instance can post on the channel only.
	Token string `yaml:"token"`
	// Timeout bounds one HTTP request; 0 falls back to defaultTimeout.
	Timeout time.Duration `yaml:"timeout"`
	// HazardsInterval is the spacing of the periodic hazard status line:
	// the counters of the active hazards (nothing at all while none is
	// active), posted on the webhook channel and sent as a direct
	// message to every registered operator. It mirrors the Meshtastic
	// channel's emcom_hazards_interval. Empty: the default (1h);
	// 0 disables the notice.
	HazardsInterval *time.Duration `yaml:"hazards_interval"`
	// UptimeAt anchors the presence line to a daily clock time ("06:00",
	// the station's local time) — the Discord counterpart of the Meshtastic
	// presence beacon, which is anchored the same way. Omitted: "06:00"
	// (or the UptimeInterval spacing, when one is set); "" falls back to the
	// spacing.
	UptimeAt *string `yaml:"uptime_at"`
	// UptimeInterval is the presence-line SPACING (1m..720h) — an
	// alternative to the daily anchor, not a companion: it fires every
	// interval from the start. Omitted together with UptimeAt: the default
	// anchor "06:00"; 0 disables the line.
	UptimeInterval *time.Duration `yaml:"uptime_interval"`
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
	invite   string
	logger   *slog.Logger

	// dmMu guards the direct-message channel cache: opening a DM channel
	// costs one request, and one notice per recipient would otherwise
	// pay it again on every broadcast.
	dmMu       sync.Mutex
	dmChannels map[string]string

	// dm is the inbound half: it answers the shared radio CLI commands
	// written in a direct message. nil without a bot token — a webhook
	// can post on a channel but never read a direct message.
	dm *dmListener

	// notices is the periodic half: the hazard status line and the
	// presence line, on the channel and (with a token) per operator.
	notices *notifier
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
	invite := strings.TrimSpace(cfg.Invite)
	if invite != "" {
		iu, err := url.Parse(invite)
		if err != nil || (iu.Scheme != "http" && iu.Scheme != "https") || iu.Host == "" {
			return nil, fmt.Errorf("discord: config.invite must be an absolute http(s) URL")
		}
	}
	token := strings.TrimSpace(cfg.Token)
	if strings.ContainsAny(token, " \t\r\n") {
		return nil, fmt.Errorf("discord: config.token must be a single token without whitespace")
	}
	hazards, err := noticeInterval(cfg.HazardsInterval, defaultHazardsInterval, "hazards_interval")
	if err != nil {
		return nil, err
	}
	uptime, uptimeAt, err := resolveUptime(cfg.UptimeAt, cfg.UptimeInterval)
	if err != nil {
		return nil, err
	}
	a := &discordAction{
		url:      u,
		username: strings.TrimSpace(cfg.Username),
		token:    token,
		client: &http.Client{
			Timeout: timeout,
			// A message is delivered by the response to THIS request. With
			// the default policy a 3xx is followed as a bodyless GET: a
			// webhook that answered 302 towards a login page answered 200 in
			// the end and the alert was reported delivered although it never
			// arrived (reported P2). The redirect is surfaced as the failure
			// it is.
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		invite:     invite,
		logger:     slog.Default(),
		dmChannels: map[string]string{},
	}
	a.notices = newNotifier(a, a.logger, hazards, uptime, uptimeAt)
	if a.token != "" {
		a.dm = newDMListener(a, a.logger)
	}
	return a, nil
}

// resolveUptime decides how the presence line is scheduled: a daily clock
// time (the default), a fixed spacing, or nothing at all. The rules match
// the Meshtastic presence beacon exactly.
//   - uptime_at set and non-empty: the daily anchor (a spacing alongside it
//     is a configuration error, not a silent override);
//   - uptime_at set and empty: the spacing from uptime_interval, where 0
//     disables the line;
//   - uptime_at omitted with uptime_interval omitted: the default anchor;
//   - uptime_at omitted with uptime_interval set: the spacing.
func resolveUptime(at *string, interval *time.Duration) (time.Duration, string, error) {
	if at != nil {
		value := strings.TrimSpace(*at)
		if value != "" {
			if interval != nil && *interval != 0 {
				return 0, "", fmt.Errorf("discord: set uptime_at (daily notice) or uptime_interval (spacing), not both")
			}
			if !schedule.Valid(value) {
				return 0, "", fmt.Errorf("discord: config.uptime_at must be a clock time like %q, got %q", schedule.AtDefault, value)
			}
			return 0, value, nil
		}
		// An explicit empty anchor means "no daily notice".
	}
	spacing, err := noticeInterval(interval, defaultUptimeInterval, "uptime_interval")
	if err != nil {
		return 0, "", err
	}
	if at == nil && interval == nil {
		// Neither configured: the daily default.
		return 0, schedule.AtDefault, nil
	}
	return spacing, "", nil
}

// noticeInterval resolves one periodic-notice spacing: an absent value
// takes the default, an explicit 0 disables the notice, anything else
// must sit between one minute and 720 hours.
func noticeInterval(value *time.Duration, fallback time.Duration, key string) (time.Duration, error) {
	if value == nil {
		return fallback, nil
	}
	v := *value
	if v == 0 {
		return 0, nil
	}
	if v < minNoticeInterval || v > maxNoticeInterval {
		return 0, fmt.Errorf("discord: config.%s must be between %s and %s (0 disables), got %s",
			key, minNoticeInterval, maxNoticeInterval, v)
	}
	return v, nil
}

// Name returns the action type name.
func (a *discordAction) Name() string { return Type }

// SharedDestination implements the optional capability: a routed delivery is
// one post in the channel this webhook belongs to, shared by everyone. Two
// groups following the same sources would therefore post the same alert twice
// (reported P2), so the routing engine collapses their deliveries into one and
// hands it the union of their mentions.
func (a *discordAction) SharedDestination() bool { return true }

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

// UsableUserID reports whether a profile Discord value can receive a
// direct message: this station needs a numeric user id (a bare snowflake
// or a mention like "<@123>"). A username or handle — the natural thing to
// type into a profile — cannot be resolved by the bot, so the panels can
// flag it instead of promising a delivery that will fail.
func UsableUserID(v string) bool { return normalizeUserID(v) != "" }

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
	a.cacheDM(userID, created.ID)
	return created.ID, nil
}

// cacheDM remembers one user's direct-message channel.
func (a *discordAction) cacheDM(userID, channel string) {
	a.dmMu.Lock()
	defer a.dmMu.Unlock()
	if len(a.dmChannels) >= dmCacheLimit {
		a.dmChannels = map[string]string{}
	}
	a.dmChannels[userID] = channel
}

// replyDM answers one inbound direct message. The message carries its
// own channel id, so answering costs no extra request — and the cache
// learns it, so a later notice to the same person skips opening it.
func (a *discordAction) replyDM(ctx context.Context, userID, channel, content string) error {
	if channel == "" {
		return a.SendDM(ctx, userID, content)
	}
	a.cacheDM(userID, channel)
	return a.postChannelMessage(ctx, channel, truncateRunes(strings.TrimSpace(content), maxContentRunes))
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
		event := i18n.EventName(lang, h.Hazard.Event)
		headline := strings.TrimSpace(h.Hazard.Headline)
		if headline == "" {
			headline = event
		}
		text = fmt.Sprintf("[%s] %s: %s — %s", prefix,
			strings.ToUpper(i18n.T(lang, "sev."+strings.ToLower(h.Hazard.Severity))),
			event, headline)
		if id := h.Hazard.MessageID(); id != "" {
			text += "\n" + i18n.T(lang, "discord.message_id") + ": " + id
		}
		if len(h.Hazard.Areas) > 0 {
			text += "\n" + i18n.T(lang, "discord.areas") + ": " + strings.Join(h.Hazard.Areas, ", ")
		}
		if h.Hazard.ExpiresAt != nil {
			// The validity is shown in the STATION's zone and says which
			// one: a bare RFC3339 stamp would silently be UTC.
			zoneLoc, zoneName := action.DisplayZone(req.App)
			text += "\n" + i18n.T(lang, "discord.valid_until") + ": " +
				h.Hazard.ExpiresAt.In(zoneLoc).Format("2006-01-02 15:04") + " (" + zoneName + ")"
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
	_ DMCommander     = (*discordAction)(nil)
	_ Notifier        = (*discordAction)(nil)
)
