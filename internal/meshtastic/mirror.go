package meshtastic

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

// MirrorConfig forwards the club channel's traffic to a Discord webhook: every
// message sent or received on that channel is posted there, so the members who
// are not sitting in front of a radio still see what the mesh is saying.
//
// The radio path must never wait for Discord, so the posts are asynchronous and
// bounded: a slow or dead webhook costs the station nothing but a log line, and
// a flood drops the excess instead of queueing for ever.
type MirrorConfig struct {
	// WebhookURL is the Discord webhook endpoint. Empty disables the mirror.
	WebhookURL string
	// Username overrides the webhook's display name (the channel-level one
	// stays untouched, exactly like the discord action does).
	Username string
	// Channel is the device channel index to mirror. The zero value mirrors
	// the announcement channel (Config.EmcomChannel), which is what a club
	// install wants: the channel the station beacons on and the members talk
	// on. 1-7 select a specific channel; the public PRIMARY channel (0) is
	// never mirrored, this station does not transmit there.
	Channel int
	// IncludeSystem also mirrors what the station sends on a schedule — the
	// presence beacon, the hazard status line. Off by default: those are
	// automated, and an hourly status line in a chat channel is noise, not
	// club traffic.
	IncludeSystem bool
}

const (
	// mirrorTimeout bounds one webhook request.
	mirrorTimeout = 10 * time.Second
	// mirrorMaxContent bounds the Discord content (it rejects >2000 runes).
	mirrorMaxContent = 1900
	// mirrorInFlight bounds the posts running at once; beyond it a message
	// is dropped rather than queued, because the station has no business
	// piling up chat traffic while someone's webhook is down.
	mirrorInFlight = 8
)

// mirrorMsg is one line of club traffic, in either direction.
type mirrorMsg struct {
	direction string // "rx" (heard on the air) or "tx" (we transmitted)
	sender    string // node id of the sender, rx only
	recipient string // node id for a direct message, tx only
	operator  string // who sent it from our side, tx only ("system" = schedule)
	channel   string // channel display name
	text      string
	hops      int
	at        time.Time
}

// trafficMirror posts club traffic to Discord. A nil *trafficMirror is the
// disabled state and every method tolerates it.
type trafficMirror struct {
	url      string
	username string
	log      *slog.Logger
	client   *http.Client
	sem      chan struct{}
}

// newTrafficMirror builds the mirror, or nil when no webhook is configured.
func newTrafficMirror(cfg MirrorConfig, log *slog.Logger) *trafficMirror {
	url := strings.TrimSpace(cfg.WebhookURL)
	if url == "" {
		return nil
	}
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &trafficMirror{
		url:      url,
		username: strings.TrimSpace(cfg.Username),
		log:      log,
		client:   &http.Client{Timeout: mirrorTimeout},
		sem:      make(chan struct{}, mirrorInFlight),
	}
}

// offer hands one message to the poster without blocking the caller.
func (m *trafficMirror) offer(msg mirrorMsg) {
	if m == nil {
		return
	}
	select {
	case m.sem <- struct{}{}:
		go func() {
			defer func() { <-m.sem }()
			m.post(msg)
		}()
	default:
		m.log.Warn("meshtastic: discord mirror backlog full, message dropped",
			"direction", msg.direction, "channel", msg.channel)
	}
}

// post sends one webhook request. Failures are logged and otherwise ignored:
// the mirror is an observer, never a delivery guarantee.
func (m *trafficMirror) post(msg mirrorMsg) {
	body := map[string]any{"content": mirrorContent(msg)}
	if m.username != "" {
		body["username"] = m.username
	}
	payload, err := json.Marshal(body)
	if err != nil {
		m.log.Warn("meshtastic: discord mirror payload failed", "error", err)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), mirrorTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, m.url, bytes.NewReader(payload))
	if err != nil {
		m.log.Warn("meshtastic: discord mirror request failed", "error", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := m.client.Do(req)
	if err != nil {
		m.log.Warn("meshtastic: discord mirror post failed", "error", err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		m.log.Warn("meshtastic: discord mirror rejected the message", "status", resp.StatusCode)
	}
}

// mirrorContent renders one message for the chat channel: a one-line header
// that says which way it went and who is talking, then the text itself.
func mirrorContent(msg mirrorMsg) string {
	arrow := "📥"
	who := msg.sender
	if msg.direction == "tx" {
		arrow = "📤"
		who = "all"
		if msg.recipient != "" {
			who = msg.recipient
		}
		if msg.operator != "" && msg.operator != "system" {
			who += " · " + msg.operator
		}
	}
	header := fmt.Sprintf("%s **%s** · %s", arrow, msg.channel, who)
	if msg.hops > 0 {
		header += fmt.Sprintf(" · %d hop", msg.hops)
	}
	content := header + "\n" + strings.TrimSpace(msg.text)
	return truncateRunes(content, mirrorMaxContent)
}

// truncateRunes shortens s to at most n runes, marking the cut so nobody thinks
// the message simply ended there.
func truncateRunes(s string, n int) string {
	runes := []rune(s)
	if len(runes) <= n {
		return s
	}
	return string(runes[:n-1]) + "…"
}
