package discord

// The direct-message command path mirrors the APRS, Meshtastic and SMS
// channels: slash commands run through the shared radiocli interpreter,
// plain texts answer with the installation banner (paced), restricted
// commands work only for senders the station's directory knows, and
// /alert events flow into the normal routing pipeline as canonical
// /events documents.

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/szporwolik/WarnFlux/internal/dispatch"
	"github.com/szporwolik/WarnFlux/internal/radiocli"
)

// messageEventSchemaVersion mirrors mqttreceiver.WireSchemaVersion: every
// transport publishes on the same /events stream, so the version must
// never drift.
const messageEventSchemaVersion = 1

// MessageEventWire is the /events envelope for one alert raised by a
// Discord command.
type MessageEventWire struct {
	SchemaVersion int                `json:"schema_version"`
	ChangeID      int64              `json:"change_id"`
	ChangeType    string             `json:"change_type"`
	EventKey      string             `json:"event_key"`
	Event         MessageEventHazard `json:"event"`
	// CommandResult is the in-band confirmation the operator saw; the
	// durable inbox records it with the lifecycle anchor.
	CommandResult string `json:"command_result,omitempty"`
}

// MessageEventHazard is the hazard block of one alert raised by a
// Discord command.
type MessageEventHazard struct {
	Source      string   `json:"source"`
	SourceID    string   `json:"source_id"`
	Category    string   `json:"category"`
	Event       string   `json:"event"`
	Severity    string   `json:"severity"`
	Urgency     string   `json:"urgency"`
	Certainty   string   `json:"certainty"`
	Headline    string   `json:"headline"`
	Description string   `json:"description"`
	Instruction string   `json:"instruction"`
	EffectiveAt *string  `json:"effective_at,omitempty"`
	ExpiresAt   *string  `json:"expires_at,omitempty"`
	Areas       []string `json:"areas"`
	Status      string   `json:"status"`
	SourceURL   string   `json:"source_url"`
	ReceivedAt  string   `json:"received_at"`
	UpdatedAt   string   `json:"updated_at"`
}

const (
	// dmSeenCap / dmSeenWindow bound the memory of message ids: Discord
	// redelivers the tail of a session after a resume, and a command must
	// never run twice because of it.
	dmSeenCap    = 512
	dmSeenWindow = 10 * time.Minute
	// dmBurst / dmBurstWindow bound the automatic replies of one sender.
	dmBurst       = 6
	dmBurstWindow = time.Minute
	// dmBannerInterval paces the banner answer to a plain message: at
	// most one per sender per interval.
	dmBannerInterval = 5 * time.Minute
	// dmPaceCap bounds the per-sender pacing maps.
	dmPaceCap = 256
	// radioAlertFallbackTTL mirrors the /alert default expiry when the
	// durable inbox holds no time anchor for the command.
	radioAlertFallbackTTL = 4 * time.Hour
)

// replyWindow bounds one sender's automatic replies.
type replyWindow struct {
	start time.Time
	count int
}

// seams returns the wired command interpreter and sender gate.
func (l *dmListener) seams() (*radiocli.Bot, func(userID string) string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.cli, l.gate
}

// handleText answers one direct message: a slash text runs the shared
// command interpreter, anything else gets the installation banner. The
// interpreter decides on authorization — public commands run for every
// sender, restricted ones only for senders the directory resolves.
func (l *dmListener) handleText(ctx context.Context, userID, channelID, text string) {
	cli, gate := l.seams()
	if cli == nil {
		l.logger.Debug("discord: no command interpreter installed — direct message ignored", "user", userID)
		return
	}
	text = strings.TrimSpace(text)
	if text == "" {
		return
	}
	owner := ""
	if gate != nil {
		owner = gate(userID)
	}
	reply := ""
	cmd := commandName(text)
	if !strings.HasPrefix(text, "/") {
		// Not a command: answer with the banner so whoever wrote to the
		// station learns what they reached — paced, so a chatty
		// conversation cannot make the station repeat itself.
		if !l.bannerAllowed(userID) {
			l.logger.Debug("discord: banner suppressed", "user", userID, "reason", "interval")
			return
		}
		reply = cli.Banner()
	} else {
		res := cli.Handle(text, radiocli.Sender{Authorized: owner != "", Name: owner})
		if res.Alert != nil {
			l.publishAlert(ctx, userID, owner, text, res.Reply, res.Alert)
		}
		reply = res.Reply
	}
	if reply == "" {
		l.logger.Debug("discord: command answered silently", "user", userID, "cmd", cmd)
		return
	}
	if !l.replyAllowed(userID) {
		l.logger.Debug("discord: reply suppressed", "user", userID, "cmd", cmd, "reason", "burst")
		return
	}
	if err := l.action.replyDM(ctx, userID, channelID, cli.Fit(reply, maxContentRunes)); err != nil {
		l.logger.Warn("discord: direct-message reply failed", "user", userID, "cmd", cmd, "error", err)
		return
	}
	l.logger.Info("discord: command reply sent", "user", userID, "owner", owner, "cmd", cmd)
}

// commandName extracts the command word of a slash text for the logs.
func commandName(text string) string {
	t := strings.TrimPrefix(strings.TrimSpace(text), "/")
	if i := strings.IndexByte(t, ' '); i >= 0 {
		t = t[:i]
	}
	return strings.ToLower(strings.TrimSpace(t))
}

// replyAllowed paces the automatic replies of one sender (command
// answers, banners, denials): a chatty conversation must not turn into a
// flood, exactly like the radio channels' burst limiter.
func (l *dmListener) replyAllowed(userID string) bool {
	now := l.now()
	l.mu.Lock()
	defer l.mu.Unlock()
	w := l.bursts[userID]
	if now.Sub(w.start) >= dmBurstWindow {
		w = replyWindow{}
	}
	if w.count >= dmBurst {
		return false
	}
	if w.start.IsZero() {
		w.start = now
	}
	w.count++
	l.bursts[userID] = w
	l.prunePacing(now)
	return true
}

// bannerAllowed paces the banner answer of one plain message.
func (l *dmListener) bannerAllowed(userID string) bool {
	now := l.now()
	l.mu.Lock()
	defer l.mu.Unlock()
	if last, ok := l.banners[userID]; ok && now.Sub(last) < dmBannerInterval {
		return false
	}
	l.banners[userID] = now
	l.prunePacing(now)
	return true
}

// prunePacing drops stale pacing entries once a map grows past its cap.
// The caller holds the lock.
func (l *dmListener) prunePacing(now time.Time) {
	if len(l.bursts) > dmPaceCap {
		cutoff := now.Add(-dmBurstWindow)
		for k, w := range l.bursts {
			if w.start.Before(cutoff) {
				delete(l.bursts, k)
			}
		}
	}
	if len(l.banners) > dmPaceCap {
		cutoff := now.Add(-dmBannerInterval)
		for k, at := range l.banners {
			if at.Before(cutoff) {
				delete(l.banners, k)
			}
		}
	}
}

// publishAlert turns a command alert into the canonical /events document
// the rest of the station routes, so the source, the severity and the
// durable inbox behave identically no matter which channel the operator
// typed /alert on.
func (l *dmListener) publishAlert(ctx context.Context, userID, owner, text, commandResult string, spec *radiocli.AlertSpec) {
	if spec == nil {
		return
	}
	l.mu.Lock()
	acceptor := l.acceptor
	times := l.times
	l.mu.Unlock()
	key := "discord:" + userID + ":alert:" + radiocli.ContentID(text)
	ttl := spec.TTL
	if ttl <= 0 {
		ttl = radioAlertFallbackTTL
	}
	eff, exp, ok := l.commandTimes(ctx, times, key)
	if !ok {
		eff = l.now().UTC()
		exp = eff.Add(ttl)
	}
	effS := eff.Format(time.RFC3339)
	expS := exp.Format(time.RFC3339)
	raisedBy := owner
	if raisedBy == "" {
		raisedBy = "Discord user " + userID
	}
	doc := MessageEventWire{
		SchemaVersion: messageEventSchemaVersion,
		ChangeID:      radiocli.StableID("discord:alert", key, spec.Headline),
		ChangeType:    "new",
		EventKey:      key,
		CommandResult: commandResult,
		Event: MessageEventHazard{
			Source:      "discord",
			SourceID:    key,
			Event:       "Discord alert",
			Severity:    "severe",
			Urgency:     "immediate",
			Certainty:   "observed",
			Headline:    spec.Headline,
			Description: "Alert raised by " + raisedBy + " over Discord",
			EffectiveAt: &effS,
			ExpiresAt:   &expS,
			Areas:       []string{},
			Status:      "active",
			ReceivedAt:  effS,
			UpdatedAt:   effS,
		},
	}
	payload, err := json.Marshal(doc)
	if err != nil {
		l.logger.Warn("discord: alert marshal failed", "error", err)
		return
	}
	if acceptor == nil {
		l.logger.Warn("discord: alert dropped — no event acceptor installed", "id", key)
		return
	}
	if acc := acceptor(payload); acc == dispatch.Rejected {
		l.logger.Warn("discord: alert rejected", "id", key, "headline", spec.Headline)
		return
	}
	l.logger.Info("discord: alert raised from a direct message", "id", key, "owner", owner, "user", userID)
}

// commandTimes resolves the durable time anchor the inbox already holds
// for a command key (a repeated command), or reports ok=false so the
// caller stamps a fresh anchor from its own clock.
func (l *dmListener) commandTimes(ctx context.Context, fn func(context.Context, string) (time.Time, time.Time, string, bool), key string) (time.Time, time.Time, bool) {
	if fn == nil {
		return time.Time{}, time.Time{}, false
	}
	callCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	eff, exp, _, ok := fn(callCtx, key)
	if !ok || eff.IsZero() || exp.IsZero() {
		return time.Time{}, time.Time{}, false
	}
	return eff, exp, true
}
