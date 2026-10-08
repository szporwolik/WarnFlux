package gsm

// The SMS command path mirrors the APRS and Meshtastic hubs: slash
// commands run through the shared radiocli interpreter, plain texts
// answer with the installation banner, restricted commands work only
// for directory-registered senders (by phone number), and /debug /
// /alert events flow into the normal routing pipeline through the same
// canonical /events documents.

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/szporwolik/WarnFlux/internal/dispatch"
	"github.com/szporwolik/WarnFlux/internal/radiocli"
)

// messageEventSchemaVersion mirrors mqttreceiver.WireSchemaVersion
// (both live on the same /events stream, so the version must never
// drift).
const messageEventSchemaVersion = 1

// MessageEventWire is the /events envelope for one routed SMS.
type MessageEventWire struct {
	SchemaVersion int                `json:"schema_version"`
	ChangeID      int64              `json:"change_id"`
	ChangeType    string             `json:"change_type"`
	EventKey      string             `json:"event_key"`
	Event         MessageEventHazard `json:"event"`
	// CommandResult carries the in-band confirmation this command
	// produced; the durable inbox acceptance records it together with
	// the lifecycle anchor, so a post-restart replay of the same text
	// replays the result instead of re-executing the job.
	CommandResult string `json:"command_result,omitempty"`
}

// MessageEventHazard is the hazard block of one routed SMS.
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

// cmdRecord is the remembered outcome of one SMS command, keyed by
// sender + content identity: a repeated text replays the reply and
// never re-executes the command.
type cmdRecord struct {
	at        time.Time
	reply     string
	retryable bool
}

// replyWindow bounds one sender's automatic replies.
type replyWindow struct {
	start time.Time
	count int
}

// cmdDedupWindow is how long a command identity stays remembered: a
// repeated text within it replays the previous reply instead of
// executing the command again.
const cmdDedupWindow = 10 * time.Minute

// cmdDedupCap bounds the remembered command outcomes.
const cmdDedupCap = 256

// bannerMinInterval is the per-sender spacing of automatic banner
// replies (plain texts): a chatty phone cannot make the station spam.
const bannerMinInterval = 5 * time.Minute

// bannerGlobalInterval is the global pacing of banner replies: the
// station answers at most one plain text per interval overall.
const bannerGlobalInterval = 10 * time.Second

// replyBurst / replyBurstWindow bound EVERY automatic reply per sender
// (command answers, banners, denials): at most replyBurst replies per
// replyBurstWindow per phone.
const (
	replyBurst     = 5
	replyBurstWin  = time.Minute
	replySendWait  = 90 * time.Second
	maxSMSReplyLen = 160 // one text-mode SMS, after transliteration
)

// isRCBSender reports whether the SMS originator is the national Alert
// RCB broadcast sender (alphanumeric "ALERT RCB", matching the
// operator delivery; case/spacing-insensitive).
func isRCBSender(from string) bool {
	f := strings.ToUpper(strings.Join(strings.Fields(from), " "))
	return f == "ALERT RCB" || f == "ALERT-RCB"
}

// rcbEventTTL bounds the generated hazard: an Alert RCB stays active as
// a 24-hour communication.
const rcbEventTTL = 24 * time.Hour

// routeMessage handles one received SMS: Alert RCB broadcasts become a
// 24-hour severe "rcb" hazard through the standard routing pipeline
// (never answered — the sender is a broadcast short code), slash texts
// go to the shared radio CLI and plain texts answer with the
// installation banner (rate-limited). Replies fit one SMS and are sent
// fire-and-forget.
func (h *Hub) routeMessage(from, text string) {
	if isRCBSender(from) {
		h.publishRCBEvent(from, text)
		return
	}
	h.mu.Lock()
	cli := h.cli
	h.mu.Unlock()
	if cli == nil {
		return
	}
	owner := h.ownerOf(from)
	t := strings.TrimSpace(text)
	if !strings.HasPrefix(t, "/") {
		if h.bannerAllowed(from) && h.replyAllowed(from) {
			h.sendReply(from, cli.Banner())
		}
		return
	}
	cid := from + ":" + radiocli.ContentID(t)
	now := h.now()
	h.mu.Lock()
	rec := h.cmds[cid]
	fresh := rec != nil && now.Sub(rec.at) < cmdDedupWindow
	recReply := ""
	if fresh {
		recReply = rec.reply
	}
	h.mu.Unlock()
	if fresh {
		if recReply != "" && h.replyAllowed(from) {
			h.sendReply(from, recReply)
		}
		return
	}
	res := cli.Handle(t, owner != "")
	// The stored reply is the FINAL confirmation (post-acceptance), so
	// a repeated text replays exactly the previous result. A durable
	// registry hit (stored result) proves the job executed before — the
	// repeat replays the result and never re-executes it, even across a
	// restart.
	reply := res.Reply
	if res.Debug {
		key := "gsm:" + from + ":msg:" + radiocli.ContentID(t)
		eff, exp, prev := h.resolveCommand(key, time.Hour)
		if prev != "" {
			reply = prev
		} else {
			acc := h.publishMessageEvent(from, t, eff, exp, res.Reply)
			reply = confirmation(res.Reply, "FAILED: debug alarm rejected", acc)
		}
	}
	if res.Alert != nil {
		ttl := res.Alert.TTL
		if ttl <= 0 {
			ttl = 4 * time.Hour
		}
		key := "gsm:" + from + ":alert:" + radiocli.ContentID(t)
		eff, exp, prev := h.resolveCommand(key, ttl)
		if prev != "" {
			reply = prev
		} else {
			acc := h.publishAlertEvent(from, t, res.Alert, eff, exp, res.Reply)
			reply = confirmation(res.Reply, "FAILED: alert rejected", acc)
		}
	}
	h.mu.Lock()
	h.pruneCmds(now)
	h.cmds[cid] = &cmdRecord{at: now, reply: reply}
	h.mu.Unlock()
	if res.Handled && reply != "" && h.replyAllowed(from) {
		h.sendReply(from, reply)
	}
}

// ownerOf resolves the sender's directory username through the
// installed allow-list gate ("" = unknown sender).
func (h *Hub) ownerOf(from string) string {
	h.mu.Lock()
	gate := h.senderGate
	h.mu.Unlock()
	if gate == nil {
		return ""
	}
	return gate(from)
}

// sendReply transmits one automatic SMS reply, fitted into a single
// message. Fire-and-forget: a slow network accept must never stall
// message polling.
func (h *Hub) sendReply(to, text string) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), replySendWait)
		defer cancel()
		if err := h.Send(ctx, to, fitReply(text)); err != nil {
			h.logger.Warn("gsm: reply send failed", "to", to, "error", err)
		}
	}()
}

// fitReply renders one automatic reply so it always fits a single
// text-mode SMS: transliteration first (the modem cannot send PDU),
// then a hard 160-character cap.
func fitReply(text string) string {
	text = TransliterateGSM7(text)
	if r := []rune(text); len(r) > maxSMSReplyLen {
		return string(r[:maxSMSReplyLen])
	}
	return text
}

// publishRCBEvent converts one received Alert RCB SMS into a canonical
// /events document (the "rcb" source): severe, active for 24 hours,
// routed through the standard matrix like every other hazard. The event
// identity is stable per message content, so a redelivered SMS maps to
// the same hazard. No automatic reply is ever sent to the broadcast
// sender.
func (h *Hub) publishRCBEvent(from, text string) {
	text = strings.TrimSpace(text)
	if text == "" {
		return
	}
	now := h.now().UTC()
	nowS := now.Format(time.RFC3339)
	expS := now.Add(rcbEventTTL).Format(time.RFC3339)
	sourceID := from + ":msg:" + radiocli.ContentID(text)
	doc := MessageEventWire{
		SchemaVersion: messageEventSchemaVersion,
		ChangeID:      radiocli.StableID("rcb:msg", sourceID, text),
		ChangeType:    "new",
		EventKey:      "rcb:" + sourceID,
		Event: MessageEventHazard{
			Source:      "rcb",
			SourceID:    sourceID,
			Event:       "Alert RCB",
			Severity:    "severe",
			Urgency:     "immediate",
			Certainty:   "observed",
			Headline:    "Alert RCB: " + text,
			Description: "Alert RCB received over SMS (" + from + ")",
			EffectiveAt: &nowS,
			ExpiresAt:   &expS,
			Areas:       []string{},
			Status:      "active",
			ReceivedAt:  nowS,
			UpdatedAt:   nowS,
		},
	}
	payload, err := json.Marshal(doc)
	if err != nil {
		h.logger.Warn("gsm: rcb event marshal failed", "error", err)
		return
	}
	h.mu.Lock()
	acceptor := h.eventAcceptor
	h.mu.Unlock()
	if acceptor == nil {
		h.logger.Warn("gsm: rcb event dropped — no event acceptor installed", "from", from)
		return
	}
	h.logger.Info("gsm: alert rcb received", "from", from)
	_ = acceptor(payload)
}

// confirmation maps the local acceptance of a command event onto the
// in-band confirmation: durable acceptance answers with the handler
// confirmation, the emergency fallback says so explicitly, and a
// rejection answers with the given failure text — never a false "OK".
func confirmation(base, failure string, acc dispatch.Acceptance) string {
	switch acc {
	case dispatch.Rejected:
		return failure
	case dispatch.AcceptedEmergency:
		if base != "" {
			return base + " (failover mode)"
		}
	}
	return base
}

// resolveCommand returns the durable command-registry entry for one
// command key: the lifecycle anchor (effective / expires) and the
// recorded result of a previous durable acceptance. On a miss (or
// without a resolver) the command starts its lifecycle fresh at
// now / now+ttl with no stored result.
func (h *Hub) resolveCommand(key string, ttl time.Duration) (eff, exp time.Time, result string) {
	h.mu.Lock()
	resolver := h.eventTimes
	h.mu.Unlock()
	if resolver != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		eff, exp, result, ok := resolver(ctx, key)
		cancel()
		if ok {
			return eff, exp, result
		}
	}
	eff = time.Now().UTC()
	return eff, eff.Add(ttl), ""
}

// bannerAllowed reports whether a plain-text banner may go out now:
// per-sender minimum spacing plus a global pacing interval, so
// answering loops between two stations cannot amplify.
func (h *Hub) bannerAllowed(from string) bool {
	now := h.now()
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.bannerNext.IsZero() && now.Before(h.bannerNext) {
		return false
	}
	if last, ok := h.bannerLast[from]; ok && now.Sub(last) < bannerMinInterval {
		return false
	}
	h.bannerLast[from] = now
	h.bannerNext = now.Add(bannerGlobalInterval)
	if len(h.bannerLast) > 256 {
		cutoff := now.Add(-bannerMinInterval)
		for k, at := range h.bannerLast {
			if at.Before(cutoff) {
				delete(h.bannerLast, k)
			}
		}
	}
	return true
}

// replyAllowed reports whether ONE automatic reply may go out to the
// sender now. Every automatic answer — command confirmations, repeats,
// /help, denials and banners — shares one per-sender burst window, so a
// flooding sender cannot make the station chatter.
func (h *Hub) replyAllowed(from string) bool {
	now := h.now()
	h.mu.Lock()
	defer h.mu.Unlock()
	w := h.replyLast[from]
	if now.Sub(w.start) >= replyBurstWin {
		w = replyWindow{}
	}
	if w.count >= replyBurst {
		return false
	}
	if w.start.IsZero() {
		w.start = now
	}
	w.count++
	h.replyLast[from] = w
	if len(h.replyLast) > 256 {
		cutoff := now.Add(-replyBurstWin)
		for k, rw := range h.replyLast {
			if rw.start.Before(cutoff) {
				delete(h.replyLast, k)
			}
		}
	}
	return true
}

// pruneCmds drops expired command memories and caps the map.
func (h *Hub) pruneCmds(now time.Time) {
	cutoff := now.Add(-cmdDedupWindow)
	for k, rec := range h.cmds {
		if rec.at.Before(cutoff) {
			delete(h.cmds, k)
		}
	}
	for len(h.cmds) > cmdDedupCap {
		for k := range h.cmds {
			delete(h.cmds, k)
			break
		}
	}
}

// publishMessageEvent re-publishes one /debug SMS as a canonical
// /events payload (the "gsm" source) so it enters the normal routing
// pipeline. The event identity is stable per message (sender + content
// digest), lifecycle times come from the durable registry and the
// result travels on the wire document for the transactional inbox
// anchor. The returned acceptance reports how the LOCAL pipeline took
// the event.
func (h *Hub) publishMessageEvent(from, text string, eff, exp time.Time, commandResult string) dispatch.Acceptance {
	nowS := eff.Format(time.RFC3339)
	expS := exp.Format(time.RFC3339)
	sourceID := from + ":msg:" + radiocli.ContentID(text)
	doc := MessageEventWire{
		SchemaVersion: messageEventSchemaVersion,
		ChangeID:      radiocli.StableID("gsm:msg", sourceID, text),
		ChangeType:    "new",
		EventKey:      "gsm:" + sourceID,
		CommandResult: commandResult,
		Event: MessageEventHazard{
			Source:      "gsm",
			SourceID:    sourceID,
			Event:       "SMS message",
			Severity:    "severe",
			Urgency:     "unknown",
			Certainty:   "unknown",
			Headline:    "Message from: " + from + ": " + text,
			Description: "Received by the WarnFlux GSM station",
			EffectiveAt: &nowS,
			ExpiresAt:   &expS,
			Areas:       []string{},
			Status:      "active",
			ReceivedAt:  nowS,
			UpdatedAt:   nowS,
		},
	}
	payload, err := json.Marshal(doc)
	if err != nil {
		h.logger.Warn("gsm: routed message marshal failed", "error", err)
		return dispatch.Rejected
	}
	h.mu.Lock()
	acceptor := h.eventAcceptor
	h.mu.Unlock()
	if acceptor != nil {
		return acceptor(payload)
	}
	return dispatch.AcceptedDurable
}

// publishAlertEvent raises one operator-requested hazard (/alert SMS)
// into the /events stream: severe, bounded by the requested TTL (4
// hours by default), with the same stable identity and durable registry
// scheme as publishMessageEvent.
func (h *Hub) publishAlertEvent(from, text string, spec *radiocli.AlertSpec, eff, exp time.Time, commandResult string) dispatch.Acceptance {
	if spec == nil {
		return dispatch.Rejected
	}
	nowS := eff.Format(time.RFC3339)
	expS := exp.Format(time.RFC3339)
	sourceID := from + ":alert:" + radiocli.ContentID(text)
	doc := MessageEventWire{
		SchemaVersion: messageEventSchemaVersion,
		ChangeID:      radiocli.StableID("gsm:alert", sourceID, spec.Headline),
		ChangeType:    "new",
		EventKey:      "gsm:" + sourceID,
		CommandResult: commandResult,
		Event: MessageEventHazard{
			Source:      "gsm",
			SourceID:    sourceID,
			Event:       "SMS alert",
			Severity:    "severe",
			Urgency:     "immediate",
			Certainty:   "observed",
			Headline:    spec.Headline,
			Description: "Alert raised by " + from + " over SMS",
			EffectiveAt: &nowS,
			ExpiresAt:   &expS,
			Areas:       []string{},
			Status:      "active",
			ReceivedAt:  nowS,
			UpdatedAt:   nowS,
		},
	}
	payload, err := json.Marshal(doc)
	if err != nil {
		h.logger.Warn("gsm: alert marshal failed", "error", err)
		return dispatch.Rejected
	}
	h.mu.Lock()
	acceptor := h.eventAcceptor
	h.mu.Unlock()
	if acceptor != nil {
		return acceptor(payload)
	}
	return dispatch.AcceptedDurable
}
