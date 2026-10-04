// Package meshtastic implements the built-in meshtastic outbound action: it
// sends SOSNA alerts as Meshtastic channel text messages through the shared
// mesh hub (the Companion serial link).
package meshtastic

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/szporwolik/WarnFlux/internal/action"
	"github.com/szporwolik/WarnFlux/internal/dispatch"
	mesh "github.com/szporwolik/WarnFlux/internal/meshtastic"
	"github.com/szporwolik/WarnFlux/internal/sanity"
)

// Type is the action type name used in the YAML configuration.
const Type = "meshtastic"

// maxMeshMessageChars bounds one channel message (133 chars per the
// Meshtastic spec).
const maxMeshMessageChars = 133

// Config is the action-specific configuration.
type Config struct {
	// Prefix is prepended to every message (e.g. the system name).
	Prefix string `yaml:"prefix"`
	// TxInterval is the minimum spacing between two transmissions of this
	// action (default 5s) — the LoRa channel is shared.
	TxInterval time.Duration `yaml:"tx_interval"`
	// Channel is the group channel index (1-7) every alert is published
	// on — the base delivery, whether or not any user registered a node
	// ID. 0 = off: only direct messages to registered node IDs go out
	// (the PRIMARY channel is never used).
	Channel int `yaml:"channel"`
}

var errHubDisabled = errors.New("meshtastic: hub is not configured")

// sender is the hub seam the action transmits through: direct messages
// to the routed group members' node IDs, or a channel broadcast on the
// configured fallback channel. *mesh.Hub satisfies it; tests use a stub.
type sender interface {
	SendContactMessage(ctx context.Context, addr, text, operator string) error
	SendChannelText(ctx context.Context, idx int, text, operator string) error
	// SendContactMessageVersioned sends the direct message and ties it
	// to the durable action-progress ledger: the hub records the
	// progress when the radio actually transmitted (sent/delivered)
	// and REVOKES it when the transmission ultimately failed — a
	// failed send stays retryable. The action id scopes the failure
	// marker and the re-arm to THIS action's jobs.
	SendContactMessageVersioned(ctx context.Context, addr, text, operator, actionID, publisher, eventKey string, changeID int64) error
	// SendChannelTextVersioned is the same for the group broadcast.
	SendChannelTextVersioned(ctx context.Context, idx int, text, operator, actionID, publisher, eventKey string, changeID int64) error
	// MeshActionProgressDone reports whether the exact versioned
	// transmission (publisher + event key + change id + recipient +
	// channel) already succeeded — the durable resume ledger of the
	// action, independent of the message history.
	MeshActionProgressDone(ctx context.Context, publisher, eventKey string, changeID int64, recipient string, channel int) (bool, error)
}

// Action sends SOSNA alerts as direct messages to the routed group
// members' node IDs and publishes every alert on the configured group
// channel (the base delivery — it reaches listeners without accounts
// too). The hub records each transmission in the durable history and
// tracks its delivery state (sent/delivered/failed).
type Action struct {
	cfg  Config
	hub  sender
	last time.Time
}

// Register wires the action type into the action registry.
func Register(reg *action.Registry, hub *mesh.Hub) error {
	return reg.Register(Type, func(node *yaml.Node) (action.Plugin, error) {
		var cfg Config
		if node != nil {
			if err := node.Decode(&cfg); err != nil {
				return nil, err
			}
		}
		if hub == nil {
			return nil, errHubDisabled
		}
		if cfg.TxInterval <= 0 {
			cfg.TxInterval = 5 * time.Second
		}
		return &Action{cfg: cfg, hub: hub}, nil
	})
}

func (a *Action) Name() string { return Type }

// Execute formats the routed hazard as one message and delivers it to
// every routed group member's registered node ID (direct message), and
// publishes it on the configured group channel whenever one is set —
// the channel broadcast goes out even when no member registered a node
// ID.
//
// The whole group shares one bounded call deadline, so a large group can
// exceed it mid-list. Every transmission is tied to a durable per-VERSION
// progress entry (publisher + event key + change id + recipient) that
// follows the TRANSMISSION OUTCOME: the hub records it when the radio
// actually transmitted (sent/delivered) and revokes it when the send
// ultimately failed, so a retry resumes exactly where the previous
// attempt stopped — recipients and the channel broadcast already
// transmitted are skipped, only the unfinished (or failed) sends go out
// again (reported P1: with a whole-group retry restart, early
// recipients received repeated alerts and the last one never got any;
// a failed transmission must stay retryable, reported P1). The ledger
// is keyed by the message VERSION, never by the text: a NEW alert whose
// text matches a historical message still transmits (reported P1).
func (a *Action) Execute(ctx context.Context, req action.ActionRequest) error {
	if req.Event.Kind != dispatch.EventHazardTransition || req.Event.Hazard == nil {
		return nil // nothing to say for non-hazard events
	}
	text := a.textFor(ctx, req)

	// The durable ledger identity: the message version. Legacy payloads
	// without a publisher/version get no ledger (fail-open: always
	// send, at-least-once).
	var pub, key string
	var ver int64
	if req.Event.Hazard != nil {
		pub, key, ver = req.Event.Hazard.Publisher, req.Event.Hazard.Key, req.Event.Hazard.ChangeID
	}
	actionID := action.RequestActionID(req)
	ledger := actionID != "" && pub != "" && ver > 0
	done := func(recipient string, channel int) bool {
		if !ledger {
			return false
		}
		d, err := a.hub.MeshActionProgressDone(ctx, pub, key, ver, recipient, channel)
		if err != nil {
			return false // ledger unreadable: fail-open, deliver again
		}
		return d
	}

	// The group channel is the base delivery: every alert is published
	// there when configured, with or without registered node IDs. The
	// durable ledger skips a broadcast an earlier attempt already got
	// out successfully.
	if a.cfg.Channel > 0 {
		if !done("", a.cfg.Channel) {
			if err := a.pace(ctx); err != nil {
				return err
			}
			var err error
			if ledger {
				err = a.hub.SendChannelTextVersioned(ctx, a.cfg.Channel, text, "system", actionID, pub, key, ver)
			} else {
				err = a.hub.SendChannelText(ctx, a.cfg.Channel, text, "system")
			}
			if err != nil {
				return fmt.Errorf("meshtastic: %w", err)
			}
			a.last = time.Now()
		}
	}

	// Registered node IDs additionally get a direct message with
	// per-recipient delivery tracking. Recipients whose exact message
	// version already has a progress row are skipped: only the
	// unfinished sends are retried, so every member is reached exactly
	// once across attempts.
	for _, id := range req.MeshNodeIDs {
		if done(id, 0) {
			continue
		}
		if err := a.pace(ctx); err != nil {
			return err
		}
		var err error
		if ledger {
			err = a.hub.SendContactMessageVersioned(ctx, id, text, "system", actionID, pub, key, ver)
		} else {
			err = a.hub.SendContactMessage(ctx, id, text, "system")
		}
		if err != nil {
			return fmt.Errorf("meshtastic: %s: %w", id, err)
		}
		a.last = time.Now()
	}
	return nil
}

// textFor builds the bounded outbound message for one routed hazard:
// prefix + severity + headline first, the LOCATION whenever the hazard
// carries coordinates or areas (the headline gives up space first, so
// the title and the location survive together), then the event type and
// the description fill whatever room is left. Worst case the text is cut
// hard at the channel limit — the prefix and severity are never
// sacrificed.
func (a *Action) textFor(ctx context.Context, req action.ActionRequest) string {
	h := req.Event.Hazard.Hazard
	norm := func(s string) string {
		s = sanity.NormalizeText(ctx, sanity.ChannelAPRS, s)
		return strings.Join(strings.Fields(s), " ")
	}

	sev := strings.ToUpper(strings.TrimSpace(h.Severity))
	headline := norm(h.Headline)
	if headline == "" {
		headline = norm(h.Event)
	}
	event := norm(h.Event)
	if event == headline {
		event = "" // the headline already carries the event name
	}
	desc := norm(h.Description)

	// Location: exact coordinates first, the area list otherwise.
	var loc string
	switch {
	case h.Latitude != nil && h.Longitude != nil:
		loc = fmt.Sprintf("%.3f,%.3f", *h.Latitude, *h.Longitude)
	case len(h.Areas) > 0:
		loc = norm(strings.Join(h.Areas, ", "))
	}

	head := strings.TrimSpace(a.cfg.Prefix)
	if sev != "" {
		if head != "" {
			head += " "
		}
		head += sev
	}
	withHead := func(s string) string {
		if head == "" {
			return s
		}
		return head + " " + s
	}
	over := func(s string) int { return len([]rune(s)) - maxMeshMessageChars }
	cut := func(s string) string {
		s = norm(s)
		r := []rune(s)
		if len(r) > maxMeshMessageChars {
			return string(r[:maxMeshMessageChars])
		}
		return s
	}

	base := withHead(headline)
	// The location always rides along when present: the headline gives
	// up space first (the fixed part — prefix, severity and location —
	// never does), even when the headline alone would already overflow
	// the channel limit.
	if loc != "" {
		// The empty-headline base is the exact formula below with an
		// empty title, so the computed room never loses a rune to a
		// separator mismatch.
		fixed := withHead("") + " | " + loc
		if over(fixed) > 0 {
			return cut(fixed)
		}
		room := maxMeshMessageChars - len([]rune(fixed))
		hs := []rune(headline)
		if len(hs) > room {
			hs = hs[:room]
		}
		base = withHead(string(hs)) + " | " + loc
	} else if over(base) > 0 {
		return cut(base)
	}
	if event != "" {
		if b, ok := appendFit(base, event); ok {
			base = b
		}
	}
	if desc != "" {
		base, _ = appendFit(base, desc)
	}
	return cut(base)
}

// appendFit appends extra to base with the " | " separator when it fits;
// otherwise it appends the truncated prefix of extra marked with "..."
// and reports false (the channel limit is hard).
func appendFit(base, extra string) (string, bool) {
	const sep = " | "
	if len([]rune(base))+len([]rune(sep))+len([]rune(extra)) <= maxMeshMessageChars {
		return base + sep + extra, true
	}
	room := maxMeshMessageChars - len([]rune(base)) - len([]rune(sep))
	if room <= 0 {
		return base, false
	}
	ex := []rune(extra)
	if room >= len(ex) {
		return base + sep + extra, true
	}
	if room <= 3 {
		return base + sep + string(ex[:room]), false
	}
	return base + sep + string(ex[:room-3]) + "...", false
}

// pace waits out the minimum spacing between transmissions; the mesh
// channel is shared airtime.
func (a *Action) pace(ctx context.Context) error {
	if wait := a.cfg.TxInterval - time.Since(a.last); wait > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
	}
	return nil
}

// Close releases nothing (the hub owns the serial link).
func (a *Action) Close(ctx context.Context) error { return nil }

var _ action.Plugin = (*Action)(nil)
