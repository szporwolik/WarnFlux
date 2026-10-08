// Package sms implements the built-in SMS action: it sends alerts as
// SMS text messages through the shared GSM hub (the serial AT session to
// the cellular modem). The message travels over the cellular network —
// a fully local channel, like APRS radio and Meshtastic.
package sms

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/szporwolik/WarnFlux/internal/action"
	"github.com/szporwolik/WarnFlux/internal/gsm"
	"github.com/szporwolik/WarnFlux/internal/i18n"
	"github.com/szporwolik/WarnFlux/internal/sanity"
)

// Type is the action type name used in the YAML configuration.
const Type = "sms"

// maxRecipients bounds the static recipient list.
const maxRecipients = 32

// maxSMSChars bounds one message (text mode, single segment; the hub
// transliterates and refuses longer texts).
const maxSMSChars = 160

// Config is the action-specific configuration.
type Config struct {
	// Phones lists the phone numbers that receive the notification
	// (besides the matched group members' registered numbers).
	Phones []string `yaml:"phones"`
	// Prefix is prepended to every message (e.g. the system name).
	Prefix string `yaml:"prefix"`
}

// sender is the hub seam the action transmits through: the modem
// session must be enabled and able to send one SMS. *gsm.Hub satisfies
// it; tests use a stub.
type sender interface {
	Enabled() bool
	Send(ctx context.Context, number, text string) error
}

// Action sends alerts as SMS to the configured phones plus the matched
// group members' registered phone numbers (de-duplicated by digits).
type Action struct {
	cfg Config
	hub sender
}

// Register wires the action type into the action registry.
func Register(reg *action.Registry, hub *gsm.Hub) error {
	return reg.Register(Type, func(node *yaml.Node) (action.Plugin, error) {
		return New(node, hub)
	})
}

// New builds one SMS action instance from its raw YAML configuration.
func New(node *yaml.Node, hub sender) (action.Plugin, error) {
	if hub == nil || !hub.Enabled() {
		return nil, errors.New("sms: the GSM hub is disabled (set gsm.enabled: true)")
	}
	var cfg Config
	if node != nil {
		if err := node.Decode(&cfg); err != nil {
			return nil, fmt.Errorf("sms: decode config: %w", err)
		}
	}
	if len(cfg.Phones) == 0 {
		return nil, errors.New("sms: config.phones must contain at least one phone number")
	}
	if len(cfg.Phones) > maxRecipients {
		return nil, fmt.Errorf("sms: config.phones has %d recipients, maximum %d", len(cfg.Phones), maxRecipients)
	}
	for i, p := range cfg.Phones {
		cfg.Phones[i] = strings.TrimSpace(p)
		if !gsm.ValidNumber(cfg.Phones[i]) {
			return nil, fmt.Errorf("sms: phone %q is not a valid phone number", p)
		}
	}
	return &Action{cfg: cfg, hub: hub}, nil
}

func (a *Action) Name() string { return Type }

func (a *Action) Close(context.Context) error { return nil }

// Execute sends one SMS per recipient: the configured phones plus the
// matched group's members' registered phones (from the rule engine,
// de-duplicated). Each recipient gets the message in their own
// notification language when they picked one.
func (a *Action) Execute(ctx context.Context, req action.ActionRequest) error {
	type recipient struct {
		phone string
		lang  string
	}
	seen := make(map[string]bool, len(a.cfg.Phones)+len(req.Phones))
	var recipients []recipient
	add := func(phone, lang string) {
		key := gsm.NumberKey(phone)
		if key == "" || seen[key] {
			return
		}
		seen[key] = true
		recipients = append(recipients, recipient{phone: phone, lang: lang})
	}
	for _, p := range a.cfg.Phones {
		add(p, req.Lang)
	}
	for i, p := range req.Phones {
		lang := req.Lang
		if i < len(req.PhoneLangs) && req.PhoneLangs[i] != "" {
			lang = req.PhoneLangs[i]
		}
		add(p, lang)
	}
	var firstErr error
	failed := 0
	for _, r := range recipients {
		text := a.textFor(ctx, req, r.lang)
		if err := a.hub.Send(ctx, r.phone, text); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			failed++
		}
	}
	if firstErr != nil {
		return fmt.Errorf("sms: %d of %d messages failed (first: %w)", failed, len(recipients), firstErr)
	}
	return nil
}

// textFor builds the bounded outbound message for one routed hazard:
// prefix + severity + headline first, then the location whenever the
// hazard carries coordinates or areas, then the short message ID. The
// text is transliterated and hard-capped at the SMS limit.
func (a *Action) textFor(ctx context.Context, req action.ActionRequest, lang string) string {
	h := req.Event.Hazard.Hazard
	lang = i18n.Effective(lang)
	t := h.For(lang)
	norm := func(s string) string {
		s = sanity.NormalizeText(ctx, sanity.ChannelAPRS, s)
		return strings.Join(strings.Fields(s), " ")
	}
	sev := strings.ToUpper(i18n.T(lang, "severity."+strings.ToLower(strings.TrimSpace(h.Severity))))
	headline := norm(t.Headline)
	if headline == "" {
		headline = norm(h.Event)
	}
	head := strings.TrimSpace(a.cfg.Prefix)
	if sev != "" {
		if head != "" {
			head += " "
		}
		head += sev
	}
	base := headline
	if head != "" {
		base = head + " " + headline
	}
	var loc string
	switch {
	case h.Latitude != nil && h.Longitude != nil:
		loc = fmt.Sprintf("%.3f,%.3f", *h.Latitude, *h.Longitude)
	case len(h.Areas) > 0:
		loc = norm(strings.Join(h.Areas, ", "))
	}
	if loc != "" {
		base += " | " + loc
	}
	if id := h.MessageID(); id != "" {
		base += " [" + id + "]"
	}
	base = gsm.TransliterateGSM7(base)
	if r := []rune(base); len(r) > maxSMSChars {
		return string(r[:maxSMSChars])
	}
	return base
}
