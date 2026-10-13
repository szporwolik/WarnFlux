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
	"sync/atomic"

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

// maxSMSChars bounds one message: text mode, single segment. The budget
// is in SEPTETS, not characters (gsm.MaxSeptets): the extension-table
// symbols cost two, and the formatter must fit the result rather than let
// the hub refuse it.
const maxSMSChars = gsm.MaxSeptets

// Config is the action-specific configuration.
type Config struct {
	// Phones lists the phone numbers that receive the notification
	// besides the matched group members' registered numbers. It may be
	// empty — a routing rule that relies entirely on the group members'
	// directory entries is the normal case.
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
	// lastUnconfirmed counts the messages the modem took without
	// confirming in the attempt that just ran; ExecuteStage turns it into
	// the reported stage. The instance executes one call at a time, so one
	// counter is enough (reset at the start of every call).
	lastUnconfirmed atomic.Int64
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
		key := gsm.CanonicalNumberKey(phone)
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
	if len(recipients) == 0 {
		// Nothing to do: the action may rely entirely on the matched
		// group's members, and none of them registered a phone number.
		return nil
	}
	// A retry carries the numbers an earlier attempt already reached
	// (ActionRequest.DoneRecipients) and the ones it handed over without a
	// confirmation (ActionRequest.UnconfirmedRecipients): neither may get a
	// second message. The comparison uses the same digit key as the dedup
	// above, so a number written in another format still matches.
	done := make(map[string]bool, len(req.DoneRecipients)+len(req.UnconfirmedRecipients))
	for _, phone := range req.DoneRecipients {
		if key := gsm.CanonicalNumberKey(phone); key != "" {
			done[key] = true
		}
	}
	for _, phone := range req.UnconfirmedRecipients {
		if key := gsm.CanonicalNumberKey(phone); key != "" {
			done[key] = true
		}
	}
	var sent, unconfirmedSent []string
	var firstErr error
	failed, unconfirmed := 0, 0
	for _, r := range recipients {
		if done[gsm.CanonicalNumberKey(r.phone)] {
			continue
		}
		text := a.textFor(ctx, req, r.lang)
		err := a.hub.Send(ctx, r.phone, text)
		if err == nil {
			sent = append(sent, r.phone)
			continue
		}
		if errors.Is(err, gsm.ErrUnconfirmed) {
			// The modem took the message; only its confirmation is
			// missing. Recorded separately: a retry triggered by ANOTHER
			// recipient never sends it twice, and the job keeps reporting
			// that this number was never confirmed (reported P2).
			unconfirmed++
			a.lastUnconfirmed.Add(1)
			unconfirmedSent = append(unconfirmedSent, r.phone)
		} else {
			failed++
		}
		if firstErr == nil {
			firstErr = err
		}
	}
	// An unconfirmed number from ANY attempt keeps the call at the weaker
	// stage: the durable payload carries it until the job is resolved.
	if len(req.UnconfirmedRecipients) > 0 {
		a.lastUnconfirmed.Add(1)
	}
	if failed > 0 {
		err := fmt.Errorf("sms: %d of %d messages failed (first: %w)", failed, len(recipients), firstErr)
		if unconfirmed > 0 {
			err = fmt.Errorf("sms: %d of %d messages failed and %d %s handed over without confirmation (first: %w)",
				failed, len(recipients), unconfirmed, plural(unconfirmed), firstErr)
		}
		if len(sent) == 0 && len(unconfirmedSent) == 0 {
			return err
		}
		// The numbers that went out ride with the failure, so the retry
		// covers only the rest instead of sending a second message to
		// everybody (action.PartialDelivery); the ones that went out
		// WITHOUT a confirmation stay in their own list.
		return &partialDeliveryError{delivered: sent, unconfirmed: unconfirmedSent, err: err}
	}
	// Nothing failed before transmission: the call succeeded, and a message
	// the modem took without confirming is reported as the weaker stage by
	// ExecuteStage — the worker settles it as "unconfirmed" and schedules
	// no resend (reported P2).
	return nil
}

// plural picks the verb form for the message count.
func plural(n int) string {
	if n == 1 {
		return "was"
	}
	return "were"
}

// ExecuteStage runs one invocation and reports the delivery stage
// alongside the error (action.ConfirmingPlugin): an unconfirmed
// transmission is a stage, never a plain failure — resending it could
// duplicate a message the modem already sent.
func (a *Action) ExecuteStage(ctx context.Context, req action.ActionRequest) (action.DeliveryStage, error) {
	a.lastUnconfirmed.Store(0)
	if err := a.Execute(ctx, req); err != nil {
		return action.StageAccepted, err
	}
	unconfirmed := a.lastUnconfirmed.Load()
	if unconfirmed > 0 {
		return action.StageUnconfirmed, nil
	}
	return action.StageAccepted, nil
}

// partialDeliveryError reports a batch that reached SOME recipients and
// then failed. The numbers that went out travel with the result of the
// attempt that produced them, so the worker narrows the retry with them
// (action.PartialDelivery): a retry after "A got it, B did not" must not
// charge A with a second SMS and a second slice of modem time during an
// alert.
type partialDeliveryError struct {
	delivered   []string
	unconfirmed []string
	err         error
}

func (e *partialDeliveryError) Error() string { return e.err.Error() }

func (e *partialDeliveryError) Unwrap() error { return e.err }

// Delivered lists the numbers this attempt delivered to.
func (e *partialDeliveryError) Delivered() []string {
	return append([]string(nil), e.delivered...)
}

// Unconfirmed lists the numbers this attempt handed to the modem without
// a confirmation (action.PartialUnconfirmed).
func (e *partialDeliveryError) Unconfirmed() []string {
	return append([]string(nil), e.unconfirmed...)
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
		headline = norm(i18n.EventName(lang, h.Event))
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
	// Transliteration first, then the septet budget: the built text
	// carries brackets and separators of its own (" | ", "[WF-…]"), so
	// counting characters would overshoot the segment (reported P2).
	return gsm.FitGSM7(gsm.TransliterateGSM7(base), maxSMSChars)
}
