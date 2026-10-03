// Package radiocli is the shared command interpreter for inbound radio
// channels (APRS messages, Meshtastic direct messages, and future
// transports): operators type slash commands over the radio and the
// server answers in-band. Anything that is not a command keeps flowing
// into the normal alarm pipeline, so the CLI is purely additive.
package radiocli

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"time"
)

// Result is the outcome of one command evaluation.
type Result struct {
	// Handled reports that the text was a recognized command (the
	// channel must NOT feed it into the alarm pipeline).
	Handled bool
	// Reply is the in-band answer sent back to the sender (may be
	// empty for silent commands).
	Reply string
	// Debug asks the channel to generate the debug alarm exactly like
	// the current default behavior for any inbound message.
	Debug bool
	// Alert asks the channel to raise a hazard through its normal
	// routing pipeline (the /alert command).
	Alert *AlertSpec
}

// AlertSpec is a command-requested hazard: the channel raises it as a
// severe event with the requested TTL (its own default applies when
// TTL is zero).
type AlertSpec struct {
	// Headline is the alert text (trimmed, non-empty).
	Headline string
	// TTL bounds how long the alert stays active.
	TTL time.Duration
}

// Handler executes one radio command. args is the text after the
// command name (trimmed, possibly empty).
type Handler func(args string) Result

// ContentID returns a short stable fingerprint (16 hex chars) of the
// given parts. The gateways use it to identify a retransmitted message
// when the transport carries no message/packet id: two copies of the
// same message produce the same ContentID.
func ContentID(parts ...string) string {
	h := sha256.New()
	for i, p := range parts {
		if i > 0 {
			h.Write([]byte{0})
		}
		h.Write([]byte(p))
	}
	return hex.EncodeToString(h.Sum(nil))[:16]
}

// Bot interprets slash commands over any radio channel.
type Bot struct {
	identity string
	handlers map[string]Handler
	descs    map[string]string
	order    []string
	// public marks commands everyone may run (true) versus commands
	// reserved for authorized senders (false).
	public map[string]bool
}

// New builds a bot with the built-in commands: /help (public, lists the
// commands the sender may run) and /debug (authorized only, fires the
// debug alarm). identity is the one-line installation banner (version,
// installation name, public domain) answered to any slash-prefixed
// message that is not a recognized command. The same bot serves APRS
// and Meshtastic — nothing to configure per channel.
func New(identity string) *Bot {
	b := &Bot{
		identity: identity,
		handlers: map[string]Handler{},
		descs:    map[string]string{},
		public:   map[string]bool{},
	}
	// /help is special-cased in Handle (its list depends on the
	// sender's authorization); register its label so the lists show it.
	b.order = append(b.order, "help")
	b.descs["help"] = "this list"
	b.public["help"] = true
	b.RegisterRestricted("debug", "alarm test", func(string) Result {
		return Result{Handled: true, Debug: true, Reply: "OK: debug alarm generated"}
	})
	return b
}

// Register installs one command available to EVERYONE. The description
// shows up in /help.
func (b *Bot) Register(name, description string, h Handler) {
	b.register(name, description, true, h)
}

// RegisterRestricted installs one command reserved for authorized
// senders (the channel's sender allow-list decides). Unauthorized
// senders get an explicit denial instead.
func (b *Bot) RegisterRestricted(name, description string, h Handler) {
	b.register(name, description, false, h)
}

func (b *Bot) register(name, description string, isPublic bool, h Handler) {
	key := strings.ToLower(strings.TrimSpace(name))
	if key == "" {
		return
	}
	if _, exists := b.handlers[key]; !exists {
		b.order = append(b.order, key)
	}
	b.handlers[key] = h
	b.descs[key] = description
	b.public[key] = isPublic
}

// Identity returns the installation banner answered to unknown
// slash-prefixed messages (public information: version, installation
// name and public domain).
func (b *Bot) Identity() string { return b.identity }

// HelpHint is appended to the banner replies, so whoever reached the
// station learns the one command that lists the rest.
const HelpHint = "type /help for help"

// maxBannerRunes keeps the banner within the APRS message limit (67
// characters). Meshtastic reuses the same string for consistency.
const maxBannerRunes = 67

// Banner returns the full installation banner with the /help hint —
// the reply for plain messages and unknown slash attempts. The channel
// fits it to its message limit through Fit.
func (b *Bot) Banner() string {
	return b.identity + " | " + HelpHint
}

// DeniedText answers an unauthorized attempt to run a restricted
// command. English on purpose: radio replies are English-only.
const DeniedText = "You are not authorized"

// Denied returns the full installation banner plus the denial — the
// reply for a restricted command fired by an unauthorized sender. The
// channel fits it to its message limit through Fit.
func (b *Bot) Denied() string {
	return b.identity + " - " + DeniedText
}

// identityVariants returns the progressively shorter identity forms:
// the full banner, the banner without the domain (address), the
// "WarnFlux vX.Y.Z" word alone, and finally nothing.
func (b *Bot) identityVariants() []string {
	variants := []string{b.identity}
	parts := strings.SplitN(b.identity, " - ", 3)
	if len(parts) >= 3 {
		variants = append(variants, parts[0]+" - "+parts[1]) // no domain
	}
	if len(parts) >= 2 {
		variants = append(variants, parts[0]) // the WarnFlux word only
	}
	return variants
}

// Fit renders one reply for a channel limit. When the text exceeds
// maxRunes, the identity prefix shortens progressively — first the
// domain goes, then the installation name, then the "WarnFlux" word
// itself — and only as the very last resort the payload end is cut.
// Texts without the identity prefix are simply truncated at the end.
func (b *Bot) Fit(text string, maxRunes int) string {
	r := []rune(text)
	if len(r) <= maxRunes {
		return text
	}
	rest := strings.TrimPrefix(text, b.identity)
	if rest == text || b.identity == "" {
		return string(r[:maxRunes])
	}
	for _, variant := range b.identityVariants() {
		if variant == "" {
			continue
		}
		if candidate := variant + rest; len([]rune(candidate)) <= maxRunes {
			return candidate
		}
	}
	// Everything but the payload went away: keep as much of it as
	// fits, cutting the end.
	trimmed := strings.TrimLeft(rest, " -|")
	if len([]rune(trimmed)) <= maxRunes {
		return trimmed
	}
	return string([]rune(trimmed)[:maxRunes])
}

// Handle evaluates one inbound text. authorized reports whether the
// sender sits on the channel's allow-list: public commands run for
// everyone, restricted commands run for authorized senders only (others
// get the public banner). Non-commands return Handled=false so the
// channel applies its normal routing.
func (b *Bot) Handle(text string, authorized bool) Result {
	t := strings.TrimSpace(text)
	if !strings.HasPrefix(t, "/") {
		return Result{}
	}
	parts := strings.SplitN(t[1:], " ", 2)
	name := strings.ToLower(strings.TrimSpace(parts[0]))
	args := ""
	if len(parts) > 1 {
		args = strings.TrimSpace(parts[1])
	}
	// /help is public and lists only the commands the sender may run.
	if name == "help" {
		return Result{Handled: true, Reply: b.helpText(authorized)}
	}
	h, ok := b.handlers[name]
	if !ok {
		// Any other slash-prefixed message is not a command: answer with
		// the installation banner (plus the /help hint) so the sender
		// knows what they reached and how to list the commands.
		return Result{Handled: true, Reply: b.Banner()}
	}
	if !b.public[name] && !authorized {
		// Restricted command from an unauthorized sender: a clear
		// denial, never the command.
		return Result{Handled: true, Reply: b.Denied()}
	}
	return h(args)
}

// helpText renders the command list available to the sender. Kept short
// on purpose: APRS messages carry at most 67 characters — when the full
// "/name - description" form would not fit, it falls back to a compact
// name-only list.
func (b *Bot) helpText(authorized bool) string {
	names := make([]string, 0, len(b.order))
	for _, key := range b.order {
		if !authorized && !b.public[key] {
			continue
		}
		names = append(names, key)
	}
	parts := make([]string, 0, len(names))
	for _, key := range names {
		parts = append(parts, "/"+key+" - "+b.descs[key])
	}
	full := "Commands: " + strings.Join(parts, "; ")
	if len([]rune(full)) <= maxBannerRunes {
		return full
	}
	compact := make([]string, 0, len(names))
	for _, key := range names {
		compact = append(compact, "/"+key)
	}
	return "Commands: " + strings.Join(compact, " ")
}
