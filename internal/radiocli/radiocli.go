// Package radiocli is the shared command interpreter for inbound radio
// channels (APRS messages, Meshtastic direct messages, and future
// transports): operators type slash commands over the radio and the
// server answers in-band. Anything that is not a command keeps flowing
// into the normal alarm pipeline, so the CLI is purely additive.
package radiocli

import "strings"

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
}

// Handler executes one radio command. args is the text after the
// command name (trimmed, possibly empty).
type Handler func(args string) Result

// Bot interprets slash commands over any radio channel.
type Bot struct {
	identity string
	handlers map[string]Handler
	descs    map[string]string
	order    []string
}

// New builds a bot with the built-in commands (/help, /debug). identity
// is the one-line installation banner (version, installation name,
// public domain) answered to any slash-prefixed message that is not a
// recognized command. Further topics register through Register.
func New(identity string) *Bot {
	b := &Bot{
		identity: identity,
		handlers: map[string]Handler{},
		descs:    map[string]string{},
	}
	b.Register("help", "this list", func(string) Result {
		return Result{Handled: true, Reply: b.helpText()}
	})
	b.Register("debug", "debug alarm", func(string) Result {
		return Result{Handled: true, Debug: true, Reply: "OK: debug alarm generated"}
	})
	return b
}

// Register installs one command. The description shows up in /help.
func (b *Bot) Register(name, description string, h Handler) {
	key := strings.ToLower(strings.TrimSpace(name))
	if key == "" {
		return
	}
	if _, exists := b.handlers[key]; !exists {
		b.order = append(b.order, key)
	}
	b.handlers[key] = h
	b.descs[key] = description
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

// Banner returns the installation banner with the /help hint — the
// reply for plain messages and unknown slash attempts. The hint always
// survives: the identity is truncated instead, so the whole banner fits
// the 67-character APRS message limit (Meshtastic reuses the string).
func (b *Bot) Banner() string {
	hint := " | " + HelpHint
	maxIdentity := maxBannerRunes - len([]rune(hint))
	id := []rune(b.identity)
	if len(id) > maxIdentity {
		id = id[:maxIdentity]
	}
	return string(id) + hint
}

// Handle evaluates one inbound text. Non-commands return Handled=false
// so the channel applies its normal routing.
func (b *Bot) Handle(text string) Result {
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
	if h, ok := b.handlers[name]; ok {
		return h(args)
	}
	// Any other slash-prefixed message is not a command: answer with the
	// installation banner (plus the /help hint) so the sender knows what
	// they reached and how to list the commands.
	return Result{Handled: true, Reply: b.Banner()}
}

// helpText renders the command list. Kept short on purpose: APRS
// messages carry at most 67 characters.
func (b *Bot) helpText() string {
	parts := make([]string, 0, len(b.order))
	for _, key := range b.order {
		parts = append(parts, "/"+key+" - "+b.descs[key])
	}
	return "Commands: " + strings.Join(parts, "; ")
}
