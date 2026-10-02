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
	b.RegisterRestricted("debug", "debug alarm", func(string) Result {
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
// senders get the public banner instead.
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
		// Restricted command from an unauthorized sender: the public
		// banner, never the command.
		return Result{Handled: true, Reply: b.Banner()}
	}
	return h(args)
}

// helpText renders the command list available to the sender. Kept short
// on purpose: APRS messages carry at most 67 characters.
func (b *Bot) helpText(authorized bool) string {
	parts := make([]string, 0, len(b.order))
	for _, key := range b.order {
		if !authorized && !b.public[key] {
			continue
		}
		parts = append(parts, "/"+key+" - "+b.descs[key])
	}
	return "Commands: " + strings.Join(parts, "; ")
}
