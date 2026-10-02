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
	handlers map[string]Handler
	descs    map[string]string
	order    []string
}

// New builds a bot with the built-in commands (/help, /debug). Further
// topics register through Register as the need arises.
func New() *Bot {
	b := &Bot{
		handlers: map[string]Handler{},
		descs:    map[string]string{},
	}
	b.Register("help", "ta lista", func(string) Result {
		return Result{Handled: true, Reply: b.helpText()}
	})
	b.Register("debug", "alarm testowy", func(string) Result {
		return Result{Handled: true, Debug: true, Reply: "OK: alarm testowy wygenerowany"}
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
	return Result{Handled: true, Reply: "Nieznana komenda: " + name + ". /help"}
}

// helpText renders the command list. Kept short on purpose: APRS
// messages carry at most 67 characters.
func (b *Bot) helpText() string {
	parts := make([]string, 0, len(b.order))
	for _, key := range b.order {
		parts = append(parts, "/"+key+" - "+b.descs[key])
	}
	return "Komendy: " + strings.Join(parts, "; ")
}
