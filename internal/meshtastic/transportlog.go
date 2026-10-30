package meshtastic

import (
	"context"
	"log/slog"
	"strings"

	"github.com/kabili207/meshtastic-go/transport/client"
)

// quietUnknowns demotes two records the transport library writes on paths this
// station causes itself:
//
//   - "unhandled protobuf from radio" (warning): the node re-sends its whole
//     configuration after every reconnect and the library reports each message
//     type it does not model, so one nightly radio restart produced ten
//     warnings about messages this station never needed;
//   - "error reading from radio" (error): the station closes the link itself on
//     shutdown, and the library reports the resulting read as an error even
//     though nothing failed — a routine restart must not paint the log red.
//
// Every other record passes through untouched.
type quietUnknowns struct{ slog.Handler }

func (h quietUnknowns) Handle(ctx context.Context, r slog.Record) error {
	switch {
	case r.Level == slog.LevelWarn && strings.HasPrefix(r.Message, "unhandled protobuf"):
		r.Level = slog.LevelDebug
	case r.Level >= slog.LevelError && r.Message == "error reading from radio" && closedByUs(r):
		r.Level = slog.LevelDebug
	}
	return h.Handler.Handle(ctx, r)
}

// closedByUs reports whether a radio read error is the shutdown of a link this
// process closed (net.ErrClosed reaches the library as a string, either way).
func closedByUs(r slog.Record) bool {
	var closed bool
	r.Attrs(func(a slog.Attr) bool {
		text := a.Value.String()
		if strings.Contains(text, "use of closed network connection") ||
			strings.Contains(text, "net.ErrClosed") {
			closed = true
			return false
		}
		return true
	})
	return closed
}

func (h quietUnknowns) WithAttrs(attrs []slog.Attr) slog.Handler {
	return quietUnknowns{Handler: h.Handler.WithAttrs(attrs)}
}

func (h quietUnknowns) WithGroup(name string) slog.Handler {
	return quietUnknowns{Handler: h.Handler.WithGroup(name)}
}

// transportConfig returns the client transport configuration for this
// station: the library defaults to slog.Default(), which would emit its
// warnings without the filter above.
func transportConfig(logger *slog.Logger) client.TransportConfig {
	if logger == nil {
		logger = slog.Default()
	}
	return client.TransportConfig{Logger: slog.New(quietUnknowns{Handler: logger.Handler()})}
}
