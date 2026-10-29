package meshtastic

import (
	"context"
	"log/slog"
	"strings"

	"github.com/kabili207/meshtastic-go/transport/client"
)

// quietUnknowns demotes the transport library's "unhandled protobuf from
// radio" warning to debug. The node re-sends its whole configuration after
// every reconnect and the library reports each message type it does not model,
// so one nightly radio restart produced ten warnings about messages this
// station never needed. Every other record passes through untouched.
type quietUnknowns struct{ slog.Handler }

func (h quietUnknowns) Handle(ctx context.Context, r slog.Record) error {
	if r.Level == slog.LevelWarn && strings.HasPrefix(r.Message, "unhandled protobuf") {
		r.Level = slog.LevelDebug
	}
	return h.Handler.Handle(ctx, r)
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
