package meshtastic

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

// TestTransportLoggerQuietsUnknownProtobufs pins the filter: the library's
// per-message warning about unmodelled configuration messages drops to debug,
// while every other warning keeps its level. One nightly radio restart used to
// write ten "unhandled protobuf from radio" warnings at WARN.
func TestTransportLoggerQuietsUnknownProtobufs(t *testing.T) {
	var buf bytes.Buffer
	base := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	logger := slog.New(quietUnknowns{Handler: base})

	logger.Warn("unhandled protobuf from radio")
	logger.Warn("something else broke")
	logger.Error("error reading from radio")

	out := buf.String()
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		switch {
		case strings.Contains(line, `msg="unhandled protobuf from radio"`):
			if !strings.Contains(line, "level=DEBUG") {
				t.Errorf("unhandled protobuf logged above debug: %s", line)
			}
		case strings.Contains(line, `msg="something else broke"`):
			if !strings.Contains(line, "level=WARN") {
				t.Errorf("unrelated warning was altered: %s", line)
			}
		case strings.Contains(line, `msg="error reading from radio"`):
			if !strings.Contains(line, "level=ERROR") {
				t.Errorf("error record was altered: %s", line)
			}
		}
	}
	if !strings.Contains(out, "unhandled protobuf from radio") {
		t.Error("filtered record vanished instead of dropping to debug")
	}
}
