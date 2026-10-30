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

// TestQuietUnknownsDemotesShutdownRead pins the second demoted record: the
// station closes the radio link itself on shutdown and the library reports the
// resulting read as an ERROR even though nothing failed — a routine restart
// must not paint the log red. A genuine read failure stays an error.
func TestQuietUnknownsDemotesShutdownRead(t *testing.T) {
	var buf bytes.Buffer
	handler := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	logger := slog.New(quietUnknowns{Handler: handler})

	logger.Error("error reading from radio",
		"client.err", "read tcp 172.18.0.2:53954->10.0.0.121:4403: use of closed network connection")
	logger.Error("error reading from radio", "client.err", "read tcp 1.2.3.4:1->10.0.0.121:4403: i/o timeout")

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("lines = %d, want 2:\n%s", len(lines), buf.String())
	}
	if !strings.Contains(lines[0], "level=DEBUG") {
		t.Errorf("closing the link ourselves was not demoted: %s", lines[0])
	}
	if !strings.Contains(lines[1], "level=ERROR") {
		t.Errorf("a real read failure was demoted: %s", lines[1])
	}
}
