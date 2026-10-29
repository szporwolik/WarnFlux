package snapshotutil

import (
	"bytes"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
)

type sink struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *sink) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *sink) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

// withLogger captures the default logger for one test.
func withLogger(t *testing.T) *sink {
	t.Helper()
	s := &sink{}
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(s, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return s
}

// TestEpisodeLogsOneWarningPerOutage pins the policy: one warning when the
// feed dies, debug lines while it stays dead, one info line on recovery. A
// five-minute cadence used to write 288 warnings a day for a single outage.
func TestEpisodeLogsOneWarningPerOutage(t *testing.T) {
	out := withLogger(t)
	e := NewEpisode("RSO feed")
	detail := slog.String("voivodeship", "malopolskie")
	boom := errors.New("connection refused")

	if e.Failing() {
		t.Error("a fresh episode must not report Failing")
	}
	for i := 0; i < 3; i++ {
		e.Fail(boom, detail, 5*time.Minute)
	}
	if !e.Recovered() {
		t.Fatal("Recovered reported no open streak")
	}
	if e.Recovered() {
		t.Error("a closed episode reported another recovery")
	}

	text := out.String()
	if got := strings.Count(text, "level=WARN"); got != 1 {
		t.Errorf("warnings = %d, want 1:\n%s", got, text)
	}
	if got := strings.Count(text, "level=DEBUG"); got != 2 {
		t.Errorf("debug lines = %d, want 2:\n%s", got, text)
	}
	if !strings.Contains(text, "failed_attempts=3") || !strings.Contains(text, "fetch recovered") {
		t.Errorf("recovery line missing the tally:\n%s", text)
	}
	if !strings.Contains(text, "voivodeship=malopolskie") {
		t.Errorf("the detail attribute was dropped:\n%s", text)
	}
}
