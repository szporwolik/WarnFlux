// Episode collapses one failure streak of a polled feed into three log lines:
// the first failure warns, later failures are debug lines with a counter, and
// the recovery reports the tally. A provider that is down for a day therefore
// costs one warning instead of one per poll (a five-minute cadence wrote 288
// lines a day while TVP's RSO feed was unreachable).
package snapshotutil

import (
	"log/slog"
	"time"
)

// Episode tracks the failures of one named feed or poll.
type Episode struct {
	label      string
	failures   int
	since      time.Time
	retryIn    time.Duration
	lastDetail slog.Attr
}

// NewEpisode names the feed in every line it writes, e.g. "RSO feed" →
// "RSO feed fetch failed".
func NewEpisode(label string) *Episode {
	return &Episode{label: label}
}

// Fail records one failed attempt. detail is attached to the warning so the
// first line of an outage still says why it started.
func (e *Episode) Fail(err error, detail slog.Attr, retryIn time.Duration) {
	e.retryIn = retryIn
	e.lastDetail = detail
	e.failures++
	if e.failures == 1 {
		e.since = time.Now()
		attrs := []any{"error", err, "retry_in", retryIn}
		if detail.Key != "" {
			attrs = append(attrs, detail)
		}
		slog.Warn(e.label+" fetch failed", attrs...)
		return
	}
	attrs := []any{"error", err, "failed_attempts", e.failures, "retry_in", retryIn}
	if detail.Key != "" {
		attrs = append(attrs, detail)
	}
	slog.Debug(e.label+" fetch still failing", attrs...)
}

// Recovered closes the episode. It reports whether a streak was open, so a
// caller can skip a log line for the common case.
func (e *Episode) Recovered() bool {
	if e.failures == 0 {
		return false
	}
	attrs := []any{"failed_attempts", e.failures, "outage", time.Since(e.since).Round(time.Second)}
	if e.lastDetail.Key != "" {
		attrs = append(attrs, e.lastDetail)
	}
	slog.Info(e.label+" fetch recovered", attrs...)
	e.failures = 0
	return true
}

// Failing reports whether the episode is open.
func (e *Episode) Failing() bool { return e.failures > 0 }
