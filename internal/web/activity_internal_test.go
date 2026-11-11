package web

import (
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/storage"
)

// fillMinutes builds one sample per minute of the given hour for `count`
// minutes, so a test can pin a tone without touching a database.
func fillMinutes(day time.Time, hour, from, count int, level string, problems ...string) []storage.HeartbeatSample {
	out := make([]storage.HeartbeatSample, 0, count)
	for i := 0; i < count; i++ {
		m := from + i
		ts := time.Date(day.Year(), day.Month(), day.Day(), hour, m, 0, 0, day.Location())
		out = append(out, storage.HeartbeatSample{
			MinuteMS: ts.UnixMilli(),
			Level:    level,
			Problems: problems,
		})
	}
	return out
}

// TestActivityStripTones pins the three answers the strip gives: a whole hour
// the station spent up and healthy is green, an hour with a gap or a subsystem
// in trouble is amber, and an hour with no heartbeat at all is dark — which is
// how "was it running then?" is answered without the journal.
func TestActivityStripTones(t *testing.T) {
	now := time.Date(2026, 11, 10, 14, 30, 0, 0, time.UTC)
	today := time.Date(2026, 11, 10, 0, 0, 0, 0, time.UTC)
	yesterday := today.AddDate(0, 0, -1)

	var samples []storage.HeartbeatSample
	// Yesterday 10:00 — an hour the station spent up with every subsystem OK.
	samples = append(samples, fillMinutes(yesterday, 10, 0, 60, "ok")...)
	// Yesterday 11:00 — half the hour missing (a restart).
	samples = append(samples, fillMinutes(yesterday, 11, 0, 30, "ok")...)
	// Yesterday 12:00 — present all hour, but the receivers were down.
	samples = append(samples, fillMinutes(yesterday, 12, 0, 60, "warn", "receivers")...)
	// Today, the hour in progress: every minute that has happened so far.
	samples = append(samples, fillMinutes(today, 14, 0, 30, "ok")...)

	v := foldActivity(samples, now, "pl")

	if v.Days != 7 || len(v.Rows) != 7 {
		t.Fatalf("strip = %d days / %d rows, want 7", v.Days, len(v.Rows))
	}
	if len(v.Rows[0].Cells) != 24 {
		t.Fatalf("row has %d cells, want 24", len(v.Rows[0].Cells))
	}
	if v.Rows[6].Day != "2026-11-10" {
		t.Errorf("last row = %q, want today", v.Rows[6].Day)
	}

	got := make(map[string]string, 4)
	for _, row := range v.Rows {
		for h, cell := range row.Cells {
			got[row.Day+"/"+itoa(h)] = cell.Tone
		}
	}
	for _, tc := range []struct{ key, want string }{
		{"2026-11-09/10", "on"},
		{"2026-11-09/11", "part"},
		{"2026-11-09/12", "part"},
		{"2026-11-09/9", "off"},
		{"2026-11-10/14", "on"},  // the live hour: every due minute present
		{"2026-11-10/15", "off"}, // has not started
	} {
		if got[tc.key] != tc.want {
			t.Errorf("cell %s = %q, want %q", tc.key, got[tc.key], tc.want)
		}
	}

	// The live hour must not be called "partly covered" just because it is not
	// over: its expected minutes are the ones already due.
	if cell := v.Rows[6].Cells[14]; cell.Expected != 30 || cell.Minutes != 30 {
		t.Errorf("live hour = %d/%d min, want 30/30", cell.Minutes, cell.Expected)
	}
	if cell := v.Rows[6].Cells[15]; cell.Expected != 0 {
		t.Errorf("future hour expected %d minutes, want 0", cell.Expected)
	}

	// The trouble is named, in the page language, in the tooltip.
	if cell := v.Rows[5].Cells[12]; len(cell.Problems) != 1 || cell.Problems[0] != "odbiorniki 60 min" {
		t.Errorf("problem cell = %v, want the receivers named in Polish", cell.Problems)
	}
	if cell := v.Rows[5].Cells[10]; cell.Title == "" {
		t.Errorf("a covered cell has no tooltip")
	}
	if cell := v.Rows[5].Cells[9]; cell.Title != "" {
		t.Errorf("an empty cell carries a tooltip: %q", cell.Title)
	}

	// Percent counts only minutes that existed: 60+30+60+30 of (7*24*60 minus
	// the hours of today that have not started).
	if v.Covered != 180 || v.Expected <= 0 || v.Percent <= 0 {
		t.Errorf("covered=%d expected=%d percent=%.1f", v.Covered, v.Expected, v.Percent)
	}
}

// itoa keeps the test free of a fmt import for one call.
func itoa(n int) string {
	if n < 10 {
		return string(rune('0' + n))
	}
	return string(rune('0'+n/10)) + string(rune('0'+n%10))
}

// TestActivityNeverExceeds100 is WF-REV-10: the heartbeat records the minute in
// progress, so counting it while the denominator stopped at now.Minute() reported
// e.g. 9391 of 9390 minutes — 100.01 %. Both sides must describe the same closed
// interval, and a clock that stepped back must not add samples from the future.
func TestActivityNeverExceeds100(t *testing.T) {
	cases := []struct {
		name string
		now  time.Time
	}{
		{"half a minute into the hour", time.Date(2026, 11, 11, 12, 30, 30, 0, time.UTC)},
		{"the first minute", time.Date(2026, 11, 11, 0, 0, 10, 0, time.UTC)},
		{"the last minute", time.Date(2026, 11, 11, 23, 59, 40, 0, time.UTC)},
	}
	for _, tc := range cases {
		// Every minute from the start of the window up to and including the one
		// the heartbeat just wrote: a perfect week of uptime.
		var samples []storage.HeartbeatSample
		start := stripStart(tc.now)
		for i := 0; ; i++ {
			ts := start.Add(time.Duration(i) * time.Minute)
			if ts.After(tc.now) {
				break
			}
			samples = append(samples, storage.HeartbeatSample{
				MinuteMS: ts.UnixMilli(),
				Level:    "ok",
			})
		}
		v := foldActivity(samples, tc.now, "pl")
		if v.Covered > v.Expected {
			t.Errorf("%s: covered=%d exceeds expected=%d", tc.name, v.Covered, v.Expected)
		}
		if v.Percent > 100 || v.Percent < 0 {
			t.Errorf("%s: percent=%.4f outside 0–100", tc.name, v.Percent)
		}
	}

	// A clock that stepped back two hours leaves samples in the future; they must
	// be ignored rather than counted against minutes that have not happened.
	now := time.Date(2026, 11, 11, 10, 30, 0, 0, time.UTC)
	samples := fillMinutes(now, 12, 0, 15, "ok")
	v := foldActivity(samples, now, "pl")
	if v.Covered != 0 || v.Expected <= 0 {
		t.Errorf("future samples: covered=%d expected=%d, want 0 covered", v.Covered, v.Expected)
	}
}
