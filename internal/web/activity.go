package web

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/szporwolik/WarnFlux/internal/health"
	"github.com/szporwolik/WarnFlux/internal/i18n"
	"github.com/szporwolik/WarnFlux/internal/storage"
)

// The dashboard's seven-day strip answers one question the health card cannot:
// "was the station working last Tuesday?". The card reports right now; a minute
// with no heartbeat row is a minute the process was not running, so the strip
// is built from the station's own history rather than from a stale log.
const (
	// activityDays is the window the strip promises.
	activityDays = 7
	// activityHours is one row of cells: 24 hours of a day, local time.
	activityHours = 24
	// activityPartialShare is how much of an hour must be covered before the
	// cell reads "on" instead of "partly" (see VoxFlux's strip, same rule).
	activityPartialShare = 0.999
)

// activityCell is one hour of one day.
type activityCell struct {
	// Minutes is how many heartbeat rows that hour has.
	Minutes int
	// Expected is how many minutes of the hour existed at the time: 60 for a
	// past hour, the elapsed minutes of the hour in progress, 0 for the hours
	// of today that have not started. The strip must not call a live hour
	// "partly covered" just because the hour is not over.
	Expected int
	// Problems names the subsystems that were not OK during that hour, in the
	// panel's language, loudest first ("bramki: 4 min").
	Problems []string
	// Tone is the CSS class: on, part, or off.
	Tone string
	// Title is the hover text: the hour, the minutes behind it, and what was
	// wrong. Empty for an hour with nothing to say.
	Title string
}

// activityRow is one day of the strip, oldest first.
type activityRow struct {
	Day   string
	Label string
	Cells []activityCell
}

// activityView is the whole strip: seven days x 24 hours plus a summary.
type activityView struct {
	Lang     string
	Days     int
	Percent  float64
	Rows     []activityRow
	Hours    []string
	Covered  int
	Expected int
}

// heartbeatTick records the current minute of station health. It runs from
// RunHeartbeat, once a minute, and never blocks the panel: a store failure is
// logged and the next tick tries again.
func (s *Server) heartbeatTick(ctx context.Context) {
	store, ok := s.users.(storage.HeartbeatStore)
	if !ok {
		return
	}
	assessment := health.Assess(s.HealthInputs())
	problems := heartbeatProblems(assessment)
	level := "ok"
	switch {
	case hasLevel(assessment, health.LevelBad):
		level = "bad"
	case hasLevel(assessment, health.LevelWarn):
		level = "warn"
	}
	minute := time.Now().Truncate(time.Minute)
	sample := storage.HeartbeatSample{
		MinuteMS: minute.UnixMilli(),
		Level:    level,
		Problems: problems,
	}
	wctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := store.RecordHeartbeat(wctx, sample); err != nil {
		s.logger.Warn("heartbeat: record failed", "error", err)
		return
	}
	// One prune a minute is cheap (an indexed delete that normally matches
	// nothing) and keeps the table from growing past the retention bound even
	// if the station runs for months without a restart.
	cutoff := minute.AddDate(0, 0, -storage.HeartbeatRetentionDays).UnixMilli()
	if _, err := store.PruneHeartbeats(wctx, cutoff); err != nil {
		s.logger.Warn("heartbeat: prune failed", "error", err)
	}
}

// hasLevel reports whether the assessment contains a finding at level.
func hasLevel(r health.Report, level health.Level) bool {
	for _, f := range r.Findings {
		if f.Level == level {
			return true
		}
	}
	return false
}

// heartbeatProblems lists the subsystems that were not OK, worst first. The
// order is stable so a minute reads the same in the history and in a tooltip.
func heartbeatProblems(r health.Report) []string {
	seen := make(map[string]bool, 4)
	var bad, warn []string
	for _, f := range r.Findings {
		code := string(f.Subsystem)
		if code == "" || seen[code] {
			continue
		}
		seen[code] = true
		if f.Level == health.LevelBad {
			bad = append(bad, code)
		} else {
			warn = append(warn, code)
		}
	}
	return append(bad, warn...)
}

// RunHeartbeat writes one heartbeat a minute until ctx is cancelled. The ticker
// is aligned to the wall clock, so every row stands for a whole minute rather
// than for "60 s after the process started".
func (s *Server) RunHeartbeat(ctx context.Context) {
	if _, ok := s.users.(storage.HeartbeatStore); !ok {
		return
	}
	for {
		now := time.Now()
		next := now.Truncate(time.Minute).Add(time.Minute)
		select {
		case <-ctx.Done():
			return
		case <-time.After(next.Sub(now)):
		}
		s.heartbeatTick(ctx)
	}
}

// buildActivityStrip folds the heartbeat history into the seven-day grid.
func (s *Server) buildActivityStrip(ctx context.Context, lang string) activityView {
	loc := s.displayLoc()
	now := time.Now().In(loc)
	start := stripStart(now)
	var samples []storage.HeartbeatSample
	if store, ok := s.users.(storage.HeartbeatStore); ok {
		rctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		got, err := store.HeartbeatsSince(rctx, start.UnixMilli())
		cancel()
		if err != nil {
			s.logger.Warn("activity: heartbeat history read failed", "error", err)
		}
		samples = got
	}
	return foldActivity(samples, now, lang)
}

// stripStart is midnight of the oldest day the strip shows.
func stripStart(now time.Time) time.Time {
	return time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location()).
		AddDate(0, 0, -(activityDays - 1))
}

// foldActivity is the grid math: it takes the samples, the clock and the page
// language and returns the seven rows. Pure on purpose — the panel only has to
// hand it the history, and a test can pin every tone without a server.
func foldActivity(samples []storage.HeartbeatSample, now time.Time, lang string) activityView {
	loc := now.Location()
	start := stripStart(now)
	v := activityView{Lang: lang, Days: activityDays}

	byMinute := make(map[int64]storage.HeartbeatSample, len(samples))
	for _, sample := range samples {
		byMinute[sample.MinuteMS] = sample
	}

	v.Hours = make([]string, activityHours)
	for h := 0; h < activityHours; h++ {
		// Every third hour carries a label; the rest stay blank so the row is
		// readable without a legend.
		if h%3 == 0 {
			v.Hours[h] = fmt.Sprintf("%02d", h)
		}
	}

	for d := 0; d < activityDays; d++ {
		day := start.AddDate(0, 0, d)
		row := activityRow{Day: day.Format("2006-01-02"), Label: day.Format("02.01")}
		row.Cells = make([]activityCell, activityHours)
		for h := 0; h < activityHours; h++ {
			cell := activityCell{Expected: expectedMinutes(day, h, now)}
			problems := make(map[string]int, 2)
			for m := 0; m < 60; m++ {
				minute := time.Date(day.Year(), day.Month(), day.Day(), h, m, 0, 0, loc)
				sample, ok := byMinute[minute.UnixMilli()]
				if !ok {
					continue
				}
				cell.Minutes++
				for _, code := range sample.Problems {
					problems[code]++
				}
			}
			cell.Problems = rankedProblems(lang, problems)
			cell.Tone = activityTone(cell, problems)
			cell.Title = activityTitle(lang, row.Label, h, cell)
			row.Cells[h] = cell
			v.Covered += cell.Minutes
			v.Expected += cell.Expected
		}
		v.Rows = append(v.Rows, row)
	}
	if v.Expected > 0 {
		v.Percent = 100 * float64(v.Covered) / float64(v.Expected)
	}
	return v
}

// expectedMinutes is how many minutes of that hour existed at the time the page
// was rendered — 0 for an hour that has not started.
func expectedMinutes(day time.Time, hour int, now time.Time) int {
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	switch {
	case day.After(today):
		return 0
	case day.Equal(today):
		if hour > now.Hour() {
			return 0
		}
		if hour == now.Hour() {
			return now.Minute()
		}
	}
	return 60
}

// activityTone is green only for a fully covered hour that was healthy: an hour
// the station spent half down is amber even when the process was up, because
// the strip answers "was it working", not "was the binary alive".
func activityTone(cell activityCell, problems map[string]int) string {
	if cell.Minutes <= 0 {
		return "off"
	}
	complete := cell.Expected <= 0 || float64(cell.Minutes) >= float64(cell.Expected)*activityPartialShare
	if complete && len(problems) == 0 {
		return "on"
	}
	return "part"
}

// rankedProblems renders the problem counters in the page language, most
// frequent first ("bramki 12 min, źródła 3 min").
func rankedProblems(lang string, counts map[string]int) []string {
	type entry struct {
		code    string
		minutes int
	}
	entries := make([]entry, 0, len(counts))
	for code, minutes := range counts {
		entries = append(entries, entry{code, minutes})
	}
	// Insertion sort: a handful of subsystems, and a stable order matters
	// more than speed here.
	for i := 1; i < len(entries); i++ {
		for j := i; j > 0; j-- {
			if entries[j].minutes > entries[j-1].minutes ||
				(entries[j].minutes == entries[j-1].minutes && entries[j].code < entries[j-1].code) {
				entries[j], entries[j-1] = entries[j-1], entries[j]
				continue
			}
			break
		}
	}
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		out = append(out, fmt.Sprintf("%s %d min", subsystemLabel(lang, e.code), e.minutes))
	}
	return out
}

// activityTitle is the hover text of one cell; empty when the hour says nothing.
func activityTitle(lang, dayLabel string, hour int, cell activityCell) string {
	if cell.Minutes <= 0 {
		return ""
	}
	text := fmt.Sprintf("%s %02d:00 · %d/%d min", dayLabel, hour, cell.Minutes, cell.Expected)
	if len(cell.Problems) > 0 {
		text += " · " + strings.Join(cell.Problems, ", ")
	}
	return text
}

// subsystemLabel translates a health subsystem code for the page language.
func subsystemLabel(lang, code string) string {
	key := "activity.sub." + code
	if label := i18n.T(lang, key); label != key {
		return label
	}
	return code
}
