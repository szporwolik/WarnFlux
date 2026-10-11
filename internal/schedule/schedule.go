// Package schedule holds the time helpers the transports share: both the
// Meshtastic presence beacon and the Discord presence line can be anchored
// to a daily clock time ("06:00", the station's local time), so both must
// agree on how that time is written and when it comes round next.
package schedule

import (
	"fmt"
	"strings"
	"time"
)

// ClockLayout is how a daily anchor time is written ("06:00").
const ClockLayout = "15:04"

// AtDefault is the daily anchor used when the configuration names neither a
// time nor a spacing — the two surfaces default to the same morning hour.
const AtDefault = "06:00"

// ParseClock splits "HH:MM" into hour and minute.
func ParseClock(at string) (int, int, error) {
	parsed, err := time.Parse(ClockLayout, strings.TrimSpace(at))
	if err != nil {
		return 0, 0, fmt.Errorf("want a clock time like %q, got %q", AtDefault, at)
	}
	return parsed.Hour(), parsed.Minute(), nil
}

// Valid reports whether at is a usable daily anchor.
func Valid(at string) bool {
	_, _, err := ParseClock(at)
	return err == nil
}

// NextDaily returns the first occurrence of the anchor at or after now, in
// now's own zone, so a DST change moves the wall clock rather than the
// anchor. An unparseable anchor yields a zero time, which callers treat as
// "not armed".
func NextDaily(now time.Time, at string) time.Time {
	return daily(now, at, false)
}

// NextDailyAfter returns the first occurrence of the anchor strictly after
// now — the re-arm after a notice has just gone out at its anchor time.
func NextDailyAfter(now time.Time, at string) time.Time {
	return daily(now, at, true)
}

func daily(now time.Time, at string, after bool) time.Time {
	hh, mm, err := ParseClock(at)
	if err != nil {
		return time.Time{}
	}
	next := time.Date(now.Year(), now.Month(), now.Day(), hh, mm, 0, 0, now.Location())
	if next.Before(now) || (after && !next.After(now)) {
		next = next.AddDate(0, 0, 1)
	}
	return next
}
