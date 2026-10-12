package smtp

import "time"

// DefaultDeadlineForTest / SetDefaultDeadlineForTest let the external test
// package shorten the no-budget fallback.
func DefaultDeadlineForTest() time.Duration { return defaultDeadline }

func SetDefaultDeadlineForTest(d time.Duration) { defaultDeadline = d }
