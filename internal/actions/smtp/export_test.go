package smtp

import "time"

// DefaultDeadlineForTest / SetDefaultDeadlineForTest let the external test
// package shorten the no-budget fallback.
func DefaultDeadlineForTest() time.Duration { return defaultDeadline }

func SetDefaultDeadlineForTest(d time.Duration) { defaultDeadline = d }

// ResetLimitersForTest drops the shared per-account limiters, so a test
// starts from an unspent budget.
func ResetLimitersForTest() {
	limiterMu.Lock()
	limiterShared = map[string]*rateLimiter{}
	limiterMu.Unlock()
}
