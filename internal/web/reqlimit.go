package web

import (
	"errors"
	"net/http"
	"strings"
	"unicode/utf8"
)

// Fixed limits for the unauthenticated forms and for any user-controlled
// value that reaches a log line or the audit trail. An anonymous request
// must not be able to make the process allocate or retain memory sized by
// its own payload.
const (
	// maxAuthFormBytes caps the body of a sign-in / recovery form. Such a
	// form is well under 1 KiB, so everything larger is refused BEFORE it
	// is parsed — throttled requests included.
	maxAuthFormBytes = 16 << 10
	// maxAuthUsernameBytes and maxAuthPasswordBytes bound the fields that
	// reach the directory lookup, the password hash and the audit trail.
	maxAuthUsernameBytes = 128
	maxAuthPasswordBytes = 256
	// maxAuthEmailBytes matches the practical e-mail address bound.
	maxAuthEmailBytes = 254
	// maxAuthTokenBytes bounds a password-reset token from a form or URL.
	maxAuthTokenBytes = 128
	// maxLoggedRunes bounds one user-controlled value written to a log
	// line (ring buffer and log file).
	maxLoggedRunes = 64
	// maxAuditDetailRunes bounds the detail field of an audit entry while
	// leaving room for the ids and counters the callers append.
	maxAuditDetailRunes = 256
)

// parseAuthForm parses a small unauthenticated form. An oversized body is
// answered with 413 instead of being buffered, and any other parse error
// with 400. It reports whether the request may continue.
func parseAuthForm(w http.ResponseWriter, r *http.Request) bool {
	r.Body = http.MaxBytesReader(w, r.Body, maxAuthFormBytes)
	if err := r.ParseForm(); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			http.Error(w, "request too large", http.StatusRequestEntityTooLarge)
			return false
		}
		http.Error(w, "bad request", http.StatusBadRequest)
		return false
	}
	return true
}

// logValue renders a user-controlled value for a log line or the audit
// trail: control characters (which would forge line breaks in the
// line-oriented log) become spaces and the value is truncated, so repeated
// anonymous attempts cannot write kilobytes each into memory.
func logValue(s string) string {
	return truncateRunes(s, maxLoggedRunes)
}

// truncateRunes strips control characters and cuts s to maxRunes runes.
func truncateRunes(s string, maxRunes int) string {
	s = strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return ' '
		}
		return r
	}, s)
	s = strings.ToValidUTF8(s, "")
	if utf8.RuneCountInString(s) <= maxRunes {
		return s
	}
	return string([]rune(s)[:maxRunes]) + "…"
}
