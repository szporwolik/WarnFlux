package gsm

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf16"
)

// ussdCodeRE accepts the short codes an operator types by hand: a star or a
// hash, digits and stars/hashes inside, ending on a hash ("*100#",
// "*#06#", "#111#"). Anything else is not a USSD code and must never reach
// the modem — the AT parser would happily take a quote and a second command.
var ussdCode = func(s string) bool {
	s = strings.TrimSpace(s)
	if len(s) < 3 || len(s) > 32 {
		return false
	}
	if s[0] != '*' && s[0] != '#' {
		return false
	}
	if s[len(s)-1] != '#' {
		return false
	}
	for _, r := range s {
		if (r < '0' || r > '9') && r != '*' && r != '#' {
			return false
		}
	}
	return true
}

// ValidUSSD reports whether the text has the shape of a USSD short code.
func ValidUSSD(code string) bool { return ussdCode(code) }

// ErrUSSDRejected reports a code the network refused (expired session, no such
// service); the modem answers ERROR or +CUSD with a failure status.
var ErrUSSDRejected = errors.New("gsm: ussd code rejected")

// ErrUSSDTimeout reports a code the network never answered: USSD replies can
// take longer than an SMS, but the operator must not be left waiting forever.
var ErrUSSDTimeout = errors.New("gsm: the network did not answer the ussd code")

// ussdWait is how long the network answer is awaited after the modem accepted
// the request. Networks that need a second leg (a menu) answer immediately
// with the menu text, so this budget only covers a slow HLR.
const ussdWait = 40 * time.Second

// SendUSSD runs one USSD short code (a balance check like "*100#") and returns
// the text the network sent back. No addressee is involved: the code carries
// its own destination, which is exactly why the panel used to be unable to run
// one — every send went through the SMS path and demanded a phone number.
func (h *Hub) SendUSSD(ctx context.Context, code string) (string, error) {
	code = strings.TrimSpace(code)
	if !ValidUSSD(code) {
		return "", fmt.Errorf("gsm: %q is not a ussd short code", code)
	}
	if err := ctx.Err(); err != nil {
		return "", fmt.Errorf("gsm: ussd %s: %w", code, err)
	}
	if err := h.mu.acquire(ctx); err != nil {
		return "", fmt.Errorf("gsm: ussd %s: %w", code, err)
	}
	defer h.mu.Unlock()
	if h.conn == nil || !h.connected.Load() {
		return "", ErrNoModem
	}

	// A previous code (or a menu nobody answered) leaves a session open, and
	// every new request is then refused with a bare ERROR — exactly what the
	// E173 did on the bench after a couple of probes. Cancelling first is free
	// and makes the feature work on the second try instead of never.
	//
	// The cancel itself goes through the context-aware exchange: it repairs the
	// session first, and that repair may reconnect and replace h.conn with nil
	// (a modem unplugged between the lock and the write). The connection is
	// therefore re-checked afterwards — writing into a repaired-away session is
	// how this call used to panic on a nil file handle.
	if _, err := h.exchangeCtxLocked(ctx, "AT+CUSD=2", "OK", atTimeout); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return "", fmt.Errorf("gsm: ussd %s: %w", code, ctxErr)
		}
		if errors.Is(err, ErrNoModem) {
			return "", ErrNoModem
		}
		// Anything else is a modem that may still be usable: fall through, the
		// real request below reports the truth.
		h.logger.Debug("gsm: ussd session reset refused", "error", err)
	}
	if h.conn == nil || !h.connected.Load() {
		return "", ErrNoModem
	}
	if _, err := fmt.Fprintf(h.conn, "AT+CUSD=1,\"%s\",15\r", code); err != nil {
		h.reconnectLocked()
		return "", fmt.Errorf("gsm: ussd %s: %w", code, err)
	}

	deadline := h.now().Add(ussdWait)
	if dl, ok := ctx.Deadline(); ok && dl.Before(deadline) {
		deadline = dl
	}
	var answer string
	for {
		if err := ctx.Err(); err != nil {
			// The modem still owes us an answer: whatever arrives next would be
			// read as the reply to the FOLLOWING command, so mark the session
			// for repair instead of leaving that trap behind.
			h.sendDirty = true
			return answer, fmt.Errorf("gsm: ussd %s: %w", code, err)
		}
		if h.now().After(deadline) {
			h.sendDirty = true
			return answer, ErrUSSDTimeout
		}
		line, err := h.readLineCtx(ctx, deadline)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				h.sendDirty = true
				return answer, fmt.Errorf("gsm: ussd %s: %w", code, ctxErr)
			}
			if errors.Is(err, ErrTimeout) {
				h.sendDirty = true
				return answer, ErrUSSDTimeout
			}
			h.reconnectLocked()
			return answer, fmt.Errorf("gsm: ussd %s: %w", code, err)
		}
		line = strings.TrimRight(line, "\r\n")
		switch {
		case line == "":
			continue
		case strings.HasPrefix(line, "+CUSD:"):
			text, status, failure := parseCUSD(line)
			if failure {
				if status == cusdStatusNotSupported {
					// The network never took the code: nothing is owed, so the
					// session is clean and the next command needs no repair.
					return "", fmt.Errorf("%w (status %d)", ErrUSSDRejected, status)
				}
				h.sendDirty = true
				return "", fmt.Errorf("%w (status %d)", ErrUSSDRejected, status)
			}
			if status != cusdStatusFinished && status != cusdStatusMenu {
				// An unrecognised status means the reply was not understood:
				// never report it as a successful empty answer.
				h.sendDirty = true
				return "", fmt.Errorf("%w: unexpected +CUSD status %d", ErrUSSDRejected, status)
			}
			return text, nil
		case line == "ERROR", strings.HasPrefix(line, "+CME ERROR"), strings.HasPrefix(line, "+CMS ERROR"):
			return "", fmt.Errorf("%w: %s", ErrUSSDRejected, strings.TrimSpace(line))
		case line == "OK":
			// The modem accepted the request; the URC follows in a moment.
			continue
		}
	}
}

// +CUSD status codes (3GPP TS 27.007): 0 nothing to report, 1 further
// information via another request, 2 network terminated, 4 not supported,
// 5 unexpected response.
const (
	cusdStatusFinished     = 0
	cusdStatusMenu         = 1
	cusdStatusNotSupported = 4
)

// parseCUSD reads one "+CUSD: <status>[,"<text>",<dcs>]" line.
//
// The status is the answer; the text is optional. A bare "+CUSD: 4" is the same
// failure as "+CUSD: 4,\"\",15" — treating the short form as an empty success
// reported a rejected code as if the network had answered nothing, so the status
// is classified before the payload is even looked at.
func parseCUSD(line string) (text string, status int, failure bool) {
	rest := strings.TrimSpace(strings.TrimPrefix(line, "+CUSD:"))
	field := rest
	if i := strings.IndexAny(rest, ","); i >= 0 {
		field = rest[:i]
		rest = rest[i+1:]
	} else {
		rest = ""
	}
	n, err := strconv.Atoi(strings.TrimSpace(field))
	if err != nil {
		// Neither a known status nor a shape this parser understands.
		return "", -1, true
	}
	switch n {
	case cusdStatusFinished, cusdStatusMenu:
	case 2, cusdStatusNotSupported, 5:
		return "", n, true
	default:
		return "", n, true
	}
	rest = strings.TrimSpace(rest)
	if !strings.HasPrefix(rest, "\"") {
		return "", n, false
	}
	rest = rest[1:]
	if i := strings.LastIndex(rest, "\""); i >= 0 {
		rest = rest[:i]
	}
	if rest == "" {
		return "", n, false
	}
	return decodeUSSDText(rest), n, false
}

// decodeUSSDText turns the modem's payload into readable text. A hex blob
// carries UTF-16BE (what a Polish network sends for "Stan konta: 12,34 zł") or
// the 7-bit default alphabet; anything else is already plain text.
func decodeUSSDText(payload string) string {
	if len(payload)%4 != 0 || !isHex(payload) {
		return payload
	}
	raw, err := hex.DecodeString(payload)
	if err != nil || len(raw) < 2 {
		return payload
	}
	var units []uint16
	for i := 0; i+1 < len(raw); i += 2 {
		units = append(units, uint16(raw[i])<<8|uint16(raw[i+1]))
	}
	out := strings.TrimSpace(string(utf16.Decode(units)))
	if out == "" || !looksLikeText(out) {
		return payload
	}
	return out
}

// looksLikeText rejects a decoded run that is mostly control characters: a
// GSM7 blob decoded as UTF-16 produces exactly that, and showing it would be
// worse than showing the raw payload.
func looksLikeText(s string) bool {
	good, bad := 0, 0
	for _, r := range s {
		switch {
		case r == '\n' || r == '\r' || r == '\t':
			good++
		case r < 0x20 || r == 0xFFFD:
			bad++
		default:
			good++
		}
	}
	return good > bad
}

// isHex reports whether the string consists of hex digits only.
func isHex(s string) bool {
	for _, r := range s {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') && (r < 'A' || r > 'F') {
			return false
		}
	}
	return true
}
