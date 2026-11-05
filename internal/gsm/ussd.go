package gsm

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
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

	// 15 = the network's own language, which is what an operator expects on a
	// Polish SIM. The reply arrives asynchronously (+CUSD URC), so the exchange
	// is written by hand instead of reusing the "wait for OK" helper.
	h.repairDirtyLocked()
	if _, err := fmt.Fprintf(h.conn, "AT+CUSD=1,\"%s\",15\r", code); err != nil {
		h.reconnectLocked()
		return "", fmt.Errorf("gsm: ussd %s: %w", code, err)
	}

	deadline := h.now().Add(ussdWait)
	var answer string
	for {
		if h.now().After(deadline) {
			return answer, ErrUSSDTimeout
		}
		if err := ctx.Err(); err != nil {
			return answer, fmt.Errorf("gsm: ussd %s: %w", code, err)
		}
		line, err := h.readLine(deadline)
		if err != nil {
			if errors.Is(err, ErrTimeout) {
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
			text, failure := parseCUSD(line)
			if failure {
				return "", fmt.Errorf("%w (status %s)", ErrUSSDRejected, strings.TrimSpace(line))
			}
			answer = text
			// A finished session (status 0) ends the wait; a menu (status 1)
			// also comes back as text, and the operator can follow it up with
			// another code.
			return answer, nil
		case line == "ERROR", strings.HasPrefix(line, "+CME ERROR"), strings.HasPrefix(line, "+CMS ERROR"):
			return "", fmt.Errorf("%w: %s", ErrUSSDRejected, strings.TrimSpace(line))
		case line == "OK":
			// The modem accepted the request; the URC follows in a moment.
			continue
		}
	}
}

// parseCUSD reads one "+CUSD: <status>,"<text>",<dcs>" line. The text is
// quoted; Huawei firmware answers it either as plain characters or as a hex
// blob of the UCS2 (or GSM7) encoding, so both shapes are decoded. failure is
// set when the status says the session ended badly.
func parseCUSD(line string) (text string, failure bool) {
	rest := strings.TrimSpace(strings.TrimPrefix(line, "+CUSD:"))
	status := ""
	if i := strings.IndexAny(rest, ","); i >= 0 {
		status, rest = strings.TrimSpace(rest[:i]), rest[i+1:]
	} else {
		return "", false
	}
	switch status {
	case "0", "1": // 0 = no further action, 1 = a menu follows
	case "2", "4", "5": // network cannot process / operation not supported / unexpected
		return "", true
	default:
		return "", false
	}
	rest = strings.TrimSpace(rest)
	if !strings.HasPrefix(rest, "\"") {
		return "", false
	}
	rest = rest[1:]
	if i := strings.LastIndex(rest, "\""); i >= 0 {
		rest = rest[:i]
	}
	if rest == "" {
		return "", false
	}
	return decodeUSSDText(rest), false
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
