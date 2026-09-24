package sanity

import "strings"

// maxSubjectRunes bounds an email subject line. RFC 5322 allows 998
// octets per header field; 150 runes is a comfortable, readable cap for a
// notification subject.
const maxSubjectRunes = 150

// polishDiacritics maps Polish diacritics to their ASCII base letters.
// Kept here (not imported from internal/aprs) so the sanity package stays
// dependency-free and remains importable by aprs itself.
var polishDiacritics = map[rune]rune{
	'ą': 'a', 'ć': 'c', 'ę': 'e', 'ł': 'l', 'ń': 'n', 'ó': 'o', 'ś': 's', 'ź': 'z', 'ż': 'z',
	'Ą': 'A', 'Ć': 'C', 'Ę': 'E', 'Ł': 'L', 'Ń': 'N', 'Ó': 'O', 'Ś': 'S', 'Ź': 'Z', 'Ż': 'Z',
}

// normalizeAPRS produces single-line 7-bit ASCII APRS text: all
// whitespace (including newlines) collapses to single spaces, Polish
// diacritics transliterate, other non-ASCII characters drop, and the
// result is trimmed. The protocol length limit is NOT applied here — the
// hub owns MaxMessageText because it knows the ack-suffix reserve.
func normalizeAPRS(in string) (string, string) {
	var b strings.Builder
	dropped := 0
	for _, r := range in {
		switch {
		case r < 0x80:
			// ASCII kept verbatim; whitespace collapse happens below.
			b.WriteRune(r)
		case polishDiacritics[r] != 0:
			b.WriteRune(polishDiacritics[r])
		default:
			dropped++
		}
	}
	out := strings.Join(strings.Fields(b.String()), " ")
	note := ""
	switch {
	case out != in:
		note = "normalized: whitespace collapsed, trim"
		if dropped > 0 {
			note += ", non-ASCII dropped"
		}
	case dropped > 0:
		note = "normalized: non-ASCII dropped"
	}
	return out, note
}

// normalizeSubject produces a single-line, bounded email subject: all
// whitespace collapses to single spaces, the result is trimmed and capped
// at maxSubjectRunes. UTF-8 content (e.g. Polish) is preserved — email
// is an 8-bit medium.
func normalizeSubject(in string) (string, string) {
	out := strings.Join(strings.Fields(in), " ")
	if len([]rune(out)) > maxSubjectRunes {
		rs := []rune(out)[:maxSubjectRunes]
		out = strings.TrimSpace(string(rs))
	}
	if out == in {
		return out, ""
	}
	return out, "normalized: subject trimmed/collapsed"
}

// normalizeEmailBody produces a clean plain-text body: CRLF normalizes to
// LF, control characters other than tab and newline are stripped, and
// blank-line runs collapse to a single blank line. Leading/trailing blank
// lines are removed.
func normalizeEmailBody(in string) (string, string) {
	// Normalize line endings and drop control characters.
	in = strings.ReplaceAll(in, "\r\n", "\n")
	in = strings.ReplaceAll(in, "\r", "\n")
	var b strings.Builder
	b.Grow(len(in))
	for _, r := range in {
		if r == '\n' || r == '\t' || r >= 0x20 {
			b.WriteRune(r)
		}
	}

	// Collapse blank-line runs to a single blank line.
	lines := strings.Split(b.String(), "\n")
	out := make([]string, 0, len(lines))
	blank := 0
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			blank++
			if blank > 1 {
				continue
			}
			out = append(out, "")
			continue
		}
		blank = 0
		out = append(out, line)
	}
	for len(out) > 0 && out[0] == "" {
		out = out[1:]
	}
	for len(out) > 0 && out[len(out)-1] == "" {
		out = out[:len(out)-1]
	}
	res := strings.Join(out, "\n")
	if res == in {
		return res, ""
	}
	return res, "normalized: body control characters and blank lines"
}
