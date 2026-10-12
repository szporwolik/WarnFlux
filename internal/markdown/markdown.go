// Package markdown renders the small markdown subset the interface
// announces to the surfaces that cannot render markdown themselves: the
// HTML body of a notification mail and its plain-text alternative. The
// browser already renders the same subset client side (see app.js), so an
// operator writes one syntax and sees it everywhere.
//
// The subset: headings (# to ####), emphasis (**bold**, *italic*), inline
// code (`code`), fenced code blocks (```), bullet lists (- or *) and links
// ([text](https://…)). Everything else stays literal text.
//
// Safety: the source is escaped FIRST, so a message can never smuggle
// markup — or a script — into a mail body. Only the syntax above becomes
// tags, and only http/https links become anchors.
package markdown

import (
	"fmt"
	"html"
	"regexp"
	"strings"
)

var (
	headingRe = regexp.MustCompile(`^(#{1,4})\s+(.*)$`)
	bulletRe  = regexp.MustCompile(`^\s*[-*]\s+(.*)$`)
	fenceRe   = regexp.MustCompile("^\\s*```")
	// inlineRe matches the inline syntax in one pass, so a marker can
	// never be re-interpreted by a later rule.
	inlineRe = regexp.MustCompile("`([^`\n]+)`|\\*\\*([^*\n]+)\\*\\*|\\*([^*\n]+)\\*|\\[([^\\]\\n]+)\\]\\((https?://[^)\\s]+)\\)")
)

// HTML renders the subset as HTML: paragraphs, headings, lists, code
// blocks and inline emphasis, with the source escaped first.
func HTML(src string) string {
	var out strings.Builder
	var para []string
	flush := func() {
		if len(para) > 0 {
			out.WriteString("<p>" + strings.Join(para, "<br>") + "</p>\n")
			para = nil
		}
	}
	lines := strings.Split(strings.ReplaceAll(src, "\r\n", "\n"), "\n")
	for i := 0; i < len(lines); i++ {
		line := lines[i]
		if fenceRe.MatchString(line) {
			flush()
			var code []string
			for i++; i < len(lines) && !fenceRe.MatchString(lines[i]); i++ {
				code = append(code, lines[i])
			}
			out.WriteString("<pre><code>" + html.EscapeString(strings.Join(code, "\n")) + "</code></pre>\n")
			continue
		}
		if h := headingRe.FindStringSubmatch(line); h != nil {
			flush()
			out.WriteString("<h4>" + inlineHTML(h[2]) + "</h4>\n")
			continue
		}
		if b := bulletRe.FindStringSubmatch(line); b != nil {
			flush()
			out.WriteString("<ul>")
			for {
				out.WriteString("<li>" + inlineHTML(b[1]) + "</li>")
				i++
				if i >= len(lines) {
					break
				}
				b = bulletRe.FindStringSubmatch(lines[i])
				if b == nil {
					i--
					break
				}
			}
			out.WriteString("</ul>\n")
			continue
		}
		if strings.TrimSpace(line) == "" {
			flush()
			continue
		}
		para = append(para, inlineHTML(line))
	}
	flush()
	return strings.TrimRight(out.String(), "\n")
}

// inlineHTML renders one line's inline syntax. The line is escaped before
// any marker is looked for, so only our own tags end up in the output.
func inlineHTML(line string) string {
	escaped := html.EscapeString(line)
	return inlineRe.ReplaceAllStringFunc(escaped, func(m string) string {
		g := inlineRe.FindStringSubmatch(m)
		switch {
		case g[1] != "":
			return "<code>" + g[1] + "</code>"
		case g[2] != "":
			return "<strong>" + g[2] + "</strong>"
		case g[3] != "":
			return "<em>" + g[3] + "</em>"
		default:
			return `<a href="` + g[5] + `" target="_blank" rel="noopener noreferrer">` + g[4] + `</a>`
		}
	})
}

// Discord rewrites the subset into what Discord renders natively. Discord
// has no heading syntax, so a heading becomes a bold line; emphasis,
// inline code, fences, bullet lists and links are Discord markdown
// already and pass through untouched.
func Discord(src string) string {
	lines := strings.Split(strings.ReplaceAll(src, "\r\n", "\n"), "\n")
	inFence := false
	for i, line := range lines {
		if fenceRe.MatchString(line) {
			inFence = !inFence
			continue
		}
		if inFence {
			continue // code is literal: never rewrite inside it
		}
		if h := headingRe.FindStringSubmatch(line); h != nil {
			lines[i] = "**" + strings.TrimSpace(h[2]) + "**"
		}
	}
	return strings.Join(lines, "\n")
}

// Plain flattens the subset into readable plain text for the text/plain
// alternative of a mail: heading and emphasis markers go away, list items
// keep their dash, links keep their label followed by the URL in
// parentheses, and code blocks keep their lines.
func Plain(src string) string {
	var out []string
	inFence := false
	for _, line := range strings.Split(strings.ReplaceAll(src, "\r\n", "\n"), "\n") {
		if fenceRe.MatchString(line) {
			inFence = !inFence
			continue
		}
		if inFence {
			out = append(out, line)
			continue
		}
		if h := headingRe.FindStringSubmatch(line); h != nil {
			out = append(out, strings.TrimSpace(h[2]))
			continue
		}
		out = append(out, plainInline(line))
	}
	return strings.TrimRight(strings.Join(out, "\n"), "\n")
}

// plainInline drops the inline markers of one line.
func plainInline(line string) string {
	return inlineRe.ReplaceAllStringFunc(line, func(m string) string {
		g := inlineRe.FindStringSubmatch(m)
		switch {
		case g[1] != "":
			return g[1] // code
		case g[2] != "":
			return g[2] // bold
		case g[3] != "":
			return g[3] // italic
		default:
			return fmt.Sprintf("%s (%s)", g[4], g[5])
		}
	})
}
