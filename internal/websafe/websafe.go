// Package websafe renders external endpoints for logs without leaking the
// secrets they carry.
//
// Webhook URLs (Discord, Slack, anything with a token in the path) are
// credentials: whoever reads the log can post as the station. Yet the failure
// that matters most — "the transport died" — arrives as a *url.Error whose
// message repeats the whole request URL, so the naive log line is a leak. One
// helper for every such call site keeps the rule in one place.
package websafe

import (
	"net/url"
	"strings"
)

// placeholder replaces whatever must not be readable in a log.
const placeholder = "[redacted]"

// RedactedURL keeps the scheme and host (that is the diagnosable part: wrong
// host, TLS, DNS) and masks the path tail, the query and any userinfo. The
// leading path segment survives so an operator can still tell which endpoint
// kind failed ("/api/webhooks/…").
func RedactedURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return placeholder
	}
	var b strings.Builder
	b.WriteString(u.Scheme)
	b.WriteString("://")
	b.WriteString(u.Host)
	b.WriteString(maskPath(u.Path))
	if u.RawQuery != "" {
		b.WriteString("?" + placeholder)
	}
	if u.Fragment != "" {
		b.WriteString("#" + placeholder)
	}
	return b.String()
}

// maskPath keeps the first segment of the path ("api", "services") and masks
// the rest: for a webhook that is exactly the split between "the endpoint kind"
// and "the credential".
func maskPath(path string) string {
	parts := pathSegments(path)
	if len(parts) == 0 {
		return ""
	}
	if len(parts) == 1 {
		return "/" + placeholder
	}
	return "/" + parts[0] + "/" + placeholder
}

// Error renders err for a log line about the given endpoint. Every occurrence
// of the raw URL, of its query string and of its final path segment (the token)
// is masked — including inside nested errors, because the reader of a log only
// ever sees text.
func Error(err error, raw string) string {
	if err == nil {
		return ""
	}
	msg := maskAll(err.Error(), raw)
	u, perr := url.Parse(raw)
	if perr != nil {
		return msg
	}
	msg = maskAll(msg, u.String())
	msg = maskAll(msg, u.EscapedPath())
	if u.RawQuery != "" {
		msg = maskAll(msg, u.RawQuery)
	}
	if u.User != nil {
		msg = maskAll(msg, u.User.String())
		msg = maskAll(msg, u.Scheme+"://"+u.User.String()+"@"+u.Host)
	}
	if parts := pathSegments(u.Path); len(parts) > 0 {
		// Host and the endpoint kind may stay; the last segment is the token.
		msg = maskAll(msg, u.Path)
		msg = maskAll(msg, parts[len(parts)-1])
	}
	return msg
}

// maskAll replaces every non-empty occurrence of needle.
func maskAll(haystack, needle string) string {
	if needle == "" || needle == "/" {
		return haystack
	}
	return strings.ReplaceAll(haystack, needle, placeholder)
}

// pathSegments splits a path into its non-empty elements.
func pathSegments(path string) []string {
	trimmed := strings.Trim(path, "/")
	if trimmed == "" {
		return nil
	}
	return strings.Split(trimmed, "/")
}
