package smtp

import (
	"context"
	"fmt"
	"mime"
	"strings"
	"time"

	"github.com/szporwolik/WarnFlux/internal/action"
	"github.com/szporwolik/WarnFlux/internal/appinfo"
	"github.com/szporwolik/WarnFlux/internal/i18n"
	"github.com/szporwolik/WarnFlux/internal/sanity"
)

// DirectNotice sends one branded notice email from a free-text message:
// a plain-text alternative plus the notification-styled HTML part with the
// embedded logo, the same envelope the routed notifications use. It is the
// branded counterpart of Direct for the admin Mass info page, so a one-off
// broadcast reads like every other WarnFlux communication. Callers must
// bound the context.
func DirectNotice(ctx context.Context, cfg Config, to []string, subject, text string, app action.AppInfo, lang string) error {
	msg := noticeMessage(ctx, cfg, to, subject, text, app, lang, time.Now())
	return DirectMessage(ctx, cfg, to, msg)
}

// noticeMessage builds the multipart/related notice: a plain-text fallback
// and the branded HTML body, plus the inline logo. Subject and plain body
// pass through the shared normalizer like the routed mail.
func noticeMessage(ctx context.Context, cfg Config, to []string, subject, text string, app action.AppInfo, lang string, now time.Time) []byte {
	lang = i18n.Effective(lang)
	subject = sanity.NormalizeText(ctx, sanity.ChannelEmailSubject, subject)
	plain := sanity.NormalizeText(ctx, sanity.ChannelEmailBody, noticePlain(app, text, lang, now))
	htmlBody := noticeHTML(app, text, lang, now)

	relatedBoundary := fmt.Sprintf("warnflux-rel-%d", now.UnixNano())
	altBoundary := fmt.Sprintf("warnflux-alt-%d", now.UnixNano())

	var b strings.Builder
	fmt.Fprintf(&b, "From: %s\r\n", senderAddress(cfg))
	fmt.Fprintf(&b, "To: %s\r\n", strings.Join(to, ", "))
	fmt.Fprintf(&b, "Subject: %s\r\n", mime.QEncoding.Encode("utf-8", subject))
	b.WriteString("MIME-Version: 1.0\r\n")
	fmt.Fprintf(&b, "Content-Type: multipart/related; boundary=\"%s\"\r\n", relatedBoundary)
	b.WriteString("\r\n")

	fmt.Fprintf(&b, "--%s\r\n", relatedBoundary)
	fmt.Fprintf(&b, "Content-Type: multipart/alternative; boundary=\"%s\"\r\n\r\n", altBoundary)

	fmt.Fprintf(&b, "--%s\r\n", altBoundary)
	b.WriteString("Content-Type: text/plain; charset=utf-8\r\n")
	b.WriteString("Content-Transfer-Encoding: 8bit\r\n\r\n")
	b.WriteString(plain)
	b.WriteString("\r\n")

	fmt.Fprintf(&b, "--%s\r\n", altBoundary)
	b.WriteString("Content-Type: text/html; charset=utf-8\r\n")
	b.WriteString("Content-Transfer-Encoding: 8bit\r\n\r\n")
	b.WriteString(htmlBody)
	b.WriteString("\r\n")
	fmt.Fprintf(&b, "--%s--\r\n", altBoundary)

	fmt.Fprintf(&b, "--%s\r\n", relatedBoundary)
	b.WriteString("Content-Type: image/png; name=\"logo.png\"\r\n")
	fmt.Fprintf(&b, "Content-ID: <%s>\r\n", logoCID)
	b.WriteString("Content-Disposition: inline; filename=\"logo.png\"\r\n")
	b.WriteString("Content-Transfer-Encoding: base64\r\n\r\n")
	b.WriteString(wrapBase64(appinfo.LogoPNG()))
	b.WriteString("\r\n")
	fmt.Fprintf(&b, "--%s--\r\n", relatedBoundary)
	return []byte(b.String())
}

// noticePlain renders the plain-text fallback: the notice first, then the
// shared branded footer.
func noticePlain(app action.AppInfo, text, lang string, now time.Time) string {
	req := action.ActionRequest{App: app}
	var b strings.Builder
	b.WriteString(i18n.T(lang, "mass.mail_subject") + "\n")
	fmt.Fprintf(&b, "%s: %s\n\n", i18n.T(lang, "mail.time"), humanTime(now))
	b.WriteString(strings.TrimSpace(text))
	b.WriteString("\n\n--\n")
	b.WriteString(footerText(req, lang))
	return b.String()
}

// noticeHTML renders the branded HTML body in the notification style: the
// accent bar, the logo + system header, the notice callout, the timestamp
// and the branded footer.
func noticeHTML(app action.AppInfo, text, lang string, now time.Time) string {
	req := action.ActionRequest{App: app}
	const accent = "#303c46"
	brand := strings.TrimSpace(app.Header1)
	if brand == "" {
		brand = "WarnFlux"
	}

	var b strings.Builder
	fmt.Fprintf(&b, `<div style="background-color:#0f1419;padding:24px;font-family:-apple-system,'Segoe UI',Roboto,Arial,sans-serif;">
<table role="presentation" width="100%%" cellpadding="0" cellspacing="0" style="max-width:640px;margin:0 auto;background-color:#151b21;border:1px solid #303c46;border-radius:8px;">
<tr><td bgcolor="%s" style="background-color:%s;height:4px;line-height:4px;font-size:0;">&nbsp;</td></tr>
<tr><td style="padding:24px 28px;color:#eef2f5;font-size:14px;">`, accent, accent)

	fmt.Fprintf(&b, `<table role="presentation" cellpadding="0" cellspacing="0" style="margin-bottom:16px;"><tr>
<td style="padding-right:10px;"><img src="cid:%s" width="36" height="36" alt="%s" style="width:36px;height:36px;border-radius:10px;display:block;border:0;"></td>
<td style="vertical-align:middle;font-size:16px;font-weight:700;color:#eef2f5;letter-spacing:.02em;">%s</td>
</tr></table>`,
		logoCID, htmlEscaper(brand), htmlEscaper(brand))

	fmt.Fprintf(&b, `<div style="font-size:11px;letter-spacing:.08em;text-transform:uppercase;color:#87939e;">%s</div>`,
		htmlEscaper(i18n.T(lang, "mass.mail_subject")))

	fmt.Fprintf(&b, `<div style="margin-top:16px;background-color:#1b232b;border-left:4px solid %s;border-radius:8px;padding:14px 18px;font-size:20px;font-weight:700;color:#eef2f5;line-height:1.4;">%s</div>`,
		accent, htmlEscaper(strings.TrimSpace(text)))

	fmt.Fprintf(&b, `<div style="margin-top:16px;color:#87939e;font-size:12px;">%s: %s</div>`,
		htmlEscaper(i18n.T(lang, "mail.updated")), htmlEscaper(humanTime(now)))

	b.WriteString(`<div style="margin-top:24px;border-top:1px solid #303c46;padding-top:14px;font-size:12px;color:#87939e;">`)
	b.WriteString(footerHTML(req, lang))
	b.WriteString(`</div>`)

	b.WriteString(`</td></tr></table></div>`)
	return b.String()
}
