package smtp

import (
	"strings"
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/action"
)

// TestNoticeEnvelopeLabelIsCallerChosen pins the one difference between the
// broadcast notice and the daily status mail: both ride the same branded
// envelope, but the envelope label ("Mass info") belongs to a broadcast, not to
// a system report. The default must stay untouched for the Mass page.
func TestNoticeEnvelopeLabelIsCallerChosen(t *testing.T) {
	app := action.AppInfo{Version: "0.8.13", Header1: "SOSNA", Domain: "sosna.sp9moa.pl"}
	now := time.Date(2026, 10, 29, 6, 0, 0, 0, time.UTC)

	broadcast := noticeHTML(app, "<p>x</p>", "en", now, true, "")
	if !strings.Contains(broadcast, "Mass info") {
		t.Errorf("default envelope lost the broadcast label:\n%s", broadcast)
	}

	report := noticeHTML(app, "<p>x</p>", "en", now, true, "digest.mail_label")
	if !strings.Contains(report, "Daily system status") {
		t.Errorf("caller-chosen label missing from the HTML part:\n%s", report)
	}
	if strings.Contains(report, "Mass info") {
		t.Errorf("status mail still carries the broadcast label:\n%s", report)
	}

	plain := noticePlain(app, "x", "pl", now, "digest.mail_label")
	if !strings.HasPrefix(plain, "Dzienny status systemu") {
		t.Errorf("plain part = %q, want the localized label first", plain)
	}
}
