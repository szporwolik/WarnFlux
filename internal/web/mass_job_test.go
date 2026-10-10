package web

import (
	"strings"
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/i18n"
)

// TestMassProgressView pins the progress math and per-channel chips: a
// running channel reports "done/total" and drives the bar, a finished job
// reports 100% plus the summary.
func TestMassProgressView(t *testing.T) {
	j := &massJob{
		id:        "job1",
		createdAt: time.Now(),
		req:       massSendRequest{lang: i18n.LangEN, channels: []string{"aprs", "email"}},
	}
	for _, k := range j.req.channels {
		j.channels = append(j.channels, &massChannelProgress{kind: k, state: "pending"})
	}

	// Before the recipients are resolved the panel says "preparing".
	v := j.progressView(i18n.LangEN)
	if v.Finished || v.Percent != 0 || v.Where != i18n.T(i18n.LangEN, "mass.progress.preparing") {
		t.Fatalf("preparing view = %+v", v)
	}
	if v.Channels[1].State != "pending" || v.Channels[1].Detail != i18n.T(i18n.LangEN, "mass.progress.queued") {
		t.Errorf("pending chip = %+v", v.Channels[1])
	}

	j.setRecipients(3)
	j.setChannelTotal("aprs", 3)
	j.record("aprs", true)
	j.record("aprs", true)

	v = j.progressView(i18n.LangEN)
	if v.Percent != 66 { // 2 of 3 steps
		t.Errorf("percent = %d, want 66", v.Percent)
	}
	if !strings.Contains(v.Where, "APRS") || !strings.Contains(v.Where, "2/3") {
		t.Errorf("running line = %q", v.Where)
	}
	if v.Channels[0].State != "running" || v.Channels[0].Detail != "2/3" {
		t.Errorf("running chip = %+v", v.Channels[0])
	}

	j.finishChannel("aprs")
	j.setChannelTotal("email", 1)
	j.record("email", false)
	j.finishChannel("email")
	j.finish("summary text")

	v = j.progressView(i18n.LangEN)
	if !v.Finished || v.Percent != 100 || v.Summary != "summary text" {
		t.Fatalf("finished view = %+v", v)
	}
	if !v.Failed {
		t.Error("a failed delivery must flag the bar")
	}
	if v.Headline != i18n.T(i18n.LangEN, "mass.progress.done") {
		t.Errorf("headline = %q", v.Headline)
	}
	if v.Channels[0].Detail != "2 ✓" || v.Channels[1].Detail != "0 ✓ · 1 ✗" {
		t.Errorf("done chips = %q / %q", v.Channels[0].Detail, v.Channels[1].Detail)
	}
}
