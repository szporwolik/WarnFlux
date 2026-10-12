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

	// The last APRS recipient is reached, so that channel completes; the
	// e-mail one fails its only recipient.
	j.record("aprs", true)
	j.finishChannel("aprs")
	j.setChannelTotal("email", 1)
	j.record("email", false)
	j.finishChannel("email")
	j.finish("summary text")

	v = j.progressView(i18n.LangEN)
	if !v.Finished || v.Percent != 100 || v.Summary != "summary text" {
		t.Fatalf("finished view = %+v", v)
	}
	if v.Incomplete {
		t.Error("a broadcast that reached every recipient is not incomplete")
	}
	if !v.Failed {
		t.Error("a failed delivery must flag the bar")
	}
	if v.Headline != i18n.T(i18n.LangEN, "mass.progress.done") {
		t.Errorf("headline = %q", v.Headline)
	}
	if v.Channels[0].State != "done" || v.Channels[0].Detail != "3 ✓" ||
		v.Channels[1].State != "done" || v.Channels[1].Detail != "0 ✓ · 1 ✗" {
		t.Errorf("done chips = %+v / %+v", v.Channels[0], v.Channels[1])
	}
}

// TestMassIncompleteViewIsNotHundredPercent pins the reported P2 on the
// view: a channel that ran out of budget before reaching everyone used to
// be reported as finished ("done", 100%, "2 ✓") — the recipients that were
// never attempted must be visible instead (see the budget test for the
// channel side).
func TestMassIncompleteViewIsNotHundredPercent(t *testing.T) {
	j := &massJob{
		id:        "job2",
		createdAt: time.Now(),
		req:       massSendRequest{lang: i18n.LangEN, channels: []string{"email"}},
		channels:  []*massChannelProgress{{kind: "email", state: "pending"}},
	}
	j.setRecipients(3)
	j.setChannelTotal("email", 3)
	j.record("email", true)
	j.record("email", true)
	j.finishChannel("email")
	j.finish("summary text")

	v := j.progressView(i18n.LangEN)
	if !v.Finished || !v.Incomplete {
		t.Fatalf("finished incomplete view = %+v", v)
	}
	if v.Percent != 66 {
		t.Fatalf("percent = %d, want 66 (two of three recipients were reached)", v.Percent)
	}
	if v.Headline != i18n.T(i18n.LangEN, "mass.progress.incomplete") {
		t.Fatalf("headline = %q, want the incomplete one", v.Headline)
	}
	if v.Channels[0].State != "incomplete" || v.Channels[0].Detail != "2 ✓ · 1 not sent" {
		t.Fatalf("chip = %+v, want the unsent recipient reported", v.Channels[0])
	}
}
