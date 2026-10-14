package web

import (
	"bytes"
	"html/template"
	"strings"
	"testing"

	"github.com/szporwolik/WarnFlux/internal/i18n"
	mesh "github.com/szporwolik/WarnFlux/internal/meshtastic"
)

// TestMeshtasticRemovalBadges pins the station-feed metric on the page: the
// status strip shows the removals the broker still owes, the ones WAITING for
// room in the ledger (the station's own backlog of quiet nodes — not a
// failure), and — in red — the genuine overflow that had to be abandoned.
func TestMeshtasticRemovalBadges(t *testing.T) {
	srv := &Server{}
	tmpl, err := template.New("root").Funcs(srv.templateFuncs()).ParseFS(templatesFS, "templates/*.html")
	if err != nil {
		t.Fatalf("parse templates: %v", err)
	}
	render := func(v meshtasticView) string {
		t.Helper()
		var buf bytes.Buffer
		if err := tmpl.ExecuteTemplate(&buf, "meshtastic", v); err != nil {
			t.Fatalf("render meshtastic: %v", err)
		}
		return buf.String()
	}
	label := i18n.T("en", "meshtastic.removals_label")

	// A feed with nothing outstanding shows no metric at all.
	if html := render(meshtasticView{Lang: "en"}); strings.Contains(html, label) {
		t.Errorf("a station with no removals outstanding shows the metric: %.300s", html)
	}

	// A backlog shows how much the broker still owes ...
	html := render(meshtasticView{
		Lang: "en",
		Snap: mesh.Snapshot{Connected: true, PendingRemovals: 3},
	})
	if !strings.Contains(html, label+" 3") {
		t.Errorf("the pending backlog is not shown: %.400s", html)
	}
	if !strings.Contains(html, i18n.T("en", "meshtastic.removals_pending")) {
		t.Errorf("the pending backlog carries no explanation: %.400s", html)
	}

	// A backlog longer than the ledger shows what is waiting, in the MUTED
	// style: the documents are not lost, they have no room yet.
	waiting := i18n.T("en", "meshtastic.removals_waiting_label")
	html = render(meshtasticView{
		Lang: "en",
		Snap: mesh.Snapshot{Connected: true, PendingRemovals: 512, RemovalsWaiting: 310},
	})
	if !strings.Contains(html, label+" 512") || !strings.Contains(html, waiting+" 310") {
		t.Errorf("the in-flight backlog and what waits for room are not both shown: %.400s", html)
	}
	if !strings.Contains(html, i18n.T("en", "meshtastic.removals_waiting")) {
		t.Errorf("what waits carries no explanation of its own: %.400s", html)
	}
	if strings.Contains(html, `class="badge bad"`) {
		t.Errorf("a waiting backlog is flagged as a failure: %.400s", html)
	}

	// ... and a genuine overflow IS shown as a failure.
	html = render(meshtasticView{
		Lang: "en",
		Snap: mesh.Snapshot{Connected: true, PendingRemovals: 512, RemovalsDropped: 88},
	})
	if !strings.Contains(html, label+" 512") || !strings.Contains(html, label+" 88") {
		t.Errorf("the backlog and the abandoned removals are not both shown: %.400s", html)
	}
	if !strings.Contains(html, `class="badge bad" title="`+i18n.T("en", "meshtastic.removals_dropped")) {
		t.Errorf("abandoned removals are not flagged: %.400s", html)
	}

	// The metric is not tied to the radio link: it shows while disconnected too.
	html = render(meshtasticView{Lang: "en", Snap: mesh.Snapshot{PendingRemovals: 1}})
	if !strings.Contains(html, i18n.T("en", "meshtastic.disconnected")) || !strings.Contains(html, label+" 1") {
		t.Errorf("the metric is missing on a disconnected station: %.400s", html)
	}
}
