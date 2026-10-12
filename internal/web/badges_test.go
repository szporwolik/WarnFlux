package web

import (
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/szporwolik/WarnFlux/internal/action"
	"github.com/szporwolik/WarnFlux/internal/dispatch"
	"github.com/szporwolik/WarnFlux/internal/dispatch/state"
	"github.com/szporwolik/WarnFlux/internal/i18n"
	"github.com/szporwolik/WarnFlux/internal/mqttreceiver"
	"github.com/szporwolik/WarnFlux/internal/plugin"
)

// TestPluginBadgeClass pins the shared severity mapping: the same
// condition reads the same colour in every card, and a plugin that is
// merely switched off is never painted as a fault.
func TestPluginBadgeClass(t *testing.T) {
	cases := []struct {
		state plugin.PluginState
		want  string
	}{
		{plugin.StateRunning, badgeOK},
		{plugin.StateDegraded, badgeWarn},
		// Suspended is both the offline-mode hold and "paused after
		// repeated failures": limited either way, never a red fault.
		{plugin.StateSuspended, badgeWarn},
		// It is enabled and not running: the one state that is a fault.
		{plugin.StateStopped, badgeBad},
		{plugin.StateDisabled, badgeMuted},
		{plugin.StateStarting, badgeMuted},
		{plugin.StateStopping, badgeMuted},
		{plugin.PluginState("some-future-state"), badgeMuted},
	}
	for _, c := range cases {
		if got := pluginBadgeClass(c.state); got != c.want {
			t.Errorf("pluginBadgeClass(%q) = %q, want %q", c.state, got, c.want)
		}
	}
}

// TestActionBadgeClass pins the action side of the same vocabulary.
func TestActionBadgeClass(t *testing.T) {
	cases := []struct {
		state action.InstanceState
		want  string
	}{
		{action.StateHealthy, badgeOK},
		{action.StateDegraded, badgeWarn},
		{action.StateDisabled, badgeMuted},
		{action.InstanceState("some-future-state"), badgeMuted},
	}
	for _, c := range cases {
		if got := actionBadgeClass(c.state); got != c.want {
			t.Errorf("actionBadgeClass(%q) = %q, want %q", c.state, got, c.want)
		}
	}
}

// TestWorse pins the verdict merge: the health verdict is the worst
// finding, and an unknown class never outranks a real one.
func TestWorse(t *testing.T) {
	cases := []struct {
		a, b, want string
	}{
		{badgeOK, badgeOK, badgeOK},
		{badgeOK, badgeWarn, badgeWarn},
		{badgeWarn, badgeBad, badgeBad},
		{badgeBad, badgeOK, badgeBad},
		{badgeMuted, badgeOK, badgeOK},
		{badgeOK, badgeMuted, badgeOK},
	}
	for _, c := range cases {
		if got := worse(c.a, c.b); got != c.want {
			t.Errorf("worse(%q, %q) = %q, want %q", c.a, c.b, got, c.want)
		}
	}
}

// TestBadgeClassesAreStyled proves every class the helpers can return has
// a rule in the stylesheet: an uncoloured badge is how a condition
// disappears from the operator's view.
func TestBadgeClassesAreStyled(t *testing.T) {
	css, err := staticFS.ReadFile("static/style.css")
	if err != nil {
		t.Fatal(err)
	}
	for _, class := range []string{badgeOK, badgeWarn, badgeBad, badgeMuted} {
		if !strings.Contains(string(css), ".badge."+class) {
			t.Errorf("no stylesheet rule for .badge.%s", class)
		}
	}
}

// verdictFixture builds the smallest station the health card can read: a
// router with the given plugin statuses, no receivers and no actions.
func verdictFixture(t *testing.T, statuses []plugin.PluginStatus) *Server {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ingress := dispatch.NewIngress(64)
	receivers, err := mqttreceiver.NewManager(nil, state.New(), ingress, logger, mqttreceiver.NewTrafficBuffer(16))
	if err != nil {
		t.Fatal(err)
	}
	actions, err := action.NewManager(nil, action.NewRegistry(), logger, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	return &Server{
		router:    statusRouter(statuses),
		receivers: receivers,
		actions:   actions,
		ingress:   ingress,
		logger:    logger,
	}
}

// statusRouter is a RouterStatuses over a fixed status list.
type statusRouter []plugin.PluginStatus

func (s statusRouter) Statuses() []plugin.PluginStatus { return s }

// TestHealthVerdict pins the card's verdict: it is the worst finding, in
// the vocabulary every other card uses — amber for something limited or
// held, red for something that should run and does not, green when
// nothing is wrong, and gray (switched off) never colours it.
func TestHealthVerdict(t *testing.T) {
	src := func(st plugin.PluginState) []plugin.PluginStatus {
		return []plugin.PluginStatus{{ID: "imgw-warnings", Type: "imgw", Kind: plugin.KindSource, State: st}}
	}
	cases := []struct {
		name                   string
		statuses               []plugin.PluginStatus
		wantClass, wantVerdict string
	}{
		{"running", src(plugin.StateRunning), badgeOK, "OK"},
		// The user-visible rule: a degraded source is amber in the table
		// AND amber in the verdict above it, never red.
		{"degraded", src(plugin.StateDegraded), badgeWarn, "DEGRADED"},
		{"suspended", src(plugin.StateSuspended), badgeWarn, "DEGRADED"},
		{"stopped", src(plugin.StateStopped), badgeBad, "FAULT"},
		{"disabled", src(plugin.StateDisabled), badgeOK, "OK"},
		{"starting", src(plugin.StateStarting), badgeOK, "OK"},
		{"degraded and stopped", append(src(plugin.StateDegraded), plugin.PluginStatus{
			ID: "mqtt-main", Type: "mqtt", Kind: plugin.KindOutput, State: plugin.StateStopped}), badgeBad, "FAULT"},
	}
	for _, c := range cases {
		v := verdictFixture(t, c.statuses).buildHealthView(i18n.LangEN)
		if v.Overall != c.wantClass || v.OverallText != c.wantVerdict {
			t.Errorf("%s: verdict = %q %q, want %q %q", c.name, v.Overall, v.OverallText, c.wantClass, c.wantVerdict)
		}
		// The verdict class and the row classes stay inside the vocabulary.
		for _, class := range []string{v.Overall, v.DB.BadgeClass, v.Queue.BadgeClass, v.Pending.BadgeClass} {
			switch class {
			case badgeOK, badgeWarn, badgeBad, badgeMuted:
			default:
				t.Errorf("%s: row carries the unknown class %q", c.name, class)
			}
		}
	}
}
