package web

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/szporwolik/WarnFlux/internal/action"
	"github.com/szporwolik/WarnFlux/internal/actions/smtp"
	"github.com/szporwolik/WarnFlux/internal/config"
	"github.com/szporwolik/WarnFlux/internal/i18n"
	"github.com/szporwolik/WarnFlux/internal/storage"
	"io"
)

// stubNoticeMailer records the notices the mass job sends and reports the
// pacer as available, so the channel under test is the real one.
type stubNoticeMailer struct {
	mu sync.Mutex
	to []string
}

func (m *stubNoticeMailer) Name() string { return smtp.Type }

func (m *stubNoticeMailer) Execute(context.Context, action.ActionRequest) error { return nil }

func (m *stubNoticeMailer) Close(context.Context) error { return nil }

func (m *stubNoticeMailer) SendPlain(context.Context, []string, string, string) error { return nil }

func (m *stubNoticeMailer) SendNotice(_ context.Context, to []string, _, _ string, _ action.AppInfo, _ string) error {
	return m.send(to)
}

func (m *stubNoticeMailer) SendNoticeMarkdown(_ context.Context, to []string, _, _ string, _ action.AppInfo, _ string) error {
	return m.send(to)
}

func (m *stubNoticeMailer) send(to []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.to = append(m.to, to...)
	return nil
}

func (m *stubNoticeMailer) sent() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.to)
}

// TestMassStopsWhenTheBudgetIsSpent pins the reported P1 on the panel
// side: a broadcast that ran out of its budget used to walk through every
// remaining recipient, and each of those calls claimed a slot from the
// shared rate budget — pushing the next alarm minutes away. The loop now
// stops at the spent budget, so nothing is claimed for a notice that was
// never attempted.
func TestMassStopsWhenTheBudgetIsSpent(t *testing.T) {
	mailer := &stubNoticeMailer{}
	srv := &Server{
		logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
		actions: massTestManager(t, mailer),
	}

	j := &massJob{id: "budget", createdAt: time.Now(),
		channels: []*massChannelProgress{{kind: "email", state: "running"}}}
	recipients := []storage.User{
		{Email: "first@example.net"}, {Email: "second@example.net"}, {Email: "third@example.net"},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // the broadcast's budget is already spent
	srv.massChannelEmail(ctx, j, recipients, "notice", action.AppInfo{}, "pl", false)
	if got := mailer.sent(); got != 0 {
		t.Fatalf("a spent broadcast mailed %d recipients, want none", got)
	}

	// The recipients that were never attempted are REPORTED: the channel
	// is incomplete (not a finished delivery), the progress shows how many
	// messages did not go out and the bar is not at 100%.
	j.finishChannel("email")
	j.finish("summary")
	v := j.progressView("pl")
	if len(v.Channels) != 1 || v.Channels[0].State != "incomplete" {
		t.Fatalf("channel states = %+v, want one incomplete channel", v.Channels)
	}
	if !strings.Contains(v.Channels[0].Detail, "3") {
		t.Fatalf("channel detail = %q, want the number of unsent recipients", v.Channels[0].Detail)
	}
	if !v.Incomplete || v.Percent != 0 {
		t.Fatalf("progress = %+v, want an incomplete broadcast at 0%%", v)
	}
	if got := j.unsentTotal(); got != len(recipients) {
		t.Fatalf("unsent messages = %d, want %d (the summary counts them)", got, len(recipients))
	}
	// The summary names the reason and the number of messages that never
	// went out.
	cut := i18n.T("pl", "mass.flash.incomplete")
	if cut == "mass.flash.incomplete" || !strings.Contains(cut, "%d") {
		t.Fatalf("the incomplete-broadcast summary string is missing: %q", cut)
	}

	// A live budget still mails everybody.
	srv.massChannelEmail(context.Background(), j, recipients, "notice", action.AppInfo{}, "pl", false)
	if got := mailer.sent(); got != len(recipients) {
		t.Fatalf("a live broadcast mailed %d recipients, want %d", got, len(recipients))
	}
}

// massTestManager wires an action manager whose smtp instance is the stub.
func massTestManager(t *testing.T, p action.Plugin) *action.Manager {
	t.Helper()
	reg := action.NewRegistry()
	reg.Register(smtp.Type, func(*yaml.Node) (action.Plugin, error) { return p, nil })
	m, err := action.NewManager([]config.Action{
		{ID: "smtp-alerts", Type: smtp.Type, Enabled: true},
	}, reg, slog.New(slog.NewTextHandler(io.Discard, nil)), nil, nil)
	if err != nil {
		t.Fatalf("manager: %v", err)
	}
	m.Start(context.Background())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = m.Shutdown(ctx)
	})
	return m
}
