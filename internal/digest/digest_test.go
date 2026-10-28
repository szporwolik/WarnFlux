package digest

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/plugin"
	"github.com/szporwolik/WarnFlux/internal/storage"
)

type fakeStore struct {
	counters storage.DigestCounters
	admins   []string
	from, to time.Time
	called   bool
}

func (f *fakeStore) DigestCounters(_ context.Context, from, to time.Time) (storage.DigestCounters, error) {
	f.called = true
	f.from, f.to = from, to
	return f.counters, nil
}

func (f *fakeStore) AdminEmails(context.Context) ([]string, error) { return f.admins, nil }

type sentMail struct {
	to      []string
	subject string
	body    string
}

func fixedSources(now time.Time, store Store, plugins []plugin.PluginStatus) (Sources, *[]sentMail) {
	sent := &[]sentMail{}
	return Sources{
		Store:     store,
		Statuses:  func() []plugin.PluginStatus { return plugins },
		Station:   "SOSNA",
		Version:   "0.8.13",
		Commit:    "abcdef1234567890",
		Lang:      func() string { return "en" },
		Zone:      "UTC",
		Loc:       time.UTC,
		StartedAt: now.Add(-50*time.Hour - 32*time.Minute),
		Now:       func() time.Time { return now },
		Mailer: func(_ context.Context, to []string, subject, body string) error {
			*sent = append(*sent, sentMail{to: to, subject: subject, body: body})
			return nil
		},
	}, sent
}

// TestSendMailsTheLastTwentyFourHours is the whole point of the feature: one
// mail the operator can read instead of the log, with the window's traffic,
// the verdict and the services.
func TestSendMailsTheLastTwentyFourHours(t *testing.T) {
	now := time.Date(2026, 10, 29, 6, 0, 0, 0, time.UTC)
	store := &fakeStore{
		counters: storage.DigestCounters{
			SMSReceived: 3, SMSSent: 1,
			MeshReceived: 812, MeshSent: 2,
			APRSReceived: 4, APRSSent: 6,
			HazardsNew: 5, HazardsActive: 11, HazardsClosed: 2,
			Deliveries: 7, DeliveriesFailed: 1,
		},
		admins: []string{"admin@example.org"},
	}
	plugins := []plugin.PluginStatus{
		{ID: "imgw-warnings", Type: "imgw", State: plugin.StateRunning, LastSummary: "42 items / 7 filtered"},
		{ID: "gddkia-roads", Type: "gddkia", State: plugin.StateDisabled},
		{ID: "adsb-main", Type: "adsb", State: plugin.StateDegraded, LastError: "TLS handshake timeout", ConsecutiveFailures: 3},
	}
	src, sent := fixedSources(now, store, plugins)

	if err := Send(context.Background(), src); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if len(*sent) != 1 {
		t.Fatalf("mails = %d, want 1", len(*sent))
	}
	mail := (*sent)[0]
	if len(mail.to) != 1 || mail.to[0] != "admin@example.org" {
		t.Errorf("recipients = %v, want the admin address", mail.to)
	}
	// Same shape as a routed mail: bracketed station, then what it is, then
	// the verdict.
	if !strings.HasPrefix(mail.subject, "[SOSNA] Daily status ") || !strings.Contains(mail.subject, "DEGRADED") {
		t.Errorf("subject = %q, want \"[SOSNA] Daily status … — DEGRADED\"", mail.subject)
	}
	for _, want := range []string{
		"SMS: 3 received · 1 sent",
		"Meshtastic: 812 received · 2 sent",
		"APRS: 4 received · 6 sent",
		"Hazards: 5 new · 11 active now · 2 ended",
		"Notifications: 7 dispatched · 1 failed",
		"**adsb-main** (adsb) — degraded",
		"last error: TLS handshake timeout",
		"**gddkia-roads** (gddkia) — disabled",
		"Uptime:** 2d 2h 32m",
	} {
		if !strings.Contains(mail.body, want) {
			t.Errorf("body missing %q:\n%s", want, mail.body)
		}
	}
	// Problem rows lead the services list.
	if strings.Index(mail.body, "adsb-main") > strings.Index(mail.body, "imgw-warnings") {
		t.Error("the degraded service is not listed first")
	}
	if !store.called {
		t.Fatal("counters were never read")
	}
	if d := store.to.Sub(store.from); d != 24*time.Hour {
		t.Errorf("window = %s, want 24h", d)
	}
	if !store.to.Equal(now) {
		t.Errorf("window end = %s, want now", store.to)
	}
}

// TestSendFallsBackToConfiguredRecipients pins the precedence: explicit
// addresses win, otherwise the admin accounts.
func TestSendFallsBackToConfiguredRecipients(t *testing.T) {
	now := time.Date(2026, 10, 29, 6, 0, 0, 0, time.UTC)
	store := &fakeStore{admins: []string{"db-admin@example.org"}}
	src, sent := fixedSources(now, store, nil)
	src.Recipients = func(context.Context) ([]string, error) {
		return []string{"configured@example.org"}, nil
	}
	if err := Send(context.Background(), src); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if got := (*sent)[0].to; len(got) != 1 || got[0] != "configured@example.org" {
		t.Errorf("recipients = %v, want the configured address", got)
	}

	// No configured address and no admin with a mail: nothing is sent, and
	// that is not an error (a station may run without mail).
	src, sent = fixedSources(now, &fakeStore{}, nil)
	if err := Send(context.Background(), src); err != nil {
		t.Fatalf("Send without recipients: %v", err)
	}
	if len(*sent) != 0 {
		t.Errorf("mails = %d, want none", len(*sent))
	}
}
