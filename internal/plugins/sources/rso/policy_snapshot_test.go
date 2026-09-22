package rso

import (
	"context"
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/core"
)

// policySource builds a Source with the high-signal policy enabled.
func policySource(t *testing.T, baseURL string) *Source {
	t.Helper()
	policy, err := buildFilterConfig(&FileFilterConfig{HighSignalOnly: boolPtr(true)})
	if err != nil {
		t.Fatalf("buildFilterConfig: %v", err)
	}
	return &Source{
		cfg: Config{
			PollInterval:   50 * time.Millisecond,
			RequestTimeout: 2 * time.Second,
			BaseURL:        baseURL,
			Voivodeships:   []string{"malopolskie"},
		},
		client: NewClient(baseURL, 2*time.Second),
		policy: policy,
	}
}

// TestFilteredSnapshotStaysComplete: intentional policy filtering never
// marks the combined snapshot incomplete and never degrades source health.
func TestFilteredSnapshotStaysComplete(t *testing.T) {
	srv := rsoServer(t, map[string]string{
		"malopolskie": newsesXML(
			newsXML("rcb1", "ALERT RCB", provinceXML("malopolskie")),
			newsXML("smog1", "Alert smogowy PM10", provinceXML("malopolskie")),
			newsXML("local1", "Wypadek w Niepołomicach, droga zablokowana", provinceXML("malopolskie")),
			newsXML("regional1", "Ostrzeżenie pierwszego stopnia dla województwa małopolskiego", provinceXML("malopolskie")),
		),
	})
	defer srv.Close()

	em := &fakeEmitter{}
	src := policySource(t, srv.URL)
	src.pollOnce(context.Background(), em, em)

	keys := em.activeKeys()
	if len(keys) != 1 || !keys["rso:local1"] {
		t.Fatalf("active keys = %v, want only rso:local1", keys)
	}
	if len(em.cancelledKeys()) != 0 {
		t.Errorf("unexpected cancellations: %v", em.cancelledKeys())
	}
	h, d := em.health()
	if h != 1 || d != 0 {
		t.Errorf("health = (%d healthy, %d degraded), want (1, 0)", h, d)
	}
	em.mu.Lock()
	defer em.mu.Unlock()
	// The emitted local event carries classified severity.
	for _, ev := range em.emitted {
		if ev.SourceID == "local1" {
			if ev.Severity != "moderate" {
				t.Errorf("local1 severity = %q, want moderate", ev.Severity)
			}
			foundCore := false
			for _, a := range ev.Areas {
				if a == "gmina:niepolomice" {
					foundCore = true
				}
			}
			if !foundCore {
				t.Errorf("local1 areas = %v, want gmina:niepolomice", ev.Areas)
			}
		}
	}
}

// TestFilteredEventReconciled: an event that was accepted before but now
// fails the policy is cancelled by the next complete snapshot — the
// filtered key set IS this source's logical snapshot.
func TestFilteredEventReconciled(t *testing.T) {
	srv := rsoServer(t, map[string]string{
		"malopolskie": newsesXML(
			newsXML("gone", "Alert smogowy dla Małopolski", provinceXML("malopolskie")),
		),
	})
	defer srv.Close()

	em := &fakeEmitter{active: []core.HazardEvent{rsoEvent("gone", nil)}}
	src := policySource(t, srv.URL)
	src.pollOnce(context.Background(), em, em)

	got := em.cancelledKeys()
	if len(got) != 1 || got[0] != "rso:gone" {
		t.Fatalf("cancelled keys = %v, want [rso:gone]", got)
	}
	h, d := em.health()
	if h != 1 || d != 0 {
		t.Errorf("health = (%d healthy, %d degraded), want (1, 0)", h, d)
	}
}
