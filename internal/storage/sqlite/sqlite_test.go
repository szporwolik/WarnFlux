package sqlite

import (
	"context"
	"testing"
	"time"

	"warnflux/internal/core"
	"warnflux/internal/storage"
)

func openTemp(t *testing.T) *Store {
	t.Helper()
	store, _, err := Open(t.TempDir() + "/test.db")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

func testEvent() core.HazardEvent {
	eff := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	exp := eff.Add(24 * time.Hour)
	lat, lon := 50.06, 19.94
	return core.HazardEvent{
		Source:      "meteoalarm",
		SourceID:    "2.49.0.1.616.0.DEU",
		Category:    "met",
		Event:       "Rain",
		Severity:    "orange",
		Urgency:     "immediate",
		Certainty:   "likely",
		Headline:    "Heavy rain expected",
		Description: "Widespread heavy rain.",
		Instruction: "Avoid flooded areas.",
		EffectiveAt: &eff,
		ExpiresAt:   &exp,
		Latitude:    &lat,
		Longitude:   &lon,
		Areas:       []string{"DE-NW", "DE-RP"},
		Status:      core.StatusActive,
		SourceURL:   "https://example.invalid/alert",
		ReceivedAt:  time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
	}
}

func TestOpenInitializesSchema(t *testing.T) {
	store := openTemp(t)
	var version int
	if err := store.db.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
		t.Fatalf("read user_version: %v", err)
	}
	if version != len(migrations) {
		t.Errorf("schema version = %d, want %d", version, len(migrations))
	}
}

func TestOpenMigrationInfo(t *testing.T) {
	path := t.TempDir() + "/test.db"

	store, info, err := Open(path)
	if err != nil {
		t.Fatalf("first Open: %v", err)
	}
	if info.From != 0 || info.To != 1 {
		t.Errorf("fresh open info = %+v, want {From:0 To:1}", info)
	}
	store.Close()

	store2, info2, err := Open(path)
	if err != nil {
		t.Fatalf("second Open: %v", err)
	}
	store2.Close()
	if info2.From != 1 || info2.To != 1 {
		t.Errorf("reopen info = %+v, want {From:1 To:1} (no migration needed)", info2)
	}
}

func TestInsertAndGetRoundTrip(t *testing.T) {
	store := openTemp(t)
	ctx := context.Background()
	fixed := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return fixed }

	event := testEvent()
	inserted, err := store.Insert(ctx, event, "fingerprint-1")
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if !inserted {
		t.Fatal("first insert should report true")
	}

	got, err := store.Get(ctx, event.Key())
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Fingerprint != "fingerprint-1" {
		t.Errorf("fingerprint = %q", got.Fingerprint)
	}
	if got.Event.Source != event.Source || got.Event.SourceID != event.SourceID {
		t.Errorf("identity not preserved: %+v", got.Event)
	}
	if got.Event.Event != "Rain" || got.Event.Severity != "orange" {
		t.Errorf("content not preserved: %+v", got.Event)
	}
	if got.Event.Status != core.StatusActive {
		t.Errorf("status = %q, want active", got.Event.Status)
	}
	if got.Event.EffectiveAt == nil || !got.Event.EffectiveAt.Equal(*event.EffectiveAt) {
		t.Errorf("effective_at = %v, want %v", got.Event.EffectiveAt, *event.EffectiveAt)
	}
	if got.Event.ExpiresAt == nil || !got.Event.ExpiresAt.Equal(*event.ExpiresAt) {
		t.Errorf("expires_at = %v, want %v", got.Event.ExpiresAt, *event.ExpiresAt)
	}
	if got.Event.Latitude == nil || *got.Event.Latitude != 50.06 {
		t.Errorf("latitude = %v, want 50.06", got.Event.Latitude)
	}
	if len(got.Event.Areas) != 2 || got.Event.Areas[0] != "DE-NW" {
		t.Errorf("areas = %v", got.Event.Areas)
	}
	if !got.Event.ReceivedAt.Equal(event.ReceivedAt) {
		t.Errorf("received_at = %v, want %v", got.Event.ReceivedAt, event.ReceivedAt)
	}
	if !got.FirstSeenAt.Equal(fixed) || !got.LastSeenAt.Equal(fixed) {
		t.Errorf("seen timestamps = %v/%v, want %v", got.FirstSeenAt, got.LastSeenAt, fixed)
	}
	if !got.Event.UpdatedAt.Equal(fixed) {
		t.Errorf("updated_at = %v, want %v", got.Event.UpdatedAt, fixed)
	}
}

func TestInsertDuplicateKeyReportsFalse(t *testing.T) {
	store := openTemp(t)
	ctx := context.Background()

	event := testEvent()
	if inserted, err := store.Insert(ctx, event, "fp"); err != nil || !inserted {
		t.Fatalf("first Insert = %v, %v; want true", inserted, err)
	}
	inserted, err := store.Insert(ctx, event, "fp")
	if err != nil {
		t.Fatalf("second Insert: %v", err)
	}
	if inserted {
		t.Error("second insert should report false")
	}
	if n, err := store.Count(ctx); err != nil || n != 1 {
		t.Errorf("row count = %d, %v; want 1", n, err)
	}
}

func TestUpdatePreservesFirstSeenAndReceived(t *testing.T) {
	store := openTemp(t)
	ctx := context.Background()

	t0 := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	t1 := t0.Add(time.Minute)
	store.now = func() time.Time { return t0 }

	event := testEvent()
	if _, err := store.Insert(ctx, event, "fp-old"); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	store.now = func() time.Time { return t1 }
	updated := event
	updated.Severity = "red"
	updated.UpdatedAt = t1
	if err := store.Update(ctx, updated, "fp-new"); err != nil {
		t.Fatalf("Update: %v", err)
	}

	got, err := store.Get(ctx, event.Key())
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !got.FirstSeenAt.Equal(t0) {
		t.Errorf("first_seen_at = %v, want %v (must be preserved)", got.FirstSeenAt, t0)
	}
	if !got.Event.ReceivedAt.Equal(event.ReceivedAt) {
		t.Errorf("received_at = %v, want %v", got.Event.ReceivedAt, event.ReceivedAt)
	}
	if !got.LastSeenAt.Equal(t1) {
		t.Errorf("last_seen_at = %v, want %v", got.LastSeenAt, t1)
	}
	if !got.Event.UpdatedAt.Equal(t1) {
		t.Errorf("updated_at = %v, want %v", got.Event.UpdatedAt, t1)
	}
	if got.Event.Severity != "red" || got.Fingerprint != "fp-new" {
		t.Error("updated content not stored")
	}
}

func TestTouchRefreshesOnlyLastSeen(t *testing.T) {
	store := openTemp(t)
	ctx := context.Background()

	t0 := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	t1 := t0.Add(time.Minute)
	store.now = func() time.Time { return t0 }

	event := testEvent()
	if _, err := store.Insert(ctx, event, "fp"); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	store.now = func() time.Time { return t1 }
	if err := store.Touch(ctx, event.Key()); err != nil {
		t.Fatalf("Touch: %v", err)
	}

	got, err := store.Get(ctx, event.Key())
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !got.FirstSeenAt.Equal(t0) {
		t.Errorf("first_seen_at = %v, want %v", got.FirstSeenAt, t0)
	}
	if !got.LastSeenAt.Equal(t1) {
		t.Errorf("last_seen_at = %v, want %v", got.LastSeenAt, t1)
	}
	if !got.Event.UpdatedAt.Equal(t0) {
		t.Errorf("updated_at = %v, want %v (must not change)", got.Event.UpdatedAt, t0)
	}
	if got.Fingerprint != "fp" {
		t.Error("fingerprint must not change on touch")
	}
}

func TestMarkExpired(t *testing.T) {
	store := openTemp(t)
	ctx := context.Background()

	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	past := now.Add(-time.Hour)
	future := now.Add(time.Hour)

	mk := func(id string, status core.EventStatus, expires *time.Time) {
		e := core.HazardEvent{Source: "test", SourceID: id, Event: "E", Status: status, ExpiresAt: expires}
		if _, err := store.Insert(ctx, e, "fp-"+id); err != nil {
			t.Fatalf("Insert %s: %v", id, err)
		}
	}

	mk("expired-soon", core.StatusActive, &past)
	mk("active-future", core.StatusActive, &future)
	mk("active-no-expiry", core.StatusActive, nil)
	mk("cancelled-past", core.StatusCancelled, &past)

	expired, err := store.MarkExpired(ctx, now)
	if err != nil {
		t.Fatalf("MarkExpired: %v", err)
	}
	if len(expired) != 1 {
		t.Fatalf("expired count = %d, want 1", len(expired))
	}
	if expired[0].SourceID != "expired-soon" || expired[0].Status != core.StatusExpired {
		t.Errorf("unexpected expired event: %+v", expired[0])
	}

	// The state change must be persistent.
	got, err := store.Get(ctx, core.EventKey("test", "expired-soon"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Event.Status != core.StatusExpired {
		t.Errorf("persisted status = %q, want expired", got.Event.Status)
	}

	// The others must be untouched.
	for id, want := range map[string]core.EventStatus{
		"active-future":    core.StatusActive,
		"active-no-expiry": core.StatusActive,
		"cancelled-past":   core.StatusCancelled,
	} {
		got, err := store.Get(ctx, core.EventKey("test", id))
		if err != nil {
			t.Fatalf("Get %s: %v", id, err)
		}
		if got.Event.Status != want {
			t.Errorf("event %s status = %q, want %q", id, got.Event.Status, want)
		}
	}
}

func TestRestartPersistence(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir() + "/test.db"

	store, _, err := Open(path)
	if err != nil {
		t.Fatalf("first Open: %v", err)
	}
	event := testEvent()
	if _, err := store.Insert(ctx, event, "fp"); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	store2, _, err := Open(path)
	if err != nil {
		t.Fatalf("second Open: %v", err)
	}
	defer store2.Close()

	got, err := store2.Get(ctx, event.Key())
	if err != nil {
		t.Fatalf("Get after reopen: %v", err)
	}
	if got.Fingerprint != "fp" {
		t.Errorf("fingerprint after reopen = %q, want %q", got.Fingerprint, "fp")
	}
	if got.Event.Severity != event.Severity {
		t.Errorf("content after reopen = %+v", got.Event)
	}
}

func TestGetMissingReturnsErrNotFound(t *testing.T) {
	store := openTemp(t)
	_, err := store.Get(context.Background(), "nope:1")
	if err != storage.ErrNotFound {
		t.Errorf("Get missing = %v, want storage.ErrNotFound", err)
	}
}
