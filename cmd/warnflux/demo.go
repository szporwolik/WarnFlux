package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/szporwolik/WarnFlux/internal/core"
	"github.com/szporwolik/WarnFlux/internal/ingest"
	"github.com/szporwolik/WarnFlux/internal/storage/sqlite"
)

// runDemo exercises the core ingestion pipeline against a temporary SQLite
// database. It is a developer tool, not a stable public interface.
//
//	go run ./cmd/warnflux demo
func runDemo(logger *slog.Logger) error {
	dir, err := os.MkdirTemp("", "warnflux-demo-*")
	if err != nil {
		return fmt.Errorf("create demo directory: %w", err)
	}
	defer os.RemoveAll(dir)

	store, _, err := sqlite.Open(filepath.Join(dir, "demo.db"))
	if err != nil {
		return fmt.Errorf("open demo database: %w", err)
	}
	defer store.Close()

	ingester := ingest.NewIngester(store, logger)
	ctx := context.Background()
	now := time.Now().UTC()

	base := core.HazardEvent{
		Source:   "demo",
		SourceID: "001",
		Event:    "Flood",
		Severity: "moderate",
		Headline: "Demo river flood warning",
		Areas:    []string{"Kraków"},
	}

	type row struct{ step, result, key string }
	var rows []row

	step := func(name string, event core.HazardEvent) {
		result, _, err := ingester.Ingest(ctx, event)
		outcome := result.String()
		if err != nil {
			outcome = "error: " + err.Error()
		}
		rows = append(rows, row{name, outcome, event.Key()})
	}

	step("first event", base)
	step("same event again", base)

	changed := base
	changed.Severity = "severe"
	step("changed severity", changed)

	cancelled := changed
	cancelled.Status = core.StatusCancelled
	step("cancelled event", cancelled)
	step("repeated cancellation", cancelled)

	// An event with an expiry becomes "expired" through the worker's
	// expiration check.
	expires := now.Add(time.Minute)
	expiring := core.HazardEvent{
		Source:    "demo",
		SourceID:  "002",
		Event:     "Frost",
		Severity:  "minor",
		ExpiresAt: &expires,
	}
	step("event with expiry", expiring)

	// An event without expiry stays active.
	noExpiry := core.HazardEvent{
		Source: "demo", SourceID: "003",
		Event: "Information", Severity: "minor",
	}
	step("event without expiry", noExpiry)

	changes, err := ingester.Expire(ctx, now.Add(2*time.Minute))
	if err != nil {
		return fmt.Errorf("expiration check: %w", err)
	}
	for _, change := range changes {
		rows = append(rows, row{"expiration check", change.Type.String(), change.Event.Key()})
	}

	stored, err := store.Get(ctx, noExpiry.Key())
	if err != nil {
		return fmt.Errorf("read demo event: %w", err)
	}
	rows = append(rows, row{"no-expiry after check", string(stored.Event.Status), noExpiry.Key()})

	fmt.Println("WarnFlux demo: exercising the core ingestion pipeline")
	fmt.Println()
	fmt.Printf("%-24s %-10s %s\n", "step", "result", "event_key")
	fmt.Println(strings.Repeat("-", 58))
	for _, r := range rows {
		fmt.Printf("%-24s %-10s %s\n", r.step, r.result, r.key)
	}
	fmt.Println()
	fmt.Println("Demo complete; the temporary database has been removed.")
	return nil
}
