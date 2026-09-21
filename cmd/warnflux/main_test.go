package main

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func testResolverLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestResolveStoragePathExplicit(t *testing.T) {
	got := resolveStoragePath("./data/events.db", testResolverLogger())
	if got != "./data/events.db" {
		t.Errorf("explicit path changed: %q", got)
	}
}

func TestResolveStoragePathFallsBackNextToBinary(t *testing.T) {
	got := resolveStoragePath("", testResolverLogger())
	if !strings.HasSuffix(got, "warnflux.db") {
		t.Fatalf("fallback path = %q, want suffix warnflux.db", got)
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	want := filepath.Join(filepath.Dir(exe), "warnflux.db")
	if got != want {
		t.Errorf("fallback = %q, want %q", got, want)
	}
}
