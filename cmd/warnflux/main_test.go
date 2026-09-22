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

func TestResolveVersionInjectedUnchanged(t *testing.T) {
	if got := resolveVersion("1.2.3"); got != "1.2.3" {
		t.Errorf("injected version changed: %q", got)
	}
}

func TestResolveVersionReadsWorkingDirFile(t *testing.T) {
	// The cwd is the package directory when tests run; a VERSION file next
	// to the repo root is not guaranteed, so test with a temp cwd.
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "VERSION"), []byte("0.7.4\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	orig, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(orig) })

	if got := resolveVersion("dev"); got != "0.7.4" {
		t.Errorf("dev build did not pick up VERSION file: %q", got)
	}
}

func TestResolveVersionIgnoresWhitespaceOnly(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "VERSION"), []byte("  \n "), 0o600); err != nil {
		t.Fatal(err)
	}
	orig, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(orig) })

	if got := resolveVersion("dev"); got != "dev" {
		t.Errorf("whitespace-only VERSION file should be ignored: %q", got)
	}
}
