# Contributing to WarnFlux

Thanks for your interest in contributing. WarnFlux is a Go daemon that
aggregates hazard information, normalizes it and delivers changes at least
once to outputs such as MQTT.

## Ground rules

- Discussion first for large changes: open an issue before spending days on
  a design that may not fit.
- Keep the core small and testable; put provider-specific logic into
  plugins.
- Every change must keep the test suite green, including the race detector.
- Follow the [plugin rules](docs/plugins.md#mandatory-rules) for anything
  touching the plugin boundary.

## Development environment

- Go 1.26 or newer.
- No CGO is required (the SQLite driver is pure Go).
- An MQTT broker (e.g. `docker run -d --rm -p 1883:1883 eclipse-mosquitto:2`)
  is only needed for manual MQTT testing.

## Workflow

```bash
# format
gofmt -w .

# static checks
go vet ./...

# tests
go test -count=1 ./...
go test -race -count=1 ./...

# fuzzing (short runs)
go test -fuzz=FuzzNormalizeValidate -fuzztime=30s ./internal/core/
go test -fuzz=FuzzLoad -fuzztime=30s ./internal/config/
```

Open a pull request against `main`. CI runs format checks, `go vet`, the
tests with the race detector and a vulnerability scan.

## Conventions

- Module path: `github.com/szporwolik/WarnFlux`.
- Package layout: core model (`internal/core`), pipeline (`internal/ingest`),
  persistence contract (`internal/storage`), plugin framework
  (`internal/plugin`), built-in plugins (`internal/plugins/...`).
- Config decoding inside plugins uses `plugin.DecodeConfig` with strict
  field matching.
- Logs use `log/slog` and must never contain secrets.
- New storage schema changes are append-only migration steps with a bump of
  the migration list; never edit an already-released migration.
- Timestamps in the database are UTC with millisecond precision
  (`expires_at_ms`, journal `created_at_ms`).

## Release process

- CI validates every pull request and every push to `main`.
- Tagging `v<semver>` (e.g. `v0.1.0`) triggers the release workflow:
  binaries for linux/amd64, linux/arm64 and windows/amd64, a `SHA256SUMS`
  file, a GitHub release and a multi-arch Docker image on
  `ghcr.io/<owner>/warnflux`.

## License

By contributing, you agree that your contributions are licensed under the
[MIT](LICENSE) license of this project.
