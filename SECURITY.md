# Security Policy

## Reporting a vulnerability

Please **do not open a public issue** for security vulnerabilities.

Report vulnerabilities privately to the repository owner (for example via
GitHub's *Report a vulnerability* feature in the Security tab). Include:

- a description of the vulnerability
- steps to reproduce it
- the affected version(s)
- any proposed mitigation

The maintainers will acknowledge the report and work on a fix. Details will
be published only after a fix is available.

## Supported versions

Only the latest release is supported. Fixes are released as new versions;
no backports are provided.

## Security model

WarnFlux is a local daemon that ingests hazard data and publishes it to
an MQTT broker. It exposes no network ports. Keep in mind:

- **Plugins are not sandboxed.** Built-in plugins run inside the
  WarnFlux process; supervision provides fault isolation (panics,
  hangs, errors) but not a security boundary. Only enable plugins you
  trust, and review plugin contributions carefully.
- **Credentials**: use `password_file` (e.g. a Docker secret mounted into
  the container) instead of embedding passwords in `config.yaml`. The
  configuration file should not be world-readable; the process never logs
  credentials.
- **MQTT transport**: use `ssl://` broker URLs with certificate validation
  in production. Plain `tcp://` connections transmit credentials and hazard
  data in cleartext.
- **Database**: the SQLite database is local to the machine. Protect the
  `/data` volume with appropriate filesystem permissions (the container
  runs as non-root UID 65532).
- **Container**: use the hardened Compose settings from the README
  (`read_only: true`, `cap_drop: [ALL]`, `security_opt:
  no-new-privileges`).
- **Supply chain**: CI runs `govulncheck` on every push. Dependencies are
  pinned in `go.mod`/`go.sum`.

## Known historical note

An MQTT broker username/password was present in `build/config.yaml` in an
earlier development revision and was removed during the hardening pass.
Treat that credential as compromised and rotate it wherever it was used.
