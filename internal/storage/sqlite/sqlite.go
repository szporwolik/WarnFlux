// Package sqlite implements storage.EventStore on top of a local SQLite
// database using the pure-Go modernc.org/sqlite driver.
package sqlite

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"golang.org/x/sys/unix"
	_ "modernc.org/sqlite"

	"github.com/szporwolik/WarnFlux/internal/core"
	"github.com/szporwolik/WarnFlux/internal/dispatch"
	"github.com/szporwolik/WarnFlux/internal/storage"
)

// migration is one append-only schema evolution step. Steps already
// released must never be edited.
type migration struct {
	// SQL is executed inside the migration transaction.
	SQL string
	// run is an optional Go migration step executed in the same transaction
	// (used when plain SQL cannot convert existing data safely).
	run func(tx *sql.Tx) error
}

// migrations holds the schema evolution steps in order. Slice index +1 is
// the target schema version, persisted via SQLite's PRAGMA user_version.
// Existing databases are upgraded in place on startup, never recreated.
//
// Migration steps are immutable historical code.
//
// A migration must never call a helper whose query depends on schema
// introduced by a LATER migration. In particular the v3 snapshot backfill
// must read events through a frozen v2/v3 column list (v3EventColumns),
// never through the current-schema eventColumns / loadEventTx.
// Migration-specific readers must remain compatible with the schema
// available at that version.
var migrations = []migration{
	{
		// v1: initial event storage.
		SQL: `
CREATE TABLE events (
	event_key     TEXT PRIMARY KEY,
	source        TEXT NOT NULL,
	source_id     TEXT NOT NULL,
	fingerprint   TEXT NOT NULL,
	status        TEXT NOT NULL,
	category      TEXT NOT NULL DEFAULT '',
	event         TEXT NOT NULL,
	severity      TEXT NOT NULL DEFAULT '',
	urgency       TEXT NOT NULL DEFAULT '',
	certainty     TEXT NOT NULL DEFAULT '',
	headline      TEXT NOT NULL DEFAULT '',
	description   TEXT NOT NULL DEFAULT '',
	instruction   TEXT NOT NULL DEFAULT '',
	effective_at  TEXT,
	expires_at    TEXT,
	latitude      REAL,
	longitude     REAL,
	areas         TEXT NOT NULL DEFAULT '[]',
	source_url    TEXT NOT NULL DEFAULT '',
	received_at   TEXT NOT NULL,
	first_seen_at TEXT NOT NULL,
	last_seen_at  TEXT NOT NULL,
	updated_at    TEXT NOT NULL
);

CREATE INDEX idx_events_source ON events(source);
CREATE INDEX idx_events_status ON events(status);
CREATE INDEX idx_events_expires_at ON events(expires_at);
CREATE INDEX idx_events_last_seen_at ON events(last_seen_at);
`,
	},
	{
		// v2: machine-sortable expiry timestamps + durable change journal.
		SQL: `
ALTER TABLE events ADD COLUMN expires_at_ms INTEGER;
CREATE INDEX idx_events_expires_at_ms ON events(expires_at_ms);

CREATE TABLE changes (
	id            INTEGER PRIMARY KEY AUTOINCREMENT,
	change_type   TEXT NOT NULL,
	event_key     TEXT NOT NULL,
	created_at_ms INTEGER NOT NULL
);
CREATE INDEX idx_changes_id ON changes(id);

CREATE TABLE output_cursors (
	output_id     TEXT PRIMARY KEY,
	last_acked_id INTEGER NOT NULL DEFAULT 0
);
`,
		// Backfill expires_at_ms from the RFC3339Nano text column, parsing
		// each value explicitly instead of relying on string manipulation.
		run: func(tx *sql.Tx) error {
			rows, err := tx.Query("SELECT event_key, expires_at FROM events WHERE expires_at IS NOT NULL")
			if err != nil {
				return fmt.Errorf("read legacy expires_at: %w", err)
			}
			defer rows.Close()
			type backfill struct {
				key string
				ms  int64
			}
			var batch []backfill
			for rows.Next() {
				var key, text string
				if err := rows.Scan(&key, &text); err != nil {
					return fmt.Errorf("scan legacy expires_at: %w", err)
				}
				parsed, err := time.Parse(time.RFC3339Nano, text)
				if err != nil {
					return fmt.Errorf("event %q has invalid legacy expires_at %q: %w", key, text, err)
				}
				batch = append(batch, backfill{key: key, ms: parsed.UnixMilli()})
			}
			if err := rows.Err(); err != nil {
				return fmt.Errorf("iterate legacy expires_at: %w", err)
			}
			for _, b := range batch {
				if _, err := tx.Exec("UPDATE events SET expires_at_ms = ? WHERE event_key = ?", b.ms, b.key); err != nil {
					return fmt.Errorf("backfill expires_at_ms for %q: %w", b.key, err)
				}
			}
			return nil
		},
	},
	{
		// v3: immutable event snapshots in the change journal. From now on
		// every change row carries the exact event state after its
		// transition; PollChanges never joins against the mutable events
		// table again.
		SQL: `ALTER TABLE changes ADD COLUMN event_snapshot TEXT NOT NULL DEFAULT '';`,
		// Pre-snapshot rows cannot be reconstructed historically with
		// perfect accuracy: backfill them with the CURRENT event state.
		// Databases created by the pre-release v2 schema therefore report
		// the present state for all their old journal rows (documented
		// limitation). Orphan rows (no matching event) are dropped: they
		// carry no reconstructable state.
		run: func(tx *sql.Tx) error {
			if _, err := tx.Exec(`
				DELETE FROM changes
				WHERE event_key NOT IN (SELECT event_key FROM events)`); err != nil {
				return fmt.Errorf("drop orphan changes: %w", err)
			}
			rows, err := tx.Query("SELECT id, event_key FROM changes WHERE event_snapshot = ''")
			if err != nil {
				return fmt.Errorf("read unsnapshotted changes: %w", err)
			}
			defer rows.Close()
			type row struct {
				id  int64
				key string
			}
			var batch []row
			for rows.Next() {
				var r row
				if err := rows.Scan(&r.id, &r.key); err != nil {
					return fmt.Errorf("scan change: %w", err)
				}
				batch = append(batch, r)
			}
			if err := rows.Err(); err != nil {
				return fmt.Errorf("iterate changes: %w", err)
			}
			for _, r := range batch {
				event, err := loadEventForV3Migration(tx, r.key)
				if err != nil {
					return fmt.Errorf("backfill snapshot for change %d: %w", r.id, err)
				}
				data, err := json.Marshal(storage.SnapshotOf(event))
				if err != nil {
					return fmt.Errorf("marshal snapshot for change %d: %w", r.id, err)
				}
				if _, err := tx.Exec("UPDATE changes SET event_snapshot = ? WHERE id = ?", string(data), r.id); err != nil {
					return fmt.Errorf("write snapshot for change %d: %w", r.id, err)
				}
			}
			return nil
		},
	},
	{
		// v4: machine-sortable last_seen (event retention must order by
		// the last provider observation, never by text); durable output
		// consumer identity includes the plugin type next to the cursor.
		SQL: `
ALTER TABLE events ADD COLUMN last_seen_at_ms INTEGER;
DROP INDEX idx_events_last_seen_at;
CREATE INDEX idx_events_retention ON events(status, last_seen_at_ms);
ALTER TABLE output_cursors ADD COLUMN output_type TEXT NOT NULL DEFAULT '';
`,
		// Backfill last_seen_at_ms from the RFC3339Nano text column by
		// parsing each value explicitly; invalid legacy values fail the
		// migration (rolled back atomically with the schema change).
		run: func(tx *sql.Tx) error {
			rows, err := tx.Query("SELECT event_key, last_seen_at FROM events WHERE last_seen_at_ms IS NULL")
			if err != nil {
				return fmt.Errorf("read legacy last_seen_at: %w", err)
			}
			defer rows.Close()
			type backfill struct {
				key string
				ms  int64
			}
			var batch []backfill
			for rows.Next() {
				var key, text string
				if err := rows.Scan(&key, &text); err != nil {
					return fmt.Errorf("scan legacy last_seen_at: %w", err)
				}
				parsed, err := time.Parse(time.RFC3339Nano, text)
				if err != nil {
					return fmt.Errorf("event %q has invalid legacy last_seen_at %q: %w", key, text, err)
				}
				batch = append(batch, backfill{key: key, ms: parsed.UnixMilli()})
			}
			if err := rows.Err(); err != nil {
				return fmt.Errorf("iterate legacy last_seen_at: %w", err)
			}
			for _, b := range batch {
				if _, err := tx.Exec("UPDATE events SET last_seen_at_ms = ? WHERE event_key = ?", b.ms, b.key); err != nil {
					return fmt.Errorf("backfill last_seen_at_ms for %q: %w", b.key, err)
				}
			}
			return nil
		},
	},
	{
		// v5: alert recipients (users) for the notification layer. The
		// admin row is seeded from the web auth username at startup and is
		// read-only at the API level (is_admin = 1).
		SQL: `
CREATE TABLE users (
	id            INTEGER PRIMARY KEY AUTOINCREMENT,
	username      TEXT NOT NULL COLLATE NOCASE UNIQUE,
	phone         TEXT NOT NULL DEFAULT '',
	email         TEXT NOT NULL DEFAULT '',
	discord       TEXT NOT NULL DEFAULT '',
	is_admin      INTEGER NOT NULL DEFAULT 0,
	created_at_ms INTEGER NOT NULL,
	updated_at_ms INTEGER NOT NULL
);
`,
	},
	{
		// v6: notification groups and the many-to-many user membership.
		// Foreign keys cascade membership rows away with either side.
		SQL: `
CREATE TABLE groups (
	id            INTEGER PRIMARY KEY AUTOINCREMENT,
	name          TEXT NOT NULL COLLATE NOCASE UNIQUE,
	created_at_ms INTEGER NOT NULL,
	updated_at_ms INTEGER NOT NULL
);

CREATE TABLE user_groups (
	user_id  INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
	group_id INTEGER NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
	PRIMARY KEY (user_id, group_id)
);
`,
	},
	{
		// v7: group notification routing. Every group carries a minimum
		// severity threshold ('unknown' delivers everything) plus the
		// configured action/output instance IDs assigned to it. The IDs
		// reference the configuration, not database rows: stale IDs after
		// a config change are skipped at delivery time.
		SQL: `
ALTER TABLE groups ADD COLUMN min_severity TEXT NOT NULL DEFAULT 'unknown';

CREATE TABLE group_actions (
	group_id  INTEGER NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
	action_id TEXT NOT NULL,
	PRIMARY KEY (group_id, action_id)
);

CREATE TABLE group_outputs (
	group_id  INTEGER NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
	output_id TEXT NOT NULL,
	PRIMARY KEY (group_id, output_id)
);
`,
	},
	{
		// v8: per-channel severity routing matrix. The single group-wide
		// threshold is replaced by one threshold per assigned action and
		// per assigned output; existing assignments inherit the group's
		// current threshold during the migration.
		SQL: `
ALTER TABLE group_actions ADD COLUMN min_severity TEXT NOT NULL DEFAULT 'unknown';
ALTER TABLE group_outputs ADD COLUMN min_severity TEXT NOT NULL DEFAULT 'unknown';

UPDATE group_actions
SET min_severity = COALESCE(
	(SELECT g.min_severity FROM groups g WHERE g.id = group_actions.group_id),
	'unknown');

UPDATE group_outputs
SET min_severity = COALESCE(
	(SELECT g.min_severity FROM groups g WHERE g.id = group_outputs.group_id),
	'unknown');

ALTER TABLE groups DROP COLUMN min_severity;
`,
	},
	{
		// v9: outputs leave the routing matrix. Output plugins receive
		// every journal change by default (at-least-once delivery), so a
		// per-group output assignment was redundant surface: the group
		// matrix now routes actions only.
		SQL: `DROP TABLE group_outputs;`,
	},
	{
		// v10: durable action-fire ledger. Every (group, action, event)
		// delivery is claimed exactly once, so retained or replayed
		// transition messages never re-fire notifications across
		// restarts. Rows are pruned by age via PruneActionFires
		// (app.notification_retention).
		SQL: `
CREATE TABLE action_fires (
	group_id    INTEGER NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
	action_id   TEXT NOT NULL,
	event_key   TEXT NOT NULL,
	dedup_key   TEXT NOT NULL,
	fired_at_ms INTEGER NOT NULL,
	PRIMARY KEY (group_id, action_id, dedup_key)
);

CREATE INDEX idx_action_fires_age ON action_fires(fired_at_ms);
`,
	},
	{
		// v11: the routing matrix gains the input-plugin (event source)
		// dimension. A cell now reads "events from source S at severity ≥
		// T fire action A". The primary key must include the source, so
		// the table is rebuilt; existing assignments become source-
		// agnostic ('') and behave exactly as before.
		SQL: `
CREATE TABLE group_actions_new (
	group_id     INTEGER NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
	source       TEXT NOT NULL DEFAULT '',
	action_id    TEXT NOT NULL,
	min_severity TEXT NOT NULL DEFAULT 'unknown',
	PRIMARY KEY (group_id, source, action_id)
);

INSERT INTO group_actions_new (group_id, source, action_id, min_severity)
	SELECT group_id, '', action_id, min_severity FROM group_actions;

DROP TABLE group_actions;
ALTER TABLE group_actions_new RENAME TO group_actions;
`,
	},
	{
		// v12: the canonical severity model closes. Events keep the raw
		// provider-scale value in provider_severity for diagnostics; the
		// severity column itself holds ONLY the canonical WarnFlux scale
		// (unknown/minor/moderate/severe/extreme) that routing uses.
		SQL: `ALTER TABLE events ADD COLUMN provider_severity TEXT NOT NULL DEFAULT '';`,
	},
	{
		// v13: user roles and directory logins. Role '' is a plain
		// notification recipient; 'emcom' is an operator that may sign
		// in and use the compose module. password_salt/password_hash
		// stay empty until the admin sets a password in /users.
		SQL: `
ALTER TABLE users ADD COLUMN role TEXT NOT NULL DEFAULT '';
ALTER TABLE users ADD COLUMN password_salt TEXT NOT NULL DEFAULT '';
ALTER TABLE users ADD COLUMN password_hash TEXT NOT NULL DEFAULT '';
`,
	},
	{
		// v14: per-user APRS callsigns (ham radio operators): one
		// recipient may own several callsigns/SSIDs so routing can
		// address messages to the right station. Rows cascade away
		// with the user.
		SQL: `
CREATE TABLE user_aprs (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id       INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
        callsign      TEXT NOT NULL COLLATE NOCASE,
        created_at_ms INTEGER NOT NULL
);
CREATE UNIQUE INDEX idx_user_aprs_unique ON user_aprs(user_id, callsign COLLATE NOCASE);
`,
	},
	{
		// v15: persistent dashboard audit log. Every state-changing
		// operation performed through the web UI lands here; entries are
		// pruned to storage.AuditRetentionEntries by the store on insert.
		SQL: `
CREATE TABLE audit_log (
	seq     INTEGER PRIMARY KEY AUTOINCREMENT,
	at_ms   INTEGER NOT NULL,
	user    TEXT NOT NULL,
	action  TEXT NOT NULL,
	detail  TEXT NOT NULL DEFAULT ''
);
CREATE INDEX idx_audit_log_seq ON audit_log(seq);
`,
	},
	{
		// v16: per-user delivery-channel opt-outs. Every user is
		// subscribed to all notification channels (APRS, email, future
		// media) by default; an opt-out row disables one channel for one
		// user. The rule engine filters the per-channel recipient lists
		// through these rows.
		SQL: `
CREATE TABLE user_channel_opts (
	user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
	channel TEXT NOT NULL,
	PRIMARY KEY (user_id, channel)
);
`,
	},
	{
		// v17: one-time password-reset tokens. A token is issued per user
		// (hashed at rest), expires after an hour and can be consumed
		// exactly once; the self-service reset flow lives in web/forgot.go.
		SQL: `
CREATE TABLE password_resets (
	id            INTEGER PRIMARY KEY AUTOINCREMENT,
	user_id       INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
	token_hash    TEXT NOT NULL,
	expires_at_ms INTEGER NOT NULL,
	used_at_ms    INTEGER NOT NULL DEFAULT 0,
	created_at_ms INTEGER NOT NULL
);
CREATE INDEX idx_password_resets_user ON password_resets(user_id);
`,
	},
	{
		// v18: durable APRS message history (received and sent), surfacing
		// on the admin /messages page; survives restarts. Retention is
		// bounded to storage.APRSMessageRetentionEntries by the store.
		SQL: `
CREATE TABLE aprs_messages (
	id            INTEGER PRIMARY KEY AUTOINCREMENT,
	direction     TEXT NOT NULL CHECK (direction IN ('rx','tx')),
	from_call     TEXT NOT NULL,
	to_call       TEXT NOT NULL,
	text          TEXT NOT NULL,
	msg_id        TEXT NOT NULL DEFAULT '',
	via           TEXT NOT NULL DEFAULT '',
	created_at_ms INTEGER NOT NULL
);
CREATE INDEX idx_aprs_messages_created ON aprs_messages(created_at_ms);
`,
	},
	{
		// v19: per-user Meshtastic public keys: one user may own several
		// mesh nodes; the keys feed the room-server ACL (setperm).
		SQL: `
CREATE TABLE user_meshkeys (
	id            INTEGER PRIMARY KEY AUTOINCREMENT,
	user_id       INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
	pubkey        TEXT NOT NULL COLLATE NOCASE,
	created_at_ms INTEGER NOT NULL
);
CREATE UNIQUE INDEX idx_user_meshkeys_unique ON user_meshkeys(user_id, pubkey COLLATE NOCASE);
`,
	},
	{
		// v20: durable Meshtastic message history (received and sent),
		// surfacing on the admin /meshtastic page; survives restarts.
		SQL: `
CREATE TABLE meshcore_messages (
	id            INTEGER PRIMARY KEY AUTOINCREMENT,
	direction     TEXT NOT NULL CHECK (direction IN ('rx','tx')),
	sender        TEXT NOT NULL DEFAULT '',
	channel       TEXT NOT NULL DEFAULT '',
	text          TEXT NOT NULL,
	created_at_ms INTEGER NOT NULL
);
CREATE INDEX idx_meshcore_messages_created ON meshcore_messages(created_at_ms);
`,
	},
	{
		// v21: mesh message radio path length (hops) from the companion
		// frame, shown in the admin history.
		SQL: `
ALTER TABLE meshcore_messages ADD COLUMN hops INTEGER NOT NULL DEFAULT 0;
`,
	},
	{
		// v22: the admin username behind sent mesh messages, shown in the
		// history next to the TX sender.
		SQL: `
ALTER TABLE meshcore_messages ADD COLUMN operator TEXT NOT NULL DEFAULT '';
`,
	},
	{
		// v23: action_fires rows become durable delivery jobs carrying an
		// execution state, so a crash or a rejected submission between
		// claim and execution can be retried on replay. Historical rows
		// were claimed-before-delivery and count as succeeded.
		SQL: `
ALTER TABLE action_fires ADD COLUMN status TEXT NOT NULL DEFAULT 'succeeded';
`,
	},
	{
		// v24: the persistent publisher UUID of this WarnFlux database,
		// stamped onto the /events journal contract so independent
		// instances can never collide in notification deduplication.
		SQL: `
CREATE TABLE instance_meta (
	id          INTEGER PRIMARY KEY CHECK (id = 1),
	instance_id TEXT NOT NULL
);
`,
	},
	{
		// v25: the durable dispatch inbox: canonical events are persisted
		// BEFORE routing evaluates them, so a full live queue or a crash
		// between acceptance and evaluation no longer loses them. Rows
		// are deleted once the routing engine acknowledges them.
		SQL: `
CREATE TABLE dispatch_inbox (
	id             INTEGER PRIMARY KEY AUTOINCREMENT,
	event_json     TEXT NOT NULL,
	received_at_ms INTEGER NOT NULL,
	receiver       TEXT NOT NULL DEFAULT ''
);
CREATE INDEX idx_dispatch_inbox_id ON dispatch_inbox(id);
`,
	},
	{
		// v26: action_fires rows become full delivery jobs: the execution
		// payload (event + recipients) travels with the row, the attempt
		// counter and the next-attempt deadline drive the retry
		// scheduler. Historical rows carry no payload and were already
		// claimed; their status stays terminal ('succeeded').
		SQL: `
ALTER TABLE action_fires ADD COLUMN payload TEXT NOT NULL DEFAULT '';
ALTER TABLE action_fires ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0;
ALTER TABLE action_fires ADD COLUMN next_attempt_at_ms INTEGER NOT NULL DEFAULT 0;
`,
	},
	{
		// v27: the LOCAL-FIRST panel. EMCOM readiness networks and panel
		// communications become local database state: the panel routes
		// them through the dispatch ingress directly (no broker
		// dependency) and the retained MQTT documents are asynchronous
		// sync copies for other instances. Expired compose rows stay as
		// authoritative tombstones until pruning.
		SQL: `
CREATE TABLE emcom_networks (
	slug          TEXT PRIMARY KEY,
	name          TEXT NOT NULL,
	level         INTEGER NOT NULL DEFAULT 0,
	updated_by    TEXT NOT NULL DEFAULT '',
	updated_at_ms INTEGER NOT NULL
);
CREATE TABLE compose_hazards (
	event_key     TEXT PRIMARY KEY,
	hazard_json   TEXT NOT NULL,
	status        TEXT NOT NULL,
	updated_at_ms INTEGER NOT NULL
);
`,
	},
	{
		// v28: delivery-scheduler indexes. Every action worker polls
		// ClaimNextDelivery every 250 ms and the manager sweeps
		// RecoverStaleClaims every second — without these the queries
		// scan the whole action_fires history each time and the cost
		// grows with the ledger.
		SQL: `
CREATE INDEX idx_action_fires_claim ON action_fires(action_id, status, next_attempt_at_ms);
CREATE INDEX idx_action_fires_recover ON action_fires(status, next_attempt_at_ms);
`,
	},
	{
		// v29: the common message lifecycle ledger. Every observed hazard
		// transition — local ingest, remote MQTT, the compose panel —
		// records its latest version and state per (publisher, event_key),
		// so a worker can refuse to transmit a job whose version was
		// superseded, cancelled or expired, no matter where the message
		// came from. Older versions never overwrite newer ones.
		SQL: `
CREATE TABLE message_lifecycle (
	publisher     TEXT NOT NULL,
	event_key     TEXT NOT NULL,
	version       INTEGER NOT NULL,
	status        TEXT NOT NULL,
	updated_at_ms INTEGER NOT NULL,
	PRIMARY KEY (publisher, event_key)
);
`,
	},
	{
		// v30: the durable HTTP-ingest outbox. An accepted ingest request
		// persists its intended broker publication BEFORE answering 202:
		// the endpoint therefore accepts locally even when the broker is
		// down, and a background worker publishes the rows (mask applied
		// at publish time) and deletes them only after the broker
		// confirmed — at-least-once, so a broker outage never loses the
		// cross-instance sync of an accepted request.
		SQL: `
CREATE TABLE ingest_outbox (
	id             INTEGER PRIMARY KEY AUTOINCREMENT,
	instance_id    TEXT NOT NULL,
	topic          TEXT NOT NULL,
	payload        TEXT NOT NULL,
	created_at_ms  INTEGER NOT NULL
);
CREATE INDEX idx_ingest_outbox_instance ON ingest_outbox(instance_id, id);
`,
	},
	{
		// v31: durable output pending-deletes. An output that maintains
		// retained broker documents persists its unresolved deletions
		// BEFORE the journal ack, so a restart can replay a delete the
		// journal no longer remembers (e.g. a cancellation collected
		// while the active category was masked — the retained doc would
		// otherwise outlive the alert forever).
		SQL: `
CREATE TABLE output_pending_deletes (
	output_id TEXT NOT NULL,
	event_key TEXT NOT NULL,
	topic     TEXT NOT NULL,
	PRIMARY KEY (output_id, event_key)
);
`,
	},
	{
		// v32: the database-owned lifecycle version counter. EMCOM
		// transitions allocate their lifecycle version from it inside
		// the save transaction: a strictly monotonic sequence
		// independent of the wall clock, so a backward clock correction
		// can never make a level drop look "older" than the activation
		// it must retire. The counter is seeded ABOVE every version
		// already in the ledger (legacy wall-clock versions), so the
		// first allocation always supersedes them.
		SQL: `
CREATE TABLE lifecycle_versions (
	id   INTEGER PRIMARY KEY CHECK (id = 1),
	next INTEGER NOT NULL
);
INSERT INTO lifecycle_versions (id, next)
VALUES (1, 1 + COALESCE((SELECT MAX(version) FROM message_lifecycle), 0));
`,
	},
	{
		// v33: the MeshCore integration is retired in favour of Meshtastic:
		// the durable message history table and the per-user mesh key table
		// follow the rename, so existing databases keep their rows (fresh
		// databases create the legacy names at v19/v20 and land here too).
		// SQLite renames the tables' indexes along with the tables.
		SQL: `
ALTER TABLE meshcore_messages RENAME TO meshtastic_messages;
ALTER TABLE user_meshkeys RENAME TO user_meshtastic_ids;
ALTER TABLE user_meshtastic_ids RENAME COLUMN pubkey TO node_id;
`,
	},
	{
		// v34: the tx delivery state of Meshtastic messages: "" (legacy),
		// "sent" (the radio transmitted the frame), "delivered" (the
		// recipient acknowledged the direct message) or "failed" (no
		// acknowledgment after the retries).
		SQL: `
ALTER TABLE meshtastic_messages ADD COLUMN status TEXT NOT NULL DEFAULT '';
`,
	},
	{
		// v35: the persistent Meshtastic heard-node directory: the hub
		// keeps every node it ever learned (the device node DB plus live
		// packets) so restarts and quiet periods do not empty the node
		// list; last_seen_ms drives the freshness display.
		SQL: `
CREATE TABLE meshtastic_nodes (
	id           TEXT PRIMARY KEY,
	name         TEXT NOT NULL DEFAULT '',
	short        TEXT NOT NULL DEFAULT '',
	lat          REAL NOT NULL DEFAULT 0,
	lon          REAL NOT NULL DEFAULT 0,
	last_seen_ms INTEGER NOT NULL DEFAULT 0,
	sends        TEXT NOT NULL DEFAULT ''
);
`,
	},
	{
		// v36: outbox lifecycle metadata. The durable HTTP-ingest outbox
		// must never age out corrective sync (cancellations/expirations)
		// or still-valid alarms: the appended row carries the hazard
		// status and expiry so the prune can tell them apart from stale
		// active rows.
		SQL: `
ALTER TABLE ingest_outbox ADD COLUMN status TEXT NOT NULL DEFAULT '';
ALTER TABLE ingest_outbox ADD COLUMN expires_at_ms INTEGER;
`,
	},
	{
		// v37: the durable radio-command registry. A command accepted
		// through the dispatch inbox records its lifecycle anchor AND
		// its result IN THE SAME TRANSACTION as the inbox row, so a
		// post-restart retransmission recovers the original TTL and —
		// when a result is stored — replays it instead of executing the
		// job again (the events table is the provider-ingest registry;
		// radio command acceptance never writes it).
		SQL: `
CREATE TABLE command_events (
	event_key      TEXT PRIMARY KEY,
	effective_at   TEXT NOT NULL,
	expires_at     TEXT,
	result         TEXT NOT NULL DEFAULT '',
	accepted_at_ms INTEGER NOT NULL
);
`,
	},
	{
		// v38: DM conversation filtering. A tx history row records the
		// recipient node id so the admin DM tab can show one peer's full
		// conversation (rx from them + tx to them). Broadcasts and rx
		// rows keep the empty default.
		SQL: `
ALTER TABLE meshtastic_messages ADD COLUMN recipient TEXT NOT NULL DEFAULT '';
`,
	},
	{
		// v39: durable outbox event identity. The outbox merges repeated
		// transitions of one EVENT (publisher + event key), never by
		// topic: one MQTT topic carries many independent events, so a
		// topic-level merge removed independent alarms.
		SQL: `
ALTER TABLE ingest_outbox ADD COLUMN event_key TEXT NOT NULL DEFAULT '';
ALTER TABLE ingest_outbox ADD COLUMN publisher TEXT NOT NULL DEFAULT '';
`,
	},
}

// eventColumns is the canonical column list used for SELECT and JOINs.
// expires_at_ms is the authoritative comparison value; expires_at remains
// as a human-readable copy.
const eventColumns = `event_key, source, source_id, fingerprint, status,
category, event, severity, provider_severity, urgency, certainty,
headline, description, instruction,
effective_at, expires_at_ms, latitude, longitude,
areas, source_url, received_at, first_seen_at, last_seen_at, last_seen_at_ms, updated_at`

// Store is a SQLite-backed storage.EventStore.
type Store struct {
	db *sql.DB

	// path is the database file path; FreeBytes stats the directory
	// holding it (the WAL and journal live there too).
	path string

	// now is the clock used for timestamps; injectable in tests.
	now func() time.Time
}

// Option customizes a Store during Open.
type Option func(*Store)

// WithClock sets the clock used for persistence timestamps. It is the
// deterministic-time seam used by tests across packages (ingest, storage);
// production leaves the wall clock in place.
func WithClock(now func() time.Time) Option {
	return func(s *Store) { s.now = now }
}

// MigrationInfo reports what happened to the schema version during Open.
type MigrationInfo struct {
	From int
	To   int
}

// Open connects to the SQLite database at path, applies the configured
// pragmas and migrations, and returns the ready store.
//
// SQLite settings chosen for a long-running, low-volume daemon:
//   - journal_mode=WAL: readers do not block the writer; crash-safe journal.
//   - synchronous=FULL: the change journal is the source of truth for
//     output delivery; FULL keeps the durability/performance balance on
//     the safe side for a low-volume daemon.
//   - busy_timeout=5000: wait for the lock instead of failing immediately.
//   - foreign_keys=ON: integrity checks active.
//   - a single database connection (MaxOpenConns=1): serializes access,
//     keeps per-connection PRAGMAs stable and avoids SQLITE_BUSY.
func Open(path string, opts ...Option) (*Store, MigrationInfo, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, MigrationInfo{}, fmt.Errorf("open sqlite database %q: %w", path, err)
	}

	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, MigrationInfo{}, fmt.Errorf("connect to sqlite database %q: %w", path, err)
	}

	for _, pragma := range []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=FULL",
		"PRAGMA busy_timeout=5000",
		"PRAGMA foreign_keys=ON",
	} {
		if _, err := db.Exec(pragma); err != nil {
			db.Close()
			return nil, MigrationInfo{}, fmt.Errorf("apply %s: %w", pragma, err)
		}
	}

	info, err := migrate(db, migrations)
	if err != nil {
		db.Close()
		return nil, MigrationInfo{}, err
	}

	store := &Store{db: db, now: time.Now, path: path}
	for _, opt := range opts {
		opt(store)
	}
	return store, info, nil
}

// migrate applies pending schema steps, each in its own transaction.
// Existing databases are never recreated or deleted; a failed step rolls
// back and leaves the schema version unchanged.
func migrate(db *sql.DB, steps []migration) (MigrationInfo, error) {
	var current int
	if err := db.QueryRow("PRAGMA user_version").Scan(&current); err != nil {
		return MigrationInfo{}, fmt.Errorf("read schema version: %w", err)
	}
	if current > len(steps) {
		return MigrationInfo{From: current, To: current},
			fmt.Errorf("database schema version %d is newer than this binary supports (%d); upgrade WarnFlux, not the database", current, len(steps))
	}

	info := MigrationInfo{From: current, To: current}
	for v := current; v < len(steps); v++ {
		target := v + 1
		tx, err := db.Begin()
		if err != nil {
			return info, fmt.Errorf("begin migration to version %d: %w", target, err)
		}
		if _, err := tx.Exec(steps[v].SQL); err != nil {
			tx.Rollback()
			return info, fmt.Errorf("apply migration to version %d: %w", target, err)
		}
		if steps[v].run != nil {
			if err := steps[v].run(tx); err != nil {
				tx.Rollback()
				return info, fmt.Errorf("run migration step %d: %w", target, err)
			}
		}
		// PRAGMA does not support bound parameters; the value is a
		// validated integer from this package, so formatting is safe.
		if _, err := tx.Exec(fmt.Sprintf("PRAGMA user_version = %d", target)); err != nil {
			tx.Rollback()
			return info, fmt.Errorf("set schema version %d: %w", target, err)
		}
		if err := tx.Commit(); err != nil {
			return info, fmt.Errorf("commit migration to version %d: %w", target, err)
		}
		info.To = target
	}
	return info, nil
}

// Close closes the underlying database connection.
func (s *Store) Close() error { return s.db.Close() }

// Ping verifies the database connection (the web health page's DB row).
func (s *Store) Ping(ctx context.Context) error { return s.db.PingContext(ctx) }

// Ingest atomically classifies and persists one normalized event. The event
// state transition and its change journal record (when meaningful) are
// written in a single transaction, so no other ingestion can observe the
// event half-way through.
func (s *Store) Ingest(ctx context.Context, event core.HazardEvent, fingerprint string) (storage.Outcome, *storage.Change, error) {
	now := s.now().UTC()
	nowMs := now.UnixMilli()
	key := event.Key()

	// The publisher identity is stamped at creation so the local-first
	// dispatch and the broker loopback produce the SAME dedup identity.
	publisher, err := s.instanceID(ctx)
	if err != nil {
		return 0, nil, fmt.Errorf("publisher id for ingest of %q: %w", key, err)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, nil, fmt.Errorf("begin ingest transaction for %q: %w", key, err)
	}
	defer tx.Rollback()

	var storedFp, storedStatus, storedReceived, storedFirstSeen string
	err = tx.QueryRowContext(ctx,
		"SELECT fingerprint, status, received_at, first_seen_at FROM events WHERE event_key = ?", key,
	).Scan(&storedFp, &storedStatus, &storedReceived, &storedFirstSeen)

	switch {
	case errors.Is(err, sql.ErrNoRows):
		// Previously unknown event. A first-seen cancellation is a
		// cancellation, never "new".
		outcome := storage.OutcomeNew
		changeType := core.ChangeNew
		if event.Status == core.StatusCancelled {
			outcome = storage.OutcomeCancelled
			changeType = core.ChangeCancelled
		}
		// ReceivedAt / UpdatedAt are CORE-owned ingestion metadata: the
		// store assigns them, never the provider.
		event.ReceivedAt = now
		event.UpdatedAt = now
		snapshot := storage.SnapshotOf(event)
		if _, err := tx.ExecContext(ctx, insertSQL, insertArgs(event, fingerprint, now, now, event.UpdatedAt, expiryMs(event))...); err != nil {
			return 0, nil, fmt.Errorf("insert event %q: %w", key, err)
		}
		change, err := insertChange(tx, ctx, changeType, key, nowMs, snapshot, publisher)
		if err != nil {
			return 0, nil, err
		}
		// LOCAL-FIRST: the canonical transition lands in the durable
		// inbox INSIDE the journal transaction, so a crash between the
		// journal commit and the live dispatch can never strand the
		// alert without a notification path.
		if err := insertInboxChange(tx, ctx, change, event, nowMs); err != nil {
			return 0, nil, err
		}
		if err := tx.Commit(); err != nil {
			return 0, nil, fmt.Errorf("commit ingest of %q: %w", key, err)
		}
		change.Event = event
		return outcome, change, nil

	case err != nil:
		return 0, nil, fmt.Errorf("get event %q: %w", key, err)
	}

	// Existing event: compare content fingerprint and lifecycle status.
	contentSame := storedFp == fingerprint
	incomingCancelled := event.Status == core.StatusCancelled
	// An incoming expiry is "live" when the provider removed it or moved
	// it into the future; a lapsed expiry by itself is NOT a
	// reactivation signal.
	incomingLive := expiryLive(event, nowMs)

	// Duplicate: no journal record, only last_seen_at refreshes. A lapsed
	// expiry never forces an update on its own — the maintenance worker
	// performs the single active → expired transition, so re-ingesting the
	// same stale provider event cannot create updated/expired flapping.
	duplicate := false
	switch {
	case incomingCancelled:
		duplicate = storedStatus == string(core.StatusCancelled) && contentSame
	case storedStatus == string(core.StatusActive):
		duplicate = contentSame
	case storedStatus == string(core.StatusExpired):
		// Keep the internal lifecycle expired unless the provider made the
		// event live again (future/removed expiry) or changed its content.
		duplicate = contentSame && !incomingLive
	default: // stored cancelled, incoming active: always a re-activation.
		duplicate = false
	}

	if duplicate {
		if _, err := tx.ExecContext(ctx,
			"UPDATE events SET last_seen_at = ?, last_seen_at_ms = ? WHERE event_key = ?", formatTime(now), nowMs, key); err != nil {
			return 0, nil, fmt.Errorf("touch event %q: %w", key, err)
		}
		if err := tx.Commit(); err != nil {
			return 0, nil, fmt.Errorf("commit duplicate of %q: %w", key, err)
		}
		return storage.OutcomeDuplicate, nil, nil
	}

	// Content and/or lifecycle changed. Preserve the original received_at
	// and first_seen_at; updated_at is the persistence transition time.
	received, err := time.Parse(time.RFC3339Nano, storedReceived)
	if err != nil {
		return 0, nil, fmt.Errorf("event %q has invalid stored received_at: %w", key, err)
	}
	event.ReceivedAt = received
	event.UpdatedAt = now

	var changeType core.ChangeType
	switch {
	case event.Status == core.StatusCancelled:
		changeType = core.ChangeCancelled
	case storedStatus == string(core.StatusExpired) && !incomingLive:
		// The provider changed content while its expiry is still lapsed:
		// persist the new content, keep the internal lifecycle expired.
		event.Status = core.StatusExpired
		changeType = core.ChangeUpdated
	default:
		changeType = core.ChangeUpdated
	}

	snapshot := storage.SnapshotOf(event)
	if _, err := tx.ExecContext(ctx, updateSQL, updateArgs(event, fingerprint, now, expiryMs(event))...); err != nil {
		return 0, nil, fmt.Errorf("update event %q: %w", key, err)
	}
	change, err := insertChange(tx, ctx, changeType, key, nowMs, snapshot, publisher)
	if err != nil {
		return 0, nil, err
	}
	// LOCAL-FIRST: same atomicity as the new-event path — the inbox row
	// commits with the journal record.
	if err := insertInboxChange(tx, ctx, change, event, nowMs); err != nil {
		return 0, nil, err
	}
	if err := tx.Commit(); err != nil {
		return 0, nil, fmt.Errorf("commit update of %q: %w", key, err)
	}
	change.Event = event

	if changeType == core.ChangeCancelled {
		return storage.OutcomeCancelled, change, nil
	}
	return storage.OutcomeUpdated, change, nil
}

// Expire atomically marks stale active events expired, creating a durable
// ChangeExpired journal record for each. Events without an expiry are never
// touched.
func (s *Store) Expire(ctx context.Context, now time.Time) ([]storage.Change, error) {
	now = now.UTC()
	nowMs := now.UnixMilli()

	publisher, err := s.instanceID(ctx)
	if err != nil {
		return nil, fmt.Errorf("publisher id for expiration: %w", err)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin expiration transaction: %w", err)
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx,
		"SELECT event_key FROM events WHERE status = ? AND expires_at_ms IS NOT NULL AND expires_at_ms <= ?",
		string(core.StatusActive), nowMs)
	if err != nil {
		return nil, fmt.Errorf("find expired events: %w", err)
	}
	var keys []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan expired event: %w", err)
		}
		keys = append(keys, key)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate expired events: %w", err)
	}

	changes := make([]storage.Change, 0, len(keys))
	for _, key := range keys {
		if _, err := tx.ExecContext(ctx,
			"UPDATE events SET status = ?, updated_at = ? WHERE event_key = ?",
			string(core.StatusExpired), formatTime(now), key); err != nil {
			return nil, fmt.Errorf("expire event %q: %w", key, err)
		}
		event, err := loadEventTx(tx, key)
		if err != nil {
			return nil, err
		}
		change, err := insertChange(tx, ctx, core.ChangeExpired, key, nowMs, storage.SnapshotOf(event), publisher)
		if err != nil {
			return nil, err
		}
		change.Event = event
		// LOCAL-FIRST: the expiry transition is accepted into the inbox
		// in the same transaction as the journal record.
		if err := insertInboxChange(tx, ctx, change, event, nowMs); err != nil {
			return nil, err
		}
		changes = append(changes, *change)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit expiration: %w", err)
	}
	return changes, nil
}

// PollChanges returns unacknowledged journal changes for an output in
// stable ID order. Each change carries the immutable event snapshot taken
// when the transition happened; the mutable events table is never joined
// to reconstruct history.
//
// Corrupted journal rows (unknown change_type, unparsable snapshot, or a
// snapshot whose source:source_id does not match the journal event_key)
// are a hard error: the cursor is NOT advanced and nothing is silently
// skipped.
// instanceID returns the persistent publisher UUID of this database,
// generated once on first use (crypto/rand, hex). Concurrent first calls
// converge on the same value.
func (s *Store) instanceID(ctx context.Context) (string, error) {
	var id string
	err := s.db.QueryRowContext(ctx, `SELECT instance_id FROM instance_meta WHERE id = 1`).Scan(&id)
	if err == nil {
		return id, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return "", fmt.Errorf("read instance id: %w", err)
	}
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generate instance id: %w", err)
	}
	id = hex.EncodeToString(b)
	if _, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO instance_meta (id, instance_id) VALUES (1, ?)`, id); err != nil {
		return "", fmt.Errorf("store instance id: %w", err)
	}
	// Another goroutine may have won the insert race: read back.
	return s.instanceID(ctx)
}

// AppendEvent persists one canonical dispatch event into the durable
// inbox (dispatch acceptance): it returns the inbox row ID that the
// routing engine must acknowledge once the event has been evaluated.
// Radio command events additionally record their lifecycle anchor and
// result (the command registry) IN THE SAME TRANSACTION, so a restart
// can never strand a durably accepted command without its registry row.
func (s *Store) AppendEvent(ctx context.Context, ev dispatch.Event) (int64, error) {
	data, err := json.Marshal(ev)
	if err != nil {
		return 0, fmt.Errorf("encode inbox event: %w", err)
	}
	nowMs := s.now().UnixMilli()
	// The command registry anchor: only radio command events carry a
	// result; provider events keep the events table as their registry.
	anchor := ev.CommandResult != "" && ev.Kind == dispatch.EventHazardTransition &&
		ev.Hazard != nil && ev.Hazard.Key != "" && ev.Hazard.Hazard.EffectiveAt != nil
	if !anchor {
		return insertInboxRow(s.db, ctx, data, ev.Origin.ReceiverID, nowMs)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin inbox acceptance for command %q: %w", ev.Hazard.Key, err)
	}
	defer tx.Rollback()
	id, err := insertInboxRow(tx, ctx, data, ev.Origin.ReceiverID, nowMs)
	if err != nil {
		return 0, err
	}
	var exp any
	if ev.Hazard.Hazard.ExpiresAt != nil {
		exp = ev.Hazard.Hazard.ExpiresAt.UTC().Format(time.RFC3339Nano)
	}
	// The FIRST acceptance wins: a replayed copy of the same command
	// must never rewrite the original TTL with a fresh one.
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO command_events (event_key, effective_at, expires_at, result, accepted_at_ms)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(event_key) DO NOTHING`,
		ev.Hazard.Key, ev.Hazard.Hazard.EffectiveAt.UTC().Format(time.RFC3339Nano),
		exp, ev.CommandResult, nowMs); err != nil {
		return 0, fmt.Errorf("anchor command %q: %w", ev.Hazard.Key, err)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit inbox acceptance for command %q: %w", ev.Hazard.Key, err)
	}
	return id, nil
}

// CommandTimes returns the stored lifecycle anchor and result of one
// radio command, or ok=false when the command was never durably
// accepted (or its anchor aged out).
func (s *Store) CommandTimes(ctx context.Context, key string) (eff, exp time.Time, result string, ok bool, err error) {
	var effS string
	var expS sql.NullString
	if err := s.db.QueryRowContext(ctx,
		"SELECT effective_at, expires_at, result FROM command_events WHERE event_key = ?", key,
	).Scan(&effS, &expS, &result); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return time.Time{}, time.Time{}, "", false, nil
		}
		return time.Time{}, time.Time{}, "", false, fmt.Errorf("command times %q: %w", key, err)
	}
	eff, err = time.Parse(time.RFC3339Nano, effS)
	if err != nil {
		return time.Time{}, time.Time{}, "", false, fmt.Errorf("command %q has invalid effective_at: %w", key, err)
	}
	if expS.Valid {
		if exp, err = time.Parse(time.RFC3339Nano, expS.String); err != nil {
			return time.Time{}, time.Time{}, "", false, fmt.Errorf("command %q has invalid expires_at: %w", key, err)
		}
	}
	return eff, exp, result, true, nil
}

// PruneCommandEvents deletes command-registry anchors older than the
// cutoff: a command retransmitted later than the retention window starts
// its lifecycle fresh (a legitimate new command, not a replay).
func (s *Store) PruneCommandEvents(ctx context.Context, olderThan time.Time) (int64, error) {
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM command_events WHERE accepted_at_ms < ?`, olderThan.UnixMilli())
	if err != nil {
		return 0, fmt.Errorf("prune command events: %w", err)
	}
	return res.RowsAffected()
}

// CommitIngest atomically persists one accepted HTTP-ingest request: the
// durable inbox row (the routing engine evaluates it), the lifecycle
// record (when the transition carries a publisher identity) and the
// durable outbox row (the broker sync) commit in ONE transaction — a
// rejected commit accepts NOTHING, and a crash can never leave a locally
// accepted event without its MQTT sync (reported P1: previously the
// inbox committed first, a failed outbox answered 503 although the local
// notification was already accepted). It returns the inbox row ID the
// caller stamps onto the event before the live handoff.
func (s *Store) CommitIngest(ctx context.Context, instanceID, topic string, payload []byte, ev dispatch.Event) (int64, error) {
	h := ev.Hazard
	nowMs := s.now().UnixMilli()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin ingest acceptance for %q: %w", instanceID, err)
	}
	defer tx.Rollback()

	// Lifecycle first: a cancellation must block previously queued jobs
	// even while the inbox evaluation is still pending.
	if h != nil && h.Publisher != "" {
		status := string(core.StatusActive)
		switch h.Type {
		case dispatch.TransitionCancelled:
			status = string(core.StatusCancelled)
		case dispatch.TransitionExpired:
			status = string(core.StatusExpired)
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO message_lifecycle (publisher, event_key, version, status, updated_at_ms)
			VALUES (?, ?, ?, ?, ?)
			ON CONFLICT(publisher, event_key) DO UPDATE SET
				version = excluded.version,
				status = excluded.status,
				updated_at_ms = excluded.updated_at_ms
			WHERE excluded.version >= message_lifecycle.version`,
			h.Publisher, h.Key, h.ChangeID, status, nowMs); err != nil {
			return 0, fmt.Errorf("record lifecycle for %q: %w", h.Key, err)
		}
	}

	data, err := json.Marshal(ev)
	if err != nil {
		return 0, fmt.Errorf("encode inbox event: %w", err)
	}
	inboxID, err := insertInboxRow(tx, ctx, data, ev.Origin.ReceiverID, nowMs)
	if err != nil {
		return 0, fmt.Errorf("inbox row for ingest %q: %w", instanceID, err)
	}

	// The outbox row mirrors AppendOutbox: repeated transitions of one
	// EVENT (publisher + event key — never the topic) merge into the
	// newest state, and the row carries the lifecycle metadata so
	// age-pruning recognizes stale rows and corrective sync
	// (cancellations/expirations) is never evicted.
	status, expiresMs := outboxLifecycle(payload)
	eventKey, publisher := outboxIdentity(payload)
	if err := mergeOutboxRows(ctx, tx, instanceID, eventKey, publisher); err != nil {
		return 0, fmt.Errorf("merge outbox rows for %q: %w", instanceID, err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO ingest_outbox (instance_id, topic, payload, created_at_ms, status, expires_at_ms, event_key, publisher)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		instanceID, topic, string(payload), nowMs, status, expiresMs, eventKey, publisher); err != nil {
		return 0, fmt.Errorf("outbox row for ingest %q: %w", instanceID, err)
	}
	if err := enforceOutboxCap(ctx, tx); err != nil {
		return 0, err
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit ingest acceptance for %q: %w", instanceID, err)
	}
	return inboxID, nil
}

// FreeBytes reports the filesystem free space for the database
// directory (the WAL and journal live next to the database file). It
// feeds the low-disk alarm and the aggressive inbox retention policy.
func (s *Store) FreeBytes(ctx context.Context) (int64, error) {
	var st unix.Statfs_t
	if err := unix.Statfs(filepath.Dir(s.path), &st); err != nil {
		return 0, fmt.Errorf("statfs %q: %w", filepath.Dir(s.path), err)
	}
	return int64(st.Bavail) * int64(st.Bsize), nil
}

// InboxCount reports the current durable dispatch-inbox backlog (rows
// persisted but not yet acknowledged by the routing engine).
func (s *Store) InboxCount(ctx context.Context) (int, error) {
	var n int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM dispatch_inbox`).Scan(&n); err != nil {
		return 0, fmt.Errorf("count inbox rows: %w", err)
	}
	return n, nil
}

// PruneInbox applies the pending-message validity policy to the durable
// inbox: a row older than olderThan is removed ONLY when its hazard
// carries an authoritative expiry that has already passed. The
// staleness gates would suppress such an event anyway, so removal
// changes nothing about delivery — it is a deliberate, explicit outcome
// the caller counts, logs and audits. Rows carrying no expiry, a future
// expiry or a non-hazard event are pending messages and are NEVER aged
// out by ordinary retention: unsent work must survive a long offline
// stretch or a disabled action. Unreadable rows are kept (fail-open).
func (s *Store) PruneInbox(ctx context.Context, olderThan time.Time) (int64, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, event_json FROM dispatch_inbox WHERE received_at_ms < ?`,
		olderThan.UnixMilli())
	if err != nil {
		return 0, fmt.Errorf("scan inbox rows for pruning: %w", err)
	}
	now := s.now().UTC()
	var expired []int64
	for rows.Next() {
		var id int64
		var payload string
		if err := rows.Scan(&id, &payload); err != nil {
			rows.Close()
			return 0, fmt.Errorf("scan inbox row: %w", err)
		}
		var ev dispatch.Event
		if err := json.Unmarshal([]byte(payload), &ev); err != nil {
			continue // unreadable rows are kept (fail-open)
		}
		if ev.Kind != dispatch.EventHazardTransition || ev.Hazard == nil || ev.Hazard.Hazard.ExpiresAt == nil {
			continue // no authoritative expiry: never age out
		}
		if ev.Hazard.Hazard.ExpiresAt.After(now) {
			continue
		}
		expired = append(expired, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("iterate inbox rows: %w", err)
	}
	if len(expired) == 0 {
		return 0, nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin inbox prune: %w", err)
	}
	defer tx.Rollback()
	for _, id := range expired {
		if _, err := tx.ExecContext(ctx, `DELETE FROM dispatch_inbox WHERE id = ?`, id); err != nil {
			return 0, fmt.Errorf("prune inbox row %d: %w", id, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit inbox prune: %w", err)
	}
	return int64(len(expired)), nil
}

// RecordLifecycle upserts the latest observed lifecycle state of one
// message (publisher + event key): the highest version wins, so a
// replayed OLD transition can never downgrade a newer cancellation or
// expiry. The compose panel records its state through the same ledger
// (empty publisher, version = updated_at_ms), which makes the worker's
// pre-transmission gate cover panel messages too.
func (s *Store) RecordLifecycle(ctx context.Context, publisher, eventKey string, version int64, status string) error {
	at := s.now().UTC().UnixMilli()
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO message_lifecycle (publisher, event_key, version, status, updated_at_ms)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(publisher, event_key) DO UPDATE SET
			version = excluded.version,
			status = excluded.status,
			updated_at_ms = excluded.updated_at_ms
		WHERE excluded.version >= message_lifecycle.version`,
		publisher, eventKey, version, status, at); err != nil {
		return fmt.Errorf("record lifecycle for %q: %w", eventKey, err)
	}
	return nil
}

// LifecycleBlocks reports whether a queued delivery for the given
// message version must NOT be transmitted: a newer version is already
// known, or exactly this version is cancelled/expired. Jobs without a
// version identity (changeID 0 — panel messages) are judged by the
// CURRENT state only. An unknown message never blocks (fail-open).
func (s *Store) LifecycleBlocks(ctx context.Context, publisher, eventKey string, changeID int64) (bool, error) {
	var version int64
	var status string
	err := s.db.QueryRowContext(ctx, `
		SELECT version, status FROM message_lifecycle
		WHERE publisher = ? AND event_key = ?`, publisher, eventKey).Scan(&version, &status)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read lifecycle for %q: %w", eventKey, err)
	}
	if changeID > 0 {
		if version > changeID {
			return true, nil // a newer version supersedes this job
		}
		if version == changeID && status != string(core.StatusActive) {
			return true, nil // exactly this version is terminal
		}
		return false, nil
	}
	return status != string(core.StatusActive), nil
}

// InstanceID returns the persistent publisher UUID of this WarnFlux
// database (created on first use). Panel-issued messages stamp it onto
// their transitions so the lifecycle ledger can identify and version
// them.
func (s *Store) InstanceID(ctx context.Context) (string, error) {
	return s.instanceID(ctx)
}

// SaveEmcomNetwork upserts one EMCOM readiness network (the panel's
// durable local record; the retained MQTT document is only a sync copy).
// The same state joins the common lifecycle ledger in the SAME
// transaction (instance publisher, event key emcom:<slug>): dropping a
// network back to monitoring records the cancellation atomically with
// the network state, so the delivery gate blocks a previously queued
// activation once the radio returns.
//
// The version is allocated from the database-owned monotonic counter
// (see lifecycle_versions), NOT the wall clock: after a backward clock
// correction the level drop must still supersede — and block — the
// activation it retires (reported P1). The allocated version is
// returned so the caller can stamp it onto the dispatched transition.
func (s *Store) SaveEmcomNetwork(ctx context.Context, net storage.EmcomNetwork) (int64, error) {
	publisher, err := s.instanceID(ctx)
	if err != nil {
		return 0, fmt.Errorf("publisher id for emcom %q: %w", net.Slug, err)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin emcom save for %q: %w", net.Slug, err)
	}
	defer tx.Rollback()

	version, err := nextLifecycleVersionTx(tx, ctx)
	if err != nil {
		return 0, fmt.Errorf("lifecycle version for emcom %q: %w", net.Slug, err)
	}

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO emcom_networks (slug, name, level, updated_by, updated_at_ms)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(slug) DO UPDATE SET
			name = excluded.name, level = excluded.level,
			updated_by = excluded.updated_by, updated_at_ms = excluded.updated_at_ms`,
		net.Slug, net.Name, net.Level, net.UpdatedBy, net.UpdatedAt.UnixMilli()); err != nil {
		return 0, fmt.Errorf("save emcom network %q: %w", net.Slug, err)
	}

	// The lifecycle state of the EMCOM hazard (active above monitoring,
	// expired below) commits with the network record, under the
	// counter-allocated version.
	status := string(core.StatusActive)
	if net.Level < 1 {
		status = string(core.StatusExpired)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO message_lifecycle (publisher, event_key, version, status, updated_at_ms)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(publisher, event_key) DO UPDATE SET
			version = excluded.version,
			status = excluded.status,
			updated_at_ms = excluded.updated_at_ms
		WHERE excluded.version >= message_lifecycle.version`,
		publisher, "emcom:"+net.Slug, version, status, net.UpdatedAt.UnixMilli()); err != nil {
		return 0, fmt.Errorf("record emcom lifecycle %q: %w", net.Slug, err)
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit emcom save for %q: %w", net.Slug, err)
	}
	return version, nil
}

// nextLifecycleVersionTx allocates the next lifecycle version INSIDE the
// caller's transaction from the database-owned counter: strictly
// monotonic and independent of the wall clock, so a backward clock
// correction can never order a cancellation below the activation it must
// retire. The single database connection serializes allocations.
func nextLifecycleVersionTx(tx *sql.Tx, ctx context.Context) (int64, error) {
	if _, err := tx.ExecContext(ctx,
		`UPDATE lifecycle_versions SET next = next + 1 WHERE id = 1`); err != nil {
		return 0, fmt.Errorf("bump lifecycle version: %w", err)
	}
	var v int64
	if err := tx.QueryRowContext(ctx,
		`SELECT next FROM lifecycle_versions WHERE id = 1`).Scan(&v); err != nil {
		return 0, fmt.Errorf("read lifecycle version: %w", err)
	}
	return v, nil
}

// DeleteEmcomNetwork removes one EMCOM readiness network.
func (s *Store) DeleteEmcomNetwork(ctx context.Context, slug string) error {
	if _, err := s.db.ExecContext(ctx,
		`DELETE FROM emcom_networks WHERE slug = ?`, slug); err != nil {
		return fmt.Errorf("delete emcom network %q: %w", slug, err)
	}
	return nil
}

// TombstoneEmcomNetwork atomically retires one EMCOM network: the
// tombstone row (level -1), the lifecycle record (expired, counter
// version) and — when the caller passes the expiry transition — its
// durable inbox row commit in ONE transaction. A crash between the
// deletion and the local dispatch can therefore never lose the
// cancellation (reported P1: the tombstone and the enqueue used to be
// separate steps). The expiry event's ChangeID is stamped with the
// allocated version INSIDE the transaction, so the stored inbox row and
// the live handoff are identical. It returns the inbox row id (0 when no
// transition was passed) and the allocated version.
func (s *Store) TombstoneEmcomNetwork(ctx context.Context, slug string, ev dispatch.Event) (int64, int64, error) {
	publisher, err := s.instanceID(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("publisher id for emcom tombstone %q: %w", slug, err)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, 0, fmt.Errorf("begin emcom tombstone for %q: %w", slug, err)
	}
	defer tx.Rollback()

	version, err := nextLifecycleVersionTx(tx, ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("lifecycle version for emcom tombstone %q: %w", slug, err)
	}
	nowMs := s.now().UnixMilli()

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO emcom_networks (slug, name, level, updated_by, updated_at_ms)
		VALUES (?, '', -1, '', ?)
		ON CONFLICT(slug) DO UPDATE SET
			name = '', level = -1,
			updated_by = '', updated_at_ms = excluded.updated_at_ms`,
		slug, nowMs); err != nil {
		return 0, 0, fmt.Errorf("tombstone emcom network %q: %w", slug, err)
	}

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO message_lifecycle (publisher, event_key, version, status, updated_at_ms)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(publisher, event_key) DO UPDATE SET
			version = excluded.version,
			status = excluded.status,
			updated_at_ms = excluded.updated_at_ms
		WHERE excluded.version >= message_lifecycle.version`,
		publisher, "emcom:"+slug, version, string(core.StatusExpired), nowMs); err != nil {
		return 0, 0, fmt.Errorf("record emcom tombstone lifecycle %q: %w", slug, err)
	}

	var inboxID int64
	if ev.Hazard != nil {
		ev.Hazard.ChangeID = version
		data, err := json.Marshal(ev)
		if err != nil {
			return 0, 0, fmt.Errorf("encode emcom expiry %q: %w", slug, err)
		}
		inboxID, err = insertInboxRow(tx, ctx, data, ev.Origin.ReceiverID, nowMs)
		if err != nil {
			return 0, 0, fmt.Errorf("inbox row for emcom expiry %q: %w", slug, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return 0, 0, fmt.Errorf("commit emcom tombstone for %q: %w", slug, err)
	}
	return inboxID, version, nil
}

// EmcomNetworks returns the persisted readiness networks sorted by name.
func (s *Store) EmcomNetworks(ctx context.Context) ([]storage.EmcomNetwork, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT slug, name, level, updated_by, updated_at_ms
		FROM emcom_networks ORDER BY name`)
	if err != nil {
		return nil, fmt.Errorf("list emcom networks: %w", err)
	}
	defer rows.Close()
	var out []storage.EmcomNetwork
	for rows.Next() {
		var n storage.EmcomNetwork
		var ms int64
		if err := rows.Scan(&n.Slug, &n.Name, &n.Level, &n.UpdatedBy, &ms); err != nil {
			return nil, fmt.Errorf("scan emcom network: %w", err)
		}
		n.UpdatedAt = time.UnixMilli(ms)
		out = append(out, n)
	}
	return out, rows.Err()
}

// SaveComposeHazard upserts one panel-issued communication (the panel's
// durable local record; the retained MQTT document is only a sync copy).
// Expired rows stay as authoritative tombstones so a stale broker copy
// can never revive them. The record and its lifecycle state commit in
// ONE transaction (reported P1: a failed lifecycle write used to leave
// an expired record with an ACTIVE lifecycle — the delivery queue then
// still allowed the retired message). The lifecycle version comes from
// the database-owned monotonic counter, independent of the wall clock.
func (s *Store) SaveComposeHazard(ctx context.Context, h storage.ComposeHazard) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin compose save for %q: %w", h.EventKey, err)
	}
	defer tx.Rollback()

	version, err := nextLifecycleVersionTx(tx, ctx)
	if err != nil {
		return fmt.Errorf("lifecycle version for compose %q: %w", h.EventKey, err)
	}

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO compose_hazards (event_key, hazard_json, status, updated_at_ms)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(event_key) DO UPDATE SET
			hazard_json = excluded.hazard_json, status = excluded.status,
			updated_at_ms = excluded.updated_at_ms`,
		h.EventKey, string(h.State), h.Status, h.UpdatedAt.UnixMilli()); err != nil {
		return fmt.Errorf("save compose hazard %q: %w", h.EventKey, err)
	}
	// The same state joins the common lifecycle ledger (empty publisher
	// — panel jobs are versionless and judged by the CURRENT status), so
	// a queued delivery of this message is blocked once the panel
	// cancels or expires it.
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO message_lifecycle (publisher, event_key, version, status, updated_at_ms)
		VALUES ('', ?, ?, ?, ?)
		ON CONFLICT(publisher, event_key) DO UPDATE SET
			version = excluded.version,
			status = excluded.status,
			updated_at_ms = excluded.updated_at_ms
		WHERE excluded.version >= message_lifecycle.version`,
		h.EventKey, version, h.Status, h.UpdatedAt.UnixMilli()); err != nil {
		return fmt.Errorf("record compose lifecycle %q: %w", h.EventKey, err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit compose save for %q: %w", h.EventKey, err)
	}
	return nil
}

// DeleteComposeHazard removes one panel-issued communication.
func (s *Store) DeleteComposeHazard(ctx context.Context, eventKey string) error {
	if _, err := s.db.ExecContext(ctx,
		`DELETE FROM compose_hazards WHERE event_key = ?`, eventKey); err != nil {
		return fmt.Errorf("delete compose hazard %q: %w", eventKey, err)
	}
	return nil
}

// ComposeHazards returns the persisted panel communications.
func (s *Store) ComposeHazards(ctx context.Context) ([]storage.ComposeHazard, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT event_key, hazard_json, status, updated_at_ms
		FROM compose_hazards ORDER BY updated_at_ms DESC`)
	if err != nil {
		return nil, fmt.Errorf("list compose hazards: %w", err)
	}
	defer rows.Close()
	var out []storage.ComposeHazard
	for rows.Next() {
		var h storage.ComposeHazard
		var raw string
		var ms int64
		if err := rows.Scan(&h.EventKey, &raw, &h.Status, &ms); err != nil {
			return nil, fmt.Errorf("scan compose hazard: %w", err)
		}
		h.State = []byte(raw)
		h.UpdatedAt = time.UnixMilli(ms)
		out = append(out, h)
	}
	return out, rows.Err()
}

// PruneComposeHazards deletes expired compose tombstone rows older than
// olderThan (the controlled retention of auxiliary panel data).
func (s *Store) PruneComposeHazards(ctx context.Context, olderThan time.Time) (int64, error) {
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM compose_hazards WHERE status = 'expired' AND updated_at_ms < ?`,
		olderThan.UnixMilli())
	if err != nil {
		return 0, fmt.Errorf("prune compose hazards: %w", err)
	}
	return res.RowsAffected()
}

// PruneEmcomNetworks deletes EMCOM tombstone rows (level < 0) older than
// olderThan: they only exist to remove stale broker copies on resync and
// stop being useful once the broker has seen them.
func (s *Store) PruneEmcomNetworks(ctx context.Context, olderThan time.Time) (int64, error) {
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM emcom_networks WHERE level < 0 AND updated_at_ms < ?`,
		olderThan.UnixMilli())
	if err != nil {
		return 0, fmt.Errorf("prune emcom networks: %w", err)
	}
	return res.RowsAffected()
}

// PendingInboxEvents returns the oldest unacknowledged inbox rows (at most
// limit, bounded 1..256), oldest first. Every returned item carries the
// inbox ID inside its event so the engine can acknowledge it.
func (s *Store) PendingInboxEvents(ctx context.Context, limit int) ([]storage.InboxItem, error) {
	if limit <= 0 || limit > 256 {
		limit = 256
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, event_json FROM dispatch_inbox ORDER BY id LIMIT ?`, limit)
	if err != nil {
		return nil, fmt.Errorf("pending inbox events: %w", err)
	}
	defer rows.Close()
	var out []storage.InboxItem
	for rows.Next() {
		var it storage.InboxItem
		var raw string
		if err := rows.Scan(&it.ID, &raw); err != nil {
			return nil, fmt.Errorf("scan inbox event: %w", err)
		}
		if err := json.Unmarshal([]byte(raw), &it.Event); err != nil {
			return nil, fmt.Errorf("decode inbox event %d: %w", it.ID, err)
		}
		it.Event.InboxID = it.ID
		out = append(out, it)
	}
	return out, rows.Err()
}

// AckInboxEvent deletes one inbox row after the routing engine evaluated
// the event (the per-(group, action) delivery jobs carry the action-level
// at-least-once semantics from there on).
func (s *Store) AckInboxEvent(ctx context.Context, id int64) error {
	if _, err := s.db.ExecContext(ctx, `DELETE FROM dispatch_inbox WHERE id = ?`, id); err != nil {
		return fmt.Errorf("ack inbox event %d: %w", id, err)
	}
	return nil
}

func (s *Store) PollChanges(ctx context.Context, outputID string, limit int) ([]storage.Change, error) {
	cursor, err := s.cursor(ctx, outputID)
	if err != nil {
		return nil, err
	}
	if limit <= 0 || limit > 256 {
		limit = 256
	}

	// The publisher identity is stable for the life of this database:
	// every journal change leaves this instance stamped with the same
	// UUID, so independent publishers can never produce colliding
	// deduplication keys.
	publisher, err := s.instanceID(ctx)
	if err != nil {
		return nil, fmt.Errorf("publisher id for %q: %w", outputID, err)
	}

	rows, err := s.db.QueryContext(ctx, `
		SELECT id, change_type, event_key, event_snapshot
		FROM changes
		WHERE id > ?
		ORDER BY id
		LIMIT ?`, cursor, limit)
	if err != nil {
		return nil, fmt.Errorf("poll changes for %q: %w", outputID, err)
	}
	defer rows.Close()

	var changes []storage.Change
	for rows.Next() {
		var (
			change     storage.Change
			changeType string
			key        string
			snapshot   string
		)
		if err := rows.Scan(&change.ID, &changeType, &key, &snapshot); err != nil {
			return nil, fmt.Errorf("scan change for %q: %w", outputID, err)
		}
		change.Publisher = publisher
		ct, ok := core.ChangeTypeOf(changeType)
		if !ok {
			return nil, fmt.Errorf("change %d has unknown change_type %q", change.ID, changeType)
		}
		event, err := decodeSnapshot(snapshot)
		if err != nil {
			return nil, fmt.Errorf("decode snapshot for change %d: %w", change.ID, err)
		}
		if event.Key() != key {
			return nil, fmt.Errorf("change %d snapshot key %q does not match journal event_key %q", change.ID, event.Key(), key)
		}
		// Semantic sanity check: structurally valid but impossible
		// snapshots must never reach outputs.
		if err := core.ValidateJournalChange(ct, event); err != nil {
			return nil, fmt.Errorf("change %d snapshot is semantically invalid: %w", change.ID, err)
		}
		change.ChangeType = ct
		change.Event = event
		changes = append(changes, change)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate changes for %q: %w", outputID, err)
	}
	return changes, nil
}

// SyncOutputs makes the output_cursors table match the set of enabled
// outputs exactly (see storage.EventStore). The durable consumer identity
// is the (id, type) pair: same id + same type preserves the cursor, same
// id + different type resets the cursor to 0 (a new consumer that replays
// the retained journal), and outputs that are no longer enabled are
// removed so they stop blocking cleanup.
func (s *Store) SyncOutputs(ctx context.Context, enabled []storage.OutputRef) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin output sync: %w", err)
	}
	defer tx.Rollback()

	for _, out := range enabled {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO output_cursors (output_id, output_type, last_acked_id) VALUES (?, ?, 0)
			ON CONFLICT(output_id) DO NOTHING`, out.ID, out.Type); err != nil {
			return fmt.Errorf("create cursor for %q: %w", out.ID, err)
		}
		// Same ID with a different plugin type: a new durable consumer.
		// Reset its cursor so it replays retained history instead of
		// inheriting another destination's progress.
		if _, err := tx.ExecContext(ctx, `
			UPDATE output_cursors SET output_type = ?, last_acked_id = 0
			WHERE output_id = ? AND output_type != ?`, out.Type, out.ID, out.Type); err != nil {
			return fmt.Errorf("reset cursor for %q: %w", out.ID, err)
		}
	}

	enabledIDs := make(map[string]bool, len(enabled))
	for _, out := range enabled {
		enabledIDs[out.ID] = true
	}
	rows, err := tx.QueryContext(ctx, "SELECT output_id FROM output_cursors")
	if err != nil {
		return fmt.Errorf("read cursors: %w", err)
	}
	var stale []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return fmt.Errorf("scan cursor: %w", err)
		}
		if !enabledIDs[id] {
			stale = append(stale, id)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate cursors: %w", err)
	}
	for _, id := range stale {
		if _, err := tx.ExecContext(ctx, "DELETE FROM output_cursors WHERE output_id = ?", id); err != nil {
			return fmt.Errorf("remove cursor for %q: %w", id, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit output sync: %w", err)
	}
	return nil
}

// AckChanges records the highest change ID an output has delivered.
// AckChanges trusts the caller to have delivered every change up to
// lastChangeID (only ordered output workers may call it), but rejects
// nonsense values: non-positive IDs and IDs beyond the existing journal.
func (s *Store) AckChanges(ctx context.Context, outputID string, lastChangeID int64) error {
	if lastChangeID <= 0 {
		return fmt.Errorf("ack changes for %q: lastChangeID must be > 0, got %d", outputID, lastChangeID)
	}
	var maxID int64
	if err := s.db.QueryRowContext(ctx, "SELECT COALESCE(MAX(id), 0) FROM changes").Scan(&maxID); err != nil {
		return fmt.Errorf("ack changes for %q: %w", outputID, err)
	}
	if lastChangeID > maxID {
		return fmt.Errorf("ack changes for %q: lastChangeID %d beyond the highest journal id %d", outputID, lastChangeID, maxID)
	}
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO output_cursors (output_id, output_type, last_acked_id) VALUES (?, '', ?)
		ON CONFLICT(output_id) DO UPDATE SET
			last_acked_id = MAX(last_acked_id, excluded.last_acked_id)`,
		outputID, lastChangeID); err != nil {
		return fmt.Errorf("ack changes for %q: %w", outputID, err)
	}
	return nil
}

// CleanupChanges deletes journal rows older than olderThan that every
// output has acknowledged. When no outputs exist, all rows are considered
// acknowledged (nothing relevant is waiting for them).
func (s *Store) CleanupChanges(ctx context.Context, olderThan time.Time) (int64, error) {
	cutoff := olderThan.UTC().UnixMilli()
	res, err := s.db.ExecContext(ctx, `
		DELETE FROM changes
		WHERE created_at_ms < ?
		  AND id <= COALESCE(
			(SELECT MIN(last_acked_id) FROM output_cursors),
			(SELECT MAX(id) FROM changes),
			0
		  )`, cutoff)
	if err != nil {
		return 0, fmt.Errorf("cleanup changes: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("cleanup changes: %w", err)
	}
	return n, nil
}

// CleanupEvents deletes cancelled/expired current-state records that have
// not been observed from the provider for olderThan. Observation is
// tracked by last_seen_at_ms (machine time, not the human-readable text),
// so a provider that keeps repeating a stale event keeps it retained.
// Active events are never touched, and the change journal is unaffected
// (it carries immutable snapshots).
func (s *Store) CleanupEvents(ctx context.Context, olderThan time.Time) (int64, error) {
	res, err := s.db.ExecContext(ctx,
		"DELETE FROM events WHERE status IN (?, ?) AND last_seen_at_ms < ?",
		string(core.StatusCancelled), string(core.StatusExpired), olderThan.UTC().UnixMilli())
	if err != nil {
		return 0, fmt.Errorf("cleanup events: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("cleanup events: %w", err)
	}
	return n, nil
}

// PendingStats reports undelivered change count and the age of the oldest
// undelivered change. It is consistent with CleanupChanges: both operate
// against the set of enabled output cursors (synchronized by SyncOutputs),
// and with no enabled outputs nothing is considered pending — retained
// changes are history, cleaned up by age.
func (s *Store) PendingStats(ctx context.Context) (int, time.Duration, error) {
	var pending int
	var oldestMs sql.NullInt64
	err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*), MIN(created_at_ms)
		FROM changes
		WHERE id > COALESCE(
			(SELECT MIN(last_acked_id) FROM output_cursors),
			(SELECT MAX(id) FROM changes),
			0
		)`).Scan(&pending, &oldestMs)
	if err != nil {
		return 0, 0, fmt.Errorf("pending stats: %w", err)
	}
	oldest := time.Duration(0)
	if oldestMs.Valid {
		oldest = time.Since(time.UnixMilli(oldestMs.Int64))
		if oldest < 0 {
			oldest = 0
		}
	}
	return pending, oldest, nil
}

// Get returns the stored event for key, or storage.ErrNotFound.
func (s *Store) Get(ctx context.Context, key string) (*storage.StoredEvent, error) {
	row := s.db.QueryRowContext(ctx, "SELECT "+eventColumns+" FROM events WHERE event_key = ?", key)
	var stored storage.StoredEvent
	event, err := scanEventRow(row.Scan, &stored)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, storage.ErrNotFound
		}
		return nil, fmt.Errorf("get event %q: %w", key, err)
	}
	stored.Event = event
	return &stored, nil
}

// Count returns the number of stored events.
func (s *Store) Count(ctx context.Context) (int, error) {
	var n int
	if err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM events").Scan(&n); err != nil {
		return 0, fmt.Errorf("count events: %w", err)
	}
	return n, nil
}

// EventTimes returns the stored lifecycle anchor of one event. A
// missing row or a NULL effective_at reports ok=false (the caller
// starts the lifecycle fresh).
func (s *Store) EventTimes(ctx context.Context, key string) (eff, exp time.Time, ok bool, err error) {
	var effS, expS sql.NullString
	if err := s.db.QueryRowContext(ctx,
		"SELECT effective_at, expires_at FROM events WHERE event_key = ?", key,
	).Scan(&effS, &expS); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return time.Time{}, time.Time{}, false, nil
		}
		return time.Time{}, time.Time{}, false, fmt.Errorf("event times %q: %w", key, err)
	}
	if !effS.Valid {
		return time.Time{}, time.Time{}, false, nil
	}
	eff, err = time.Parse(time.RFC3339Nano, effS.String)
	if err != nil {
		return time.Time{}, time.Time{}, false, fmt.Errorf("event %q has invalid effective_at: %w", key, err)
	}
	if expS.Valid {
		if exp, err = time.Parse(time.RFC3339Nano, expS.String); err != nil {
			return time.Time{}, time.Time{}, false, fmt.Errorf("event %q has invalid expires_at: %w", key, err)
		}
	} else {
		exp = time.Time{}
	}
	return eff, exp, true, nil
}

// CountActive returns the number of currently active events (the
// warnflux_events_active gauge).
func (s *Store) CountActive(ctx context.Context) (int, error) {
	var n int
	if err := s.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM events WHERE status = ?", string(core.StatusActive)).Scan(&n); err != nil {
		return 0, fmt.Errorf("count active events: %w", err)
	}
	return n, nil
}

// HazardActive reports the local freshness verdict for one event key,
// scoped to the PUBLISHER identity. The events table holds THIS
// instance's current-state records: a transition stamped with a
// different publisher is not governed by our local row (independent
// publishers never collide), so it answers HazardUnknown (fail-open) —
// a local cancellation can never suppress another instance's alert.
// An empty publisher (legacy, panel) keeps the key-only check.
// An unknown key is HazardUnknown (the transition may come from a
// producer without local storage); a known key is HazardInactive when
// its status is not active (cancelled/expired — an older update must
// never outrank a known cancellation) or its expires_at_ms has passed.
// The nullable expiry is read through sql.NullInt64, so a cancelled or
// expired event WITHOUT an expiry still yields a clean verdict instead
// of a scan error that callers would fail open on.
func (s *Store) HazardActive(ctx context.Context, publisher, eventKey string, now time.Time) (storage.HazardVerdict, error) {
	if publisher != "" {
		local, err := s.instanceID(ctx)
		if err != nil {
			return storage.HazardUnknown, fmt.Errorf("publisher id for hazard freshness of %q: %w", eventKey, err)
		}
		if publisher != local {
			// A remote publisher's hazard: the local events table has no
			// authority over it. The publisher-scoped lifecycle ledger
			// remains the gate for remote transitions.
			return storage.HazardUnknown, nil
		}
	}
	var status string
	var expiresMs sql.NullInt64
	err := s.db.QueryRowContext(ctx,
		`SELECT status, expires_at_ms FROM events WHERE event_key = ?`, eventKey).
		Scan(&status, &expiresMs)
	if errors.Is(err, sql.ErrNoRows) {
		return storage.HazardUnknown, nil
	}
	if err != nil {
		return 0, fmt.Errorf("hazard freshness for %q: %w", eventKey, err)
	}
	if status != string(core.StatusActive) {
		return storage.HazardInactive, nil
	}
	if expiresMs.Valid && expiresMs.Int64 > 0 && expiresMs.Int64 <= now.UnixMilli() {
		return storage.HazardInactive, nil
	}
	return storage.HazardActive, nil
}

// ---- internals ----

const insertSQL = `
INSERT INTO events (
	event_key, source, source_id, fingerprint, status,
	category, event, severity, provider_severity, urgency, certainty,
	headline, description, instruction,
	effective_at, expires_at, expires_at_ms, latitude, longitude,
	areas, source_url, received_at, first_seen_at, last_seen_at, last_seen_at_ms, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(event_key) DO NOTHING`

func insertArgs(event core.HazardEvent, fingerprint string, firstSeen, lastSeen, updated time.Time, expiresMs int64) []any {
	received := orNow(event.ReceivedAt, updated)
	areas, _ := json.Marshal(event.Areas)
	return []any{
		event.Key(), event.Source, event.SourceID, fingerprint, string(event.Status),
		event.Category, event.Event, event.Severity, event.ProviderSeverity, event.Urgency, event.Certainty,
		event.Headline, event.Description, event.Instruction,
		nullableTime(event.EffectiveAt), nullableTime(event.ExpiresAt), nullableInt64(expiresMs),
		nullableFloat(event.Latitude), nullableFloat(event.Longitude),
		string(areas), event.SourceURL,
		formatTime(received), formatTime(firstSeen), formatTime(lastSeen), lastSeen.UTC().UnixMilli(), formatTime(updated),
	}
}

// expiryMs returns the event's expiry as epoch milliseconds, or 0 when the
// event never expires.
func expiryMs(event core.HazardEvent) int64 {
	if event.ExpiresAt == nil {
		return 0
	}
	return event.ExpiresAt.UTC().UnixMilli()
}

// expiryLive reports whether the provider made the event live again: the
// expiry is absent or in the future relative to nowMs. A lapsed expiry by
// itself is NOT a reactivation signal — that would let a stale provider
// event create updated/expired flapping on every poll.
func expiryLive(event core.HazardEvent, nowMs int64) bool {
	if event.ExpiresAt == nil {
		return true
	}
	return event.ExpiresAt.UTC().UnixMilli() > nowMs
}

const updateSQL = `
UPDATE events SET
	source = ?, source_id = ?, fingerprint = ?, status = ?,
	category = ?, event = ?, severity = ?, provider_severity = ?, urgency = ?, certainty = ?,
	headline = ?, description = ?, instruction = ?,
	effective_at = ?, expires_at = ?, expires_at_ms = ?, latitude = ?, longitude = ?,
	areas = ?, source_url = ?, received_at = ?, last_seen_at = ?, last_seen_at_ms = ?, updated_at = ?
	WHERE event_key = ?`

func updateArgs(event core.HazardEvent, fingerprint string, now time.Time, expiresMs int64) []any {
	received := orNow(event.ReceivedAt, now)
	areas, _ := json.Marshal(event.Areas)
	return append([]any{
		event.Source, event.SourceID, fingerprint, string(event.Status),
		event.Category, event.Event, event.Severity, event.ProviderSeverity, event.Urgency, event.Certainty,
		event.Headline, event.Description, event.Instruction,
		nullableTime(event.EffectiveAt), nullableTime(event.ExpiresAt), nullableInt64(expiresMs),
		nullableFloat(event.Latitude), nullableFloat(event.Longitude),
		string(areas), event.SourceURL,
		formatTime(received), formatTime(now), now.UTC().UnixMilli(), formatTime(orNow(event.UpdatedAt, now)),
	}, event.Key())
}

// insertChange writes one journal record (with its immutable event
// snapshot and the publisher identity) and returns its stable ID.
func insertChange(tx *sql.Tx, ctx context.Context, changeType core.ChangeType, key string, nowMs int64, snapshot storage.EventSnapshot, publisher string) (*storage.Change, error) {
	data, err := json.Marshal(snapshot)
	if err != nil {
		return nil, fmt.Errorf("marshal snapshot for %q: %w", key, err)
	}
	res, err := tx.ExecContext(ctx,
		"INSERT INTO changes (change_type, event_key, event_snapshot, created_at_ms) VALUES (?, ?, ?, ?)",
		string(changeType), key, string(data), nowMs)
	if err != nil {
		return nil, fmt.Errorf("journal change for %q: %w", key, err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return nil, fmt.Errorf("journal change id for %q: %w", key, err)
	}
	return &storage.Change{ID: id, ChangeType: changeType, Publisher: publisher}, nil
}

// insertInboxChange persists the canonical dispatch transition of one
// journal change into the durable inbox INSIDE the caller's transaction:
// the journal record and its local acceptance commit atomically, so a
// crash between them can never strand a journaled alert without a
// notification path (the inbox row is recovered after a restart). The
// receiver identity is "local" — the same the live dispatch uses for
// journal changes.
func insertInboxChange(tx *sql.Tx, ctx context.Context, change *storage.Change, event core.HazardEvent, nowMs int64) error {
	de := dispatch.EventForJournalChange(change.ChangeType, change.ID, change.Publisher, event, "local", time.UnixMilli(nowMs).UTC())
	data, err := json.Marshal(de)
	if err != nil {
		return fmt.Errorf("encode inbox change %d: %w", change.ID, err)
	}
	id, err := insertInboxRow(tx, ctx, data, "local", nowMs)
	if err != nil {
		return fmt.Errorf("inbox row for change %d: %w", change.ID, err)
	}
	change.InboxID = id
	return nil
}

// execer is the shared surface of *sql.DB and *sql.Tx used to insert
// inbox rows (AppendEvent outside a transaction, insertInboxChange
// inside one).
type execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func insertInboxRow(e execer, ctx context.Context, data []byte, receiver string, nowMs int64) (int64, error) {
	res, err := e.ExecContext(ctx,
		`INSERT INTO dispatch_inbox (event_json, received_at_ms, receiver) VALUES (?, ?, ?)`,
		string(data), nowMs, receiver)
	if err != nil {
		return 0, fmt.Errorf("append inbox event: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("inbox event id: %w", err)
	}
	return id, nil
}

// AppendOutbox persists one accepted HTTP-ingest request as a durable
// broker-publication record. The row is deleted only after the broker
// confirmed the publish (AckOutbox), so an outage between acceptance and
// publication never loses the cross-instance sync. Pending rows of the
// SAME EVENT (publisher + event key — never the topic) are MERGED into
// the newest state: the broker only needs the final document per event,
// while independent alarms sharing one topic all survive. The append
// runs in one transaction together with the capacity enforcement, so a
// full outbox either evicts only evictable rows or fails with
// storage.ErrOutboxFull — corrective sync is never silently dropped.
func (s *Store) AppendOutbox(ctx context.Context, instanceID, topic string, payload []byte) (int64, error) {
	status, expiresMs := outboxLifecycle(payload)
	eventKey, publisher := outboxIdentity(payload)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin outbox append: %w", err)
	}
	defer tx.Rollback()
	if err := mergeOutboxRows(ctx, tx, instanceID, eventKey, publisher); err != nil {
		return 0, fmt.Errorf("merge outbox rows for %q: %w", instanceID, err)
	}
	res, err := tx.ExecContext(ctx, `
		INSERT INTO ingest_outbox (instance_id, topic, payload, created_at_ms, status, expires_at_ms, event_key, publisher)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		instanceID, topic, string(payload), s.now().UnixMilli(), status, expiresMs, eventKey, publisher)
	if err != nil {
		return 0, fmt.Errorf("append outbox row for %q: %w", instanceID, err)
	}
	if err := enforceOutboxCap(ctx, tx); err != nil {
		return 0, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("outbox row id for %q: %w", instanceID, err)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit outbox append for %q: %w", instanceID, err)
	}
	return id, nil
}

// outboxHazard is the minimal wire decode of an outbox payload: the
// lifecycle status and expiry decide how the row may be pruned.
// Unparseable payloads get an empty status and are only ever removed by
// the capacity bound.
type outboxHazard struct {
	Event struct {
		Status    string  `json:"status"`
		ExpiresAt *string `json:"expires_at"`
	} `json:"event"`
}

// outboxIdentity extracts the event identity of one outbox payload:
// publisher + event key. A payload without a recognizable event key has
// no identity ("", "") — such rows are never merged.
func outboxIdentity(payload []byte) (eventKey, publisher string) {
	var we struct {
		EventKey  string `json:"event_key"`
		Publisher string `json:"publisher"`
	}
	if err := json.Unmarshal(payload, &we); err != nil || we.EventKey == "" {
		return "", ""
	}
	return we.EventKey, we.Publisher
}

// mergeOutboxRows removes, inside the append transaction, the pending
// rows of the SAME EVENT (identity merge): the broker only needs the
// final document per event. Rows without an identity are never merged —
// one MQTT topic carries many independent events, so merging by topic
// removed independent alarms (reported P2).
func mergeOutboxRows(ctx context.Context, tx *sql.Tx, instanceID, eventKey, publisher string) error {
	if eventKey == "" {
		return nil
	}
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM ingest_outbox WHERE instance_id = ? AND event_key = ? AND publisher = ?`,
		instanceID, eventKey, publisher); err != nil {
		return err
	}
	return nil
}

// outboxLifecycle extracts the lifecycle metadata of one outbox payload.
func outboxLifecycle(payload []byte) (status string, expiresMs *int64) {
	var h outboxHazard
	if err := json.Unmarshal(payload, &h); err != nil || h.Event.Status == "" {
		return "", nil
	}
	status = h.Event.Status
	if h.Event.ExpiresAt != nil {
		if t, err := time.Parse(time.RFC3339Nano, *h.Event.ExpiresAt); err == nil {
			ms := t.UnixMilli()
			return status, &ms
		}
	}
	return status, nil
}

// PendingOutbox returns the oldest unpublished outbox rows of one
// ingest instance, in insertion order (at-least-once delivery).
func (s *Store) PendingOutbox(ctx context.Context, instanceID string, limit int) ([]storage.OutboxItem, error) {
	if limit <= 0 || limit > 256 {
		limit = 256
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, topic, payload FROM ingest_outbox
		WHERE instance_id = ?
		ORDER BY id
		LIMIT ?`, instanceID, limit)
	if err != nil {
		return nil, fmt.Errorf("pending outbox for %q: %w", instanceID, err)
	}
	defer rows.Close()
	var out []storage.OutboxItem
	for rows.Next() {
		var it storage.OutboxItem
		var payload string
		if err := rows.Scan(&it.ID, &it.Topic, &payload); err != nil {
			return nil, fmt.Errorf("scan outbox row: %w", err)
		}
		it.Payload = []byte(payload)
		out = append(out, it)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate outbox for %q: %w", instanceID, err)
	}
	return out, nil
}

// AckOutbox deletes one outbox row after the broker confirmed its
// publication.
func (s *Store) AckOutbox(ctx context.Context, id int64) error {
	if _, err := s.db.ExecContext(ctx, `DELETE FROM ingest_outbox WHERE id = ?`, id); err != nil {
		return fmt.Errorf("ack outbox row %d: %w", id, err)
	}
	return nil
}

// OutboxCount reports the current durable HTTP-ingest outbox backlog
// (rows awaiting broker publication), feeding the /metrics gauge and the
// operator's visibility of a degraded broker sync.
func (s *Store) OutboxCount(ctx context.Context) (int, error) {
	var n int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM ingest_outbox`).Scan(&n); err != nil {
		return 0, fmt.Errorf("count outbox rows: %w", err)
	}
	return n, nil
}

// PruneOutbox bounds the durable outbox WITHOUT losing corrective sync:
//   - AGE: only STALE ACTIVE rows (their expiry already passed) age out.
//     Cancellations, expirations and still-valid alarms are current or
//     corrective sync — the broker may still hold an old active document
//     they must retire — and never age out.
//   - CAPACITY: beyond outboxRowCap the oldest EVICTABLE rows (empty or
//     active status) go — a hard bound for 24/7/365 operation.
//     Corrective rows (cancelled/expired) are NEVER capacity-evicted:
//     dropping them would leave a stale active document on the broker
//     forever. When corrective rows alone exceed the cap, appends fail
//     with storage.ErrOutboxFull (explicit backpressure) instead.
func (s *Store) PruneOutbox(ctx context.Context, olderThan time.Time) (int64, error) {
	cutoff := olderThan.UnixMilli()
	nowMs := s.now().UnixMilli()
	res, err := s.db.ExecContext(ctx, `
		DELETE FROM ingest_outbox
		WHERE created_at_ms < ?
		  AND status = 'active'
		  AND expires_at_ms IS NOT NULL
		  AND expires_at_ms <= ?`, cutoff, nowMs)
	if err != nil {
		return 0, fmt.Errorf("prune stale outbox rows: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("prune stale outbox rows count: %w", err)
	}
	res2, err := s.db.ExecContext(ctx, `
		DELETE FROM ingest_outbox WHERE id IN (
			SELECT id FROM ingest_outbox
			WHERE status IN ('', 'active')
			ORDER BY id DESC LIMIT -1 OFFSET ?
		)`, outboxRowCap)
	if err != nil {
		return 0, fmt.Errorf("prune outbox over capacity: %w", err)
	}
	n2, err := res2.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("prune outbox over capacity count: %w", err)
	}
	return n + n2, nil
}

// outboxRowCap hard-bounds the EVICTABLE durable outbox rows: beyond it
// the oldest empty/active rows are dropped (the append-time merge keeps
// the newest state per topic). Corrective rows (cancelled/expired) sit
// outside the bound and are never evicted.
const outboxRowCap = 10000

// enforceOutboxCap runs inside the append transaction: it evicts the
// oldest EVICTABLE rows beyond outboxRowCap and then verifies that the
// corrective backlog alone does not exceed the bound — in that case
// there is nothing left to evict and storage.ErrOutboxFull is the
// explicit backpressure.
func enforceOutboxCap(ctx context.Context, tx *sql.Tx) error {
	if _, err := tx.ExecContext(ctx, `
		DELETE FROM ingest_outbox WHERE id IN (
			SELECT id FROM ingest_outbox
			WHERE status IN ('', 'active')
			ORDER BY id DESC LIMIT -1 OFFSET ?
		)`, outboxRowCap); err != nil {
		return fmt.Errorf("evict outbox overflow: %w", err)
	}
	var corrective int
	if err := tx.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM ingest_outbox WHERE status NOT IN ('', 'active')`).Scan(&corrective); err != nil {
		return fmt.Errorf("count corrective outbox rows: %w", err)
	}
	if corrective > outboxRowCap {
		return storage.ErrOutboxFull
	}
	return nil
}

// SavePendingDeletes atomically replaces the durable unresolved-deletion
// set of one output with the given snapshot: the plugin's in-memory map
// is authoritative and the rows are its mirror. It runs in one
// transaction so a reader never observes a half-replaced set.
func (s *Store) SavePendingDeletes(ctx context.Context, outputID string, deletes []storage.PendingDelete) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin pending-delete save for %q: %w", outputID, err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM output_pending_deletes WHERE output_id = ?`, outputID); err != nil {
		return fmt.Errorf("clear pending deletes for %q: %w", outputID, err)
	}
	for _, d := range deletes {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO output_pending_deletes (output_id, event_key, topic)
			VALUES (?, ?, ?)`, outputID, d.Key, d.Topic); err != nil {
			return fmt.Errorf("save pending delete %q for %q: %w", d.Key, outputID, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit pending-delete save for %q: %w", outputID, err)
	}
	return nil
}

// LoadPendingDeletes returns the persisted unresolved deletions of one
// output in stable key order.
func (s *Store) LoadPendingDeletes(ctx context.Context, outputID string) ([]storage.PendingDelete, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT event_key, topic FROM output_pending_deletes
		WHERE output_id = ?
		ORDER BY event_key`, outputID)
	if err != nil {
		return nil, fmt.Errorf("load pending deletes for %q: %w", outputID, err)
	}
	defer rows.Close()
	var out []storage.PendingDelete
	for rows.Next() {
		var d storage.PendingDelete
		if err := rows.Scan(&d.Key, &d.Topic); err != nil {
			return nil, fmt.Errorf("scan pending delete for %q: %w", outputID, err)
		}
		out = append(out, d)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate pending deletes for %q: %w", outputID, err)
	}
	return out, nil
}

func (s *Store) cursor(ctx context.Context, outputID string) (int64, error) {
	var cursor int64
	err := s.db.QueryRowContext(ctx,
		"SELECT last_acked_id FROM output_cursors WHERE output_id = ?", outputID).Scan(&cursor)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read cursor for %q: %w", outputID, err)
	}
	if cursor < 0 {
		// A negative cursor is never legitimate. Clamp to 0: replay-from-
		// the-beginning is the safe at-least-once behavior.
		return 0, nil
	}
	return cursor, nil
}

// ListActiveEvents implements storage.ActiveEventLister: it pages through
// the CURRENT active events (status = active) in stable event_key order.
// This reads the authoritative current-state table directly — the
// historical change journal is never replayed to reconstruct active state.
func (s *Store) ListActiveEvents(ctx context.Context, afterKey string, limit int) ([]core.HazardEvent, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("limit must be positive, got %d", limit)
	}
	rows, err := s.db.QueryContext(ctx,
		"SELECT "+eventColumns+" FROM events WHERE status = ? AND event_key > ? ORDER BY event_key LIMIT ?",
		string(core.StatusActive), afterKey, limit)
	if err != nil {
		return nil, fmt.Errorf("query active events: %w", err)
	}
	defer rows.Close()

	var out []core.HazardEvent
	for rows.Next() {
		event, err := scanEventRow(rows.Scan, nil)
		if err != nil {
			return nil, fmt.Errorf("scan active event: %w", err)
		}
		out = append(out, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate active events: %w", err)
	}
	return out, nil
}

// ListArchiveEvents implements the public archive query: every
// current-state event last seen on or after since, newest first, as one
// bounded page (offset/limit). ACTIVE events are excluded — they already
// live in the public Active hazards section, so the archive spans only
// ended states (expired/cancelled) with their final state.
func (s *Store) ListArchiveEvents(ctx context.Context, since time.Time, offset, limit int) ([]storage.StoredEvent, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("limit must be positive, got %d", limit)
	}
	rows, err := s.db.QueryContext(ctx,
		"SELECT "+eventColumns+" FROM events WHERE last_seen_at_ms >= ? AND status != ? ORDER BY last_seen_at_ms DESC, event_key ASC LIMIT ? OFFSET ?",
		since.UnixMilli(), string(core.StatusActive), limit, offset)
	if err != nil {
		return nil, fmt.Errorf("query archive events: %w", err)
	}
	defer rows.Close()

	var out []storage.StoredEvent
	for rows.Next() {
		var stored storage.StoredEvent
		event, err := scanEventRow(rows.Scan, &stored)
		if err != nil {
			return nil, fmt.Errorf("scan archive event: %w", err)
		}
		stored.Event = event
		out = append(out, stored)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate archive events: %w", err)
	}
	return out, nil
}

// CountArchiveEvents reports how many ENDED current-state events were
// last seen on or after since (the archive total used for pagination;
// active events are excluded — see ListArchiveEvents).
func (s *Store) CountArchiveEvents(ctx context.Context, since time.Time) (int, error) {
	var n int
	err := s.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM events WHERE last_seen_at_ms >= ? AND status != ?",
		since.UnixMilli(), string(core.StatusActive)).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("count archive events: %w", err)
	}
	return n, nil
}

// loadEventTx reads a full event row from tx by key.
func loadEventTx(tx *sql.Tx, key string) (core.HazardEvent, error) {
	row := tx.QueryRow("SELECT "+eventColumns+" FROM events WHERE event_key = ?", key)
	return scanEventRow(row.Scan, nil)
}

// v3EventColumns is the events column list exactly as it exists while
// migration v3 runs (schema v2 + v3's own changes-column addition).
// It must NEVER be extended with columns added by later migrations: the
// v3 snapshot backfill executes before v4 creates last_seen_at_ms.
const v3EventColumns = `event_key, source, source_id, fingerprint, status,
category, event, severity, urgency, certainty,
headline, description, instruction,
effective_at, expires_at_ms, latitude, longitude,
areas, source_url, received_at, first_seen_at, last_seen_at, updated_at`

// loadEventForV3Migration reads an event using only the schema available
// to migration v3. It deliberately does not share SQL or scanning with the
// current-schema loaders: migration code is immutable history and must
// stay compatible with real v2 databases forever.
func loadEventForV3Migration(tx *sql.Tx, key string) (core.HazardEvent, error) {
	row := tx.QueryRow("SELECT "+v3EventColumns+" FROM events WHERE event_key = ?", key)
	return scanV3MigrationEvent(row.Scan)
}

// scanV3MigrationEvent scans the frozen v3EventColumns list in order. It
// is a self-contained copy of the current-schema scan minus
// last_seen_at_ms, and must not be refactored to depend on current schema.
func scanV3MigrationEvent(scan func(dest ...any) error) (core.HazardEvent, error) {
	var (
		key, fingerprint, status string
		effectiveAt              sql.NullString
		expiresMs                sql.NullInt64
		lat, lon                 sql.NullFloat64
		areasJSON                string
		received, first, last    string
		updated                  string
	)
	var event core.HazardEvent
	dests := []any{
		&key, &event.Source, &event.SourceID, &fingerprint, &status,
		&event.Category, &event.Event, &event.Severity, &event.Urgency, &event.Certainty,
		&event.Headline, &event.Description, &event.Instruction,
		&effectiveAt, &expiresMs, &lat, &lon,
		&areasJSON, &event.SourceURL,
		&received, &first, &last, &updated,
	}
	if err := scan(dests...); err != nil {
		return event, err
	}

	event.Status = core.EventStatus(status)
	if effectiveAt.Valid {
		t, err := time.Parse(time.RFC3339Nano, effectiveAt.String)
		if err != nil {
			return event, fmt.Errorf("parse effective_at: %w", err)
		}
		event.EffectiveAt = &t
	}
	if expiresMs.Valid {
		t := time.UnixMilli(expiresMs.Int64).UTC()
		event.ExpiresAt = &t
	}
	if lat.Valid {
		event.Latitude = &lat.Float64
	}
	if lon.Valid {
		event.Longitude = &lon.Float64
	}
	if err := json.Unmarshal([]byte(areasJSON), &event.Areas); err != nil {
		return event, fmt.Errorf("decode areas: %w", err)
	}
	for _, t := range []struct {
		dest *time.Time
		src  string
	}{
		{&event.ReceivedAt, received},
		{&event.UpdatedAt, updated},
	} {
		parsed, err := time.Parse(time.RFC3339Nano, t.src)
		if err != nil {
			return event, fmt.Errorf("parse stored timestamp: %w", err)
		}
		*t.dest = parsed
	}
	return event, nil
}

// decodeSnapshot reconstructs the event state captured when a journal
// change was written.
func decodeSnapshot(data string) (core.HazardEvent, error) {
	var snap storage.EventSnapshot
	if err := json.Unmarshal([]byte(data), &snap); err != nil {
		return core.HazardEvent{}, fmt.Errorf("parse event snapshot: %w", err)
	}
	return snap.ToEvent(), nil
}

// scanEventRow scans the canonical event column list (eventColumns) in
// order. When stored is non-nil it also receives the persistence metadata.
func scanEventRow(scan func(dest ...any) error, stored *storage.StoredEvent) (core.HazardEvent, error) {
	var (
		key, fingerprint, status string
		effectiveAt              sql.NullString
		expiresMs                sql.NullInt64
		lat, lon                 sql.NullFloat64
		areasJSON                string
		received, first, last    string
		lastSeenMs               sql.NullInt64
		updated                  string
	)
	var event core.HazardEvent
	dests := []any{
		&key, &event.Source, &event.SourceID, &fingerprint, &status,
		&event.Category, &event.Event, &event.Severity, &event.ProviderSeverity, &event.Urgency, &event.Certainty,
		&event.Headline, &event.Description, &event.Instruction,
		&effectiveAt, &expiresMs, &lat, &lon,
		&areasJSON, &event.SourceURL,
		&received, &first, &last, &lastSeenMs, &updated,
	}
	if err := scan(dests...); err != nil {
		return event, err
	}

	event.Status = core.EventStatus(status)
	if effectiveAt.Valid {
		t, err := time.Parse(time.RFC3339Nano, effectiveAt.String)
		if err != nil {
			return event, fmt.Errorf("parse effective_at: %w", err)
		}
		event.EffectiveAt = &t
	}
	if expiresMs.Valid {
		t := time.UnixMilli(expiresMs.Int64).UTC()
		event.ExpiresAt = &t
	}
	if lat.Valid {
		event.Latitude = &lat.Float64
	}
	if lon.Valid {
		event.Longitude = &lon.Float64
	}
	if err := json.Unmarshal([]byte(areasJSON), &event.Areas); err != nil {
		return event, fmt.Errorf("decode areas: %w", err)
	}
	storedTimes := []struct {
		dest *time.Time
		src  string
	}{
		{&event.ReceivedAt, received},
		{&event.UpdatedAt, updated},
	}
	for _, t := range storedTimes {
		parsed, err := time.Parse(time.RFC3339Nano, t.src)
		if err != nil {
			return event, fmt.Errorf("parse stored timestamp: %w", err)
		}
		*t.dest = parsed
	}

	if stored != nil {
		stored.Fingerprint = fingerprint
		for _, t := range []struct {
			dest *time.Time
			src  string
		}{
			{&stored.FirstSeenAt, first},
			{&stored.LastSeenAt, last},
		} {
			parsed, err := time.Parse(time.RFC3339Nano, t.src)
			if err != nil {
				return event, fmt.Errorf("parse stored timestamp: %w", err)
			}
			*t.dest = parsed
		}
	}
	return event, nil
}

func nullableTime(t *time.Time) any {
	if t == nil {
		return nil
	}
	return formatTime(*t)
}

func nullableFloat(f *float64) any {
	if f == nil {
		return nil
	}
	return *f
}

func nullableInt64(ms int64) any {
	if ms == 0 {
		return nil
	}
	return ms
}

// formatTime renders timestamps in UTC RFC3339Nano for human-readable
// columns; SQL comparisons use the integer millisecond columns instead.
func formatTime(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}

// orNow returns t when it is set, otherwise fallback.
func orNow(t, fallback time.Time) time.Time {
	if t.IsZero() {
		return fallback
	}
	return t
}
