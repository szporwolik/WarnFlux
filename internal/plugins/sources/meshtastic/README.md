# meshtastic — the LoRa mesh integration

This package is the *source plugin* around the process-wide Meshtastic hub.
The hub is the real integration: it owns the Companion session (USB serial or
the node's WiFi API), tracks the mesh, routes messages and drives the
station's mesh-side behaviour. The plugin's own job is narrow — it runs the
hub and publishes the node directory as an informational document every 30
seconds:

- kind: `mesh-nodes` (retained MQTT information document)
- payload: the hub snapshot — `Connected`, the device's own identity
  (`Self`), the device channel names (`Channels`), `NodeTTL` and the full
  node directory (`Nodes`: id, names, position, distance/bearing from us,
  battery, telemetry, last seen). Consumers are the map layers and the admin
  mesh page.

## No plugin configuration

This source has **no `config:` keys**: everything lives in the top-level
`meshtastic:` block of `config.yaml` (plus the Config page toggles), because
the hub is shared with the actions, the message history and the web panel. An
instance is therefore just:

```yaml
sources:
  - id: meshtastic-main
    type: meshtastic
    enabled: true
    runtime:
      restart: true
      shutdown_timeout: 10s
```

It requires `meshtastic.enabled: true`; without a hub the plugin refuses to
start (`meshtastic: hub is not configured`) instead of running silently.

Keep it to ONE instance: the hub is process-wide and the plugin attaches to
it, so a second instance would drive the same session twice.

## What the hub does (configured in `meshtastic:`)

- **Connection**: `device` (a comma-separated list of serial ports, so a node
  that re-enumerates after a USB reset is found anyway) or `transport: tcp`
  with `host` for the node's WiFi API.
- **Node tracking**: neighbours are kept for `node_ttl` (30m default,
  1m..24h, editable on the Config page); an unheard neighbour drops off the
  map and the station list with it.
- **Messages**: direct messages from node IDs registered in the user
  directory become canonical alert documents with `route_messages: true`,
  exactly like APRS messages (the alarm pipeline, `/messages`, the archive).
- **Broadcasts**: hazard announcements on the group channel and as direct
  messages to users' registered node IDs (`channel_alerts`, `dm_alerts`),
  APRS station announcements inside the operational range, the presence
  beacon and the mesh status/EMCOM layers — the cadence and the switches are
  on the Config page.
- **Probes**: on-demand traceroute and range probes from the admin panel.

## Direct messages need the peer's key (PKI)

Group announcements are channel BROADCASTS: they are encrypted with the
channel key, so every node on the channel receives them and nothing else is
needed. A DIRECT message is different: firmware 2.5+ encrypts it with the
RECIPIENT's public key, which the radio can only have learned from that node's
**NodeInfo**. When the key is missing the radio refuses the frame outright —
it never falls back to channel encryption, because a direct message must not
be readable by the whole channel — and the message history records the reason
`pki_unknown_key` ("the recipient's key is unknown"; `pki_no_key` means the
peer announced a key the radio cannot use). The frame never reaches the air:
the alert was on the group channel, only the DM is missing.

The station does what it can about it: on such a refusal the hub asks that
peer to re-announce its NodeInfo (`NODEINFO_APP` with `want_response`, so the
answer comes straight back to us), at most once per peer per 6 hours — the
peer's firmware throttles replies to the same requester for hours, so asking
more often only spends airtime. **The refused message is then re-sent by
itself**: when that key lands (within five minutes of the refusal) the hub
transmits the same text again and the SAME history row flips to sent or
delivered — the operator does not have to retype it. A node that has never
announced (fresh node, a factory reset or a reflash that changed its key)
stays undeliverable by DM until it does, and the message stays failed with the
refusal reason attached.

The wait is kept per PEER and holds every message that peer owes an answer to
(the operator may have typed two texts before the key landed): they go out one
after another when the key arrives, each re-sent exactly once, each with its
own five-minute deadline. Two bounds keep that memory finite — four messages
per peer and thirty-two in the whole hub. Past them the message is not
remembered, and the hub says so out loud (`meshtastic: too many messages wait
for a peer key, this one stays failed`): that text keeps its failed row and
its reason, so it is visible instead of silently lost. The same recovery runs
whichever way the refusal arrives — the device's `queueStatus` answer or a
`ROUTING_APP` frame carrying the error.

**Alert messages are the exception**: a group alert is a durable action job
(`ProgressRef`), and recording its failure already re-arms that job — the
action worker re-sends it by itself. Such a message is therefore NOT put on
the key queue: the station still refreshes the peer's key (that is what
unblocks every later send), but the retry stays with the worker, so the alert
goes out twice at most (the refused copy and the worker's retry) instead of
three times.

The heard-nodes table has a two-way filter next to its heading — every node,
or `★ Pinned only` for the nodes the station keeps on the radio. It is a view
switch: nothing is hidden from the radio or the station feed by it.

**The panel shows the key state.** In the heard-nodes table every row carries
a mark next to the name: 🔑 when the station has seen that node announce a
public key (the radio can encrypt a DM to it) and ⚠ when it has not — that
node's direct messages will be refused until its firmware announces one. The
state comes from the radio's own node table at the handshake and from every
NodeInfo heard afterwards, so it follows the mesh without a refresh step.

That bookkeeping is bounded, because it describes nodes the station holds:
a key mark is dropped together with its row when the node directory is pruned,
and the NodeInfo refresh entries expire once the six-hour gap has passed (they
rate-limit nothing after it). A hard cap of 4096 marks protects a long
connection: past it the hub first drops marks for nodes it no longer holds and
then stops remembering new ones, saying so in the log
(`meshtastic: peer key marks are at the cap, new ones are not remembered`) —
never growing in silence.

Two more reasons come from the STATION, not the device, and name a failure the
radio itself never reported: `no_response` — the device transmitted the frame
and the recipient never acknowledged it (the acknowledgment window,
`meshtastic.ack_settle_timeout`, ran out) — and `not_confirmed` — the radio
never confirmed the transmission at all, so the frame may never have left it.
Neither leaves a failed row without an explanation.

What an operator can do: bring the node on the air and let it announce —
every device broadcasts its NodeInfo at boot and then every
`node_info_broadcast_secs` (default about 3 hours, configurable in the phone
app). A restart is the quickest way to force one. Verify with a message from
the admin panel (Meshtastic → send to that node id); a DM that still fails
belongs to a node whose key changed — it must announce again.

**The station pins them.** Because the key comes from the node and can be
evicted from a full node table, WarnFlux marks every node id registered in the
user directory as a FAVORITE on the radio (`meshtastic.pin_user_nodes`, ON by
default): the firmware never evicts a favorite and keeps favorites across a
node-db reset, so the station's own people stay messageable without waiting
for their next announcement. The pass runs right after the handshake and then
every `pin_user_nodes_interval` (default 6h). It reconciles both ways: a node
that leaves the user directory gets its star taken back — the radio keeps
favorites forever, so without that the node table would slowly fill with pins
for people the station stopped serving. Only stars the station set itself are
ever removed; a star tapped on the device by hand stays. The list of its own
pins survives a restart (it is a small table in the database), and a pass that
could not read the directory or could not reach the radio changes nothing and
is retried within minutes.

The list is ownership, not the radio's state. On every handshake the station
reads the favorites the device reports in its own node table and re-sets any of
its stars the device no longer has — that is how a node-db reset (or a
replacement radio, which starts with an empty table) repairs itself at the next
session. A radio that still reports the stars costs no admin traffic at all.

The same applies to every DM path: routed group members' node ids, the
`/alert` direct message and the mass-info DMs. Watching the reason column of
the message history (and the log lines `meshtastic: device rejected a send`
and `meshtastic: requested node info to refresh the peer key`) tells the two
apart: a missing KEY is fixed on the peer, a missing ROUTE is a radio
coverage problem.

## Retained documents of quiet nodes

Every node heard over the radio gets one retained MQTT document under
`<topic_prefix>/meshtastic/stations/<id>`. A node that stops transmitting
keeps its last document forever unless something removes it, so the hub queues
a removal once a node has been quiet for `meshtastic.node_ttl` and publishes an
empty retained payload — only then does the broker forget it.

Two properties of that work are worth knowing, because the panel shows them:

- **A confirmed removal is a fact about an ACTIVITY VERSION.** The version (the
  node's last-heard observation at the time) is stored with the node, so a
  restart never queues that removal again, and a node heard afterwards is a new
  version that owes a fresh removal. Without this the station re-published the
  removals of documents that were long gone on every restart, and the overflow
  counter grew each time for no reason.
- **The ledger is bounded, and what does not fit WAITS.** The station keeps at
  most `maxPendingStationRemovals` (512) removals in flight; further quiet nodes
  stay due and are offered again as the broker confirms what is ahead of them.
  Nothing is lost while the node stays in the directory (30 days), and the
  panel's muted badge (`MQTT removals waiting: N`) is that backlog — not a
  failure. The red badge (`MQTT removals: N`) is the genuine overflow: removals
  restored from a database whose backlog is longer than the bound, or nodes that
  left the directory while their removal was still waiting for room.

`internal/meshtastic/hub.go` is the authoritative implementation; the

`meshtastic:` section of `config.example.yaml` documents every key. The mesh
session is local-only (serial or a LAN TCP node), so the offline-mode switch
never suspends it — see `docs/offline-mode.md`.

## Tests

`meshtastic_test.go` covers this wrapper's whole contract: the hub-required
guard, the published `mesh-nodes` document (source, kind, key, payload) and
the shutdown path. The hub's own behaviour — sessions, node tracking,
messages, broadcasts, probes — is where the substance lives and is covered by
`internal/meshtastic/hub_test.go`.
