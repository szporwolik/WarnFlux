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
