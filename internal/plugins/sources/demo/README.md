# Demo source plugin

Development-only synthetic hazard source. It emits one synthetic
`HazardEvent` per configured interval and exists so the event pipeline has
something to show during development and smoke tests.

**Do not use it in production.** It does not contact any real provider.

## Configuration

```yaml
sources:
  - id: demo
    type: demo
    enabled: true
    runtime:
      restart: true
      shutdown_timeout: 10s
    config:
      interval: 30s
```

`interval` defaults to 30 s when omitted.
