# Architecture

```
  config.toml (sources, sinks, redis)   rules/*.toml (rules, thresholds)
                   └─────────────────┬─────────────────┘
                                     ▼
             ┌────────────────── programs ───────────────────┐
             │   stream rules: instruction / account / tx    │
             │   poll rules:   fetch accounts on a timer     │
             └─────┬───────────────────────────────────┬─────┘
    subscriptions  │                                   │ accounts to poll
                   ▼                                   ▼
     ┌─────── data source ───────┐       ┌─────────── RPC ───────────┐
     │ Yellowstone gRPC          │       │ getMultipleAccounts       │
     │ websocket                 │       │ on an interval            │
     │ Kafka                     │       │                           │
     └─────────────┬─────────────┘       └─────────────┬─────────────┘
                   ▼                                   ▼
     ┌──── stream pipelines ─────┐       ┌─────── poll loops ────────┐
     │ parse ─▶ evaluate rules   │       │ parse ─▶ evaluate rules   │
     └─────────────┬─────────────┘       └─────────────┬─────────────┘
                   └─────────────────┬─────────────────┘
                                     │ alerts
                                     ▼
              ┌───────────────── alerting ──────────────────┐
              │                                             │
              │  ╔════════════ COOLDOWN GATE ════════════╗  │
              │  ║                                       ║  │
              │  ║  ┌─────────── local gate ──────────┐  ║  │
              │  ║  │ same alert again within its     │  ║  │
              │  ║  │ cooldown window?  drop it       │  ║  │
              │  ║  │ per node · per rule · sidecar   │  ║  │
              │  ║  └────────────────┬────────────────┘  ║  │
              │  ║                   ▼                   ║  │
              │  ║  ┌─────────── fleet gate ──────────┐  ║  │
              │  ║  │ another node already sent it?   │  ║  │
              │  ║  │ drop it · shared Redis          │  ║  │
              │  ║  └────────────────┬────────────────┘  ║  │
              │  ║                   │                   ║  │
              │  ╚═══════════════════╪═══════════════════╝  │
              │                      ▼                      │
              │                route by rule                │
              └──────────────────────┬──────────────────────┘
                ┌─────────────┬──────┴──────┬─────────────┐
                ▼             ▼             ▼             ▼
             stdout      ClickHouse       Slack      Betterstack

      also running: slot checkpoint · /metrics · /health · heartbeat
```
