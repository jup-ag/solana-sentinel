# Solana Sentinel

A framework for on-chain security monitoring and alerting on Solana.

Ingests account / transaction updates (Yellowstone gRPC, websocket, Kafka, or your own source),
evaluates them against typed per-program rules, and dispatches alerts to humans and storage.
It is a library: a deployment workspace depends on it, adds its own program crates, and
calls `solana_sentinel_service::run` from a tiny binary.

## Architecture

```
Yellowstone gRPC / websocket / Kafka (janus) / any shipstern source ──▶ stream pipelines (parser + rules) ──┐
RPC getMultipleAccounts ─────────────────────────▶ poll loops (source + rules) ─────────┼──▶ Router ──▶ AlertingService ──▶ Sinks
                                                                                         ┘        (cooldown gate + fleet gate)
```

- **`ProgramSpec`** — what a program crate exports: a parser per feed and the rules registered on it.
- **`StreamRule` / `PollRule`** — one rule, one file, one trait; typed config, subscription, `evaluate`.
- **`ProgramDeps`** — a program's shared cache, bootstrapped before any rule; owns chain-discovered watch sets.
- **Data sources** — shipstern `SourceTrait` impls (Yellowstone gRPC, PubSub websocket, janus Kafka topics, or your own); rules never see the transport.
- **`solana_sentinel_service::run`** — the whole service behind one call; a deployment binary passes its name and program list.
- **`Router`** — `HashMap<RuleId, Vec<SinkId>>` fan-out from rules to sinks.
- **`AlertingService`** — dedups (cooldown + fleet gates, each optional via `[alerting]`), batches and forwards alerts to sinks.

See `CONTRIBUTING.md` for adding a rule or a program, and `docs/rfc-adapters-and-wiring.md`
for the design.

## Using it

Start from `examples/spl-token-monitor/`, a complete deployment with an instruction, an
account and a poll rule. `CONTRIBUTING.md` shows the deployment binary, how to write a program and a rule,
and the data sources. `config.example.toml` documents every service setting;
`<binary> check` validates a deployment's `rules.toml` without connecting anywhere.

## Configuration

Split by sensitivity:

| File | Contents | Source |
|---|---|---|
| `config.toml` | secrets, connection strings | your secret store (gitignored) |
| `rules.toml` | rule definitions, thresholds | version-controlled in the deployment |

Both live in the deployment workspace; `CONFIG_FILE` / `RULES_FILE` override the default paths.

## Storage & observability

- **ClickHouse** — alert archival via `klickhouse` (native TCP, port 9440, TLS); `refinery` migrations.
- **Slack** — human notification sink.
- **Betterstack** — incident reporting, resolution and tracking
- **Prometheus / Grafana** — metrics and dashboards.
