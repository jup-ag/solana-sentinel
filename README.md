# Solana Sentinel

A framework for on-chain security monitoring and alerting on Solana.

Ingests account / transaction updates (Yellowstone gRPC, websocket, or your own source),
evaluates them against typed per-program rules, and dispatches alerts to humans and storage.
It is a library: a deployment workspace depends on it, adds its own program crates, and
calls `solana_sentinel_service::run` from a tiny binary.

## Architecture

```
Yellowstone gRPC / websocket / any StreamSource ──▶ stream pipelines (parser + rules) ──┐
RPC getMultipleAccounts ─────────────────────────▶ poll loops (source + rules) ─────────┼──▶ Router ──▶ AlertingService ──▶ Sinks
                                                                                         ┘        (cooldown gate + fleet gate)
```

- **`ProgramSpec`** — what a program crate exports: a parser per feed and the rules registered on it.
- **`RuleDef` + `GrpcRule` / `RpcRule`** — one rule, one file; typed config, static prefilter, `evaluate`.
- **`ProgramDeps`** — a program's shared cache, bootstrapped before any rule; owns chain-discovered watch sets.
- **`StreamSource`** — the transport (shipstern, plain gRPC, PubSub websocket, or your own); rules never see it.
- **`solana_sentinel_service::run`** — the whole service behind one call; a deployment binary passes its name and program list.
- **`Router`** — `HashMap<RuleId, Vec<SinkId>>` fan-out from rules to sinks.
- **`AlertingService`** — dedups (cooldown + fleet gates), batches and forwards alerts to sinks.

See `CONTRIBUTING.md` for adding a rule or a program, and `docs/rfc-adapters-and-wiring.md`
for the design.

## Using it

See `CONTRIBUTING.md`: it shows the deployment binary, how to write a program and a rule,
and the ingestion sources. `config.example.toml` documents every service setting;
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
