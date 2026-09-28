# Solana Sentinel

On-chain security monitoring and alerting for Jupiter Programs and services

Ingests account/transaction updates via Yellowstone gRPC, evaluates them against
typed per-program rulesets, and dispatches alerts to humans and storage.

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
- **`Router`** — `HashMap<RuleId, Vec<SinkId>>` fan-out from rules to sinks.
- **`AlertingService`** — dedups (cooldown + fleet gates), batches and forwards alerts to sinks.

See `CONTRIBUTING.md` for adding a rule or a program, and `docs/rfc-adapters-and-wiring.md`
for the design.

## Running

```sh
cp config.example.toml config.toml
make check-rules       # validate rules.toml + config.toml, print resolved subscriptions; no network
make run
```

Local runs need only an RPC URL and a Yellowstone endpoint: use the `stdout` sink and omit
`[redis]` and `[observability.betterstack]`.

## Monitored programs

- Squads V4 multisig
- Pyth oracle feeds
- SPL token mints
- BPF Loader Upgradeable
- JupUSD
- JupLend
- Sentinel — not an on-chain program; watches multisig member wallets and Anchor IDL admin calls
- System — not an on-chain program; service health (gRPC slot lag vs RPC)

## Configuration

Split by sensitivity:

| File | Contents | Source |
|---|---|---|
| `config.toml` | secrets, connection strings | your secret store (gitignored) |
| `rules.toml` | rule definitions, thresholds | version-controlled |

`rules.toml` ships as a trimmed example; point `RULES_FILE` at your own file per environment.

## Storage & observability

- **ClickHouse** — alert archival via `klickhouse` (native TCP, port 9440, TLS); `refinery` migrations.
- **Slack** — human notification sink.
- **Betterstack** — incident reporting, resolution and tracking
- **Prometheus / Grafana** — metrics and dashboards.
