# Solana Sentinel

A Rust framework for on-chain security monitoring and alerting on Solana.

- **Ingests** account and transaction updates from Yellowstone gRPC, websocket, Kafka, or your own source.
- **Evaluates** each update against typed, per-program rules.
- **Dispatches** alerts to humans (Slack, Betterstack) and storage (ClickHouse).
- **Is a library**, not a binary: a deployment workspace depends on it, adds its own program crates, and calls `solana_sentinel_service::run` from a tiny `main.rs`.

## Table of contents

- [How it works](#how-it-works)
- [Core concepts](#core-concepts)
- [Quick start](#quick-start)
- [Configuration](#configuration)
- [Data sources](#data-sources)
- [Alerting and sinks](#alerting-and-sinks)
- [Repository layout](#repository-layout)
- [Development](#development)
- [Further reading](#further-reading)

## How it works

```
  Yellowstone gRPC · websocket · Kafka ──▶ stream pipelines (parse ▸ evaluate) ──┐
                                                                                 ├──▶ alerting ──▶ sinks
  RPC getMultipleAccounts ─────────────────────▶ poll loops (parse ▸ evaluate) ──┘
                                                                                      cooldown gate (local ▸ fleet)
                                                                                      then route by rule
```

1. **Programs declare what they watch.** Each program crate exports a `spec()` listing its parsers and the rules registered on them.
2. **Subscriptions are unioned.** The framework merges every rule's subscription into one filter set for the data source.
3. **Updates are parsed and evaluated.** Stream rules run on live updates; poll rules fetch accounts over RPC on a timer.
4. **Alerts are deduplicated.** The cooldown gate drops repeats: a local gate (this node) then a fleet gate (any node).
5. **Alerts are routed.** Each rule names the sinks it goes to in its rules file.

The full diagram lives in [`docs/arch.md`](docs/arch.md).

## Core concepts

- **`ProgramSpec`** — what a program crate exports: one parser per feed and the rules registered on it.
- **`StreamRule`** — a rule fed by live updates (instruction, account, or transaction). One rule, one file, one trait.
- **`PollRule`** — a rule fed by `getMultipleAccounts` on an interval. Rules on the same cadence share one RPC call per tick.
- **`ProgramDeps`** — a program's shared cache, bootstrapped before any rule runs. Owns chain-discovered watch sets (vault lists, scanned PDAs) and can push a changed set to the live subscription without a restart.
- **Data sources** — shipstern `SourceTrait` implementations. Rules never see the transport.
- **`solana_sentinel_service::run`** — the whole service behind one call. A deployment binary passes its name and program list.
- **`Router`** — fan-out from rule IDs to sink IDs.
- **`AlertingService`** — dedups, batches, and forwards alerts to sinks.

## Quick start

The fastest way in is the example deployment at [`examples/spl-token-monitor/`](examples/spl-token-monitor/). It has one program crate with an instruction rule, an account rule, and a poll rule.

1. **Validate the example config** without connecting anywhere:

   ```sh
   CONFIG_FILE=examples/spl-token-monitor/config.toml \
   RULES_FILE=examples/spl-token-monitor/rules \
   cargo run -p spl-token-monitor-example -- check
   ```

2. **Run it for real.** Set a Yellowstone endpoint in `config.toml`, or switch `[data_source]` to `websocket` against a public RPC node, then drop `check`. Alerts print as JSON lines on stdout.

3. **Build your own deployment.** A deployment is a small Cargo workspace with:
   - a binary that calls `run(RunOptions { service_name, programs })`
   - one or more program crates, each exporting a `spec()`
   - a `config.toml` and a `rules.toml` or `rules/` directory

   ```rust
   use solana_sentinel::ProgramSpec;
   use solana_sentinel_service::{run, RunOptions};

   const PROGRAMS: &[fn() -> ProgramSpec] = &[my_program::spec];

   #[tokio::main]
   async fn main() -> anyhow::Result<()> {
       run(RunOptions { service_name: "my_monitor", programs: PROGRAMS }).await
   }
   ```

4. **Add rules and programs.** [`CONTRIBUTING.md`](CONTRIBUTING.md) walks through both step by step.

### Ready-made rules

`crates/rules-registry` ships programs and rules you can run without writing any Rust. A deployment passes its `PROGRAMS` list to `run` and activates rules from its rules files:

```rust
use solana_sentinel_rules_registry::PROGRAMS;
use solana_sentinel_service::{run, RunOptions};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    run(RunOptions { service_name: "my_monitor", programs: PROGRAMS }).await
}
```

| Program | Rule | Fires when |
|---|---|---|
| `system` | `lamports_below` | a watched account's SOL balance is under a floor |
| `system` | `large_sol_transfer` | a SOL transfer out of a watched account is at or above a size |
| `system` | `nonce_activity` | a watched authority initializes, re-authorizes or drains a nonce account |
| `spl_token` | `supply_increase` | a mint's supply grows by at least N basis points |
| `spl_token` | `mint_authority_change` | a mint's mint or freeze authority changes |
| `bpf_loader` | `program_upgrade` | a watched upgradeable program is redeployed |
| `bpf_loader` | `upgrade_authority_change` | a program's upgrade authority changes or is removed |
| `wallet` | `unapproved_program_invoke` | a watched wallet signs a call into a non-allowlisted program |
| `wallet` | `anchor_idl_instruction` | an Anchor IDL admin instruction hits a watched program |
| `health` | `slot_lag` | the stream's slot and the RPC slot drift apart |

[`examples/registry-monitor/`](examples/registry-monitor/) activates every one of them with sample config:

```sh
CONFIG_FILE=examples/registry-monitor/config.toml \
RULES_FILE=examples/registry-monitor/rules \
cargo run -p registry-monitor-example -- check
```

To mix registry programs with your own, concatenate the lists: `PROGRAMS` is a plain `&[fn() -> ProgramSpec]`. Each registry module is also a template to copy into your own program crate.

## Configuration

Config is split by sensitivity into two files that both live in the deployment workspace.

| File | Contents | Where it lives |
|---|---|---|
| `config.toml` | secrets, connection strings, sinks, data source | your secret store (gitignored) |
| `rules.toml` or `rules/*.toml` | rule definitions, thresholds, sink routing | version-controlled in the deployment |

- `CONFIG_FILE` and `RULES_FILE` override the default paths.
- `RULES_FILE` may be a single file or a directory. A directory is read as every `*.toml` in it, in name order, so each project or scope can own its own file. Without an override, `./rules.toml` is used if present, else `./rules/`.
- [`config.example.toml`](config.example.toml) documents every service setting.
- `<binary> check` validates both files and prints the resolved subscriptions without connecting.
- For a local run, use the `stdout` sink, point `redis_sidecar_url` at a local Redis and leave `redis_fleet_url` and `[observability.betterstack]` out. The fleet gate becomes in-memory and no heartbeat is sent.

## Data sources

Pick one with `[data_source] type = ...`. Every source feeds the same dispatcher, so rules are unaffected by the choice.

| `type` | Backed by | Notes |
|---|---|---|
| `yellowstone_grpc` | shipstern's Yellowstone client | production default; server-side filters, reconnect with slot replay and dedup |
| `websocket` | Solana PubSub | for nodes without Geyser; transactions fetched over `[rpc]` on each log notification |
| `kafka` | janus raw topics | `raw.<program>.{accounts,transactions}`; records matched locally |

To add a transport, implement `shipstern::sources::SourceTrait`. See the data sources section of `CONTRIBUTING.md`.

## Alerting and sinks

The cooldown gate runs before an alert reaches a sink. It has two layers, each optional via `[alerting.cooldown_gate]`.

- **Local gate** — drops the same alert again within the rule's cooldown window. Per node, per rule, backed by a sidecar Redis.
- **Fleet gate** — drops an alert another node already sent. Backed by a shared Redis cluster.
- Without `redis_fleet_url`, the fleet gate runs in-memory. Fine for one process, not for a fleet.

Sinks are declared as `[[sinks]]` blocks with a `type` and a unique `id`. The same type can appear more than once, and rules route to any subset by `id`.

- **`stdout`** — JSON lines; enough for a local run.
- **`slack`** — human notification via webhook.
- **`betterstack`** — incident creation, resolution, and tracking.
- **`clickhouse`** — alert archival over native TCP with TLS; migrations via `refinery`.

Metrics are exposed for **Prometheus / Grafana** on `[observability.server] bind_addr`.

## Repository layout

| Path | Purpose |
|---|---|
| `crates/core` | rule traits, `ProgramSpec`, subscriptions, caches, testing helpers |
| `crates/service` | `run()`, config loading, validation, wiring |
| `crates/domain` | shared types: alerts, rules, sinks, programs |
| `crates/alerting` | router, cooldown gate (local + fleet), sink implementations |
| `crates/data-source` | websocket and Kafka sources, update matching |
| `crates/observability` | metrics HTTP server, slot tracker |
| `crates/migration` | ClickHouse migrations for the alerts sink |
| `crates/utility` | pubkey, signature, RPC and math helpers |
| `crates/rules-registry` | ready-made programs and rules: system, SPL Token, BPF loader, wallet watchlist, service health |
| `examples/spl-token-monitor` | a complete, compiling deployment with its own program crate |
| `examples/registry-monitor` | the smallest deployment: the registry's programs plus a rules directory |
| `docs/arch.md` | the full architecture diagram |

## Development

```sh
make lint                    # nightly rustfmt + clippy -D warnings
make run-tests               # cargo test --workspace
make run-tests-integration   # Redis-backed gate tests, needs Docker
make help                    # every other target
```

## Further reading

- [`CONTRIBUTING.md`](CONTRIBUTING.md) — adding a rule, adding a program, data sources, checks.
- [`docs/arch.md`](docs/arch.md) — the design and full pipeline diagram.
- [`examples/README.md`](examples/README.md) — a file-by-file tour of the example deployment.
- [`config.example.toml`](config.example.toml) — every service setting, commented.
