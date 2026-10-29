# Solana Sentinel

A Rust framework for on-chain security monitoring and alerting on Solana.

It ingests account and transaction updates (Yellowstone gRPC, websocket, Kafka), evaluates them against typed per-program rules, and sends alerts to Slack, Betterstack, ClickHouse or stdout. It is a library: your deployment calls `solana_sentinel_service::run` from a small `main.rs`.

## How it works

```
  ┌────────────────────────────────────────────────────────────────┐
  │ ProgramSpec (one per program)                                  │
  │ parsers · StreamRules · PollRules · ProgramDeps cache          │
  └─────────────┬────────────────────────────────────┬─────────────┘
                │                                    │ 
                │ subscriptions                      │ accounts to poll
                ▼                                    ▼
  ┌───────────────────────────┐        ┌───────────────────────────┐
  │ Stream source             │        │ PollSource                │
  │ Yellowstone · ws · Kafka  │        │ RPC, on a timer           │
  └─────────────┬─────────────┘        └─────────────┬─────────────┘
                │                                    │
                ▼ updates                            ▼ items
         ┌─────────────┐                      ┌─────────────┐
         │ StreamRules │                      │  PollRules  │
         └──────┬──────┘                      └──────┬──────┘
                └─────────────────┬──────────────────┘
                                  │
                                  ▼
                  ┌──────────────────────────────┐
                  │  evaluator: rule.evaluate()  │
                  └───────────────┬──────────────┘
                                  │
                                  ▼ alerts
                  ┌──────────────────────────────┐
                  │ cooldown gate: local ▸ fleet │
                  └───────────────┬──────────────┘
                                  │
                                  ▼
                  ┌──────────────────────────────┐
                  │  router: rule id → sink ids  │
                  └───────────────┬──────────────┘
                                  │
                                  ▼
            ┌───────────────────────────────────────────┐ 
            │                   Sinks                   │  
            │ stdout · Slack · Betterstack · ClickHouse │  
            └───────────────────────────────────────────┘
```

Each program crate exports a `ProgramSpec`. Its stream rules' subscriptions are merged into one filter set for the stream source, and its poll rules tell the `PollSource` which accounts to fetch. Alerts from both paths pass the cooldown gate (per node, then fleet-wide), and the router sends each one to the sinks named in the rules file. See [`docs/arch.md`](docs/arch.md) for the full design.

## Quick start

Validate the example deployment without connecting anywhere:

```sh
CONFIG_FILE=examples/spl-token-monitor/config.toml \
RULES_FILE=examples/spl-token-monitor/rules \
cargo run -p spl-token-monitor-example -- check
```

Drop `check` to run it for real. Alerts print as JSON lines on stdout.

A deployment of your own is a small workspace with program crates, a `config.toml`, a rules file, and this binary:

```rust
use solana_sentinel::ProgramSpec;
use solana_sentinel_service::{run, RunOptions};

const PROGRAMS: &[fn() -> ProgramSpec] = &[my_program::spec];

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    run(RunOptions { service_name: "my_monitor", programs: PROGRAMS }).await
}
```

[`CONTRIBUTING.md`](CONTRIBUTING.md) covers adding rules and programs.

## Ready-made rules

`crates/rules-registry` ships rules you can turn on from a rules file without writing Rust. Pass `solana_sentinel_rules_registry::PROGRAMS` to `run`, or concatenate it with your own list.

| Program | Rule | Fires when |
|---|---|---|
| `system` | `lamports_below` | a watched account's SOL balance is under a floor |
| `system` | `large_sol_transfer` | a large SOL transfer leaves a watched account |
| `system` | `nonce_activity` | a watched authority touches a nonce account |
| `spl_token` | `supply_increase` | a mint's supply grows by at least N bps |
| `spl_token` | `mint_authority_change` | a mint or freeze authority changes |
| `bpf_loader` | `program_upgrade` | a watched program is redeployed |
| `bpf_loader` | `upgrade_authority_change` | a program's upgrade authority changes |
| `wallet` | `unapproved_program_invoke` | a watched wallet calls a non-allowlisted program |
| `wallet` | `anchor_idl_instruction` | an Anchor IDL admin instruction hits a watched program |
| `health` | `slot_lag` | the stream slot drifts from the RPC slot |
| `pyth` | `oracle_staleness` | a Pyth feed stops publishing |
| `pyth` | `reference_price_drift` | a Pyth price leaves its reference band |
| `stable_coin` | `depeg_monitor` | a stablecoin trades outside its peg band |

[`examples/registry-monitor/`](examples/registry-monitor/) turns all of them on with sample config.

## Configuration

- **`config.toml`**: secrets, data source and sinks. Keep it out of git. Every setting is documented in [`config.example.toml`](config.example.toml).
- **`rules.toml` or `rules/*.toml`**: rules, thresholds and sink routing. Version-controlled.
- `CONFIG_FILE` and `RULES_FILE` override the default paths. `<binary> check` validates both.

**Data sources** (`[data_source] type`): `yellowstone_grpc` (default), `websocket` or `kafka`.

**Sinks** (`[[sinks]]`): `stdout`, `slack`, `betterstack`, `clickhouse`.

**Cooldown gate**: a per-node Redis gate followed by a fleet-wide Redis gate. The fleet gate runs in memory when `redis_fleet_url` is unset.

Prometheus metrics are served on `[observability.server] bind_addr`.

## Development

```sh
make lint                    # rustfmt + clippy
make run-tests               # cargo test --workspace
make run-tests-integration   # Redis-backed tests, needs Docker
```