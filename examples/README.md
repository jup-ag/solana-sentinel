# Examples

## `spl-token-monitor`

A complete deployment against the SPL Token program: one program crate with a rule
on each of the three feeds, the binary that runs it, and its config.

| File | Shows |
|---|---|
| `src/main.rs` | The deployment binary: `run(RunOptions { service_name, programs })`. |
| `src/lib.rs` | `PROGRAM`, `spec()` registering a parser + rules per feed, and a `PollSource`. |
| `src/rules/large_transfer.rs` | Instruction rule (`GrpcRule`): transfers out of watched accounts above a threshold, per-transaction dedup. |
| `src/rules/balance_drop.rs` | Account rule (`GrpcRule`) with a `StateCache` seeded from RPC in `bootstrap`. |
| `src/rules/low_balance.rs` | Poll rule (`RpcRule`): its own account set and cadence, monitor-scoped dedup. |
| `rules.toml` | Activates the rules, routes them to sinks, and carries each rule's typed config. |
| `config.toml` | Data source, RPC, observability and a stdout sink; nothing else is required locally. |

Validate without connecting anywhere:

```sh
CONFIG_FILE=examples/spl-token-monitor/config.toml \
RULES_FILE=examples/spl-token-monitor/rules.toml \
cargo run -p spl-token-monitor-example -- check
```

Run for real by setting a Yellowstone endpoint and token in `config.toml` (or switch
`[data_source]` to `websocket` against a public RPC node), then drop `check`. Alerts
print as JSON lines on stdout.

Run the rule tests:

```sh
cargo test -p spl-token-monitor-example
```

### Adding a rule to this program

1. Copy one of the files in `src/rules/` and change `ID`, `Config`, `static_subscription`
   (or `accounts` for a poll rule) and `evaluate`.
2. Add `pub mod <rule>;` to `src/rules/mod.rs` and `.register::<Rule>()` on the matching
   feed in `spec()`. Registering a stream rule on `.poll(..)` is a compile error.
3. Add a `[[rules]]` block to `rules.toml`. `check` reports config errors by rule and field.

### Adding a program

Copy this crate, replace the parsers (a codama parser from an IDL via
`include_shipstern_parser!`, the SPL parsers, or `solana_sentinel::raw` for undecoded
bytes), write the rules, and add `<crate>::spec` to `PROGRAMS` in your binary.

A program whose rules share chain-discovered state (vault lists, PDAs found by scanning)
implements `ProgramDeps`, calls `.deps::<MyDeps>()` before the first pipeline, and returns
those accounts from `dynamic_subscription`; `refresh_interval` + `refresh` push a changed set
to the live subscription without a restart. This example needs none, so its rules use
`type Deps = ()`.
