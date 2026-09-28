# Contributing

Solana Sentinel is a library; this repo holds only the framework crates under `crates/`.
A *deployment* is a small Cargo workspace that depends on it: a binary calling
`solana_sentinel_service::run(RunOptions { service_name, programs })`, its own *program*
crates, and its `config.toml` / `rules.toml`.

A program is a crate exporting one `spec()` that lists its parsers and the *rules* registered on
them. The core crate (`solana_sentinel`) does everything else: typed config validation,
shared-cache bootstrap, prefilter union, pipeline install, alert forwarding.

```
[dependencies]                                   # in a deployment's Cargo.toml
solana-sentinel = { git = "…/solana-sentinel" }          # rule traits, ProgramSpec, testing helpers
solana-sentinel-service = { git = "…/solana-sentinel" }  # run(), config, validation
solana-sentinel-migration = { git = "…/solana-sentinel" } # ClickHouse migrations for the alerts sink
```

```rust
// src/main.rs of a deployment
use solana_sentinel::ProgramSpec;
use solana_sentinel_service::{run, RunOptions};

const PROGRAMS: &[fn() -> ProgramSpec] = &[my_program::spec, my_other_program::spec];

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    run(RunOptions { service_name: "my_monitor", programs: PROGRAMS }).await
}
```

`my_monitor check` validates `rules.toml` + `config.toml` and prints the resolved
subscriptions without connecting anywhere.

## Run a deployment locally

Copy `config.example.toml` to your deployment as `config.toml`, point `[rpc]` and
`[geyser_source]` at your endpoints, use the `stdout` sink, and leave `[redis]` and
`[observability.betterstack]` out: gates become in-memory and no heartbeat is sent.

## Add a rule

1. Create `<rule>.rs` in the program crate's `rules/` module:

   ```rust
   #[derive(Debug, Deserialize)]
   pub struct MyRuleConfig { /* the [rules.config] table */ }

   #[derive(Debug)]
   pub struct MyRule { config: MyRuleConfig }

   impl RuleDef for MyRule {
       const ID: RuleId = RuleId::new(crate::PROGRAM, "my_rule");
       type Config = MyRuleConfig;
       type Deps = ();                       // or the program's shared cache type
       fn build(config: Self::Config, _deps: &()) -> anyhow::Result<Self> { Ok(Self { config }) }
       // async fn bootstrap(&mut self, ctx: &BootstrapCtx) -> anyhow::Result<()> for RPC warm-up
       // (needs #[async_trait] on the impl)
   }

   impl GrpcRule for MyRule {                // stream-fed; or `RpcRule` for poll-fed
       type Event = <the parser's output>;
       fn static_prefilter(cfg: &Self::Config) -> Prefilter { Prefilter::new().accounts(...) }
       fn evaluate(&self, ev: &Self::Event, ctx: &StreamCtx) -> Option<Alert> {
           Some(Alert::for_rule(Self::ID).severity(..).message(..).meta(k, v).key([..]).scope(ctx.tx_scope()).build())
       }
   }
   ```

2. Add `pub mod <rule>;` to `rules/mod.rs` and one `.register::<MyRule>()` line in `lib.rs::spec()`.
   Registering a `GrpcRule` on `.poll(..)` or an `RpcRule` on `.account(..)` is a compile error.
3. Add the `[[rules]]` entry to `rules.toml`.
4. Add a fixture test with `solana_sentinel::testing` (`rule_from_toml`, `account_update`,
   `ix_update`, `stream_ctx`, `assert_fires!`); enable the `testing` feature in dev-dependencies.

Guidance:

- `static_prefilter` must be a pure function of config and constants. Chain-discovered
  accounts (vaults, PDAs found by scanning) belong on the program's `ProgramDeps`.
- Prefilter semantics follow yellowstone: within one subscription `accounts`, `owners` and
  data filters are ANDed. `Prefilter::owned(owner, [memcmp])` becomes its own subscription,
  so a rule can watch "every account of type X" and another "these pubkeys" on one pipeline.
- Instruction pipelines are bundled across programs by shipstern; the framework re-checks each
  pipeline's transaction accounts locally, so a rule never sees another program's traffic.
- Rules that compare against last-seen state should use `StateCache`; the default policy stores
  an unknown key as the baseline instead of ignoring it forever.
- `evaluate` is sync and must not do I/O. Anything it needs comes from the parser output,
  the ctx, the config, or a cache seeded in `bootstrap`.

## Add a program

A program crate is `lib.rs` with `PROGRAM` and `spec()`, a parser (a codama parser via
`include_shipstern_parser!("idls/...json")`, or the framework's raw parsers in
`solana_sentinel::raw`), and one file per rule:

```rust
pub const PROGRAM: Program = Program::new("my_program");

pub fn spec() -> ProgramSpec {
    ProgramSpec::builder(PROGRAM)
        .account(MyAccountParser::new(), StreamRules::new().register::<MyAccountRule>())
        .instruction(MyIxParser::new(), StreamRules::new().register::<MyIxRule>())
        .poll(MySource, Duration::from_secs(60), PollRules::new().register::<MyPollRule>())
        .finish()
}
```

1. Add the crate to your deployment workspace and to your binary's `Cargo.toml`.
2. Add `<name>::spec` to the `PROGRAMS` list your binary passes to `run`.

A program with a shared cache implements `ProgramDeps` and calls `.deps::<Cache>()` before
registering pipelines. `dynamic_prefilter` feeds every pipeline; `refresh_interval` + `refresh`
push a changed set to the live subscription without a restart.

A poll-fed program implements `PollSource` and registers `RpcRule`s with
`.poll(source, default_interval, rules)`. Rules on the same cadence share one
`getMultipleAccounts` per tick.

## Ingestion sources

Rules never see the transport. `[geyser_source]` picks it:

| `type` | Path | Notes |
|---|---|---|
| `yellowstone` (alias `shipstern`) | shipstern runtime, its own dispatch and reconnect | production default |
| `grpc` | `solana_sentinel_ingest::GrpcSource` → framework `Dispatcher` | plain Yellowstone client, honours filter updates |
| `websocket` | `solana_sentinel_ingest::WebsocketSource` → `Dispatcher` | PubSub `accountSubscribe` / `programSubscribe` / `logsSubscribe` + `getTransaction`; commitment ≥ `confirmed` |

To add a transport, implement `solana_sentinel::StreamSource`: receive the named filter set
(`shipstern_core::Filters`), emit `SourceUpdate { filters, update }` tagged with the names each
update matched, and optionally react to later sets on the `watch` channel. The dispatcher does
the rest, including building the instruction tree once per transaction.

## Checks

```sh
make lint                    # nightly rustfmt + clippy -D warnings
make run-tests               # cargo test --workspace
make run-tests-integration   # Redis-backed gate tests, needs Docker
```
