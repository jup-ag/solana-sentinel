# Contributing

Solana Sentinel is a library; this repo holds only the framework crates under `crates/`.
A *deployment* is a small Cargo workspace that depends on it: a binary calling
`solana_sentinel_service::run(RunOptions { service_name, programs })`, its own *program*
crates, and its `config.toml` / `rules.toml`.

A program is a crate exporting one `spec()` that lists its parsers and the *rules* registered on
them. The core crate (`solana_sentinel`) does everything else: typed config validation,
shared-cache bootstrap, subscription union, pipeline install, alert forwarding.

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

`examples/spl-token-monitor/` is a complete, compiling deployment with a rule on every feed;
`examples/README.md` walks through it.

## Run a deployment locally

Copy `config.example.toml` to your deployment as `config.toml`, point `[rpc]` and
`[data_source]` at your endpoints, use the `stdout` sink, and leave `[redis]` and
`[observability.betterstack]` out: gates become in-memory and no heartbeat is sent.
To skip dedup altogether set `cooldown_gate = false` and `fleet_gate = false` under `[alerting]`.

## Add a rule

1. Create `<rule>.rs` in the program crate's `rules/` module:

   ```rust
   #[derive(Debug, Deserialize)]
   pub struct MyRuleConfig { /* the [rules.config] table */ }

   #[derive(Debug)]
   pub struct MyRule { config: MyRuleConfig }

   impl StreamRule for MyRule {              // stream-fed; or `PollRule` for poll-fed
       const ID: RuleId = RuleId::new(crate::PROGRAM, "my_rule");
       type Config = MyRuleConfig;
       type Event = <the parser's output>;
       fn subscription(cfg: &Self::Config) -> Subscription { Subscription::new().accounts(...) }
       fn build(config: Self::Config, _deps: &Deps) -> anyhow::Result<Self> { Ok(Self { config }) }
       // `deps.get::<Cache>()?` reaches the program's shared state
       // async fn bootstrap(&mut self, ctx: &BootstrapCtx) -> anyhow::Result<()> for RPC warm-up
       // (needs #[async_trait] on the impl)
       // fn sweep_interval(cfg: &Self::Config) -> Option<Duration> + fn sweep(&self) for periodic checks
       fn evaluate(&self, ev: &Self::Event, ctx: &StreamCtx) -> Option<Alert> {
           Some(Alert::for_rule(Self::ID).severity(..).message(..).meta(k, v).key([..]).scope(ctx.tx_scope()).build())
       }
   }
   ```

2. Add `pub mod <rule>;` to `rules/mod.rs` and one `.register::<MyRule>()` line in `lib.rs::spec()`.
   Registering a `StreamRule` on `.poll(..)` or a `PollRule` on `.account(..)` is a compile error,
   as is a rule whose `Event` is not the parser's output.
3. Add the `[[rules]]` entry to `rules.toml`.
4. Add a fixture test with `solana_sentinel::testing` (`config_from_toml`, `account_update`,
   `ix_update`, `stream_ctx`, `assert_fires!`); enable the `testing` feature in dev-dependencies.

Guidance:

- `subscription` must be a pure function of config and constants. Chain-discovered
  accounts (vaults, PDAs found by scanning) belong on the program's `ProgramDeps`.
- `Subscription` is the framework's ask; it expands into shipstern prefilters with yellowstone
  semantics: within one prefilter `accounts`, `owners` and data filters are ANDed.
  `Subscription::owned(owner, [memcmp])` becomes its own prefilter,
  so a rule can watch "every account of type X" and another "these pubkeys" on one pipeline.
- Instruction pipelines are bundled across programs by shipstern; the framework re-checks each
  pipeline's transaction accounts locally, so a rule never sees another program's traffic.
- `evaluate` takes `&self`; the handler and a rule's sweep loop may call it concurrently.
  Rules that compare against last-seen state should use `StateCache` (locked inside, with a
  policy for unknown keys: by default store as baseline and skip once).
- Updates are evaluated in stream order: the runtime runs one worker, so a rule sees an
  account's updates in the sequence the source delivered them.
- `evaluate` is sync and must not do I/O. Anything it needs comes from the parser output,
  the ctx, the config, or state seeded in `bootstrap`.

## Add a program

A program crate is `lib.rs` with `PROGRAM` and `spec()`, a parser (a codama parser via
`include_shipstern_parser!("idls/...json")`, or the framework's raw parsers in
`solana_sentinel::raw`), and one file per rule:

```rust
pub const PROGRAM: Program = Program::new("my_program");

pub fn spec() -> ProgramSpec {
    ProgramSpec::new(PROGRAM)
        .account(MyAccountParser::new(), StreamRules::new().register::<MyAccountRule>())
        .instruction(MyIxParser::new(), StreamRules::new().register::<MyIxRule>())
        .poll(MySource, Duration::from_secs(60), PollRules::new().register::<MyPollRule>())
}
```

1. Add the crate to your deployment workspace and to your binary's `Cargo.toml`.
2. Add `<name>::spec` to the `PROGRAMS` list your binary passes to `run`.

A program with a shared cache implements `ProgramDeps` and calls `.deps::<Cache>()`; its rules
reach it with `deps.get::<Cache>()?` in `build` (a type mismatch fails at startup).
`dynamic_subscription` feeds every pipeline; `refresh_interval` + `refresh`
push a changed set to the live subscription without a restart.

A poll-fed program implements `PollSource` and registers `PollRule`s with
`.poll(source, default_interval, rules)`. Rules on the same cadence share one
`getMultipleAccounts` per tick.

## Data sources

Rules never see the transport. Every source implements shipstern's `SourceTrait` and runs in
its runtime, which dispatches by filter name; `[data_source]` picks it:

| `type` | Source | Notes |
|---|---|---|
| `yellowstone_grpc` | `shipstern_yellowstone_grpc_source::YellowstoneGrpcSource` | server-side filter names, filter updates, reconnect with slot replay + dedup; production default |
| `websocket` | `WebsocketSource` (`solana-sentinel-data-source`) | PubSub `accountSubscribe` / `programSubscribe` / `logsSubscribe` + `getTransaction`; commitment ≥ `confirmed` |
| `kafka` | `KafkaSource` (`solana-sentinel-data-source`) | janus `raw.<program>.{accounts,transactions}` topics; slot feed synthesized from record slots |

To add a transport, implement `shipstern::sources::SourceTrait` (and `FilterUpdateSource` if
it can apply later filter sets): emit `SubscribeUpdate`s whose `filters` name the parsers they
match. `data_source::matching::match_update` computes those names for records that arrive
untagged. Add a config variant and a `launch.run::<MySource>(..)` arm in the service consumer.

## Checks

```sh
make lint                    # nightly rustfmt + clippy -D warnings
make run-tests               # cargo test --workspace
make run-tests-integration   # Redis-backed gate tests, needs Docker
```
