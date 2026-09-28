# Contributing

The monitor is a set of *programs*, each a crate under `programs/`, each exporting one
`spec()` that lists its parsers and the *rules* registered on them. The framework in
`programs/interface` does everything else: typed config validation, shared-cache bootstrap,
prefilter union, pipeline install, alert forwarding.

## Run it locally

```sh
cp config.example.toml config.toml   # point [rpc] and [geyser_source] at your endpoints
make check-rules                     # validates rules.toml + config.toml, prints the resolved subscriptions, no network
make run                   # RUST_LOG=info, alerts on the sinks rules.toml names
```

For a local run use the `stdout` sink and leave `[redis]` and `[observability.betterstack]`
out of `config.toml`: gates become in-memory and no heartbeat is sent.

## Add a rule

1. Create `programs/<program>/src/rules/<rule>.rs`:

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
4. Add a fixture test with `interface::testing` (`rule_from_toml`, `account_update`, `ix_update`,
   `stream_ctx`, `assert_fires!`); `programs/_template` shows the shape.

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

1. `cp -r programs/_template programs/<name>`; set the package name in `Cargo.toml`, `PROGRAM`
   in `lib.rs`, and replace the raw parsers with the program's codama parser
   (`include_shipstern_parser!("idls/...json")`, see `squads_v4/src/parser.rs`).
2. Add `<name> = { path = "programs/<name>" }` under `[workspace.dependencies]` in the root
   `Cargo.toml`, and `<name> = { workspace = true }` in `programs/registry/Cargo.toml`.
3. Add `<name>::spec` to `PROGRAMS` in `programs/registry/src/lib.rs`.
4. Add the program to the README list.

A program with a shared cache implements `ProgramDeps` (see `jup_lend/src/rules/cache.rs`) and
calls `.deps::<Cache>()` before registering pipelines. `dynamic_prefilter` feeds every pipeline;
`refresh_interval` + `refresh` push a changed set to the live subscription without a restart.

A poll-fed program implements `PollSource` (see `jup_usd/src/parser.rs`) and registers
`RpcRule`s with `.poll(source, default_interval, rules)`. Rules on the same cadence share one
`getMultipleAccounts` per tick.

## Ingestion sources

Rules never see the transport. `[geyser_source]` picks it:

| `type` | Path | Notes |
|---|---|---|
| `yellowstone` (alias `shipstern`) | shipstern runtime, its own dispatch and reconnect | production default |
| `grpc` | `ingest::GrpcSource` → framework `Dispatcher` | plain Yellowstone client, honours filter updates |
| `websocket` | `ingest::WebsocketSource` → `Dispatcher` | PubSub `accountSubscribe` / `programSubscribe` / `logsSubscribe` + `getTransaction`; commitment ≥ `confirmed` |

To add a transport, implement `interface::StreamSource`: receive the named filter set
(`shipstern_core::Filters`), emit `SourceUpdate { filters, update }` tagged with the names each
update matched, and optionally react to later sets on the `watch` channel. The dispatcher does
the rest, including building the instruction tree once per transaction.

## Checks

```sh
make lint                    # nightly rustfmt + clippy -D warnings
make run-tests               # cargo test --workspace
make run-tests-integration   # Redis-backed gate tests, needs Docker
```
