# Contributing

Solana Sentinel is a library; this repo holds only the framework crates under `crates/`.
A *deployment* is a small Cargo workspace that depends on it: a binary calling
`solana_sentinel_service::run(RunOptions { service_name, programs })`, its own *program*
crates, and its `config.toml` plus a `rules.toml` or a `rules/` directory of them.

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

`my_monitor check` validates the rules files + `config.toml` and prints the resolved
subscriptions without connecting anywhere. `RULES_FILE` names one file or a directory;
a directory's `*.toml` files are concatenated in name order, so a project or scope can
own its rules in a file of its own. A rule name may appear in only one of them.

`examples/spl-token-monitor/` is a complete, compiling deployment with a rule on every feed;
`examples/README.md` walks through it. `crates/rules-registry/` holds ready-made programs
(system, SPL Token, BPF loader, wallet watchlist, service health) a deployment can run as-is
via `solana_sentinel_rules_registry::PROGRAMS`, or copy from; `examples/registry-monitor/`
runs all of them.

## Run a deployment locally

Copy `config.example.toml` to your deployment as `config.toml`, point `[rpc]` and
`[data_source]` at your endpoints, use the `stdout` sink, point `redis_sidecar_url` at a
local Redis, and leave `redis_fleet_url` and `[observability.betterstack]` out: the fleet
gate becomes in-memory and no heartbeat is sent.
To skip dedup altogether set `local_gate = false` and `fleet_gate = false` under `[alerting.cooldown_gate]`.

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
3. Add the `[[rules]]` entry to the rules file for its scope (or `rules.toml`).
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

### Reactors: rules fed by alerts

A `ReactorRule` reacts to other rules' alerts (every program's) instead of chain events: an
automated response such as building and simulating a pause transaction. The service's alert
tap hands it each alert as the rule raises it, before the cooldown / fleet gates and the
sinks' batching, then forwards the alert unchanged.

```rust
#[async_trait]
impl ReactorRule for MyReactor {
    const ID: RuleId = RuleId::new(crate::PROGRAM, "my_reactor");
    type Config = MyReactorConfig;
    fn build(cfg: Self::Config, deps: &Deps) -> anyhow::Result<Self> { .. }
    // bootstrap / refresh_interval + refresh: pre-build what the hot path needs
    fn accepts(&self, alert: &Alert) -> bool { alert.metadata.get("action").is_some_and(|a| a == "L1") }
    async fn react(&self, alert: Alert, ctx: ReactCtx) -> Vec<Alert> { .. } // own task per alert
}
```

Register it with `.reactor::<MyReactor>()` in `spec()` and configure it in the rules file like
any rule; its alerts are routed by its own `[[rules]]` entry (sinks, cooldown). `accepts` runs on
the alert path for every alert, so keep it a metadata check. A reactor never sees its own
alerts and its output is not fed back to reactors. The copy it gets carries no slot hold
(`ctx.trigger_slot` has the slot), so a slow reaction does not hold the replay checkpoint.

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

## Monitoring the monitor

The framework records what every feed receives: each stream pipeline
(`{program}:{account|instruction|transaction}:{parser}`), each rule's share of its pipeline
(`{program}:rule:{rule}`, matched against the rule's static subscription; a rule fed only by the
dynamic subscription counts every update) and each poll bucket (`{program}:poll:{Source}:{n}s`,
successes and failed fetches). Two rules in the registry's `health` program use it, both on a
`TickSource` (no RPC, so they keep running when the RPC is down):

- `feed_liveness`: Critical `action = alert` when the gRPC stream delivers no slot for
  `stream_stall_seconds`, a poll source fails `poll_failure_threshold` fetches in a row, or a
  feed that normally sees traffic is silent past its bound (`p99_factor` × p99 inter-arrival
  gap, floor `min_silence_seconds`, learned after `min_samples` gaps; or a configured
  `[[rules.config.bounds]]`). A rule feed is not alerted while its pipeline is.
- `canary`: every `interval_seconds` one synthetic alert per route (`scope = "route"`: each
  distinct sink set; `"program"`: each program's sinks) through the tap, gates, batching and
  sinks. Canaries carry `canary = "true"`; reactors skip them, Slack acknowledges them without
  posting (or posts to `canary_channel`), Betterstack opens no incident, ClickHouse stores them.
  The service's canary watchdog raises a Critical alert, routed by the `canary` rule's own sinks,
  for each route/sink pair whose freshest canary is older than `interval + deadline`, and the
  Betterstack heartbeat is withheld meanwhile (`gate_on_canary`, default on).

`GET /liveness` returns the per-feed and canary state; metrics `feed_*`, `canary_*`,
`stream_slot_age_seconds`, `heartbeat_withheld_total`. `solana_sentinel_service::testing`
(feature `testing`) runs programs end to end on a scripted source with in-memory sinks.

## Data sources

Rules never see the transport. Every source implements shipstern's `SourceTrait` and runs in
its runtime, which dispatches by filter name; `[data_source]` picks it:

| `type` | Source | Notes |
|---|---|---|
| `yellowstone_grpc` | `shipstern_yellowstone_grpc_source::YellowstoneGrpcSource` | server-side filter names, filter updates, reconnect with slot replay + dedup; production default |
| `websocket` | `WebsocketSource` (`solana-sentinel-data-source`) | PubSub `accountSubscribe` / `programSubscribe` / `logsSubscribe` + `getTransaction`; commitment ≥ `confirmed` |
| `kafka` | `KafkaSource` (`solana-sentinel-data-source`) | janus `raw.<program>.{accounts,transactions}` topics; slot feed synthesized from record slots |
| `multi` | `MultiSource` (`solana-sentinel-data-source`) | N children of the types above, same filters, merged and deduplicated; per-child health for `health/source_divergence` |
| `replay` | `ReplaySource` (`solana-sentinel-data-source`) | a recorded slot window (JSON lines, `replay::format`) in slot order at original speed × `speed`; finite, the service exits 0 after it |

### Two providers (`type = "multi"`)

`MultiSource` (crates/data-source/src/multi.rs) runs every `[[data_source.sources]]` child
in its own task with the runtime's filters, and fans each filter update out to all of them.
Their updates are merged:

- **Dedup.** Transactions by signature; account updates by `(pubkey, slot, hash of
  lamports/owner/executable/data)` (PubSub has no write version, so it cannot be in a
  cross-provider key); slots by `(slot, status)`. The first delivery is forwarded. A later
  copy is forwarded only for filter names the first did not carry, restricted to those
  names, so a rule sees each event once whichever child tagged it.
- **Divergence.** Per item (signature, or account `(pubkey, slot)`): which children
  delivered it and how long after the first. When `grace_seconds` closes, a child that was
  connected and already past that slot but never delivered it counts it `missing`; items
  older than a child's first slot since its (re)connect are not expected from it (a
  replaying sibling, its own downtime). Copies after the window count `late` and are still
  deduplicated for `retention_seconds`.
- **Isolation.** A child that fails, ends or panics is restarted with backoff (1 s doubling
  to 30 s); the others keep streaming. Errors a child emits are logged and dropped, never
  forwarded (the runtime stops on the first error it receives).
- **Health.** `SourceHealth` (crates/observability/src/source_health.rs) is shared through
  `BootstrapCtx::source_health`. The `health/source_divergence` poll rule alerts when a child
  is silent (`silence_seconds`, default 60), more than `max_lag_slots` (50) behind the most
  advanced child, or missing more than `max_missing_pct` (5 %) of the others' items over
  `window_seconds` (300, judged from `min_expected` = 100 items). Metrics:
  `data_source_up`, `data_source_slot{kind=slot|lag}`, `data_source_items_total{kind=
  delivered|first|expected|missing|late}`, `data_source_delay_seconds`,
  `data_source_restarts_total`, all labelled `source`.

Compare like with like: gRPC defaults to `processed` (every write, forks included) while
PubSub is `confirmed` (one notification per slot, at most). A gRPC child at `processed`
therefore shows PubSub "missing" intermediate writes and dropped-fork transactions; set the
gRPC child's `commitment = "confirmed"` when measuring divergence, or raise
`max_missing_pct`. A websocket child's `getTransaction` goes to `[rpc]` unless it has its own
`rpc_url` (and `rpc_headers`); give it one on the same provider, or it is not independent.

Live check: `cargo run -p solana-sentinel-data-source --example multi_live -- --minutes 10`
splits the Solana CLI's configured RPC (Triton/rpcpool layout: gRPC endpoint = scheme +
host, x-token = the path token) into a gRPC child and a PubSub child and prints delivery
share, delays and gaps; `--config <file>` takes any `MultiConfig` TOML instead (keep it
git-ignored). It refuses public RPCs and never prints endpoints or tokens.

### Replays (incident drills)

`sentinel-replay-record` (bin of `solana-sentinel-data-source`) records a window around an
exploit transaction: every transaction touching the given addresses (`--scan signatures`, or
`--scan blocks` for every block of the window), the exploit transactions themselves, block
times, and today's snapshot of the accounts they wrote. RPC: `$SENTINEL_RECORD_RPC_URL`, else
the Solana CLI config's `json_rpc_url`; never printed, public endpoints refused.

```sh
cargo run -p solana-sentinel-data-source --bin sentinel-replay-record -- \
  --out drift.jsonl --name drift --exploit-signature 5cvV… --address GXWq… \
  --before-slots 150 --after-slots 600
```

The source emits, per slot, a `processed` slot update, then each transaction and the account
post-states rebuilt from its metadata (`replay::accounts`): token accounts (mint, owner,
amount, lamports exact; other fields from the snapshot) and system wallets (lamports) only.
Program-owned state cannot be rebuilt (no historical data on RPC), so account rules on it
see nothing in a replay; rules still bootstrap from today's RPC state. With
`initial_state = true` each rebuildable account's pre-state goes out at the first slot.
`emit_log` records each emitted slot/transaction with its wall time; with a `jsonl` sink
(`emitted_at`) that gives exploit tx -> alert latency.

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
