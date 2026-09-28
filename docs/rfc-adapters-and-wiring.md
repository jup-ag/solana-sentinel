# RFC: Adapter, rule and wiring redesign for open-sourcing

Status: implemented on `open-source-solana-sentinel` (phases 1–5). Sections 1 and 3 describe the code as it was at `f97ba15`; section 2 is the design as built. Deviations: no `SEVERITY` const (severity stays per alert), `Prefilter::owned` subscriptions instead of a flat `account_owners` field (yellowstone ANDs the fields of one filter), `common-rules` folded into sentinel as `anchor_idl_instruction`, and the liquidation sweep rule stays on its own branch.
Also surveyed: `naga/secon-175-tracking-liquidation` (hybrid stream + sweep rule) and shipstern 0.9.0 sources.

Goal: make "add a program", "add a rule" and "run it locally" each a small, mechanical task
for an outside contributor, without weakening what the monitor does in production.

---

## 1. What exists today

```
rules.toml ──▶ SentinelConfig ──▶ wiring::build_routes (validates names, builds Router)
                                └─▶ ConsumerService
                                      └─▶ registry::create_adapter(name)
                                            └─▶ ProgramAdapter::init(InitCtx{rpc, raw_rules})
                                                  └─▶ <program>::rules::build_rules  (7 copies)
                                                        └─▶ Rule::build(Value) → Rule::bootstrap(&RpcClient)
                                            └─▶ ProgramAdapter::into_installers()
                                                  ├─▶ GrpcProgramInstaller::install(builder)   → FilterPipeline(parser, [Handler], prefilter)
                                                  └─▶ RpcProgramInstaller::install(tracker,…)  → hand-rolled tokio interval loop
```

Per program crate: `lib.rs` (adapter + installer), `parser.rs`, `handler.rs`, `rules/mod.rs`
(`names()`, `build_rules()`, `merge_rule_prefilters()`), one file per rule.

### 1.1 Findings, grounded in the code

**Prefilters**

| # | Finding | Where |
|---|---|---|
| P1 | Static and dynamic prefilters are indistinguishable. All rules implement `extend_prefilter` on one accumulator. Squads copies config pubkeys, bpf_loader derives PDAs, jup_lend pushes a `watched` vector filled only during `bootstrap`, the lending rule inserts a compile-time program id. | `interface/src/rule.rs`, every `rules/*.rs` |
| P2 | Ordering is implicit and silently fatal. If `install` ever ran before `bootstrap`, jup_lend's account prefilter would be empty and the pipeline would receive nothing. Nothing enforces the order except each adapter's hand-written `init`. | `jup_lend/src/rules/liquidity_reserve_solvency.rs:215-261` |
| P3 | `Rule::prefilter_pubkeys()` has zero callers. | `interface/src/rule.rs:59` |
| P4 | Dynamic prefilters freeze at boot. A jup_lend reserve created after startup is never watched. Shipstern supports `account_owners` and memcmp `account_filters`; neither is used, so reserves could be subscribed by owner + discriminator today. | `jup_lend/src/rules/cache.rs` (uses memcmp for RPC, not for the stream) |
| P5 | Shipstern exposes `RuntimeHandle::update_filters` for live filter changes. The service drops the runtime straight into `try_run_async` and never keeps the handle, so no refresh path exists. | `service/src/consumer.rs:140-160`, shipstern `handle.rs` |
| P6 | Shipstern bundles all instruction pipelines into one and fans every instruction of any matching transaction to every instruction parser. Codama parsers self-filter by program id, but a raw parser must gate itself. Sentinel does this by hand in `matches_own_prefilter`. Any new raw-instruction program will have to rediscover this. | `sentinel/src/parser.rs:22-36`, shipstern `instruction.rs:77-90` |
| P7 | Two account pipelines with the same parser id is a fatal `AccountPipelineCollision` at build. Parser ids are hard-coded string literals per crate with no program prefix. | shipstern `builder.rs:252-270` |
| P8 | Sentinel's `transaction_accounts_include` on members drops `InitializeNonceAccount` transactions where the member is authority-only (false negative, M16 in the July review). A static prefilter that is visible per rule at validation time would have made this reviewable. | `sentinel/src/rules/multisig_member_nonce_account_interaction.rs` |

**Rule trait and construction**

| # | Finding | Where |
|---|---|---|
| R1 | `Rule<P, R>` has two type params. `R` is `()` in sentinel, jup_lend and jup_usd, and the raw update in squads, spl_token, bpf_loader and pyth. Rules that need slot or tx signature extract them by hand from `R`. | all `rules/mod.rs` |
| R2 | `data_source()` is a per-rule method whose only caller is pyth's `into_installers`. It encodes a pipeline decision on the rule. | `pyth/src/lib.rs:92` |
| R3 | `bootstrap(&mut self, &RpcClient)` takes a bare client, not the `BootstrapCtx`, so any new shared dependency changes every signature. Return value is a count used only for a log line. | `interface/src/rule.rs:49` |
| R4 | `build(Value)` repeats `try_deserialize().context("failed to deserialize <name> config")` in every rule with the name typed by hand. | every `rules/*.rs` |
| R5 | Seven `build_rules` copies with a `match` arm per rule and a separate `names()` list with a "keep in sync" comment. Unknown rule names `warn!` here but were already rejected as an error in `wiring::build_routes`, so the branch is dead. | 7 × `rules/mod.rs` |
| R6 | Every rule hand-builds `BTreeMap` metadata, inserts `tx_sig`, builds `HashKey` and `DedupScope`. Roughly 15 lines per rule are the same shape. | every `rules/*.rs` |
| R7 | Three rules hand-roll `Mutex<HashMap>` caches seeded at bootstrap and refreshed from the stream, and two of them `warn!("not in cache; ignoring")` on unknown keys. That pattern is the root of H9 (programs missing at bootstrap are permanently ignored) and M10 (gap between bootstrap read and subscription start) in the July review. | `bpf_loader/src/rules/program_upgrade.rs`, `spl_token/src/rules/token_supply_increase.rs`, `jup_lend/src/rules/cache.rs` |
| R8 | `common_rules::AnchorIdlInstruction` is generic over any parser output and is used by no program. It wants a program-agnostic raw-instruction pipeline that doesn't exist. | `programs/common-rules/src/anchor_idl.rs` |
| R9 | The liquidation branch adds `sweep_interval()` and `sweep()` to the rule trait plus a `JupLendSweepInstaller`, a third copy of the interval loop. Hybrid stream + periodic re-evaluation is a real pattern and needs a framework home. | `naga/secon-175-tracking-liquidation` |
| R10 | One `Rule<P, R>` signature serves both feeds, so the RPC monitors fabricate a dummy `AccountUpdate` per polled account to satisfy `R`. A poll-fed rule has no tx signature and its slot comes from the RPC response context, but the trait cannot say so; rules that read either field must tolerate the dummy. | `pyth/src/monitor.rs:72`, `utility/src/geyser.rs:32`, `docs/rpc-monitors.md` §5 |

**Handlers, installers, wiring**

| # | Finding | Where |
|---|---|---|
| W1 | Eight copies of the evaluate → severity log → `alerts_tx.send` loop, each also calling `record_grpc_event`. | 8 × `handler.rs` / `monitor.rs` |
| W2 | Three copies of the `tokio::interval` + `select!(cancel)` polling loop (pyth, jup_usd, and the sweep on the liquidation branch). | `pyth/src/lib.rs:140`, `jup_usd/src/lib.rs:90` |
| W3 | `ProgramAdapter` is a two-phase object (`init` then `into_installers(self: Box<Self>)`) and every adapter is also its own `GrpcProgramInstaller`. Seven installers do the same accumulate → `into_builder` → `FilterPipeline::new` dance. | 7 × `lib.rs` |
| W4 | `registry::create_adapter` is called twice per program: once in `wiring::build_routes` to validate names, once in `consumer::load_and_bootstrap_programs` to init. Rule config shape is only validated inside `init`, after Redis and RPC are connected. | `service/src/wiring.rs:151`, `service/src/consumer.rs:103` |
| W5 | Programs bootstrap sequentially. Logs show one program taking 2.18 s; seven would be additive. | `service/src/consumer.rs:98-110` |
| W6 | `programs_with_rules` groups via `HashMap` so bootstrap and log order are nondeterministic run to run. | `service/src/config/mod.rs:63` |
| W7 | The slot-health alert is routed by a hard-coded sink id `"betterstack"` with no existence check (M23). It is the only alert whose route is not in `rules.toml`. | `service/src/wiring.rs:178` |
| W8 | `InitCtx`/`BootstrapCtx` carry only `rpc`. Cancel token, task tracker and alert sender arrive by a different path for RPC installers. | `interface/src/lib.rs` |

**Onboarding and local run**

| # | Finding | Where |
|---|---|---|
| O1 | New program crate needs: crate dir, workspace dependency entry, `registry` Cargo dependency, `registry` match arm, a `COPY programs/<x>/Cargo.toml` line in the Dockerfile, README list entry. The Dockerfile hand-lists 11 manifests even though `.cargo/prep-workspace.sh` already globs members. | `Dockerfile:44-58`, `programs/registry/src/lib.rs` |
| O2 | `make run-tests` runs `cargo test -p alerting` only. Programs have three `#[cfg(test)]` modules total and no fixture harness. Alerting tests need Docker (testcontainers). | `Makefile:126`, `.github/workflows/test.yml` |
| O3 | Running the binary requires two Redis endpoints (sidecar and cluster), a Betterstack heartbeat URL, and at least one configured sink. There is no stdout sink and no in-memory gate. | `service/src/bin/main.rs:37-70` |
| O4 | Adding a rule touches: rule file, `names()`, `build_rules` arm, `rules.toml`. The two `rules/mod.rs` edits are the ones contributors will get wrong. | every `rules/mod.rs` |

### 1.2 What is fine and stays

`domain` (Alert, DedupScope, HashKey, RuleId, Program, SinkId), the two-layer dedup design
(cooldown gate + fleet gate), `alerting` (Router, AlertingService, sinks), `observability`
metrics, the codama-generated parsers, one crate per program, and `rules.toml` as the
single operator-facing file. None of these need to move.

---

## 2. Design

### 2.1 Prefilter model: static on the rule, dynamic on the program

```rust
// interface
#[derive(Default, Clone, Debug, PartialEq)]
pub struct Prefilter {
    pub accounts: HashSet<Pubkey>,
    pub account_owners: HashSet<Pubkey>,
    pub account_filters: Vec<AccountFilter>,     // memcmp / datasize, maps 1:1 to shipstern
    pub tx_accounts_include: HashSet<Pubkey>,
    pub tx_accounts_required: HashSet<Pubkey>,
}
impl Prefilter { pub fn union(self, other: Self) -> Self; pub fn into_builder(self) -> PrefilterBuilder; }
```

- A rule's prefilter is a pure function of constants and its typed config.
  It is computed during config validation, before any network connection.
- Chain-discovered watch sets belong to the program's shared state (`Deps`), because
  in the current codebase the only genuinely dynamic case (jup_lend vaults) is copied
  out of a shared cache by three rules.
- The framework builds each pipeline's prefilter as
  `∪ rule.static_prefilter(cfg)  ∪  deps.dynamic_prefilter()`.
  `Deps` is built before any rule, so the ordering in P2 is enforced by construction.
- `bootstrap` on a rule seeds caches and returns `()`. It does not shape the subscription.
- Poll-fed rules have no prefilter. They declare the accounts to fetch on `RpcRule::accounts`
  (2.2), and the framework unions those into one `getMultipleAccounts` per cadence.

### 2.2 Rule traits: `RuleDef` shared, `GrpcRule` and `RpcRule` per feed

Today one `Rule<P, R>` serves both the stream and the pollers, which is what forces
`data_source()` (R2), the dummy `AccountUpdate` (R10) and the "either poll or sweep"
interval confusion (R9). The two feeds want different things from a rule:

| | Stream-fed | Poll-fed |
|---|---|---|
| What it declares | a `Prefilter` that shapes the subscription | the account set to fetch, and a cadence |
| What it receives | one parsed event, with slot and tx signature | one parsed item from a fetch tick, with the response slot |
| Periodic work | optional `sweep` between events (liquidation branch) | the poll itself; a second interval makes no sense |

So the design splits the trait. `RuleDef` carries what both share; `GrpcRule` and
`RpcRule` add what only one feed needs. A rule implements exactly one of the two.

```rust
/// Identity, config, construction and bootstrap. Shared by both feeds.
pub trait RuleDef: Sized + Send + Sync + 'static {
    const NAME: &'static str;
    const SEVERITY: Severity;                    // default severity, rule may override per alert
    type Config: DeserializeOwned + Send + Sync;
    type Deps: ProgramDeps;                      // () for most programs

    fn build(cfg: Self::Config, deps: &Self::Deps) -> anyhow::Result<Self>;
    async fn bootstrap(&mut self, _ctx: &BootstrapCtx) -> anyhow::Result<()> { Ok(()) }
}

/// Fed by a shipstern account / instruction / transaction pipeline.
pub trait GrpcRule: RuleDef {
    type Event;                                  // the pipeline's parser output

    fn static_prefilter(cfg: &Self::Config) -> Prefilter;
    fn evaluate(&self, ev: &Self::Event, ctx: &StreamCtx) -> Option<Alert>;

    /// Optional periodic re-evaluation with no incoming event (interest accrual, staleness).
    fn sweep_interval() -> Option<Duration> { None }
    fn sweep(&self) -> Vec<Alert> { Vec::new() }
}

/// Fed by a `PollLoop` that fetches `accounts()` on a cadence and parses through a `PollSource`.
pub trait RpcRule: RuleDef {
    type Item;                                   // the program's `PollSource::Item`

    fn accounts(cfg: &Self::Config) -> HashSet<Pubkey>;
    /// `None` means the program default cadence. Same-cadence rules share one fetch.
    fn poll_interval(cfg: &Self::Config) -> Option<Duration> { None }
    fn evaluate(&self, item: &Self::Item, ctx: &PollCtx) -> Option<Alert>;
}

pub struct StreamCtx { pub slot: u64, pub tx_sig: Option<String> }
pub struct PollCtx   { pub slot: u64, pub fetched_at: Instant }
```

- No `EventSource` enum and no dummy raw update. The context type says which feed the
  rule is on, and each carries only fields that feed can actually fill (R1, R6, R10).
- `data_source()` goes away. A rule is stream-fed or poll-fed by which trait it
  implements, and `StreamRules::register` / `PollRules::register` (2.4) only accept the
  matching trait, so a rule cannot land in the wrong pipeline (R2).
- `sweep` lives only on `GrpcRule`; `poll_interval` lives only on `RpcRule`. The two
  intervals from R9 and `docs/rpc-monitors.md` no longer share a trait.
- `bootstrap` receives `&BootstrapCtx` on `RuleDef` (R3), so a poll rule can still seed a
  cache before its first tick.
- Object safety comes from blanket impls `impl<R: GrpcRule> DynGrpcRule<R::Event> for R`
  and `impl<R: RpcRule> DynRpcRule<R::Item> for R`, so handlers hold
  `Vec<Box<dyn DynGrpcRule<Ev>>>` exactly as today.

### 2.3 Program shared state

```rust
pub trait ProgramDeps: Clone + Send + Sync + 'static {
    async fn bootstrap(ctx: &BootstrapCtx) -> anyhow::Result<Self>;
    fn dynamic_prefilter(&self) -> Prefilter { Prefilter::default() }
    /// Optional: re-derive the dynamic prefilter later and push it through the runtime handle.
    fn refresh_interval() -> Option<Duration> { None }
}
impl ProgramDeps for () { async fn bootstrap(_: &BootstrapCtx) -> Result<()> { Ok(()) } }
```

`JupLendCache` becomes `Arc<JupLendCacheInner>` implementing `ProgramDeps`. Its
`dynamic_prefilter` returns vault pubkeys. Reserves move to a static owner + memcmp filter
(P4). `refresh_interval` plus the kept `RuntimeHandle` gives a path to pick up new vaults
without a restart (P5); this is phase 5 and optional.

### 2.4 Rule registries and pipelines: the generic layer that replaces `rules/mod.rs`, `handler.rs` and installers

```rust
// One registry per feed. Both validate typed config with no I/O, then build + bootstrap each rule.
pub struct StreamRules<Ev, Deps> { entries: Vec<StreamEntry<Ev, Deps>> }   // one entry per GrpcRule
pub struct PollRules<It, Deps>   { entries: Vec<PollEntry<It, Deps>> }     // one entry per RpcRule

impl<Ev, Deps> StreamRules<Ev, Deps> {
    pub fn register<R: GrpcRule<Event = Ev, Deps = Deps>>(self) -> Self;
    pub fn names(&self) -> Vec<&'static str>;
    pub fn validate(&self, raw: &RawRuleMap) -> Result<ValidatedStream<Ev, Deps>, RuleConfigError>;
}
impl<Ev, Deps> ValidatedStream<Ev, Deps> {
    pub fn static_prefilter(&self) -> &Prefilter;                          // ∪ over rules
    pub async fn build(self, deps: &Deps, ctx: &BootstrapCtx) -> Result<Vec<Box<dyn DynGrpcRule<Ev>>>>;
}

impl<It, Deps> PollRules<It, Deps> {
    pub fn register<R: RpcRule<Item = It, Deps = Deps>>(self) -> Self;
    pub fn names(&self) -> Vec<&'static str>;
    pub fn validate(&self, raw: &RawRuleMap) -> Result<ValidatedPoll<It, Deps>, RuleConfigError>;
}
impl<It, Deps> ValidatedPoll<It, Deps> {
    pub fn accounts(&self) -> &HashSet<Pubkey>;                            // ∪ over rules
    /// Rules bucketed by cadence; each bucket becomes one PollLoop with one fetch per tick.
    pub async fn build(self, deps: &Deps, ctx: &BootstrapCtx) -> Result<Vec<PollBucket<It>>>;
}

/// Turns one fetch into the items poll rules evaluate. One impl per poll-fed program.
pub trait PollSource: Send + Sync + 'static {
    type Item;
    fn parse(&self, fetched: &HashMap<Pubkey, Account>, ctx: &PollCtx) -> Vec<Self::Item>;
}
```

Pipeline kinds the framework owns:

| Kind | Source | Replaces today |
|---|---|---|
| `Account<P>` | shipstern account stream | spl_token, bpf_loader, pyth-grpc, jup_lend liquidity |
| `Instruction<P>` | shipstern instruction stream, with a local tx-accounts gate applied before `parse` (P6) | squads, jup_lend lending, sentinel ix |
| `Transaction<P>` | shipstern transaction stream | sentinel tx |
| `Poll<S>` | `PollLoop` per cadence: fetch `∪ RpcRule::accounts` → `S::parse` → `RpcRule::evaluate` | pyth-rpc monitor, jup_usd monitor (W2) |
| `Sweep` | any `GrpcRule` with `sweep_interval()`, in any stream pipeline | liquidation branch sweep installer (R9) |
| `RawInstruction` / `RawTransaction` | shared parser from the framework, program-agnostic | sentinel parsers, home for `AnchorIdlInstruction` (R8) |

One `RuleHandler<Ev>` implements shipstern `Handler` for every stream kind: record metric,
build `StreamCtx`, evaluate rules, log by severity, send (W1). One `PollLoop<S>` owns the
interval + cancel select, the chunked fetch, `S::parse` and `PollCtx` for every poll bucket;
one `SweepLoop` does the same for sweeps (W2). Per-account items (pyth) and cross-account
snapshots (jup_usd `build_snapshot`) are both just what `S::parse` returns.

Parser ids are prefixed by the framework as `"{program}:{parser_id}"` so P7 cannot
happen across programs.

### 2.5 Program spec: what an adapter crate exports

```rust
// programs/jup_lend/src/lib.rs
pub const PROGRAM: Program = Program::new("jup_lend");

pub fn spec() -> ProgramSpec {
    ProgramSpec::new(PROGRAM, PUBKEY)
        .deps::<JupLendCache>()
        .account(LiquidityProgramStateParser::new(), StreamRules::new()
            .register::<LiquidityReserveChange>()
            .register::<LiquidityReserveSolvency>()
            .register::<LiquidityAuthListChange>()
            .register::<LiquidityUtilizationRate>())
        .instruction(LendingProgramIxParser::new(), StreamRules::new()
            .register::<LendWhaleMovement>())
}
```

A mixed program registers each feed once. Today pyth partitions one rule list on
`data_source()` at runtime; after the change the split is in the spec:

```rust
// programs/pyth/src/lib.rs
pub fn spec() -> ProgramSpec {
    ProgramSpec::new(PROGRAM, PUBKEY)
        .account(PythPriceFeedParser::new(), StreamRules::new()
            .register::<ReferencePriceDrift>())
        .poll(PriceFeedSource::new(), Duration::from_secs(5), PollRules::new()   // program default cadence
            .register::<OracleStaleness>())
}
```

`.poll` on a `GrpcRule`, or `.account` on an `RpcRule`, is a type error.

```rust
// programs/registry/src/lib.rs
pub const PROGRAMS: &[fn() -> ProgramSpec] = &[
    squads_v4::spec, spl_token::spec, sentinel::spec, pyth::spec,
    jup_usd::spec, bpf_loader::spec, jup_lend::spec, system::spec,
];
```

`ProgramAdapter`, `GrpcProgramInstaller`, `RpcProgramInstaller`, `ProgramInstaller`,
`DataSource` and `RulePrefilterAccumulator` are deleted at the end of phase 3.

### 2.6 Service flow after the change

```
1. load        rules.toml + config.toml
2. validate    for each program: spec() → rules.validate(raw) → typed configs, static prefilters
               (stream) and account sets + cadences (poll)
               errors here name program, rule and field; nothing has connected yet      (W4)
               `solana-sentinel check` stops here and prints the resolved prefilters per pipeline
               and the polled accounts per cadence
3. wire        Router / cooldowns from validated rules (same as today, minus the second create_adapter)
4. bootstrap   try_join_all over programs: Deps::bootstrap → rules.build(deps, ctx)        (W5)
5. install     framework: prefilter = static ∪ deps.dynamic; FilterPipeline(parser, RuleHandler)
               keep RuntimeHandle for refresh                                              (P2, P5)
6. run         shipstern runtime + PollLoops + SweepLoops on the TaskTracker
```

Slot health becomes program `system` with rule `slot_lag_detected` registered like any
other, so its route lives in `rules.toml` and is validated (W7). Programs are iterated in
config order (W6).

### 2.7 Rule-authoring helpers

- `StateCache<K, V>` in `interface`: `seed(iter)`, `replace(k, v) -> Option<V>`,
  `on_unknown: Policy::{Warn, InsertAndSkip}`. Default `InsertAndSkip` closes H9 by
  default for new rules. bpf_loader, spl_token and jup_lend adopt it (R7).
- `Alert::for_rule(Self::ID)` builder: `.severity().message().meta().key([..]).scope(..)`.
  Handler appends `slot` and, on the stream side, `tx_sig` from the ctx unless the rule set them (R6).
- Config helpers already exist in `utility::pubkey`; the typed `Config` in `RuleDef`
  replaces the hand-written `try_deserialize` + context string (R4).
- `interface::testing` behind a `testing` feature: `rule_from_toml::<R>(&str)`,
  `account_update(pubkey, owner, data)`, `ix_update(program, accounts, data)`,
  `stream_ctx(slot, tx_sig)`, `poll_ctx(slot)`, `assert_fires!(rule, event, ctx)`. `make run-tests` becomes `cargo test --workspace`
  with the testcontainers tests behind `--ignored` (O2).

### 2.8 Onboarding artifacts

- `programs/_template`: a compiling crate with one account rule and one instruction
  rule, fixture tests, and `spec()`. Copy, rename, register.
- `CONTRIBUTING.md` sections "Add a rule" (file, one `.register` line, `rules.toml`,
  fixture test) and "Add a program" (copy template, one line in `PROGRAMS`).
- Dockerfile planner stage copies `programs/*/Cargo.toml` with a glob so new crates need no
  Dockerfile edit (O1). Workspace dependency entries per program stay; they are one line.
- Local run (O3): `[redis]` optional with in-memory cooldown/fleet gates, Betterstack
  heartbeat optional, a `stdout` sink type. Production config is unchanged.

---

## 3. Before and after

**Add a rule**

| Today | After |
|---|---|
| `rules/<name>.rs` | `rules/<name>.rs` |
| `rules/mod.rs`: `names()` entry | `lib.rs`: `.register::<Name>()` |
| `rules/mod.rs`: `build_rules` match arm | |
| `rules.toml` entry | `rules.toml` entry |
| no test convention | fixture test using `interface::testing` |

**Add a program**

| Today | After |
|---|---|
| new crate + `Cargo.toml` | copy `programs/_template` |
| root `Cargo.toml` workspace dep | root `Cargo.toml` workspace dep |
| `registry/Cargo.toml` dep + match arm | `registry`: one entry in `PROGRAMS` |
| `Dockerfile` COPY line | |
| `lib.rs` adapter + installer, `handler.rs`, `rules/mod.rs` | `lib.rs` with `spec()`, `parser.rs`, `rules/*.rs` |
| README list | README list (or generated from `PROGRAMS`) |

**Estimated deletion**: about 1,100 lines of duplicated adapter, handler, installer and
`rules/mod.rs` code across the seven crates, replaced by roughly 500 lines in `interface`.

---

## 4. Decisions and rejected alternatives

| Decision | Alternative rejected | Why |
|---|---|---|
| Dynamic prefilter lives on `ProgramDeps` | `bootstrap` returns a `Prefilter` | Conflates cache seeding with subscription shaping; three jup_lend rules would return the same set. |
| Explicit `PROGRAMS` array in `registry` | `inventory` / `linkme` auto-registration | Static musl build and greppability; one line per program is cheap. |
| Builder-style `ProgramSpec` | proc-macro DSL | Builder is plain Rust, IDE-navigable, no extra crate. |
| One crate per program stays | single `programs` crate | Codama `include_shipstern_parser!` and per-crate `build.rs`; compile isolation. |
| Two traits, `GrpcRule` and `RpcRule`, over a shared `RuleDef` | one `RuleDef` with `EventSource` on the ctx and `sweep` / `poll_interval` both as defaulted methods | The feeds differ in what a rule declares (prefilter vs account set + cadence), what it receives (tx sig vs response slot) and what periodic work means. One trait gives every rule the other feed's no-op defaults and keeps the dummy update (R10). |
| Feed chosen by trait, enforced at registration | `data_source()` on the rule, partitioned at runtime | `StreamRules::register` / `PollRules::register` reject the wrong trait at compile time; pyth's runtime partition becomes two entries in `spec()`. |
| Reserves subscribed by owner + memcmp | keep per-pubkey list from bootstrap | Picks up new reserves live; shipstern already supports it. |

---

## 5. Phases

Each phase is one PR that compiles, passes `make lint`, and leaves production behaviour unchanged.

1. **Interface v2 + squads_v4.** `Prefilter`, `RuleDef`, `GrpcRule`, `ProgramDeps`,
   `StreamRules`, `RuleHandler`, `StreamCtx`, `Alert` builder, `StateCache`, `interface::testing`.
   `ProgramSpec` with `account`/`instruction`/`transaction` kinds. Migrate squads_v4.
   Old trait stays; the registry returns either shape. Add `solana-sentinel check` (validate only).
2. **jup_lend.** `JupLendCache` as `Deps`, vaults dynamic, reserves owner + memcmp.
   Add `Sweep` kind and port the liquidation rule from `naga/secon-175` onto it.
3. **Remaining programs.** spl_token, bpf_loader (adopt `StateCache`), sentinel (framework
   `RawInstruction`/`RawTransaction` pipelines, delete `matches_own_prefilter`), pyth and
   jup_usd (`RpcRule`, `PollRules`, `PollSource`, `PollLoop`; delete both `monitor.rs` and
   the dummy update), `system` program for slot health. Delete the old trait, installers,
   `common-rules` folds into a `RawInstruction` rule set. Concurrent bootstrap, config-order iteration.
4. **Onboarding.** `programs/_template`, `CONTRIBUTING.md`, Dockerfile glob, workspace
   tests in CI, `stdout` sink, optional Redis/heartbeat, README refresh.
5. **Optional.** `ProgramDeps::refresh_interval` pushing updated dynamic prefilters through
   the retained `RuntimeHandle`.

---

## 6. Open questions for the maintainers

1. Should `SEVERITY` be a `RuleDef` const with per-alert override, or stay entirely per alert as today?
   Recommendation: const default, override allowed. It makes `rules.toml` docs generatable.
2. Do we want `rules.toml` to allow a per-rule `severity` override by operators? It's cheap
   once `SEVERITY` exists; Betterstack routing already depends on severity.
3. Is `system` acceptable as the program name for service-health rules, or should it be `solana-sentinel`?
