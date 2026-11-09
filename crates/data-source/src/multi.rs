//! Composite source: N child sources (any mix of Yellowstone gRPC, websocket,
//! Kafka) subscribed with the same filters, merged into one stream.
//!
//! - **Fan-out**: every child gets the runtime's filters at connect and every
//!   later filter update.
//! - **Dedup**: transactions by signature, account updates by
//!   `(pubkey, slot, hash of lamports/owner/executable/data)`,
//!   slots by `(slot, status)`. The first delivery is forwarded; a later
//!   copy is forwarded only for filter names the first did not carry (the
//!   sources tag filters independently), restricted to those names. Rules
//!   therefore see each event once per filter, from whichever child was first.
//!   Write versions are not part of the key: PubSub has none, so they could
//!   never match across providers.
//! - **Divergence**: per item (transaction signature, or account
//!   `(pubkey, slot)`), which children delivered it and how long after the
//!   first. When the grace window closes, a child that was connected and past
//!   the item's slot but never delivered it counts it as missing. Counts,
//!   slots and delays land in [`SourceHealth`], read by the
//!   `health/source_divergence` rule and exported as metrics.
//! - **Isolation**: each child runs in its own task. One that fails, ends or
//!   panics is restarted with backoff; the others keep streaming. Errors a
//!   child puts on its channel are logged and dropped, never forwarded, since
//!   the runtime stops on the first error it receives.

use crate::kafka::next_filters;
use crate::{
    KafkaConfig, KafkaSource, WebsocketConfig, WebsocketSource, YellowstoneGrpcConfig,
    YellowstoneGrpcSource,
};
use async_trait::async_trait;
use observability::source_health::{DeliveryCounts, SourceHealth};
use serde::Deserialize;
use shipstern::sources::{
    FilterUpdateSource, FromConfig, SourceContext, SourceExitStatus, SourceTrait,
};
use shipstern_core::Filters;
use std::collections::hash_map::{DefaultHasher, Entry};
use std::collections::{HashMap, VecDeque};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};
use utility::redact::redact_urls;
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::geyser::SubscribeUpdate;
use yellowstone_grpc_proto::tonic;

/// A bitmask tracks which children delivered an item.
pub const MAX_CHILDREN: usize = 64;
const SWEEP_INTERVAL: Duration = Duration::from_secs(1);
const RESTART_BACKOFF_MIN: Duration = Duration::from_secs(1);
const RESTART_BACKOFF_MAX: Duration = Duration::from_secs(30);
/// A child that ran this long before ending restarts with the minimum backoff again.
const RESTART_BACKOFF_RESET: Duration = Duration::from_secs(60);
const CHILD_CHANNEL: usize = 4096;

type Item = Result<SubscribeUpdate, tonic::Status>;

fn default_grace_seconds() -> u64 {
    30
}
fn default_retention_seconds() -> u64 {
    300
}
fn default_max_entries() -> usize {
    1_000_000
}

#[derive(Debug, Clone, Deserialize, clap::Args)]
pub struct MultiConfig {
    /// Child sources, in order; names label metrics and alerts.
    #[arg(skip)]
    pub sources: Vec<ChildConfig>,
    /// How long after the first delivery the other children have to deliver the same
    /// item before it counts as missing from them.
    #[arg(long, default_value_t = default_grace_seconds())]
    #[serde(default = "default_grace_seconds")]
    pub grace_seconds: u64,
    /// How long delivered keys are remembered for dedup; a copy arriving later is
    /// forwarded again. Must cover the slowest child's delay.
    #[arg(long, default_value_t = default_retention_seconds())]
    #[serde(default = "default_retention_seconds")]
    pub retention_seconds: u64,
    /// Cap on remembered keys (dedup and divergence each); the oldest go first.
    #[arg(long, default_value_t = default_max_entries())]
    #[serde(default = "default_max_entries")]
    pub max_entries: usize,
    /// Where per-child health is published; the service shares it with the health rules.
    #[arg(skip)]
    #[serde(skip)]
    pub health: SourceHealth,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ChildConfig {
    pub name: String,
    #[serde(flatten)]
    pub source: ChildSourceConfig,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ChildSourceConfig {
    YellowstoneGrpc(YellowstoneGrpcConfig),
    Websocket(WebsocketConfig),
    Kafka(KafkaConfig),
}

impl ChildSourceConfig {
    fn build(self) -> Arc<dyn SourceTrait> {
        match self {
            Self::YellowstoneGrpc(c) => Arc::new(YellowstoneGrpcSource::from_config(c)),
            Self::Websocket(c) => Arc::new(WebsocketSource::from_config(c)),
            Self::Kafka(c) => Arc::new(KafkaSource::from_config(c)),
        }
    }
}

impl MultiConfig {
    /// Names unique and non-empty, 1..=64 children.
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.sources.is_empty(),
            "multi data source needs at least one child in `sources`"
        );
        anyhow::ensure!(
            self.sources.len() <= MAX_CHILDREN,
            "multi data source takes at most {MAX_CHILDREN} children"
        );
        let mut seen = std::collections::HashSet::new();
        for c in &self.sources {
            anyhow::ensure!(!c.name.is_empty(), "multi data source child without a name");
            anyhow::ensure!(
                seen.insert(c.name.as_str()),
                "multi data source child name `{}` is used twice",
                c.name
            );
        }
        anyhow::ensure!(self.grace_seconds > 0, "grace_seconds must be > 0");
        anyhow::ensure!(
            self.retention_seconds >= self.grace_seconds,
            "retention_seconds must be >= grace_seconds"
        );
        Ok(())
    }
}

/// See the module docs.
pub struct MultiSource {
    children: Vec<(String, Arc<dyn SourceTrait>)>,
    settings: MergeSettings,
    health: SourceHealth,
    validation: Option<String>,
}

/// Names only: child configs carry endpoints and tokens.
impl std::fmt::Debug for MultiSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MultiSource")
            .field(
                "children",
                &self
                    .children
                    .iter()
                    .map(|(n, _)| n.as_str())
                    .collect::<Vec<_>>(),
            )
            .field("settings", &self.settings)
            .finish_non_exhaustive()
    }
}

impl MultiSource {
    /// Build from already-constructed children (tests, custom transports).
    pub fn from_children(
        children: Vec<(String, Arc<dyn SourceTrait>)>,
        settings: MergeSettings,
        health: SourceHealth,
    ) -> Self {
        let names: Vec<String> = children.iter().map(|(n, _)| n.clone()).collect();
        health.register(&names);
        Self {
            children,
            settings,
            health,
            validation: None,
        }
    }

    pub fn health(&self) -> &SourceHealth {
        &self.health
    }
}

impl FromConfig for MultiSource {
    type Config = MultiConfig;

    fn from_config(config: Self::Config) -> Self {
        let validation = config.validate().err().map(|e| format!("{e:#}"));
        let settings = MergeSettings {
            grace: Duration::from_secs(config.grace_seconds),
            retention: Duration::from_secs(config.retention_seconds),
            max_entries: config.max_entries,
        };
        let children = config
            .sources
            .into_iter()
            .take(MAX_CHILDREN)
            .map(|c| (c.name, c.source.build()))
            .collect();
        let mut source = Self::from_children(children, settings, config.health);
        source.validation = validation;
        source
    }
}

impl FilterUpdateSource for MultiSource {}

/// What a child task reports to the merge loop.
enum Msg {
    Connected { child: usize, restart: bool },
    Update { child: usize, item: Box<Item> },
    Disconnected { child: usize, reason: String },
}

#[async_trait]
impl SourceTrait for MultiSource {
    async fn connect(&self, ctx: SourceContext) -> Result<SourceExitStatus, shipstern::Error> {
        if let Some(e) = &self.validation {
            return Err(shipstern::Error::Other(e.clone().into()));
        }
        let SourceContext {
            filters,
            tx,
            mut filter_updates,
            ..
        } = ctx;
        info!(
            children = ?self.children.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>(),
            grace_secs = self.settings.grace.as_secs(),
            "Multi source starting"
        );
        // One slot every child subscribes to; a restarted child reads the newest set from it.
        let (fan_out, _) = watch::channel(filters);
        let (merged_tx, mut merged_rx) = mpsc::channel::<Msg>(CHILD_CHANNEL);
        let tasks: Vec<JoinHandle<()>> = self
            .children
            .iter()
            .enumerate()
            .map(|(i, (name, source))| {
                tokio::spawn(supervise(
                    i,
                    name.clone(),
                    Arc::clone(source),
                    fan_out.clone(),
                    merged_tx.clone(),
                ))
            })
            .collect();
        drop(merged_tx);

        let mut merger = Merger::new(self.children.len(), self.settings, self.health.clone());
        let mut sweep = tokio::time::interval(SWEEP_INTERVAL);
        sweep.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        let status = loop {
            tokio::select! {
                Some(next) = next_filters(&mut filter_updates) => {
                    debug!("Multi source fanning out a filter update");
                    fan_out.send_replace(next);
                }
                _ = sweep.tick() => merger.sweep(Instant::now()),
                msg = merged_rx.recv() => {
                    let Some(msg) = msg else {
                        // Every supervisor gone: they only stop once we do, so this is a bug.
                        break SourceExitStatus::Error("all multi-source children stopped".into());
                    };
                    match msg {
                        Msg::Connected { child, restart } => merger.connected(child, restart, Instant::now()),
                        Msg::Disconnected { child, reason } => merger.disconnected(child, &reason),
                        Msg::Update { child, item } => match *item {
                        Ok(update) => {
                            if let Some(out) = merger.on_update(child, update, Instant::now()) {
                                if tx.send(Ok(out)).await.is_err() {
                                    break SourceExitStatus::ReceiverDropped;
                                }
                            }
                        }
                        Err(status) => {
                            warn!(
                                source = %self.children[child].0,
                                code = ?status.code(),
                                error = %redact_urls(status.message()),
                                "Multi-source child sent an error; dropped"
                            );
                        }
                        },
                    }
                }
            }
        };
        for t in &tasks {
            t.abort();
        }
        Ok(status)
    }
}

/// Run child `i` until the merge loop goes away, restarting it whenever it stops.
async fn supervise(
    i: usize,
    name: String,
    source: Arc<dyn SourceTrait>,
    filters: watch::Sender<Filters>,
    merged: mpsc::Sender<Msg>,
) {
    let mut backoff = RESTART_BACKOFF_MIN;
    let mut restart = false;
    loop {
        if merged
            .send(Msg::Connected { child: i, restart })
            .await
            .is_err()
        {
            return;
        }
        let started = Instant::now();
        let (child_tx, mut child_rx) = mpsc::channel::<Item>(CHILD_CHANNEL);
        let mut ctx = SourceContext::new(filters.borrow().clone(), child_tx);
        ctx.filter_updates = filters.subscribe();
        let run = {
            let source = Arc::clone(&source);
            tokio::spawn(async move { source.connect(ctx).await })
        };
        let pump = async {
            while let Some(item) = child_rx.recv().await {
                if merged
                    .send(Msg::Update {
                        child: i,
                        item: Box::new(item),
                    })
                    .await
                    .is_err()
                {
                    return false;
                }
            }
            true
        };
        let (result, merge_alive) = tokio::join!(run, pump);
        if !merge_alive || merged.is_closed() {
            return;
        }
        let reason = match result {
            Ok(Ok(status)) => format!("stream ended: {status:?}"),
            Ok(Err(e)) => format!("stream failed: {}", redact_urls(&e.to_string())),
            Err(e) if e.is_panic() => "stream panicked".to_string(),
            Err(e) => format!("stream task stopped: {e}"),
        };
        if started.elapsed() >= RESTART_BACKOFF_RESET {
            backoff = RESTART_BACKOFF_MIN;
        }
        error!(source = %name, %reason, retry_in_secs = backoff.as_secs(), "Multi-source child stopped; restarting");
        if merged
            .send(Msg::Disconnected {
                child: i,
                reason: reason.clone(),
            })
            .await
            .is_err()
        {
            return;
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(RESTART_BACKOFF_MAX);
        restart = true;
    }
}

/// Dedup and divergence tuning; see [`MultiConfig`].
#[derive(Debug, Clone, Copy)]
pub struct MergeSettings {
    pub grace: Duration,
    pub retention: Duration,
    pub max_entries: usize,
}

impl Default for MergeSettings {
    fn default() -> Self {
        Self {
            grace: Duration::from_secs(default_grace_seconds()),
            retention: Duration::from_secs(default_retention_seconds()),
            max_entries: default_max_entries(),
        }
    }
}

/// What dedup keys on.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum DedupKey {
    Tx(Vec<u8>),
    Account {
        pubkey: Vec<u8>,
        slot: u64,
        hash: u64,
    },
    Slot {
        slot: u64,
        status: i32,
    },
}

/// What divergence compares on: an account is covered by any update at that slot,
/// since a processed stream reports every write and PubSub only a slot's last.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum CoverKey {
    Tx(Vec<u8>),
    Account(Vec<u8>, u64),
}

#[derive(Debug)]
struct Cover {
    at: Instant,
    slot: u64,
    seen: u64,
    evaluated: bool,
}

#[derive(Debug, Clone, Copy, Default)]
struct ChildState {
    up: bool,
    /// First slot delivered since the last (re)connect: items before it are not
    /// expected from this child (a replaying sibling, or our own downtime).
    start_slot: Option<u64>,
}

/// The pure merge logic: no I/O, clock passed in. One per `connect`.
#[derive(Debug)]
pub struct Merger {
    settings: MergeSettings,
    health: SourceHealth,
    children: Vec<ChildState>,
    dedup: HashMap<DedupKey, Vec<String>>,
    dedup_order: VecDeque<(Instant, DedupKey)>,
    covers: HashMap<CoverKey, Cover>,
    to_evaluate: VecDeque<(Instant, CoverKey)>,
    to_expire: VecDeque<(Instant, CoverKey)>,
}

impl Merger {
    pub fn new(children: usize, settings: MergeSettings, health: SourceHealth) -> Self {
        Self {
            settings,
            health,
            children: vec![ChildState::default(); children.min(MAX_CHILDREN)],
            dedup: HashMap::new(),
            dedup_order: VecDeque::new(),
            covers: HashMap::new(),
            to_evaluate: VecDeque::new(),
            to_expire: VecDeque::new(),
        }
    }

    pub fn connected(&mut self, child: usize, restart: bool, now: Instant) {
        if let Some(c) = self.children.get_mut(child) {
            *c = ChildState {
                up: true,
                start_slot: None,
            };
        }
        self.health.connected(child, restart, now);
    }

    pub fn disconnected(&mut self, child: usize, reason: &str) {
        if let Some(c) = self.children.get_mut(child) {
            c.up = false;
        }
        self.health.disconnected(child, reason);
    }

    /// Account for `update` from `child`; returns what to forward, if anything.
    pub fn on_update(
        &mut self,
        child: usize,
        mut update: SubscribeUpdate,
        now: Instant,
    ) -> Option<SubscribeUpdate> {
        if child >= self.children.len() {
            return None;
        }
        // Pings and anything else without an identity pass through.
        let Some((dedup, cover, slot)) = keys(&update) else {
            return Some(update);
        };
        self.health.delivered_any(child, slot, now);
        if slot > 0 {
            let state = &mut self.children[child];
            if state.start_slot.is_none() {
                state.start_slot = Some(slot);
            }
        }
        if let Some(cover) = cover {
            self.cover(child, cover, slot, now);
        }

        match self.dedup.entry(dedup) {
            Entry::Vacant(v) => {
                self.dedup_order.push_back((now, v.key().clone()));
                v.insert(update.filters.clone());
                self.trim_dedup();
                Some(update)
            }
            Entry::Occupied(mut o) => {
                let forwarded = o.get_mut();
                let new: Vec<String> = update
                    .filters
                    .iter()
                    .filter(|f| !forwarded.contains(f))
                    .cloned()
                    .collect();
                if new.is_empty() {
                    return None;
                }
                forwarded.extend(new.iter().cloned());
                update.filters = new;
                Some(update)
            }
        }
    }

    fn cover(&mut self, child: usize, key: CoverKey, slot: u64, now: Instant) {
        let bit = 1u64 << child;
        let mut delta = DeliveryCounts {
            delivered: 1,
            ..Default::default()
        };
        match self.covers.entry(key) {
            Entry::Vacant(v) => {
                let key = v.key().clone();
                v.insert(Cover {
                    at: now,
                    slot,
                    seen: bit,
                    evaluated: false,
                });
                self.to_evaluate.push_back((now, key.clone()));
                self.to_expire.push_back((now, key));
                delta.first = 1;
            }
            Entry::Occupied(mut o) => {
                let c = o.get_mut();
                if c.seen & bit == 0 {
                    c.seen |= bit;
                    if c.evaluated {
                        delta.late = 1;
                    }
                    self.health
                        .delay(child, now.saturating_duration_since(c.at));
                }
            }
        }
        self.health.count(child, delta, now);
        self.trim_covers(now);
    }

    /// Close grace windows and forget expired keys. Call about once a second.
    pub fn sweep(&mut self, now: Instant) {
        while let Some((at, _)) = self.to_evaluate.front() {
            if now.saturating_duration_since(*at) < self.settings.grace {
                break;
            }
            let (_, key) = self.to_evaluate.pop_front().expect("front checked");
            self.evaluate(&key, now);
        }
        while let Some((at, _)) = self.to_expire.front() {
            if now.saturating_duration_since(*at) < self.settings.retention {
                break;
            }
            let (_, key) = self.to_expire.pop_front().expect("front checked");
            self.covers.remove(&key);
        }
        while let Some((at, _)) = self.dedup_order.front() {
            if now.saturating_duration_since(*at) < self.settings.retention {
                break;
            }
            let (_, key) = self.dedup_order.pop_front().expect("front checked");
            self.dedup.remove(&key);
        }
        self.health.publish_slots();
    }

    fn evaluate(&mut self, key: &CoverKey, now: Instant) {
        let Some(cover) = self.covers.get_mut(key) else {
            return;
        };
        if cover.evaluated {
            return;
        }
        cover.evaluated = true;
        let (seen, slot) = (cover.seen, cover.slot);
        for (j, state) in self.children.iter().enumerate() {
            let bit = 1u64 << j;
            let others = seen & !bit != 0;
            let eligible = state.up && state.start_slot.is_some_and(|s| slot >= s);
            if !others || !eligible {
                continue;
            }
            let missing = seen & bit == 0;
            self.health.count(
                j,
                DeliveryCounts {
                    expected: 1,
                    missing: u64::from(missing),
                    ..Default::default()
                },
                now,
            );
        }
    }

    fn trim_dedup(&mut self) {
        while self.dedup.len() > self.settings.max_entries {
            let Some((_, key)) = self.dedup_order.pop_front() else {
                break;
            };
            self.dedup.remove(&key);
        }
    }

    fn trim_covers(&mut self, now: Instant) {
        while self.covers.len() > self.settings.max_entries {
            let Some((_, key)) = self.to_expire.pop_front() else {
                break;
            };
            self.evaluate(&key, now);
            self.covers.remove(&key);
        }
    }
}

/// Dedup key, divergence key and slot of an update; `None` for updates without identity.
fn keys(update: &SubscribeUpdate) -> Option<(DedupKey, Option<CoverKey>, u64)> {
    match update.update_oneof.as_ref()? {
        UpdateOneof::Slot(s) => Some((
            DedupKey::Slot {
                slot: s.slot,
                status: s.status,
            },
            None,
            s.slot,
        )),
        UpdateOneof::Transaction(t) => {
            let sig = t.transaction.as_ref()?.signature.clone();
            Some((DedupKey::Tx(sig.clone()), Some(CoverKey::Tx(sig)), t.slot))
        }
        UpdateOneof::Account(a) => {
            let acc = a.account.as_ref()?;
            let mut h = DefaultHasher::new();
            acc.lamports.hash(&mut h);
            acc.owner.hash(&mut h);
            acc.executable.hash(&mut h);
            acc.data.hash(&mut h);
            Some((
                DedupKey::Account {
                    pubkey: acc.pubkey.clone(),
                    slot: a.slot,
                    hash: h.finish(),
                },
                Some(CoverKey::Account(acc.pubkey.clone(), a.slot)),
                a.slot,
            ))
        }
        _ => None,
    }
}

#[cfg(test)]
#[path = "multi_tests.rs"]
mod tests;
