//! Replay source: streams a recorded slot window (see [`format`]) into the runtime in
//! original slot order, at original speed or a multiple of it, so recorded incidents run
//! through the whole service — every pipeline, rule, reactor, gate and sink — as if live.
//!
//! Per slot of the window, in order: a slot update (`processed`), then each recorded
//! transaction followed by the account post-states it rebuilt ([`accounts`]). Slot `s`
//! goes out at `start + (s - first_slot) · slot_ms / speed`; `slot_ms` is 400 ms nominal
//! or, with `timing = "block_time"`, the window's mean slot time from recorded block
//! times. Each update is matched against the current filter set like the Kafka and
//! websocket sources ([`crate::matching`]), including filter sets published mid-replay.
//!
//! With `emit_log` set, every emitted slot and transaction is appended there with its
//! wall-clock time (`unix_us`), which is what drill reports measure latency from.
//! After the last slot the source waits `linger_seconds` (sweeps, RPC follow-ups,
//! reactors and sinks finish) and completes; the service then stops cleanly.

pub mod accounts;
pub mod format;
pub mod record;

use crate::convert::transaction_update;
use crate::kafka::next_filters;
use crate::matching::match_update;
use accounts::{writable_flags, ReconstructStats, Reconstructor, TxAccounts};
use anyhow::Context;
use async_trait::async_trait;
use format::Recording;
use serde::Deserialize;
use shipstern::sources::{
    FilterUpdateSource, FromConfig, SourceContext, SourceExitStatus, SourceTrait,
};
use solana_signature::Signature;
use std::collections::HashSet;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::Instant;
use tracing::{info, warn};
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::geyser::{SubscribeUpdate, SubscribeUpdateSlot};

pub const NOMINAL_SLOT_MS: f64 = 400.0;
/// Shortest block-time span the mean slot time is taken from; below it, nominal.
const MIN_BLOCK_TIME_SPAN_SLOTS: u64 = 50;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum Timing {
    /// `slot_ms` per slot.
    Nominal,
    /// The window's mean slot time from recorded block times (whole seconds), clamped
    /// to 200–1000 ms; nominal when the window is too short to measure.
    BlockTime,
}

#[derive(Debug, Clone, Deserialize, clap::Args)]
pub struct ReplayConfig {
    /// Replay file (JSON lines, see `replay::format`).
    #[arg(long)]
    pub path: PathBuf,
    /// 1.0 = original speed, 2.0 = twice as fast; 0 = as fast as the pipelines take it.
    #[arg(long, default_value_t = 1.0)]
    #[serde(default = "defaults::speed")]
    pub speed: f64,
    #[arg(long, value_enum, default_value_t = Timing::BlockTime)]
    #[serde(default = "defaults::timing")]
    pub timing: Timing,
    /// Slot time for `timing = "nominal"`, and the fallback for `block_time`.
    #[arg(long, default_value_t = NOMINAL_SLOT_MS)]
    #[serde(default = "defaults::slot_ms")]
    pub slot_ms: f64,
    /// Wait after the last slot before completing.
    #[arg(long, default_value_t = 30)]
    #[serde(default = "defaults::linger_seconds")]
    pub linger_seconds: u64,
    /// Emit each rebuildable account's pre-state (first sight in the window) at the first
    /// slot, so account rules start from the window's state rather than today's.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    #[serde(default = "defaults::yes")]
    pub initial_state: bool,
    /// Rebuild account post-states from transaction metadata (see `replay::accounts`).
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    #[serde(default = "defaults::yes")]
    pub reconstruct_accounts: bool,
    /// Append one JSON line per emitted slot / transaction, with its wall-clock time.
    #[arg(long)]
    #[serde(default)]
    pub emit_log: Option<PathBuf>,
}

mod defaults {
    pub const fn speed() -> f64 {
        1.0
    }
    pub const fn timing() -> super::Timing {
        super::Timing::BlockTime
    }
    pub const fn slot_ms() -> f64 {
        super::NOMINAL_SLOT_MS
    }
    pub const fn linger_seconds() -> u64 {
        30
    }
    pub const fn yes() -> bool {
        true
    }
}

/// What an emitted update was, for the emit log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Item {
    Slot,
    Transaction(String),
    Account,
}

#[derive(Debug)]
pub struct Scheduled {
    pub slot: u64,
    pub item: Item,
    pub update: SubscribeUpdate,
}

/// A recording turned into the update sequence, with its pacing.
#[derive(Debug)]
pub struct Schedule {
    pub name: String,
    pub first_slot: u64,
    pub last_slot: u64,
    pub slot_ms: f64,
    pub items: Vec<Scheduled>,
    pub transactions: usize,
    pub undecodable: usize,
    pub stats: ReconstructStats,
}

impl Schedule {
    pub fn build(rec: &Recording, cfg: &ReplayConfig) -> Self {
        let first = rec.header.first_slot;
        let last = rec.header.last_slot;
        let slot_ms = match cfg.timing {
            Timing::Nominal => cfg.slot_ms,
            Timing::BlockTime => block_time_slot_ms(rec).unwrap_or(cfg.slot_ms),
        };
        let mut recon = Reconstructor::new(&rec.snapshots);
        let mut items = Vec::new();
        let mut undecodable = 0;
        let txs = rec
            .transactions
            .iter()
            .filter(|t| (first..=last).contains(&t.slot));

        // Convert once; the initial-state pass needs every transaction before the first slot.
        let mut converted = Vec::new();
        for t in txs {
            let sig = Signature::from_str(&t.signature)
                .map(|s| s.as_ref().to_vec())
                .unwrap_or_default();
            match transaction_update(t.slot, &sig, &t.transaction.transaction) {
                Some(c) => converted.push((t.signature.clone(), c)),
                None => undecodable += 1,
            }
        }

        let mut initial = Vec::new();
        if cfg.reconstruct_accounts && cfg.initial_state {
            let mut seen = HashSet::new();
            for (_, c) in &converted {
                if let Some(view) = tx_accounts(c) {
                    initial.extend(recon.pre_states(&view.as_ref(first), &mut seen));
                }
            }
        }
        let mut converted = converted.into_iter().peekable();
        for slot in first..=last {
            items.push(Scheduled {
                slot,
                item: Item::Slot,
                update: slot_update(slot),
            });
            if slot == first {
                items.extend(std::mem::take(&mut initial).into_iter().map(|a| Scheduled {
                    slot,
                    item: Item::Account,
                    update: wrap(UpdateOneof::Account(a)),
                }));
            }
            while converted.peek().is_some_and(|(_, c)| c.update.slot == slot) {
                let Some((sig, c)) = converted.next() else {
                    break;
                };
                let accounts = if cfg.reconstruct_accounts {
                    tx_accounts(&c)
                        .map(|v| recon.post_states(&v.as_ref(slot)))
                        .unwrap_or_default()
                } else {
                    Vec::new()
                };
                items.push(Scheduled {
                    slot,
                    item: Item::Transaction(sig),
                    update: wrap(UpdateOneof::Transaction(c.update)),
                });
                items.extend(accounts.into_iter().map(|a| Scheduled {
                    slot,
                    item: Item::Account,
                    update: wrap(UpdateOneof::Account(a)),
                }));
            }
        }
        Self {
            name: rec.header.name.clone(),
            first_slot: first,
            last_slot: last,
            slot_ms,
            transactions: items
                .iter()
                .filter(|i| matches!(i.item, Item::Transaction(_)))
                .count(),
            items,
            undecodable,
            stats: recon.stats,
        }
    }

    /// Offset of `slot` from the replay start at `speed` (`None`: unpaced).
    pub fn offset(&self, slot: u64, speed: f64) -> Option<Duration> {
        (speed > 0.0).then(|| {
            Duration::from_secs_f64(
                (slot.saturating_sub(self.first_slot)) as f64 * self.slot_ms / speed / 1000.0,
            )
        })
    }
}

/// Mean slot time over the window from the earliest and latest recorded block times.
fn block_time_slot_ms(rec: &Recording) -> Option<f64> {
    let (s0, t0) = rec.block_times.first_key_value()?;
    let (s1, t1) = rec.block_times.last_key_value()?;
    if s1 - s0 < MIN_BLOCK_TIME_SPAN_SLOTS {
        return None;
    }
    let ms = (t1 - t0) as f64 * 1000.0 / (s1 - s0) as f64;
    Some(ms.clamp(200.0, 1000.0))
}

struct OwnedTxAccounts<'a> {
    keys: Vec<solana_pubkey::Pubkey>,
    writable: Vec<bool>,
    meta: &'a yellowstone_grpc_proto::solana::storage::confirmed_block::TransactionStatusMeta,
    signature: &'a [u8],
}

impl OwnedTxAccounts<'_> {
    fn as_ref(&self, slot: u64) -> TxAccounts<'_> {
        TxAccounts {
            keys: &self.keys,
            writable: &self.writable,
            meta: self.meta,
            signature: self.signature,
            slot,
        }
    }
}

fn tx_accounts(c: &crate::convert::ConvertedTransaction) -> Option<OwnedTxAccounts<'_>> {
    let info = c.update.transaction.as_ref()?;
    let meta = info.meta.as_ref()?;
    // A failed transaction writes nothing but the fee payer's lamports.
    let message = info.transaction.as_ref()?.message.as_ref()?;
    let header = message.header.as_ref()?;
    let mut writable = writable_flags(
        header.num_required_signatures,
        header.num_readonly_signed_accounts,
        header.num_readonly_unsigned_accounts,
        message.account_keys.len(),
        meta.loaded_writable_addresses.len(),
        meta.loaded_readonly_addresses.len(),
    );
    if c.failed {
        writable.iter_mut().skip(1).for_each(|w| *w = false);
    }
    Some(OwnedTxAccounts {
        keys: c.account_keys.clone(),
        writable,
        meta,
        signature: &info.signature,
    })
}

fn slot_update(slot: u64) -> SubscribeUpdate {
    wrap(UpdateOneof::Slot(SubscribeUpdateSlot {
        slot,
        parent: slot.checked_sub(1),
        status: 0,
        dead_error: None,
    }))
}

const fn wrap(update: UpdateOneof) -> SubscribeUpdate {
    SubscribeUpdate {
        filters: Vec::new(),
        update_oneof: Some(update),
        created_at: None,
    }
}

fn unix_us() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros())
        .unwrap_or_default()
}

/// Append-only emit log; a write failure is logged once and the replay goes on.
struct EmitLog {
    out: Option<std::io::BufWriter<std::fs::File>>,
}

impl EmitLog {
    fn open(path: Option<&Path>) -> anyhow::Result<Self> {
        let out = path
            .map(|p| {
                std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(p)
                    .with_context(|| format!("cannot open emit log {}", p.display()))
            })
            .transpose()?
            .map(std::io::BufWriter::new);
        Ok(Self { out })
    }

    fn write(&mut self, line: &serde_json::Value) {
        let Some(out) = self.out.as_mut() else { return };
        let res = serde_json::to_writer(&mut *out, line)
            .map_err(std::io::Error::from)
            .and_then(|()| out.write_all(b"\n"))
            .and_then(|()| out.flush());
        if let Err(e) = res {
            warn!(error = %e, "replay emit log write failed; no more emit log lines");
            self.out = None;
        }
    }
}

#[derive(Debug)]
pub struct ReplaySource {
    config: ReplayConfig,
}

impl ReplaySource {
    async fn run(&self, ctx: SourceContext) -> anyhow::Result<SourceExitStatus> {
        let SourceContext {
            filters,
            tx,
            mut filter_updates,
            ..
        } = ctx;
        let cfg = self.config.clone();
        let schedule = tokio::task::spawn_blocking(move || {
            Recording::load(&cfg.path).map(|rec| (Schedule::build(&rec, &cfg), rec.header))
        })
        .await??;
        let (mut schedule, header) = schedule;
        let speed = self.config.speed;
        info!(
            name = %schedule.name,
            first_slot = schedule.first_slot,
            last_slot = schedule.last_slot,
            slots = schedule.last_slot - schedule.first_slot + 1,
            transactions = schedule.transactions,
            undecodable = schedule.undecodable,
            updates = schedule.items.len(),
            slot_ms = schedule.slot_ms,
            speed,
            stats = ?schedule.stats,
            "Replay loaded"
        );
        let mut log = EmitLog::open(self.config.emit_log.as_deref())?;
        let start = Instant::now();
        log.write(&serde_json::json!({
            "event": "start",
            "name": schedule.name,
            "first_slot": schedule.first_slot,
            "last_slot": schedule.last_slot,
            "slot_ms": schedule.slot_ms,
            "speed": speed,
            "exploit_signatures": header.exploit_signatures,
            "transactions": schedule.transactions,
            "stats": format!("{:?}", schedule.stats),
            "unix_us": unix_us(),
        }));

        let mut current = filters;
        let items = std::mem::take(&mut schedule.items);
        for item in items {
            if let Some(at) = schedule.offset(item.slot, speed) {
                let deadline = start + at;
                loop {
                    tokio::select! {
                        () = tokio::time::sleep_until(deadline) => break,
                        Some(next) = next_filters(&mut filter_updates) => {
                            current = next;
                            info!("Applied filter update to replay matching");
                        }
                    }
                }
            }
            let matched = match_update(&current, item.update);
            let filters_hit: Vec<String> = matched
                .as_ref()
                .map(|u| u.filters.clone())
                .unwrap_or_default();
            if let Some(update) = matched {
                if tx.send(Ok(update)).await.is_err() {
                    return Ok(SourceExitStatus::ReceiverDropped);
                }
            }
            match &item.item {
                Item::Slot => log.write(&serde_json::json!({
                    "event": "slot", "slot": item.slot, "unix_us": unix_us(),
                })),
                Item::Transaction(sig) => log.write(&serde_json::json!({
                    "event": "tx", "slot": item.slot, "signature": sig,
                    "filters": filters_hit, "unix_us": unix_us(),
                })),
                Item::Account => {}
            }
        }
        log.write(&serde_json::json!({ "event": "end", "unix_us": unix_us() }));
        info!(
            elapsed_ms = start.elapsed().as_millis() as u64,
            linger_seconds = self.config.linger_seconds,
            "Replay finished; lingering before completing"
        );
        let linger = tokio::time::sleep(Duration::from_secs(self.config.linger_seconds));
        tokio::pin!(linger);
        loop {
            tokio::select! {
                () = &mut linger => break,
                Some(next) = next_filters(&mut filter_updates) => current = next,
            }
        }
        drop(current);
        Ok(SourceExitStatus::Completed)
    }
}

#[async_trait]
impl SourceTrait for ReplaySource {
    async fn connect(&self, ctx: SourceContext) -> Result<SourceExitStatus, shipstern::Error> {
        self.run(ctx)
            .await
            .map_err(|e| shipstern::Error::Other(format!("replay: {e:#}").into()))
    }
}

impl FromConfig for ReplaySource {
    type Config = ReplayConfig;

    fn from_config(config: Self::Config) -> Self {
        Self { config }
    }
}

impl FilterUpdateSource for ReplaySource {}

#[cfg(test)]
mod tests;
