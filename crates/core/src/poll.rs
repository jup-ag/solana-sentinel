//! The poll feed: one loop per cadence bucket fetching `getMultipleAccounts`
//! (or the source's own async fetch), evaluating the program's [`PollRule`]s.
//!
//! [`PollRule`]: crate::rule::PollRule

use crate::handler::forward_alert;
use crate::rule::PollCtx;
use crate::rule_set::PollBucket;
use crate::BootstrapCtx;
use async_trait::async_trait;
use domain::alert::Alert;
use domain::programs::Program;
use observability::liveness::FeedKind;
use solana_account::Account;
use solana_pubkey::Pubkey;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{error, info, warn};
use utility::rpc::{try_fetch_accounts_with_slot, try_fetch_latest_slot};

/// Turns one fetch into the items poll rules evaluate. One impl per poll-fed program.
#[async_trait]
pub trait PollSource: Send + Sync + 'static {
    type Item: Send + Sync + 'static;

    /// Per-account items or one cross-account snapshot; both are just what this returns.
    fn parse(&self, _fetched: &HashMap<Pubkey, Account>, _ctx: &PollCtx) -> Vec<Self::Item> {
        Vec::new()
    }

    /// One tick: fetch the rules' accounts, then `parse`. Override for sources that need
    /// program scans or follow-up reads; `accounts` is then only what the rules asked for.
    async fn poll(
        &self,
        rpc: &RpcClient,
        accounts: &[Pubkey],
    ) -> anyhow::Result<(Vec<Self::Item>, PollCtx)> {
        let (fetched, ctx) = fetch(rpc, accounts).await?;
        Ok((self.parse(&fetched, &ctx), ctx))
    }
}

/// A clock tick and nothing else: for poll rules that read in-process state (feed liveness,
/// canaries) and must keep running when the RPC is down. Makes no RPC call.
#[derive(Debug, Clone, Copy, Default)]
pub struct TickSource;

/// What a [`TickSource`] hands its rules each tick.
#[derive(Debug, Clone, Copy)]
pub struct Tick;

#[async_trait]
impl PollSource for TickSource {
    type Item = Tick;

    async fn poll(
        &self,
        _rpc: &RpcClient,
        _accounts: &[Pubkey],
    ) -> anyhow::Result<(Vec<Tick>, PollCtx)> {
        Ok((
            vec![Tick],
            PollCtx {
                slot: 0,
                fetched_at: Instant::now(),
            },
        ))
    }
}

/// `MySource` for `my_crate::poll::MySource`; names a poll feed.
fn source_name<S>() -> &'static str {
    std::any::type_name::<S>()
        .rsplit("::")
        .next()
        .unwrap_or("poll")
}

pub fn spawn_poll_loop<S: PollSource>(
    program: Program,
    source: Arc<S>,
    bucket: PollBucket<S::Item>,
    bootstrap: &BootstrapCtx,
    alerts_tx: mpsc::Sender<Alert>,
    cancel_token: CancellationToken,
    tracker: &TaskTracker,
) {
    let PollBucket {
        interval,
        accounts,
        rules,
    } = bucket;
    let rpc = Arc::clone(&bootstrap.rpc);
    let feed = bootstrap.liveness.feed(
        program.as_str(),
        FeedKind::Poll,
        &format!("{}:{}s", source_name::<S>(), interval.as_secs()),
        None,
    );
    let rule_feeds: Vec<_> = rules
        .iter()
        .map(|r| {
            bootstrap
                .liveness
                .feed(program.as_str(), FeedKind::Rule, r.id.name, Some(&feed))
        })
        .collect();
    info!(
        program = %program,
        poll_interval_secs = interval.as_secs(),
        accounts = accounts.len(),
        rules = ?rules.iter().map(|r| r.id.name).collect::<Vec<_>>(),
        "Spawning poll loop"
    );

    let mut tick = tokio::time::interval(interval);
    // A slow fetch must not be followed by a burst of catch-up ticks.
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    tracker.spawn(async move {
        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    info!(program = %program, "Cancellation requested, stopping poll loop");
                    break;
                }
                _ = tick.tick() => {
                    let (items, ctx) = match source.poll(&rpc, &accounts).await {
                        Ok(polled) => polled,
                        Err(e) => {
                            let error = format!("{e:#}");
                            error!(program = %program, error, "Poll fetch failed");
                            feed.record_failure(&error);
                            continue;
                        }
                    };
                    feed.record_event(ctx.slot);
                    for rule_feed in &rule_feeds {
                        rule_feed.record_event(ctx.slot);
                    }
                    for item in items {
                        for rule in &rules {
                            for alert in (rule.evaluate)(&item, &ctx) {
                                forward_alert(alert, Some(ctx.slot), None, &alerts_tx).await;
                            }
                        }
                    }
                }
            }
        }
    });
}

/// `getMultipleAccounts` for `accounts` (or just the slot when empty), stamped as a [`PollCtx`].
pub async fn fetch(
    rpc: &RpcClient,
    accounts: &[Pubkey],
) -> anyhow::Result<(HashMap<Pubkey, Account>, PollCtx)> {
    let fetched_at = Instant::now();
    let (map, slot) = if accounts.is_empty() {
        (HashMap::new(), try_fetch_latest_slot(rpc).await?)
    } else {
        let (map, slot) = try_fetch_accounts_with_slot(rpc, accounts, None).await?;
        if map.len() != accounts.len() {
            warn!(
                expected = accounts.len(),
                actual = map.len(),
                "fetched fewer accounts than requested; some alerts may be missed"
            );
        }
        (map, slot)
    };
    Ok((map, PollCtx { slot, fetched_at }))
}
