use parking_lot::RwLock;
use solana_hash::Hash;
use solana_rpc_client::api::config::CommitmentConfig;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::{timeout, MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::warn;

/// Refresh interval, half a slot.
const REFRESH: Duration = Duration::from_millis(200);
const FETCH_TIMEOUT: Duration = Duration::from_secs(1);
/// Older than this, responses fail closed instead of using a hash that may expire in
/// flight: a fresh hash lives ~150 slots (~60 s), so 5 s leaves the tx ~55 s to land.
const MAX_AGE: Duration = Duration::from_secs(5);

/// Latest `confirmed` blockhash and its last valid block height, refreshed in the background
/// so a response never waits on RPC.
#[derive(Clone, Default)]
pub struct BlockhashCache(Arc<RwLock<Option<(Hash, u64, Instant)>>>);

impl BlockhashCache {
    pub fn spawn(rpc: Arc<RpcClient>, cancel: CancellationToken, tracker: &TaskTracker) -> Self {
        let cache = Self::default();
        let writer = cache.clone();
        tracker.spawn(async move {
            let mut tick = tokio::time::interval(REFRESH);
            // A slow call shouldn't trigger a burst of catch-up fetches.
            tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    () = cancel.cancelled() => return,
                    _ = tick.tick() => {}
                }
                // Bounded well under MAX_AGE, so one slow call doesn't leave the cache stale.
                let fetch = timeout(
                    FETCH_TIMEOUT,
                    rpc.get_latest_blockhash_with_commitment(CommitmentConfig::confirmed()),
                );
                let fetched = tokio::select! {
                    () = cancel.cancelled() => return,
                    fetched = fetch => fetched,
                };
                match fetched {
                    Ok(Ok((hash, last_valid))) => writer.set(hash, last_valid),
                    Ok(Err(e)) => warn!(error = %e, "riposte blockhash refresh failed"),
                    Err(_) => warn!("riposte blockhash refresh timed out"),
                }
            }
        });
        cache
    }

    pub fn set(&self, hash: Hash, last_valid_block_height: u64) {
        *self.0.write() = Some((hash, last_valid_block_height, Instant::now()));
    }

    /// `(hash, last valid block height)`; `None` when never fetched or stale.
    pub fn get(&self) -> Option<(Hash, u64)> {
        self.0
            .read()
            .filter(|(_, _, at)| at.elapsed() < MAX_AGE)
            .map(|(hash, last_valid, _)| (hash, last_valid))
    }
}
