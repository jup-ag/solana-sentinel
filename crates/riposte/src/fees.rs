use parking_lot::RwLock;
use serde::Deserialize;
use serde_json::json;
use solana_rpc_client::api::request::RpcRequest;
use solana_rpc_client::api::response::RpcPrioritizationFee;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::warn;

const REFRESH: Duration = Duration::from_secs(2);
const MAX_AGE: Duration = Duration::from_secs(10);
/// EMA period in slots; one fetch returns up to 150, enough to warm it up.
const EMA_PERIOD: f64 = 50.0;

#[derive(Debug, Clone, Copy, Deserialize)]
pub struct FeeConfig {
    /// Triton percentile in basis points, 1..=10_000 (5_000 = median).
    pub percentile_bps: u16,
    pub min_micro_lamports: u64,
    pub max_micro_lamports: u64,
}

/// CU price from Triton's percentile `getRecentPrioritizationFees`, refreshed in the background.
#[derive(Clone)]
pub struct PriorityFeeCache {
    cfg: FeeConfig,
    latest: Arc<RwLock<Option<(u64, Instant)>>>,
}

impl PriorityFeeCache {
    pub fn new(cfg: FeeConfig) -> Self {
        Self {
            cfg,
            latest: Arc::default(),
        }
    }

    pub fn spawn(
        cfg: FeeConfig,
        rpc: Arc<RpcClient>,
        cancel: CancellationToken,
        tracker: &TaskTracker,
    ) -> Self {
        let cache = Self::new(cfg);
        let writer = cache.clone();
        // Triton extension: a plain Agave RPC rejects the percentile param.
        let params = json!([[], { "percentile": cfg.percentile_bps }]);
        tracker.spawn(async move {
            let mut tick = tokio::time::interval(REFRESH);
            // A slow call shouldn't trigger a burst of catch-up fetches.
            tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    () = cancel.cancelled() => return,
                    _ = tick.tick() => {}
                }
                let fetch = rpc.send::<Vec<RpcPrioritizationFee>>(
                    RpcRequest::GetRecentPrioritizationFees,
                    params.clone(),
                );
                // The client may retry for minutes; don't hold up shutdown on it.
                let fees = tokio::select! {
                    () = cancel.cancelled() => return,
                    fees = fetch => fees,
                };
                match fees.map(estimate) {
                    Ok(Some(fee)) => writer.set(fee),
                    Ok(None) => warn!("riposte priority fee refresh returned no slots"),
                    Err(e) => warn!(error = %e, "riposte priority fee refresh failed"),
                }
            }
        });
        cache
    }

    pub fn set(&self, micro_lamports: u64) {
        *self.latest.write() = Some((micro_lamports, Instant::now()));
    }

    /// CU price clamped to `[min, max]`; `max` when stale or never fetched, so a response still lands.
    pub fn get(&self) -> u64 {
        let FeeConfig {
            min_micro_lamports: min,
            max_micro_lamports: max,
            ..
        } = self.cfg;
        self.latest
            .read()
            .filter(|(_, at)| at.elapsed() < MAX_AGE)
            // Not `clamp`: it panics on min > max, and `new` is public.
            .map_or(max, |(fee, _)| fee.max(min).min(max))
    }
}

/// EMA50 of the per-slot percentile, oldest slot first (the RPC does not order entries), but
/// never below the newest slot: the EMA lags a spike, and a response must land during one.
/// Recomputed from scratch each fetch, so slots overlapping the last poll aren't counted twice.
fn estimate(mut fees: Vec<RpcPrioritizationFee>) -> Option<u64> {
    fees.sort_unstable_by_key(|f| f.slot);
    let newest = fees.last()?.prioritization_fee;
    let alpha = 2.0 / (EMA_PERIOD + 1.0);
    let mut values = fees.iter().map(|f| f.prioritization_fee as f64);
    let seed = values.next()?;
    let ema = values
        .fold(seed, |ema, x| alpha.mul_add(x - ema, ema))
        .ceil() as u64;
    Some(ema.max(newest))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn estimate_is_ema50_floored_at_newest_slot() {
        let fee = |slot, prioritization_fee| RpcPrioritizationFee {
            slot,
            prioritization_fee,
        };
        // Given newest first. Drop: 51 + 2/51 * (0 - 51) = 49, above the newest 0.
        assert_eq!(estimate(vec![fee(2, 0), fee(1, 51)]), Some(49));
        // Spike: EMA is only 2, so the newest slot's 51 wins.
        assert_eq!(estimate(vec![fee(2, 51), fee(1, 0)]), Some(51));
        let flat: Vec<_> = (1..=150).map(|s| fee(s, 7_000)).collect();
        assert_eq!(estimate(flat), Some(7_000));
        assert_eq!(estimate(Vec::new()), None);
    }
}
