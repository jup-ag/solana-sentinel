use crate::prometheus::metrics::{
    record_grpc_event, record_rpc_fetch_status, record_rpc_latency, GrpcEvent, RpcFetchStatus,
};
use anyhow::bail;
use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use shipstern::Handler;
use shipstern_core::SlotUpdate;
use solana_rpc_client::api::config::CommitmentConfig;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, error, info, warn};

const SLOT_CHECKPOINT_RECORD_INTERVAL: Duration = Duration::from_secs(5); // 5 seconds
const SLOT_CHECKPOINT_MAX_DIFF_THRESHOLD: u64 = 11_000; //  ~70 minutes at 400ms slot time (NOTE: please change this when slot times change)
const SLOT_CHECKPOINT_KEY_REDIS: &str = "slot_checkpoint:last_seen_slot"; // Redis key to store the last seen slot checkpoint.

#[derive(Debug, Clone)]
pub struct FirstGrpcSlotSeenGuard(Arc<OnceLock<()>>);

impl Default for FirstGrpcSlotSeenGuard {
    fn default() -> Self {
        Self::new()
    }
}

impl FirstGrpcSlotSeenGuard {
    pub fn new() -> Self {
        Self(Arc::new(OnceLock::new()))
    }

    /// Marks the first gRPC slot as seen. No-op after the first call.
    pub fn mark(&self) {
        let _ = self.0.set(());
    }

    /// Whether the first gRPC slot has been observed.
    pub fn is_seen(&self) -> bool {
        self.0.get().is_some()
    }
}

/// Read-only, cheaply cloneable view of the last slot seen on the gRPC stream.
/// Handed to rules through the bootstrap context so service health can be a rule.
#[derive(Debug, Clone)]
pub struct GrpcSlotReader {
    last_seen: LastSeenGrpcSlot,
    first_seen: FirstGrpcSlotSeenGuard,
}

impl GrpcSlotReader {
    /// A reader with no stream behind it; `load` stays `None`. For `check` mode and tests.
    pub fn detached() -> Self {
        Self {
            last_seen: LastSeenGrpcSlot::live(),
            first_seen: FirstGrpcSlotSeenGuard::new(),
        }
    }

    /// Last gRPC slot, or `None` until the stream has delivered its first slot.
    pub fn load(&self) -> Option<u64> {
        self.first_seen.is_seen().then(|| self.last_seen.load())
    }
}

#[derive(Debug, Clone)]
pub struct SlotTracker {
    last_seen_grpc_slot: LastSeenGrpcSlot,
    slot_checkpoint: SlotCheckpoint,
    cancel_token: CancellationToken,
    first_grpc_slot_seen: FirstGrpcSlotSeenGuard,
}

impl SlotTracker {
    /// Initialize the slot_tracker: seed the in-memory slot from the checkpoint and
    /// spawn the task that periodically persists it back. Without Redis the
    /// tracker is in-memory only and every start is a fresh start.
    pub async fn try_init(
        rpc: &RpcClient,
        redis_conn: ConnectionManager,
        pod_id: &str,
        cancel_token: CancellationToken,
        tracker: &TaskTracker,
    ) -> anyhow::Result<Self> {
        let slot_checkpoint = SlotCheckpoint::new(redis_conn, pod_id);

        let last_seen_grpc_slot = Self::try_seed_from_checkpoint(rpc, &slot_checkpoint).await?;

        let slot_tracker = Self {
            last_seen_grpc_slot,
            slot_checkpoint,
            cancel_token,
            first_grpc_slot_seen: FirstGrpcSlotSeenGuard::new(),
        };
        slot_tracker.spawn_checkpoint_recorder(tracker);
        Ok(slot_tracker)
    }

    pub fn load_last_seen_grpc_slot(&self) -> u64 {
        self.last_seen_grpc_slot.load()
    }

    pub fn reader(&self) -> GrpcSlotReader {
        GrpcSlotReader {
            last_seen: self.last_seen_grpc_slot.clone(),
            first_seen: self.first_grpc_slot_seen.clone(),
        }
    }

    fn store_last_seen_grpc_slot(&self, slot: u64) {
        self.last_seen_grpc_slot.store(slot);
    }

    // Try to seed the last seen gRPC slot from the checkpoint.
    // If the checkpoint is not available or is too far behind the latest RPC slot, start fresh with a live slot of 0.
    async fn try_seed_from_checkpoint(
        rpc: &RpcClient,
        slot_checkpoint: &SlotCheckpoint,
    ) -> anyhow::Result<LastSeenGrpcSlot> {
        let Some(checkpoint_slot) = slot_checkpoint.read().await else {
            info!("No checkpoint found, starting fresh");
            return Ok(LastSeenGrpcSlot::live());
        };

        let rpc_slot = match fetch_latest_slot_with_retry(rpc).await {
            Ok(slot) => slot,
            Err(e) => {
                bail!("Failed to fetch latest slot from RPC: {}", e);
            }
        };

        if rpc_slot < checkpoint_slot {
            bail!(
                "Checkpoint slot {} is ahead of the latest RPC slot {}; this should not happen",
                checkpoint_slot,
                rpc_slot
            );
        }

        let diff = rpc_slot.abs_diff(checkpoint_slot);
        if diff > SLOT_CHECKPOINT_MAX_DIFF_THRESHOLD {
            warn!(
                "Checkpoint slot {} is too far behind the latest RPC slot {} (diff: {}). Ignoring \
                 checkpoint.",
                checkpoint_slot, rpc_slot, diff
            );
            return Ok(LastSeenGrpcSlot::live());
        }

        info!(
            "Loaded slot from checkpoint: {} (latest RPC slot: {}, diff: {})",
            checkpoint_slot, rpc_slot, diff
        );

        Ok(LastSeenGrpcSlot::seeded(checkpoint_slot))
    }

    fn spawn_checkpoint_recorder(&self, tracker: &TaskTracker) {
        let Self {
            last_seen_grpc_slot,
            slot_checkpoint,
            cancel_token,
            first_grpc_slot_seen,
        } = self.clone();
        info!(
            "Starting slot checkpoint recorder task with interval: {:?}",
            SLOT_CHECKPOINT_RECORD_INTERVAL
        );
        tracker.spawn(async move {
            let mut tick = tokio::time::interval(SLOT_CHECKPOINT_RECORD_INTERVAL);
            loop {
                tokio::select! {
                    _ = cancel_token.cancelled() => {
                        info!("Cancellation requested, stopping slot checkpoint recorder task");
                        break;
                    }
                    _ = tick.tick() => {
                        if !first_grpc_slot_seen.is_seen() {
                            debug!("No gRPC slot seen yet, skipping checkpoint record");
                            continue;
                        }
                        slot_checkpoint.write(last_seen_grpc_slot.load()).await;
                    }
                }
            }
        });
    }
}

impl Handler<SlotUpdate, SlotUpdate> for SlotTracker {
    async fn handle(&self, slot: &SlotUpdate, _raw: &SlotUpdate) -> shipstern::HandlerResult<()> {
        record_grpc_event(GrpcEvent::SlotUpdate);
        self.store_last_seen_grpc_slot(slot.slot);
        if !self.first_grpc_slot_seen.is_seen() {
            info!("First gRPC slot seen: {}", slot.slot);
            self.first_grpc_slot_seen.mark();
        }
        Ok(())
    }
}

async fn fetch_latest_slot_with_retry(rpc: &RpcClient) -> anyhow::Result<u64> {
    for attempt in 1..=5 {
        let start = Instant::now();
        let response = rpc
            .get_slot_with_commitment(CommitmentConfig::processed())
            .await;
        record_rpc_latency("get_slot", start.elapsed().as_secs_f64());
        match response {
            Ok(slot) => {
                record_rpc_fetch_status("get_slot", RpcFetchStatus::Success);
                return Ok(slot);
            }
            Err(e) => {
                record_rpc_fetch_status("get_slot", RpcFetchStatus::Failure);
                error!("Error fetching latest slot (attempt {attempt}/5): {e}");
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    }
    Err(anyhow::anyhow!(
        "Failed to fetch latest slot after 5 attempts"
    ))
}

#[derive(Debug, Clone)]
struct LastSeenGrpcSlot(Arc<AtomicU64>);

impl LastSeenGrpcSlot {
    pub fn live() -> Self {
        Self(Arc::new(AtomicU64::new(0)))
    }

    pub fn seeded(slot: u64) -> Self {
        Self(Arc::new(AtomicU64::new(slot)))
    }

    pub fn store(&self, slot: u64) {
        self.0.store(slot, Ordering::Relaxed);
    }
    pub fn load(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

#[derive(Clone)]
struct SlotCheckpoint {
    redis_conn: ConnectionManager,
    redis_key: String,
}

impl fmt::Debug for SlotCheckpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SlotCheckpoint").finish_non_exhaustive()
    }
}

impl SlotCheckpoint {
    pub fn new(conn: ConnectionManager, pod_id: &str) -> Self {
        let redis_key = format!("{pod_id}:{SLOT_CHECKPOINT_KEY_REDIS}");
        Self {
            redis_conn: conn,
            redis_key,
        }
    }

    /// Fire-and-forget persist; failures are logged, not propagated, since the
    /// recorder loop should keep running regardless.
    pub async fn write(&self, slot: u64) {
        let mut conn = self.redis_conn.clone();
        if let Err(e) = conn.set::<_, _, ()>(&self.redis_key, slot).await {
            error!("Failed to persist slot {} to checkpoint: {}", slot, e);
        }
    }

    /// Returns the checkpointed slot, or `None` if unset or on error.
    pub async fn read(&self) -> Option<u64> {
        let mut conn = self.redis_conn.clone();
        match conn.get::<_, Option<u64>>(&self.redis_key).await {
            Ok(slot) => slot,
            Err(e) => {
                error!("Failed to read slot from checkpoint: {}", e);
                None
            }
        }
    }
}
