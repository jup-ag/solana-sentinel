//! Compares the last slot seen on the gRPC stream with the latest RPC slot and
//! alerts when they diverge, which means the stream is stalled or lagging.

use crate::{SlotSample, SystemDeps};
use serde::Deserialize;
use solana_pubkey::Pubkey;
use solana_sentinel::observability::prometheus::metrics::record_tracked_slot;
use solana_sentinel::{
    Alert, DedupScope, GrpcSlotReader, PollCtx, RpcRule, RuleDef, RuleId, Severity,
};
use std::collections::HashSet;
use std::time::Duration;
use tracing::{info, warn};

fn default_threshold_slots() -> u64 {
    100
}

fn default_poll_interval_seconds() -> u64 {
    crate::DEFAULT_POLL_INTERVAL.as_secs()
}

#[derive(Debug, Deserialize)]
pub struct SlotLagDetectedConfig {
    /// Alert once the two sources differ by at least this many slots.
    #[serde(default = "default_threshold_slots")]
    pub threshold_slots: u64,
    #[serde(default = "default_poll_interval_seconds")]
    pub poll_interval_seconds: u64,
}

#[derive(Debug)]
pub struct SlotLagDetected {
    config: SlotLagDetectedConfig,
    grpc_slot: GrpcSlotReader,
}

impl RuleDef for SlotLagDetected {
    const ID: RuleId = RuleId::new(crate::PROGRAM, "slot_lag_detected");
    type Config = SlotLagDetectedConfig;
    type Deps = SystemDeps;

    fn build(config: Self::Config, deps: &SystemDeps) -> anyhow::Result<Self> {
        Ok(Self {
            config,
            grpc_slot: deps.grpc_slot.clone(),
        })
    }
}

impl RpcRule for SlotLagDetected {
    type Item = SlotSample;

    fn accounts(_cfg: &Self::Config) -> HashSet<Pubkey> {
        HashSet::new()
    }

    fn poll_interval(cfg: &Self::Config) -> Option<Duration> {
        Some(Duration::from_secs(cfg.poll_interval_seconds))
    }

    fn evaluate(&self, sample: &SlotSample, _ctx: &PollCtx) -> Option<Alert> {
        // Until the stream has delivered a slot there is nothing to compare; the
        // source may still be catching up after start.
        let last_seen_slot = self.grpc_slot.load()?;
        let rpc_slot = sample.rpc_slot;
        record_tracked_slot(rpc_slot, last_seen_slot);

        let diff = rpc_slot.abs_diff(last_seen_slot);
        let direction = if rpc_slot >= last_seen_slot {
            "grpc_behind"
        } else {
            "rpc_behind"
        };
        info!(
            "Slot health check: last seen gRPC slot is {last_seen_slot}, latest RPC slot is \
             {rpc_slot}, difference is {diff}"
        );

        if diff < self.config.threshold_slots {
            return None;
        }
        warn!(
            "Slot lag detected: last seen gRPC slot is {last_seen_slot}, latest RPC slot is \
             {rpc_slot}, difference is {diff}"
        );

        Some(
            Alert::for_rule(Self::ID)
                .severity(Severity::Critical)
                .message(format!(
                    "Slot lag detected: last seen gRPC slot is {last_seen_slot}, latest RPC slot \
                     is {rpc_slot}, difference is {diff}"
                ))
                .meta("last_seen_grpc_slot", last_seen_slot)
                .meta("rpc_slot", rpc_slot)
                .meta("slot_diff", diff)
                .meta("direction", direction)
                .key([last_seen_slot])
                // Per-pod value: dedups repeats for the same stalled slot but not across
                // pods, since lag is a per-pod condition. The cooldown period rate-limits it.
                .scope(DedupScope::Slot(last_seen_slot))
                .build(),
        )
    }
}
