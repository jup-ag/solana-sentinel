//! Poll rule: the last slot seen on the stream and the latest RPC slot differ
//! by at least `threshold_slots`, meaning the stream is stalled or lagging.

use crate::health::{HealthDeps, SlotSample, DEFAULT_POLL_INTERVAL, PROGRAM};
use serde::Deserialize;
use solana_pubkey::Pubkey;
use solana_sentinel::observability::prometheus::metrics::record_tracked_slot;
use solana_sentinel::{
    Alert, DedupScope, Deps, GrpcSlotReader, PollCtx, PollRule, RuleId, Severity,
};
use std::collections::HashSet;
use std::time::Duration;
use tracing::{info, warn};

fn default_threshold_slots() -> u64 {
    100
}

#[derive(Debug, Deserialize)]
pub struct SlotLagConfig {
    #[serde(default = "default_threshold_slots")]
    pub threshold_slots: u64,
    pub poll_interval_seconds: Option<u64>,
}

#[derive(Debug)]
pub struct SlotLag {
    threshold_slots: u64,
    grpc_slot: GrpcSlotReader,
}

impl PollRule for SlotLag {
    const ID: RuleId = RuleId::new(PROGRAM, "slot_lag");
    type Config = SlotLagConfig;
    type Item = SlotSample;

    fn accounts(_cfg: &Self::Config) -> HashSet<Pubkey> {
        HashSet::new()
    }

    fn poll_interval(cfg: &Self::Config) -> Option<Duration> {
        Some(
            cfg.poll_interval_seconds
                .map_or(DEFAULT_POLL_INTERVAL, Duration::from_secs),
        )
    }

    fn build(cfg: Self::Config, deps: &Deps) -> anyhow::Result<Self> {
        Ok(Self {
            threshold_slots: cfg.threshold_slots,
            grpc_slot: deps.get::<HealthDeps>()?.grpc_slot.clone(),
        })
    }

    fn evaluate(&self, sample: &SlotSample, _ctx: &PollCtx) -> Option<Alert> {
        // Nothing to compare until the stream has delivered its first slot.
        let grpc_slot = self.grpc_slot.load()?;
        let rpc_slot = sample.rpc_slot;
        record_tracked_slot(rpc_slot, grpc_slot);

        let diff = rpc_slot.abs_diff(grpc_slot);
        let direction = if rpc_slot >= grpc_slot {
            "stream_behind"
        } else {
            "rpc_behind"
        };
        info!(grpc_slot, rpc_slot, diff, "slot health check");
        if diff < self.threshold_slots {
            return None;
        }
        warn!(grpc_slot, rpc_slot, diff, direction, "slot lag detected");
        Some(
            Alert::for_rule(Self::ID)
                .severity(Severity::Critical)
                .message(format!(
                    "Slot lag: stream at {grpc_slot}, RPC at {rpc_slot}, {diff} slots apart \
                     ({direction})"
                ))
                .meta("grpc_slot", grpc_slot)
                .meta("rpc_slot", rpc_slot)
                .meta("slot_diff", diff)
                .meta("direction", direction)
                .key([grpc_slot])
                // Lag is a per-node condition; the slot scope keeps other nodes' alerts distinct.
                .scope(DedupScope::Slot(grpc_slot))
                .build(),
        )
    }
}
