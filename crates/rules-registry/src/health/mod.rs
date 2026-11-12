//! Service health: not an on-chain program, it watches the monitor itself.
//!
//! - `slot_lag`: the stream's slot vs the RPC's (poll source fetches only the slot).
//! - `feed_liveness`: every pipeline, rule input and poll feed still receives data; the gRPC
//!   stream still delivers slots; poll sources are not failing in a row.
//! - `canary`: synthetic alerts through the real alert path to every sink route; the canary
//!   watchdog and the Betterstack heartbeat alert when one does not arrive.
//! - `source_divergence`: the children of a `multi` data source stay up, in step and complete.

pub mod canary;
pub mod feed_liveness;
pub mod slot_lag;
pub mod source_divergence;

use async_trait::async_trait;
use solana_account::Account;
use solana_pubkey::Pubkey;
use solana_sentinel::{
    BootstrapCtx, GrpcSlotReader, Liveness, PollCtx, PollRules, PollSource, Program, ProgramDeps,
    ProgramSpec, SourceHealth, TickSource,
};
use std::collections::HashMap;
use std::time::Duration;

pub const PROGRAM: Program = Program::new("health");
pub const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(120);
/// Default cadence of the in-process checks (feed liveness); no RPC involved.
pub const DEFAULT_CHECK_INTERVAL: Duration = Duration::from_secs(15);

/// Handles the service exposes to its own health rules.
#[derive(Debug, Clone)]
pub struct HealthDeps {
    pub grpc_slot: GrpcSlotReader,
    pub pod_id: String,
    /// Feed liveness and canary state the service shares with every pipeline.
    pub liveness: Liveness,
    /// Per-child health of a `multi` data source (empty otherwise).
    pub source_health: SourceHealth,
}

#[async_trait]
impl ProgramDeps for HealthDeps {
    async fn bootstrap(ctx: &BootstrapCtx) -> anyhow::Result<Self> {
        Ok(Self {
            grpc_slot: ctx.grpc_slot.clone(),
            pod_id: ctx.pod_id.clone(),
            liveness: ctx.liveness.clone(),
            source_health: ctx.source_health.clone(),
        })
    }
}

/// One sample per tick: the slot the RPC answered at.
#[derive(Debug, Clone, Copy)]
pub struct SlotSample {
    pub rpc_slot: u64,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct SlotSource;

impl PollSource for SlotSource {
    type Item = SlotSample;

    fn parse(&self, _fetched: &HashMap<Pubkey, Account>, ctx: &PollCtx) -> Vec<SlotSample> {
        vec![SlotSample { rpc_slot: ctx.slot }]
    }
}

pub fn spec() -> ProgramSpec {
    ProgramSpec::new(PROGRAM)
        .deps::<HealthDeps>()
        .poll(
            SlotSource,
            DEFAULT_POLL_INTERVAL,
            PollRules::new().register::<slot_lag::SlotLag>(),
        )
        // In-process checks: they read the liveness registry and the multi-source health,
        // so they keep running (and alerting) when the RPC is down.
        .poll(
            TickSource,
            DEFAULT_CHECK_INTERVAL,
            PollRules::new()
                .register::<feed_liveness::FeedLiveness>()
                .register::<canary::Canary>()
                .register::<source_divergence::SourceDivergence>(),
        )
}
