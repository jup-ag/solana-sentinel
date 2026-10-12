//! Service health: not an on-chain program, it watches the monitor itself.
//! Its poll source fetches no accounts; each tick just carries the RPC slot.

pub mod slot_lag;

use async_trait::async_trait;
use solana_account::Account;
use solana_pubkey::Pubkey;
use solana_sentinel::{
    BootstrapCtx, GrpcSlotReader, PollCtx, PollRules, PollSource, Program, ProgramDeps, ProgramSpec,
};
use std::collections::HashMap;
use std::time::Duration;

pub const PROGRAM: Program = Program::new("health");
pub const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(120);

/// Handles the service exposes to its own health rules.
#[derive(Debug, Clone)]
pub struct HealthDeps {
    pub grpc_slot: GrpcSlotReader,
}

#[async_trait]
impl ProgramDeps for HealthDeps {
    async fn bootstrap(ctx: &BootstrapCtx) -> anyhow::Result<Self> {
        Ok(Self {
            grpc_slot: ctx.grpc_slot.clone(),
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
    ProgramSpec::new(PROGRAM).deps::<HealthDeps>().poll(
        SlotSource,
        DEFAULT_POLL_INTERVAL,
        PollRules::new().register::<slot_lag::SlotLag>(),
    )
}
