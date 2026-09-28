//! Service-health rules. Not an on-chain program: it watches the monitor itself.

pub mod rules;

use async_trait::async_trait;
use rules::slot_lag_detected::SlotLagDetected;
use solana_account::Account;
use solana_pubkey::Pubkey;
use solana_sentinel::{
    BootstrapCtx, GrpcSlotReader, PollCtx, PollRules, PollSource, Program, ProgramDeps, ProgramSpec,
};
use std::collections::HashMap;
use std::time::Duration;

/// Snake_case identifier this program is known by in config and logs.
pub const PROGRAM: Program = Program::new("system");

pub const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(120);

/// Handles the service exposes to its own health rules.
#[derive(Debug, Clone)]
pub struct SystemDeps {
    pub grpc_slot: GrpcSlotReader,
}

#[async_trait]
impl ProgramDeps for SystemDeps {
    async fn bootstrap(ctx: &BootstrapCtx) -> anyhow::Result<Self> {
        Ok(Self {
            grpc_slot: ctx.grpc_slot.clone(),
        })
    }
}

/// One sample per tick carrying the RPC slot the fetch was served at.
#[derive(Debug, Clone, Copy)]
pub struct SlotSample {
    pub rpc_slot: u64,
}

#[derive(Debug, Clone, Default)]
pub struct SlotSource;

impl PollSource for SlotSource {
    type Item = SlotSample;

    fn parse(&self, _fetched: &HashMap<Pubkey, Account>, ctx: &PollCtx) -> Vec<Self::Item> {
        vec![SlotSample { rpc_slot: ctx.slot }]
    }
}

pub fn spec() -> ProgramSpec {
    ProgramSpec::builder(PROGRAM)
        .deps::<SystemDeps>()
        .poll(
            SlotSource,
            DEFAULT_POLL_INTERVAL,
            PollRules::new().register::<SlotLagDetected>(),
        )
        .finish()
}
