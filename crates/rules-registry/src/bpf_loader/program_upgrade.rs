//! Account rule: a watched program's `last_upgrade_slot` moved, meaning a new
//! binary was deployed.

use crate::bpf_loader::parser::ProgramData;
use crate::bpf_loader::{WatchedPrograms, PROGRAM};
use async_trait::async_trait;
use solana_pubkey::Pubkey;
use solana_sentinel::{
    Alert, BootstrapCtx, Deps, RuleId, Severity, StateCache, StreamCtx, StreamRule, Subscription,
};
use std::collections::HashMap;

#[derive(Debug)]
pub struct ProgramUpgrade {
    config: WatchedPrograms,
    program_ids: HashMap<Pubkey, Pubkey>,
    last: StateCache<Pubkey, ProgramData>,
}

#[async_trait]
impl StreamRule for ProgramUpgrade {
    const ID: RuleId = RuleId::new(PROGRAM, "program_upgrade");
    type Config = WatchedPrograms;
    type Event = ProgramData;

    fn subscription(cfg: &Self::Config) -> Subscription {
        cfg.subscription()
    }

    fn build(cfg: Self::Config, _deps: &Deps) -> anyhow::Result<Self> {
        anyhow::ensure!(!cfg.programs.is_empty(), "programs must not be empty");
        Ok(Self {
            program_ids: cfg.program_ids(),
            config: cfg,
            last: StateCache::default(),
        })
    }

    async fn bootstrap(&mut self, ctx: &BootstrapCtx) -> anyhow::Result<()> {
        self.config.seed(ctx, &self.last).await
    }

    fn evaluate(&self, pd: &ProgramData, ctx: &StreamCtx) -> Option<Alert> {
        let program_id = self.program_ids.get(&pd.address)?;
        let previous = self.last.replace(Self::ID, pd.address, pd.clone())?;
        if pd.last_upgrade_slot == previous.last_upgrade_slot {
            return None;
        }
        Some(
            Alert::for_rule(Self::ID)
                .severity(Severity::Warn)
                .message(format!(
                    "Program {program_id} upgraded at slot {} (previous upgrade slot {})",
                    pd.last_upgrade_slot, previous.last_upgrade_slot
                ))
                .meta("program_id", program_id)
                .meta("program_data", pd.address)
                .meta("upgrade_slot", pd.last_upgrade_slot)
                .meta("previous_upgrade_slot", previous.last_upgrade_slot)
                .meta_opt("upgrade_authority", pd.upgrade_authority)
                .key([program_id.to_string(), pd.last_upgrade_slot.to_string()])
                .scope(ctx.tx_scope())
                .build(),
        )
    }
}
