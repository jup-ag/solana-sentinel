//! Account rule: a watched program's upgrade authority changed hands or was
//! removed (the program became immutable).

use crate::bpf_loader::parser::ProgramData;
use crate::bpf_loader::{WatchedPrograms, PROGRAM};
use async_trait::async_trait;
use solana_pubkey::Pubkey;
use solana_sentinel::{
    Alert, BootstrapCtx, Deps, RuleId, Severity, StateCache, StreamCtx, StreamRule, Subscription,
};
use std::collections::HashMap;

#[derive(Debug)]
pub struct UpgradeAuthorityChange {
    config: WatchedPrograms,
    program_ids: HashMap<Pubkey, Pubkey>,
    last: StateCache<Pubkey, ProgramData>,
}

#[async_trait]
impl StreamRule for UpgradeAuthorityChange {
    const ID: RuleId = RuleId::new(PROGRAM, "upgrade_authority_change");
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
        let previous = self
            .last
            .replace(Self::ID, pd.address, pd.clone())?
            .upgrade_authority;
        let current = pd.upgrade_authority;
        if previous == current {
            return None;
        }
        let fmt = |k: Option<Pubkey>| k.map_or_else(|| "none".to_string(), |k| k.to_string());
        let message = match current {
            None => format!("Program {program_id} is now immutable (upgrade authority removed)"),
            Some(curr) => format!(
                "Program {program_id} upgrade authority changed from {} to {curr}",
                fmt(previous)
            ),
        };
        Some(
            Alert::for_rule(Self::ID)
                .severity(Severity::Critical)
                .message(message)
                .meta("program_id", program_id)
                .meta("program_data", pd.address)
                .meta("previous_upgrade_authority", fmt(previous))
                .meta("current_upgrade_authority", fmt(current))
                .meta("upgrade_slot", pd.last_upgrade_slot)
                .key([program_id.to_string(), fmt(previous), fmt(current)])
                .scope(ctx.tx_scope())
                .build(),
        )
    }
}
