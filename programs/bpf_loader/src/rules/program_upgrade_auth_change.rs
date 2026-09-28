use crate::parser::ProgramMetadata;
use crate::rules::WatchedPrograms;
use async_trait::async_trait;
use interface::{
    Alert, BootstrapCtx, GrpcRule, Prefilter, RuleDef, RuleId, Severity, StateCache, StreamCtx,
};
use solana_pubkey::Pubkey;
use std::collections::HashMap;

#[derive(Debug)]
pub struct ProgramUpgradeAuthChange {
    config: WatchedPrograms,
    /// ProgramData pubkey → most recently observed metadata.
    cache: StateCache<Pubkey, ProgramMetadata>,
    program_data_to_id: HashMap<Pubkey, Pubkey>,
}

#[async_trait]
impl RuleDef for ProgramUpgradeAuthChange {
    const ID: RuleId = RuleId::new(crate::PROGRAM, "program_upgrade_auth_change");
    type Config = WatchedPrograms;
    type Deps = ();

    fn build(config: Self::Config, _deps: &()) -> anyhow::Result<Self> {
        Ok(Self {
            program_data_to_id: config.program_data_to_id(),
            cache: StateCache::default(),
            config,
        })
    }

    async fn bootstrap(&mut self, ctx: &BootstrapCtx) -> anyhow::Result<()> {
        self.config.seed(ctx, &self.cache).await
    }
}

impl GrpcRule for ProgramUpgradeAuthChange {
    type Event = ProgramMetadata;

    fn static_prefilter(cfg: &Self::Config) -> Prefilter {
        cfg.prefilter()
    }

    fn evaluate(&self, pm: &ProgramMetadata, ctx: &StreamCtx) -> Option<Alert> {
        let program_id = self.program_data_to_id.get(&pm.program_data_pubkey)?;
        let previous = self
            .cache
            .replace(Self::ID, pm.program_data_pubkey, pm.clone())?
            .upgrade_authority_address;
        let current = pm.upgrade_authority_address;

        let alert = Alert::for_rule(Self::ID)
            .severity(Severity::Warn)
            .meta("program_id", program_id)
            .meta("last_upgrade_slot", pm.last_upgrade_slot)
            .scope(ctx.tx_scope());

        match (previous, current) {
            (Some(_), None) => Some(
                alert
                    .message(format!(
                        "Program {program_id} has been marked immutable (upgrade authority set to \
                         None)"
                    ))
                    .key([program_id.to_string()])
                    .build(),
            ),
            (Some(prev), Some(curr)) if prev != curr => Some(
                alert
                    .message(format!(
                        "Program {program_id} upgrade authority has changed from {prev} to {curr}"
                    ))
                    .meta("previous upgrade_authority", prev)
                    .meta("current upgrade_authority", curr)
                    .key([program_id.to_string(), prev.to_string(), curr.to_string()])
                    .build(),
            ),
            _ => None,
        }
    }
}
