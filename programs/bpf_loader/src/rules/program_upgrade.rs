use crate::parser::ProgramMetadata;
use crate::rules::WatchedPrograms;
use async_trait::async_trait;
use solana_pubkey::Pubkey;
use solana_sentinel::{
    Alert, BootstrapCtx, GrpcRule, Prefilter, RuleDef, RuleId, Severity, StateCache, StreamCtx,
};
use std::collections::HashMap;

#[derive(Debug)]
pub struct ProgramUpgrade {
    config: WatchedPrograms,
    /// ProgramData pubkey → most recently observed metadata.
    cache: StateCache<Pubkey, ProgramMetadata>,
    program_data_to_id: HashMap<Pubkey, Pubkey>,
}

#[async_trait]
impl RuleDef for ProgramUpgrade {
    const ID: RuleId = RuleId::new(crate::PROGRAM, "program_upgrade");
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

impl GrpcRule for ProgramUpgrade {
    type Event = ProgramMetadata;

    fn static_prefilter(cfg: &Self::Config) -> Prefilter {
        cfg.prefilter()
    }

    fn evaluate(&self, pm: &ProgramMetadata, ctx: &StreamCtx) -> Option<Alert> {
        let program_id = self.program_data_to_id.get(&pm.program_data_pubkey)?;
        let previous = self
            .cache
            .replace(Self::ID, pm.program_data_pubkey, pm.clone())?;

        if pm.last_upgrade_slot == previous.last_upgrade_slot {
            return None; // not an upgrade
        }

        Some(
            Alert::for_rule(Self::ID)
                .severity(Severity::Warn)
                .message(format!(
                    "Program {} has been upgraded at slot {}",
                    program_id, pm.last_upgrade_slot
                ))
                .meta("program_id", program_id)
                .meta("program_data_pubkey", pm.program_data_pubkey)
                .meta("current last_upgrade_slot", pm.last_upgrade_slot)
                .meta("previous last_upgrade_slot", previous.last_upgrade_slot)
                .meta(
                    "upgrade_authority_address",
                    pm.upgrade_authority_address
                        .map_or_else(|| "none".to_string(), |p| p.to_string()),
                )
                .key([program_id.to_string(), pm.last_upgrade_slot.to_string()])
                .scope(ctx.tx_scope())
                .build(),
        )
    }
}
