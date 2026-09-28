//! Instruction rule: alert whenever a watched program is invoked at all.

use interface::raw::RawInstruction;
use interface::{Alert, GrpcRule, Prefilter, RuleDef, RuleId, Severity, StreamCtx};
use serde::Deserialize;
use solana_pubkey::Pubkey;
use utility::pubkey::deserialize_pubkey_vec;

#[derive(Debug, Deserialize)]
pub struct ProgramInvokedConfig {
    #[serde(deserialize_with = "deserialize_pubkey_vec")]
    pub programs: Vec<Pubkey>,
}

#[derive(Debug)]
pub struct ProgramInvoked {
    config: ProgramInvokedConfig,
}

impl RuleDef for ProgramInvoked {
    const ID: RuleId = RuleId::new(crate::PROGRAM, "program_invoked");
    type Config = ProgramInvokedConfig;
    type Deps = ();

    fn build(config: Self::Config, _deps: &()) -> anyhow::Result<Self> {
        Ok(Self { config })
    }
}

impl GrpcRule for ProgramInvoked {
    type Event = RawInstruction;

    /// Transactions that touch any watched program. Instruction pipelines are shared across
    /// programs, so the framework re-checks this gate locally before parsing.
    fn static_prefilter(cfg: &Self::Config) -> Prefilter {
        Prefilter::new().tx_accounts_include(cfg.programs.iter().copied())
    }

    fn evaluate(&self, ix: &RawInstruction, ctx: &StreamCtx) -> Option<Alert> {
        if !self.config.programs.contains(&ix.program_id) {
            return None;
        }
        Some(
            Alert::for_rule(Self::ID)
                .severity(Severity::Info)
                .message(format!("Program {} invoked", ix.program_id))
                .meta("program", ix.program_id)
                .meta("data_len", ix.data.len())
                .key([ix.program_id])
                .scope(ctx.tx_scope())
                .build(),
        )
    }
}
