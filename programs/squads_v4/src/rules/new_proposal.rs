use crate::parser::{SquadsV4Ix, SquadsV4Ixs};
use serde::Deserialize;
use solana_pubkey::Pubkey;
use solana_sentinel::utility::pubkey::deserialize_pubkey_vec;
use solana_sentinel::{Alert, GrpcRule, Prefilter, RuleDef, RuleId, Severity, StreamCtx};

#[derive(Debug, Deserialize)]
pub struct NewProposalConfig {
    #[serde(deserialize_with = "deserialize_pubkey_vec")]
    pub multisigs: Vec<Pubkey>,
}

#[derive(Debug)]
pub struct NewProposal {
    config: NewProposalConfig,
}

impl RuleDef for NewProposal {
    const ID: RuleId = RuleId::new(crate::PROGRAM, "new_proposal");
    type Config = NewProposalConfig;
    type Deps = ();

    fn build(config: Self::Config, _deps: &()) -> anyhow::Result<Self> {
        Ok(Self { config })
    }
}

impl GrpcRule for NewProposal {
    type Event = SquadsV4Ixs;

    fn static_prefilter(cfg: &Self::Config) -> Prefilter {
        Prefilter::new().tx_accounts_include(cfg.multisigs.iter().copied())
    }

    fn evaluate(&self, parsed: &SquadsV4Ixs, ctx: &StreamCtx) -> Option<Alert> {
        let _ = &self.config;
        let SquadsV4Ix::ProposalCreate { accounts, .. } = &parsed.instruction else {
            return None;
        };

        Some(
            Alert::for_rule(Self::ID)
                .severity(Severity::Warn)
                .message(format!("New proposal on multisig {}", accounts.multisig))
                .meta("multisig", accounts.multisig)
                .meta("proposal", accounts.proposal)
                .meta("creator", accounts.creator)
                .key([accounts.multisig.to_string(), accounts.proposal.to_string()])
                .scope(ctx.tx_scope())
                .build(),
        )
    }
}
