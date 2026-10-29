//! Instruction rule: a proposal is created on a watched multisig. Every vault,
//! batch or config transaction needs one before members can vote on it.

use crate::squads::parser::{SquadsV4Ix, SquadsV4Ixs};
use crate::squads::{squads_app_url, to_pubkey, transaction_pda, WatchedMultisigs, PROGRAM};
use solana_pubkey::Pubkey;
use solana_sentinel::{Alert, Deps, RuleId, Severity, StreamCtx, StreamRule, Subscription};
use std::collections::HashSet;

#[derive(Debug)]
pub struct NewProposal {
    watched: HashSet<Pubkey>,
}

impl StreamRule for NewProposal {
    const ID: RuleId = RuleId::new(PROGRAM, "new_proposal");
    type Config = WatchedMultisigs;
    type Event = SquadsV4Ixs;

    fn subscription(cfg: &Self::Config) -> Subscription {
        cfg.subscription()
    }

    fn build(cfg: Self::Config, _deps: &Deps) -> anyhow::Result<Self> {
        Ok(Self {
            watched: cfg.into_set()?,
        })
    }

    fn evaluate(&self, parsed: &SquadsV4Ixs, ctx: &StreamCtx) -> Option<Alert> {
        let SquadsV4Ix::ProposalCreate { accounts, args } = &parsed.instruction else {
            return None;
        };
        let multisig = to_pubkey(accounts.multisig);
        if !self.watched.contains(&multisig) {
            return None;
        }
        let index = args.transaction_index;
        let transaction = transaction_pda(&multisig, index);
        Some(
            Alert::for_rule(Self::ID)
                .severity(Severity::Warn)
                .message(format!("New proposal #{index} on multisig {multisig}"))
                .meta("multisig", multisig)
                .meta("proposal", accounts.proposal)
                .meta("creator", accounts.creator)
                .meta("transaction", transaction)
                .meta("transaction_index", index)
                .meta("draft", args.draft)
                .meta("squads_url", squads_app_url(&multisig, &transaction))
                .meta("slot", ctx.slot)
                .key([multisig.to_string(), accounts.proposal.to_string()])
                .scope(ctx.tx_scope())
                .build(),
        )
    }
}
