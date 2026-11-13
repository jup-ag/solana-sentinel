//! Instruction rule: a transaction is activated on a watched multisig. v3 transactions
//! start as drafts that collect instructions; activation freezes them and opens voting,
//! so this fires once per proposal, not on draft creation.

use crate::squads::{to_pubkey, WatchedMultisigs};
use crate::squads_v3::parser::{SquadsV3Ix, SquadsV3Ixs};
use crate::squads_v3::PROGRAM;
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
    type Event = SquadsV3Ixs;

    fn subscription(cfg: &Self::Config) -> Subscription {
        cfg.subscription()
    }

    fn build(cfg: Self::Config, _deps: &Deps) -> anyhow::Result<Self> {
        Ok(Self {
            watched: cfg.into_set()?,
        })
    }

    fn evaluate(&self, parsed: &SquadsV3Ixs, ctx: &StreamCtx) -> Option<Alert> {
        let SquadsV3Ix::ActivateTransaction { accounts, .. } = &parsed.instruction else {
            return None;
        };
        let multisig = to_pubkey(accounts.multisig);
        if !self.watched.contains(&multisig) {
            return None;
        }
        let transaction = to_pubkey(accounts.transaction);
        Some(
            Alert::for_rule(Self::ID)
                .severity(Severity::Warn)
                .message(format!("New proposal {transaction} on multisig {multisig}"))
                .meta("multisig", multisig)
                .meta("transaction", transaction)
                .meta("creator", accounts.creator)
                .meta("slot", ctx.slot)
                .key([multisig.to_string(), transaction.to_string()])
                .scope(ctx.tx_scope())
                .build(),
        )
    }
}
