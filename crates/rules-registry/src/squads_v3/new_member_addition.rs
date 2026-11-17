//! Instruction rule: a member is added to a watched multisig, when proposed and again
//! when executed.

use crate::squads::WatchedMultisigs;
use crate::squads_v3::parser::{SquadsV3Ix, SquadsV3Ixs};
use crate::squads_v3::{config_change_alert, watched_config_change, PROGRAM};
use solana_pubkey::Pubkey;
use solana_sentinel::{Alert, Deps, RuleId, Severity, StreamCtx, StreamRule, Subscription};
use std::collections::HashSet;

#[derive(Debug)]
pub struct NewMemberAddition {
    watched: HashSet<Pubkey>,
}

impl StreamRule for NewMemberAddition {
    const ID: RuleId = RuleId::new(PROGRAM, "new_member_addition");
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
        let change = watched_config_change(parsed, &self.watched, |ix| match ix {
            SquadsV3Ix::AddMember { accounts, args } => Some((accounts.multisig, args.new_member)),
            SquadsV3Ix::AddMemberAndChangeThreshold { accounts, args } => {
                Some((accounts.multisig, args.new_member))
            }
            _ => None,
        })?;
        let member = change.values[0].to_string();
        Some(
            config_change_alert(Self::ID, &change, "new_member", &member, ctx)
                .severity(Severity::Critical)
                .message(format!(
                    "New member {member} added to multisig {} ({})",
                    change.multisig, change.path
                ))
                .build(),
        )
    }
}
