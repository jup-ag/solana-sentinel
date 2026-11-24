//! Instruction rule: a member is removed from a watched multisig, when proposed and again
//! when executed. A plain `remove_member` also lowers the threshold, with no threshold
//! instruction, when it would exceed the remaining members.

use crate::squads::WatchedMultisigs;
use crate::squads_v3::parser::{SquadsV3Ix, SquadsV3Ixs};
use crate::squads_v3::{config_change_alert, watched_config_change, PROGRAM};
use solana_pubkey::Pubkey;
use solana_sentinel::{Alert, Deps, RuleId, Severity, StreamCtx, StreamRule, Subscription};
use std::collections::HashSet;

#[derive(Debug)]
pub struct MemberRemoval {
    watched: HashSet<Pubkey>,
}

impl StreamRule for MemberRemoval {
    const ID: RuleId = RuleId::new(PROGRAM, "member_removal");
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
            SquadsV3Ix::RemoveMember { accounts, args } => {
                Some((accounts.multisig, args.old_member))
            }
            SquadsV3Ix::RemoveMemberAndChangeThreshold { accounts, args } => {
                Some((accounts.multisig, args.old_member))
            }
            _ => None,
        })?;
        let member = change.values[0].to_string();
        Some(
            config_change_alert(Self::ID, &change, "removed_member", &member, ctx)
                .severity(Severity::Critical)
                .message(format!(
                    "Member {member} removed from multisig {} ({}); the threshold may drop with it",
                    change.multisig, change.path
                ))
                .build(),
        )
    }
}
