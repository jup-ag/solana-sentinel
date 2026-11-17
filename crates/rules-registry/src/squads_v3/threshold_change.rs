//! Instruction rule: the approval threshold of a watched multisig changes, when proposed
//! and again when executed. The value is the requested threshold; the program clamps it
//! to the member count. A plain `remove_member` can also lower it; `member_removal` covers that.

use crate::squads::WatchedMultisigs;
use crate::squads_v3::parser::{SquadsV3Ix, SquadsV3Ixs};
use crate::squads_v3::{config_change_alert, watched_config_change, PROGRAM};
use solana_pubkey::Pubkey;
use solana_sentinel::{Alert, Deps, RuleId, Severity, StreamCtx, StreamRule, Subscription};
use std::collections::HashSet;

#[derive(Debug)]
pub struct ThresholdChange {
    watched: HashSet<Pubkey>,
}

impl StreamRule for ThresholdChange {
    const ID: RuleId = RuleId::new(PROGRAM, "threshold_change");
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
            SquadsV3Ix::ChangeThreshold { accounts, args } => {
                Some((accounts.multisig, args.new_threshold))
            }
            SquadsV3Ix::AddMemberAndChangeThreshold { accounts, args } => {
                Some((accounts.multisig, args.new_threshold))
            }
            SquadsV3Ix::RemoveMemberAndChangeThreshold { accounts, args } => {
                Some((accounts.multisig, args.new_threshold))
            }
            _ => None,
        })?;
        let threshold = change.values[0].to_string();
        Some(
            config_change_alert(Self::ID, &change, "requested_threshold", &threshold, ctx)
                .severity(Severity::Critical)
                .message(format!(
                    "Threshold change to {threshold} on multisig {} ({})",
                    change.multisig, change.path
                ))
                .build(),
        )
    }
}
