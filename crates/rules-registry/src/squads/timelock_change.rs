//! Instruction rule: the time lock of a watched multisig changes, directly by
//! its config authority or proposed in a config transaction.

use crate::squads::parser::{config_action, SquadsV4Ix, SquadsV4Ixs};
use crate::squads::{config_change_alert, join, watched_config_change, WatchedMultisigs, PROGRAM};
use solana_pubkey::Pubkey;
use solana_sentinel::{Alert, Deps, RuleId, Severity, StreamCtx, StreamRule, Subscription};
use std::collections::HashSet;

#[derive(Debug)]
pub struct TimelockChange {
    watched: HashSet<Pubkey>,
}

impl StreamRule for TimelockChange {
    const ID: RuleId = RuleId::new(PROGRAM, "timelock_change");
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
        let change = watched_config_change(
            parsed,
            &self.watched,
            |ix| match ix {
                SquadsV4Ix::MultisigSetTimeLock { accounts, args } => {
                    Some((accounts.multisig, args.time_lock, args.memo.clone()))
                }
                _ => None,
            },
            |kind| match kind {
                config_action::Kind::SetTimeLock(tl) => Some(tl.new_time_lock),
                _ => None,
            },
        )?;
        let timelocks = join(&change.values, ",");
        let value_key = if change.values.len() == 1 {
            "new_timelock"
        } else {
            "new_timelocks"
        };
        Some(
            config_change_alert(Self::ID, &change, value_key, &timelocks, ctx)
                .severity(Severity::Critical)
                .message(format!("Timelock changed on multisig {}", change.multisig))
                .build(),
        )
    }
}
