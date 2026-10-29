//! Instruction rule: the approval threshold of a watched multisig changes,
//! directly by its config authority or proposed in a config transaction.

use crate::squads::parser::{config_action, SquadsV4Ix, SquadsV4Ixs};
use crate::squads::{config_change_alert, join, watched_config_change, WatchedMultisigs, PROGRAM};
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
                SquadsV4Ix::MultisigChangeThreshold { accounts, args } => {
                    Some((accounts.multisig, args.new_threshold, args.memo.clone()))
                }
                _ => None,
            },
            |kind| match kind {
                config_action::Kind::ChangeThreshold(ct) => Some(ct.new_threshold),
                _ => None,
            },
        )?;
        let thresholds = join(&change.values, ",");
        // Usually one change, but a config transaction may bundle several.
        let value_key = if change.values.len() == 1 {
            "new_threshold"
        } else {
            "new_thresholds"
        };
        Some(
            config_change_alert(Self::ID, &change, value_key, &thresholds, ctx)
                .severity(Severity::Critical)
                .message(format!("Threshold changed on multisig {}", change.multisig))
                .build(),
        )
    }
}
