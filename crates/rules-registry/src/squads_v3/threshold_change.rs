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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::squads_v3::parser::SquadsV3IxParser;
    use crate::squads_v3::SQUADS_V3_PROGRAM_ID;
    use solana_sentinel::testing::{ix_update, parse, stream_ctx};

    /// `sha256("global:remove_member_and_change_threshold")[..8]`.
    const REMOVE_MEMBER_AND_CHANGE_THRESHOLD: [u8; 8] =
        [0xe6, 0x61, 0xb7, 0xf8, 0x2b, 0xbe, 0x9a, 0x1d];

    #[tokio::test]
    async fn fires_on_member_removed_with_threshold_change() {
        let multisig = Pubkey::new_unique();
        let mut data = REMOVE_MEMBER_AND_CHANGE_THRESHOLD.to_vec();
        data.extend_from_slice(Pubkey::new_unique().as_ref());
        data.extend_from_slice(&1u16.to_le_bytes());
        let parsed = parse(
            &SquadsV3IxParser {},
            &ix_update(SQUADS_V3_PROGRAM_ID, &[multisig], data),
        )
        .await;

        let rule = ThresholdChange {
            watched: [multisig].into(),
        };
        let alert = rule.evaluate(&parsed, &stream_ctx(1, Some("sig"))).unwrap();
        assert_eq!(alert.metadata["requested_threshold"], "1");
    }
}
