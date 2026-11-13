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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::squads_v3::parser::SquadsV3IxParser;
    use crate::squads_v3::SQUADS_V3_PROGRAM_ID;
    use solana_sentinel::testing::{ix_update, parse, stream_ctx};

    /// `sha256("global:add_member_and_change_threshold")[..8]`.
    const ADD_MEMBER_AND_CHANGE_THRESHOLD: [u8; 8] =
        [0x72, 0xd5, 0x3b, 0x2f, 0xd6, 0x9d, 0x96, 0xaa];

    #[tokio::test]
    async fn fires_on_member_added_with_threshold_change() {
        let (multisig, member) = (Pubkey::new_unique(), Pubkey::new_unique());
        let mut data = ADD_MEMBER_AND_CHANGE_THRESHOLD.to_vec();
        data.extend_from_slice(member.as_ref());
        data.extend_from_slice(&3u16.to_le_bytes());
        let accounts = [multisig, Pubkey::default(), Pubkey::default()];
        let parsed = parse(
            &SquadsV3IxParser {},
            &ix_update(SQUADS_V3_PROGRAM_ID, &accounts, data),
        )
        .await;

        let rule = NewMemberAddition {
            watched: [multisig].into(),
        };
        let alert = rule.evaluate(&parsed, &stream_ctx(1, Some("sig"))).unwrap();
        assert_eq!(alert.metadata["new_member"], member.to_string());
        assert_eq!(alert.metadata["path"], "executed");
    }
}
