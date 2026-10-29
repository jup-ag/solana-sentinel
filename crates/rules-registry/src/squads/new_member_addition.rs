//! Instruction rule: a member is added to a watched multisig, directly by its
//! config authority or proposed in a config transaction.

use crate::squads::parser::{config_action, SquadsV4Ix, SquadsV4Ixs};
use crate::squads::{config_change_alert, join, watched_config_change, WatchedMultisigs, PROGRAM};
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
        // Each value is "member_key (permission_mask)".
        let change = watched_config_change(
            parsed,
            &self.watched,
            |ix| match ix {
                SquadsV4Ix::MultisigAddMember { accounts, args } => Some((
                    accounts.multisig,
                    format!(
                        "{} ({})",
                        args.new_member.key, args.new_member.permissions.mask
                    ),
                    args.memo.clone(),
                )),
                _ => None,
            },
            |kind| match kind {
                config_action::Kind::AddMember(m) => Some(format!(
                    "{} ({})",
                    m.new_member.key, m.new_member.permissions.mask
                )),
                _ => None,
            },
        )?;
        let members = join(&change.values, ", ");
        let value_key = if change.values.len() == 1 {
            "new_member_with_permission_mask"
        } else {
            "new_members_with_permission_mask"
        };
        Some(
            config_change_alert(Self::ID, &change, value_key, &members, ctx)
                .severity(Severity::Critical)
                .message(format!(
                    "New member(s) added to multisig {}",
                    change.multisig
                ))
                .build(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::squads::parser::SquadsV4IxParser;
    use crate::squads::SQUADS_V4_PROGRAM_ID;
    use solana_sentinel::testing::{ix_update, parse, stream_ctx};

    /// `sha256("global:config_transaction_create")[..8]`.
    const CONFIG_TRANSACTION_CREATE: [u8; 8] = [0x9b, 0xec, 0x57, 0xe4, 0x89, 0x4b, 0x51, 0x27];

    #[tokio::test]
    async fn fires_on_member_added_through_config_transaction() {
        let (multisig, tx, creator, member) = (
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
        );
        // A spending limit with two members and one destination precedes the member add.
        let mut data = CONFIG_TRANSACTION_CREATE.to_vec();
        data.extend_from_slice(&2u32.to_le_bytes());
        data.push(4);
        data.extend_from_slice(&[7; 32 + 1 + 32 + 8]);
        data.push(1); // Period::Day
        data.extend_from_slice(&2u32.to_le_bytes());
        data.extend_from_slice(&[8; 64]);
        data.extend_from_slice(&1u32.to_le_bytes());
        data.extend_from_slice(&[9; 32]);
        data.push(0);
        data.extend_from_slice(member.as_ref());
        data.push(7);
        data.push(0);
        let accounts = [multisig, tx, creator, creator, Pubkey::default()];
        let parsed = parse(
            &SquadsV4IxParser {},
            &ix_update(SQUADS_V4_PROGRAM_ID, &accounts, data),
        )
        .await;

        let rule = NewMemberAddition {
            watched: [multisig].into(),
        };
        let alert = rule.evaluate(&parsed, &stream_ctx(1, Some("sig"))).unwrap();
        assert_eq!(
            alert.metadata["new_member_with_permission_mask"],
            format!("{member} (7)")
        );
        assert_eq!(alert.metadata["path"], "config_transaction");

        let other = NewMemberAddition {
            watched: [Pubkey::new_unique()].into(),
        };
        assert!(other
            .evaluate(&parsed, &stream_ctx(1, Some("sig")))
            .is_none());
    }
}
