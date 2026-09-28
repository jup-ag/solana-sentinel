use crate::parser::squads_multisig_program::config_action;
use crate::parser::{SquadsV4Ix, SquadsV4Ixs};
use crate::rules::config_change;
use serde::Deserialize;
use solana_pubkey::Pubkey;
use solana_sentinel::utility::pubkey::deserialize_pubkey_vec;
use solana_sentinel::{Alert, GrpcRule, Prefilter, RuleDef, RuleId, Severity, StreamCtx};

#[derive(Debug, Deserialize)]
pub struct NewMemberAdditionConfig {
    #[serde(deserialize_with = "deserialize_pubkey_vec")]
    pub multisigs: Vec<Pubkey>,
}

#[derive(Debug)]
pub struct NewMemberAddition {
    config: NewMemberAdditionConfig,
}

impl RuleDef for NewMemberAddition {
    const ID: RuleId = RuleId::new(crate::PROGRAM, "new_member_addition");
    type Config = NewMemberAdditionConfig;
    type Deps = ();

    fn build(config: Self::Config, _deps: &()) -> anyhow::Result<Self> {
        Ok(Self { config })
    }
}

impl GrpcRule for NewMemberAddition {
    type Event = SquadsV4Ixs;

    fn static_prefilter(cfg: &Self::Config) -> Prefilter {
        Prefilter::new().tx_accounts_include(cfg.multisigs.iter().copied())
    }

    fn evaluate(&self, parsed: &SquadsV4Ixs, ctx: &StreamCtx) -> Option<Alert> {
        let _ = &self.config;
        // Each value is "member_key (permission_mask)".
        let change = config_change(
            parsed,
            |ix| match ix {
                SquadsV4Ix::MultisigAddMember { accounts, args } => Some((
                    accounts.multisig.to_string(),
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

        let members = change.values.join(", ");
        let key = if change.values.len() == 1 {
            "new_member_with_permission_mask"
        } else {
            "new_members_with_permission_mask"
        };

        Some(
            Alert::for_rule(Self::ID)
                .severity(Severity::Critical)
                .message(format!(
                    "New member(s) added to multisig {}",
                    change.multisig
                ))
                .meta("multisig", &change.multisig)
                .meta("path", change.path)
                .meta(key, &members)
                .meta("memo", change.memo)
                .key([change.multisig, members])
                .scope(ctx.tx_scope())
                .build(),
        )
    }
}
