use serde::Deserialize;
use solana_pubkey::Pubkey;
use solana_sdk_ids::system_program::ID as SYSTEM_PROGRAM_ID;
use solana_sentinel::raw::RawInstruction;
use solana_sentinel::utility::pubkey::deserialize_pubkey_vec;
use solana_sentinel::{Alert, GrpcRule, Prefilter, RuleDef, RuleId, Severity, StreamCtx};
use solana_system_interface::instruction::SystemInstruction;

#[derive(Debug, Deserialize)]
pub struct MultisigMemberNonceAccInteractionConfig {
    #[serde(deserialize_with = "deserialize_pubkey_vec")]
    pub members: Vec<Pubkey>,
}

#[derive(Debug)]
pub struct MultisigMemberNonceAccInteraction {
    config: MultisigMemberNonceAccInteractionConfig,
}

impl RuleDef for MultisigMemberNonceAccInteraction {
    const ID: RuleId = RuleId::new(crate::PROGRAM, "multisig_member_nonce_account_interaction");
    type Config = MultisigMemberNonceAccInteractionConfig;
    type Deps = ();

    fn build(config: Self::Config, _deps: &()) -> anyhow::Result<Self> {
        Ok(Self { config })
    }
}

impl GrpcRule for MultisigMemberNonceAccInteraction {
    type Event = RawInstruction;

    fn static_prefilter(cfg: &Self::Config) -> Prefilter {
        Prefilter::new().tx_accounts_include(cfg.members.iter().copied())
    }

    fn evaluate(&self, parsed: &RawInstruction, ctx: &StreamCtx) -> Option<Alert> {
        if parsed.program_id != SYSTEM_PROGRAM_ID {
            return None;
        }

        let ix: SystemInstruction = wincode::deserialize(&parsed.data).ok()?;

        let (ix_variant, nonce_account, authority, new_authority) = match ix {
            // nonce created with `authority`
            SystemInstruction::InitializeNonceAccount(authority) => {
                if !self.config.members.contains(&authority) {
                    return None;
                }
                let nonce_account = parsed.accounts.first()?;
                (
                    "initialize_nonce_account",
                    nonce_account.to_string(),
                    authority.to_string(),
                    None,
                )
            }
            // authority changed to `new_authority`
            SystemInstruction::AuthorizeNonceAccount(new_authority) => {
                let authority_account = parsed.accounts.get(1)?;
                if !self.config.members.contains(authority_account) {
                    return None;
                }
                let nonce_account = parsed.accounts.first()?;
                (
                    "authorize_nonce_account",
                    nonce_account.to_string(),
                    authority_account.to_string(),
                    Some(new_authority.to_string()),
                )
            }
            _ => return None,
        };

        let mut key_parts = vec![
            ix_variant.to_string(),
            nonce_account.clone(),
            authority.clone(),
        ];
        key_parts.extend(new_authority.clone());

        Some(
            Alert::for_rule(Self::ID)
                .severity(Severity::Warn)
                .message(
                    "Nonce account instruction observed in transaction signed by multisig signer",
                )
                .meta("nonce_account", nonce_account)
                .meta("authority", authority)
                .meta_opt("new_authority", new_authority)
                .key(key_parts)
                .scope(ctx.tx_scope())
                .build(),
        )
    }
}
