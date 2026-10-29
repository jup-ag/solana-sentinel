//! Squads v4 multisig: governance changes and new proposals on watched
//! multisigs, over an instruction parser generated from the program's IDL.

pub mod new_member_addition;
pub mod new_proposal;
pub mod parser;
pub mod threshold_change;
pub mod timelock_change;

use crate::squads::parser::{config_action, SquadsV4Ix, SquadsV4IxParser, SquadsV4Ixs};
use serde::Deserialize;
use shipstern_core::KeyBytes;
use solana_pubkey::Pubkey;
use solana_sentinel::domain::alert::AlertBuilder;
use solana_sentinel::utility::pubkey::deserialize_pubkey_vec;
use solana_sentinel::{Alert, Program, ProgramSpec, RuleId, StreamCtx, StreamRules, Subscription};
use std::collections::HashSet;

pub const PROGRAM: Program = Program::new("squads_v4");
pub const SQUADS_V4_PROGRAM_ID: Pubkey =
    Pubkey::from_str_const("SQDS4ep65T869zMMBKyuUq6aD6EgTu8psMjkvj52pCf");

/// Web app base URL; a squad is addressed by its default (index 0) vault, not the multisig.
pub const SQUADS_APP_BASE_URL: &str = "https://app.squads.so";

pub fn spec() -> ProgramSpec {
    ProgramSpec::new(PROGRAM).instruction(
        SquadsV4IxParser {},
        StreamRules::new()
            .register::<new_member_addition::NewMemberAddition>()
            .register::<new_proposal::NewProposal>()
            .register::<threshold_change::ThresholdChange>()
            .register::<timelock_change::TimelockChange>(),
    )
}

/// Shared config of every Squads rule: the multisig accounts to watch.
#[derive(Debug, Deserialize)]
pub struct WatchedMultisigs {
    #[serde(deserialize_with = "deserialize_pubkey_vec")]
    pub multisigs: Vec<Pubkey>,
}

impl WatchedMultisigs {
    pub fn subscription(&self) -> Subscription {
        Subscription::new().tx_accounts_include(self.multisigs.iter().copied())
    }

    pub fn into_set(self) -> anyhow::Result<HashSet<Pubkey>> {
        anyhow::ensure!(!self.multisigs.is_empty(), "multisigs must not be empty");
        Ok(self.multisigs.into_iter().collect())
    }
}

/// A multisig config change seen either directly or bundled in a config transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigChange<T> {
    pub multisig: Pubkey,
    /// The config transaction account; `None` on the direct path.
    pub transaction: Option<Pubkey>,
    pub values: Vec<T>,
    pub memo: Option<String>,
    pub path: &'static str,
}

/// Normalize the direct `multisig_*` instruction and the config-transaction path into one
/// shape; `None` unless the multisig is watched and at least one value was picked.
pub fn watched_config_change<T>(
    parsed: &SquadsV4Ixs,
    watched: &HashSet<Pubkey>,
    direct: impl FnOnce(&SquadsV4Ix) -> Option<(KeyBytes<32>, T, Option<String>)>,
    from_action: impl Fn(&config_action::Kind) -> Option<T>,
) -> Option<ConfigChange<T>> {
    let change = if let Some((multisig, value, memo)) = direct(&parsed.instruction) {
        ConfigChange {
            multisig: to_pubkey(multisig),
            transaction: None,
            values: vec![value],
            memo,
            path: "direct",
        }
    } else {
        let SquadsV4Ix::ConfigTransactionCreate { accounts, args } = &parsed.instruction else {
            return None;
        };
        ConfigChange {
            multisig: to_pubkey(accounts.multisig),
            transaction: Some(to_pubkey(accounts.transaction)),
            values: args
                .actions
                .iter()
                .filter_map(|a| from_action(&a.kind))
                .collect(),
            memo: args.memo.clone(),
            path: "config_transaction",
        }
    };
    (!change.values.is_empty() && watched.contains(&change.multisig)).then_some(change)
}

/// Alert skeleton shared by the config-change rules; `values` are the new settings joined.
pub fn config_change_alert<T>(
    id: RuleId,
    change: &ConfigChange<T>,
    value_key: &str,
    values: &str,
    ctx: &StreamCtx,
) -> AlertBuilder {
    Alert::for_rule(id)
        .meta("multisig", change.multisig)
        .meta("path", change.path)
        .meta(value_key, values)
        .meta("memo", change.memo.as_deref().unwrap_or("none"))
        .meta_opt(
            "squads_url",
            change
                .transaction
                .map(|tx| squads_app_url(&change.multisig, &tx)),
        )
        .meta("slot", ctx.slot)
        .key([change.multisig.to_string(), values.to_string()])
        .scope(ctx.tx_scope())
}

pub const fn to_pubkey(key: KeyBytes<32>) -> Pubkey {
    Pubkey::new_from_array(key.0)
}

pub fn join<T: ToString>(values: &[T], sep: &str) -> String {
    values
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(sep)
}

/// Vault `vault_index` of `multisig`.
pub fn vault_pda(multisig: &Pubkey, vault_index: u8) -> Pubkey {
    Pubkey::find_program_address(
        &[b"multisig", multisig.as_ref(), b"vault", &[vault_index]],
        &SQUADS_V4_PROGRAM_ID,
    )
    .0
}

/// Transaction (vault, config or batch) account for `transaction_index` of `multisig`.
pub fn transaction_pda(multisig: &Pubkey, transaction_index: u64) -> Pubkey {
    Pubkey::find_program_address(
        &[
            b"multisig",
            multisig.as_ref(),
            b"transaction",
            &transaction_index.to_le_bytes(),
        ],
        &SQUADS_V4_PROGRAM_ID,
    )
    .0
}

/// Squads web app page for `transaction`, e.g.
/// `https://app.squads.so/squads/<default vault>/transactions/<transaction>`.
pub fn squads_app_url(multisig: &Pubkey, transaction: &Pubkey) -> String {
    let vault = vault_pda(multisig, 0);
    format!("{SQUADS_APP_BASE_URL}/squads/{vault}/transactions/{transaction}")
}
