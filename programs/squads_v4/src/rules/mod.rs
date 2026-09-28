pub mod new_member_addition;
pub mod new_proposal;
pub mod threshold_change;
pub mod timelock_change;

use crate::parser::squads_multisig_program::config_action;
use crate::parser::{SquadsV4Ix, SquadsV4Ixs};

/// A multisig config change seen either directly or bundled in a config transaction.
pub(crate) struct ConfigChange<T> {
    pub multisig: String,
    pub values: Vec<T>,
    pub memo: String,
    pub path: &'static str,
}

/// Normalize the direct instruction and the config-transaction path into one shape.
pub(crate) fn config_change<T>(
    parsed: &SquadsV4Ixs,
    direct: impl FnOnce(&SquadsV4Ix) -> Option<(String, T, Option<String>)>,
    from_action: impl Fn(&config_action::Kind) -> Option<T>,
) -> Option<ConfigChange<T>> {
    if let Some((multisig, value, memo)) = direct(&parsed.instruction) {
        return Some(ConfigChange {
            multisig,
            values: vec![value],
            memo: memo.unwrap_or_else(|| "none".into()),
            path: "direct",
        });
    }
    let SquadsV4Ix::ConfigTransactionCreate { accounts, args } = &parsed.instruction else {
        return None;
    };
    let values: Vec<T> = args
        .actions
        .iter()
        .filter_map(|a| from_action(&a.kind))
        .collect();
    if values.is_empty() {
        return None;
    }
    Some(ConfigChange {
        multisig: accounts.multisig.to_string(),
        values,
        memo: args.memo.clone().unwrap_or_else(|| "none".into()),
        path: "config_transaction",
    })
}

/// Join values for metadata; `single` / `plural` pick the metadata key by count.
pub(crate) fn joined<T: ToString>(values: &[T]) -> String {
    values
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(",")
}
