//! Local filter matching for transports that cannot tag updates with filter
//! names (PubSub, Kafka). Mirrors the yellowstone server-side rules: within one
//! prefilter every non-empty field must hold.

use shipstern_core::{
    AccountFilter, AccountPrefilter, Filters, LamportsCmp, MemcmpData, TransactionPrefilter,
};
use solana_pubkey::Pubkey;
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::geyser::SubscribeUpdate;

/// Offset of the `state` byte in an SPL token account; 0 is uninitialized.
const TOKEN_ACCOUNT_STATE_OFFSET: usize = 108;
const TOKEN_ACCOUNT_LEN: usize = 165;

/// Re-tag an update with the local filter names it matches, dropping it when
/// none do. Pings, blocks and status-only updates yield `None`.
pub fn match_update(filters: &Filters, mut update: SubscribeUpdate) -> Option<SubscribeUpdate> {
    let names = match update.update_oneof.as_ref()? {
        UpdateOneof::Account(acc) => {
            let info = acc.account.as_ref()?;
            let pubkey = Pubkey::try_from(info.pubkey.as_slice()).ok()?;
            let owner = Pubkey::try_from(info.owner.as_slice()).ok()?;
            matching_names(filters, |p| {
                p.account
                    .as_ref()
                    .is_some_and(|a| account_matches(a, &pubkey, &owner, info.lamports, &info.data))
            })
        }
        UpdateOneof::Transaction(tx) => {
            let info = tx.transaction.as_ref()?;
            let keys = transaction_keys(info);
            let failed = info.meta.as_ref().is_some_and(|m| m.err.is_some());
            matching_names(filters, |p| {
                p.transaction
                    .as_ref()
                    .is_some_and(|t| tx_matches(t, &keys, failed, info.is_vote))
            })
        }
        UpdateOneof::Slot(_) => matching_names(filters, |p| p.slot.is_some()),
        _ => return None,
    };
    if names.is_empty() {
        return None;
    }
    update.filters = names;
    Some(update)
}

fn matching_names(
    filters: &Filters,
    pred: impl Fn(&shipstern_core::Prefilter) -> bool,
) -> Vec<String> {
    filters
        .parser_ids()
        .filter(|name| filters.get(name).is_some_and(&pred))
        .map(str::to_string)
        .collect()
}

/// Every account key a transaction touches, static and loaded, for filter matching.
fn transaction_keys(
    info: &yellowstone_grpc_proto::geyser::SubscribeUpdateTransactionInfo,
) -> Vec<Pubkey> {
    let statics = info
        .transaction
        .as_ref()
        .and_then(|t| t.message.as_ref())
        .map(|m| m.account_keys.as_slice())
        .unwrap_or_default();
    let (writable, readonly) = info
        .meta
        .as_ref()
        .map(|m| {
            (
                m.loaded_writable_addresses.as_slice(),
                m.loaded_readonly_addresses.as_slice(),
            )
        })
        .unwrap_or_default();
    statics
        .iter()
        .chain(writable)
        .chain(readonly)
        .filter_map(|k| Pubkey::try_from(k.as_slice()).ok())
        .collect()
}

/// The same test yellowstone applies server-side, on the transaction's keys.
pub fn tx_matches(
    filter: &TransactionPrefilter,
    keys: &[Pubkey],
    failed: bool,
    is_vote: bool,
) -> bool {
    if failed && filter.failed == Some(false) {
        return false;
    }
    if filter.vote.is_some_and(|want_vote| want_vote != is_vote) {
        return false;
    }
    let has = |k: &shipstern_core::Pubkey| keys.contains(&Pubkey::new_from_array(k.0));
    let included = filter.accounts_include.is_empty() || filter.accounts_include.iter().any(has);
    let required = filter.accounts_required.iter().all(has);
    let excluded = filter.accounts_exclude.iter().any(has);
    included && required && !excluded
}

/// `accounts`, `owners` and `filters` are ANDed; an empty list matches everything.
pub fn account_matches(
    filter: &AccountPrefilter,
    pubkey: &Pubkey,
    owner: &Pubkey,
    lamports: u64,
    data: &[u8],
) -> bool {
    let by_key = filter.accounts.is_empty() || filter.accounts.contains(&to_core(pubkey));
    let by_owner = filter.owners.is_empty() || filter.owners.contains(&to_core(owner));
    by_key
        && by_owner
        && filter
            .filters
            .iter()
            .all(|f| data_filter_matches(f, lamports, data))
}

fn data_filter_matches(filter: &AccountFilter, lamports: u64, data: &[u8]) -> bool {
    match filter {
        AccountFilter::Memcmp { offset, data: want } => {
            let bytes = match want {
                MemcmpData::Bytes(b) => b.clone(),
                MemcmpData::Base58(s) => match bs58::decode(s).into_vec() {
                    Ok(b) => b,
                    Err(_) => return false,
                },
                MemcmpData::Base64(s) => match crate::convert::decode_base64(s) {
                    Some(b) => b,
                    None => return false,
                },
            };
            let start = *offset as usize;
            data.get(start..start + bytes.len()) == Some(bytes.as_slice())
        }
        AccountFilter::DataSize(n) => data.len() as u64 == *n,
        AccountFilter::TokenAccountState(initialized) => {
            let is_init = data.len() >= TOKEN_ACCOUNT_LEN
                && data
                    .get(TOKEN_ACCOUNT_STATE_OFFSET)
                    .is_some_and(|s| *s != 0);
            is_init == *initialized
        }
        AccountFilter::Lamports(cmp) => match cmp {
            LamportsCmp::Eq(v) => lamports == *v,
            LamportsCmp::Ne(v) => lamports != *v,
            LamportsCmp::Lt(v) => lamports < *v,
            LamportsCmp::Gt(v) => lamports > *v,
        },
    }
}

fn to_core(key: &Pubkey) -> shipstern_core::Pubkey {
    shipstern_core::KeyBytes(key.to_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use shipstern_core::PrefilterBuilder;
    use std::collections::HashMap;
    use yellowstone_grpc_proto::geyser::{SubscribeUpdateAccount, SubscribeUpdateAccountInfo};

    fn filters(name: &str, b: PrefilterBuilder) -> Filters {
        let mut map = HashMap::new();
        map.insert(name.to_string(), b.build().unwrap());
        Filters::new(map)
    }

    fn account_update(pubkey: Pubkey, owner: Pubkey, data: Vec<u8>) -> SubscribeUpdate {
        SubscribeUpdate {
            filters: vec!["upstream".into()],
            update_oneof: Some(UpdateOneof::Account(SubscribeUpdateAccount {
                account: Some(SubscribeUpdateAccountInfo {
                    pubkey: pubkey.to_bytes().to_vec(),
                    owner: owner.to_bytes().to_vec(),
                    data,
                    lamports: 1,
                    ..Default::default()
                }),
                slot: 7,
                is_startup: false,
            })),
            created_at: None,
        }
    }

    #[test]
    fn owner_plus_memcmp_retags_with_local_name() {
        let owner = Pubkey::new_unique();
        let f = filters(
            "prog:acct",
            PrefilterBuilder::default()
                .account_owners([shipstern_core::KeyBytes(owner.to_bytes())])
                .account_filters([AccountFilter::Memcmp {
                    offset: 2,
                    data: MemcmpData::Bytes(vec![9, 9]),
                }]),
        );
        let hit = match_update(
            &f,
            account_update(Pubkey::new_unique(), owner, vec![0, 0, 9, 9]),
        );
        assert_eq!(hit.unwrap().filters, vec!["prog:acct".to_string()]);

        let miss = match_update(
            &f,
            account_update(Pubkey::new_unique(), owner, vec![0, 0, 9, 8]),
        );
        assert!(miss.is_none());
        let wrong_owner = match_update(
            &f,
            account_update(Pubkey::new_unique(), Pubkey::new_unique(), vec![0, 0, 9, 9]),
        );
        assert!(wrong_owner.is_none());
    }
}
