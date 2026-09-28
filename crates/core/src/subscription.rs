//! The subscription a rule or program asks for, in framework terms; distinct
//! from `shipstern_core::Prefilter`, the wire-level filter it expands into.
//!
//! One [`Subscription`] can expand to several yellowstone filters: the by-pubkey
//! account list is one, and each owner + data-filter set is another, because
//! within a single yellowstone filter `account`, `owner` and `filters` are
//! ANDed while lists inside one field are ORed.

use shipstern_core::{AccountFilter, MemcmpData, PrefilterBuilder};
use solana_pubkey::Pubkey;
use std::collections::HashSet;

/// Accounts selected by owner program, optionally narrowed by data filters.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OwnedAccounts {
    pub owners: HashSet<Pubkey>,
    /// ANDed server-side; empty means every account of the owners.
    pub filters: Vec<AccountFilter>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Subscription {
    /// Account updates for exactly these pubkeys.
    pub accounts: HashSet<Pubkey>,
    /// Account updates selected by owner program (+ memcmp / datasize), one subscription each.
    pub owned: Vec<OwnedAccounts>,
    /// Transactions touching at least one of these accounts.
    pub tx_accounts_include: HashSet<Pubkey>,
    /// Transactions touching all of these accounts.
    pub tx_accounts_required: HashSet<Pubkey>,
}

impl Subscription {
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn accounts<I: IntoIterator<Item = Pubkey>>(mut self, it: I) -> Self {
        self.accounts.extend(it);
        self
    }

    /// Subscribe every account owned by `owner` that passes `filters`.
    #[must_use]
    pub fn owned<I: IntoIterator<Item = AccountFilter>>(
        mut self,
        owner: Pubkey,
        filters: I,
    ) -> Self {
        let mut sub = OwnedAccounts::default();
        sub.owners.insert(owner);
        sub.filters = filters.into_iter().collect();
        self.push_owned(sub);
        self
    }

    #[must_use]
    pub fn tx_accounts_include<I: IntoIterator<Item = Pubkey>>(mut self, it: I) -> Self {
        self.tx_accounts_include.extend(it);
        self
    }

    #[must_use]
    pub fn tx_accounts_required<I: IntoIterator<Item = Pubkey>>(mut self, it: I) -> Self {
        self.tx_accounts_required.extend(it);
        self
    }

    /// Memcmp on raw bytes at `offset`; the usual way to select one account type by discriminator.
    pub fn memcmp(offset: usize, bytes: &[u8]) -> AccountFilter {
        AccountFilter::Memcmp {
            offset: offset as u64,
            data: MemcmpData::Bytes(bytes.to_vec()),
        }
    }

    /// Union of two subscriptions. Owned subscriptions with the same data filters merge their owners.
    #[must_use]
    pub fn union(mut self, other: Self) -> Self {
        self.accounts.extend(other.accounts);
        self.tx_accounts_include.extend(other.tx_accounts_include);
        self.tx_accounts_required.extend(other.tx_accounts_required);
        for sub in other.owned {
            self.push_owned(sub);
        }
        self
    }

    fn push_owned(&mut self, sub: OwnedAccounts) {
        match self.owned.iter_mut().find(|o| o.filters == sub.filters) {
            Some(existing) => existing.owners.extend(sub.owners),
            None => self.owned.push(sub),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.accounts.is_empty()
            && self.owned.is_empty()
            && self.tx_accounts_include.is_empty()
            && self.tx_accounts_required.is_empty()
    }

    pub fn watches_accounts(&self) -> bool {
        !self.accounts.is_empty() || !self.owned.is_empty()
    }

    pub fn watches_transactions(&self) -> bool {
        !self.tx_accounts_include.is_empty() || !self.tx_accounts_required.is_empty()
    }

    /// One shipstern prefilter per yellowstone account filter this expands to.
    pub fn account_prefilters(&self) -> Vec<PrefilterBuilder> {
        let mut subs = Vec::new();
        if !self.accounts.is_empty() {
            subs.push(PrefilterBuilder::default().accounts(&self.accounts));
        }
        for owned in &self.owned {
            let mut b = PrefilterBuilder::default().account_owners(&owned.owners);
            if !owned.filters.is_empty() {
                b = b.account_filters(owned.filters.iter().cloned());
            }
            subs.push(b);
        }
        subs
    }

    /// The single shipstern prefilter for an instruction or transaction pipeline.
    pub fn tx_prefilter(&self) -> PrefilterBuilder {
        let mut b = PrefilterBuilder::default();
        if !self.tx_accounts_include.is_empty() {
            b = b.transaction_accounts_include(&self.tx_accounts_include);
        }
        if !self.tx_accounts_required.is_empty() {
            b = b.transaction_accounts(&self.tx_accounts_required);
        }
        b
    }
}
