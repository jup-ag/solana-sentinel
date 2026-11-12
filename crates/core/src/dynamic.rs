//! A program's chain-discovered subscription, split by purpose so each stream
//! pipeline takes only the accounts its rules decode.
//!
//! A program's deps hand back one [`DynamicSubscription`]: an untagged part
//! (what [`ProgramDeps::dynamic_subscription`](crate::ProgramDeps::dynamic_subscription)
//! returns) plus any number of parts tagged with a [`DynamicTag`]. Each stream
//! pipeline declares a [`DynamicScope`] through
//! [`StreamRules::dynamic`](crate::StreamRules::dynamic):
//!
//! - [`DynamicScope::All`] (the default) takes every part, tagged or not: the
//!   behaviour of programs that never tag anything.
//! - [`DynamicScope::only`] takes the listed tags and nothing else, the untagged
//!   part included; [`DynamicScope::none`] takes nothing.

use crate::subscription::Subscription;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

/// Names one purpose of a program's dynamic accounts ("oracle_sources", "listed_mints").
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DynamicTag(&'static str);

impl DynamicTag {
    pub const fn new(name: &'static str) -> Self {
        Self(name)
    }

    pub const fn name(self) -> &'static str {
        self.0
    }
}

impl fmt::Display for DynamicTag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

/// Which parts of the program's dynamic subscription one stream pipeline takes.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum DynamicScope {
    /// Every part, tagged or untagged.
    #[default]
    All,
    /// Only these tags; the untagged part is left out.
    Only(BTreeSet<DynamicTag>),
}

impl DynamicScope {
    pub fn only<I: IntoIterator<Item = DynamicTag>>(tags: I) -> Self {
        Self::Only(tags.into_iter().collect())
    }

    /// Static subscription only; refreshes never touch the pipeline.
    pub fn none() -> Self {
        Self::Only(BTreeSet::new())
    }
}

impl fmt::Display for DynamicScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::All => f.write_str("all"),
            Self::Only(tags) if tags.is_empty() => f.write_str("none"),
            Self::Only(tags) => {
                let names: Vec<&str> = tags.iter().map(|t| t.name()).collect();
                write!(f, "{}", names.join(","))
            }
        }
    }
}

/// A program's dynamic subscription, by purpose.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DynamicSubscription {
    untagged: Subscription,
    tagged: BTreeMap<DynamicTag, Subscription>,
}

impl DynamicSubscription {
    pub fn new() -> Self {
        Self::default()
    }

    /// Everything in `sub` is untagged: every [`DynamicScope::All`] pipeline takes it.
    pub fn untagged(sub: Subscription) -> Self {
        Self {
            untagged: sub,
            tagged: BTreeMap::new(),
        }
    }

    /// Add `sub` under `tag`, merged with whatever the tag already holds.
    #[must_use]
    pub fn tag(mut self, tag: DynamicTag, sub: Subscription) -> Self {
        let slot = self.tagged.entry(tag).or_default();
        *slot = std::mem::take(slot).union(sub);
        self
    }

    /// The part held under `tag`, empty when none.
    pub fn get(&self, tag: DynamicTag) -> Subscription {
        self.tagged.get(&tag).cloned().unwrap_or_default()
    }

    /// Every part, tagged or not.
    pub fn all(&self) -> Subscription {
        self.tagged
            .values()
            .fold(self.untagged.clone(), |acc, s| acc.union(s.clone()))
    }

    /// What a pipeline with `scope` subscribes to on top of its static subscription.
    pub fn select(&self, scope: &DynamicScope) -> Subscription {
        match scope {
            DynamicScope::All => self.all(),
            DynamicScope::Only(tags) => tags
                .iter()
                .filter_map(|t| self.tagged.get(t))
                .fold(Subscription::default(), |acc, s| acc.union(s.clone())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_pubkey::Pubkey;

    const ORACLES: DynamicTag = DynamicTag::new("oracles");
    const MINTS: DynamicTag = DynamicTag::new("mints");
    const FOLLOW: DynamicTag = DynamicTag::new("follow");

    fn key(n: u8) -> Pubkey {
        Pubkey::new_from_array([n; 32])
    }

    fn sample() -> DynamicSubscription {
        DynamicSubscription::untagged(Subscription::new().accounts([key(1)]))
            .tag(ORACLES, Subscription::new().accounts([key(2)]))
            .tag(MINTS, Subscription::new().accounts([key(3)]))
            .tag(FOLLOW, Subscription::new().tx_accounts_include([key(4)]))
    }

    #[test]
    fn scope_routes_tags() {
        let dynamic = sample();
        assert_eq!(
            dynamic.select(&DynamicScope::only([ORACLES])),
            Subscription::new().accounts([key(2)])
        );
        assert_eq!(
            dynamic.select(&DynamicScope::only([MINTS, FOLLOW])),
            Subscription::new()
                .accounts([key(3)])
                .tx_accounts_include([key(4)])
        );
        assert!(dynamic.select(&DynamicScope::none()).is_empty());
        // An unknown tag selects nothing, and the untagged part never leaks into `Only`.
        assert!(dynamic
            .select(&DynamicScope::only([DynamicTag::new("other")]))
            .is_empty());
        assert_eq!(
            dynamic.select(&DynamicScope::All),
            Subscription::new()
                .accounts([key(1), key(2), key(3)])
                .tx_accounts_include([key(4)])
        );
    }

    #[test]
    fn tagging_twice_merges() {
        let dynamic = DynamicSubscription::new()
            .tag(ORACLES, Subscription::new().accounts([key(1)]))
            .tag(ORACLES, Subscription::new().accounts([key(2)]));
        assert_eq!(
            dynamic.get(ORACLES),
            Subscription::new().accounts([key(1), key(2)])
        );
    }

    #[test]
    fn scope_display() {
        assert_eq!(DynamicScope::All.to_string(), "all");
        assert_eq!(DynamicScope::none().to_string(), "none");
        assert_eq!(
            DynamicScope::only([ORACLES, MINTS]).to_string(),
            "mints,oracles"
        );
    }
}
