//! Keyed last-seen state shared by change-detection rules: seed at bootstrap,
//! `replace` on every update, compare against what came back. Locked inside,
//! so a rule can use it from `evaluate(&self)`.

use domain::rules::RuleId;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::fmt::Display;
use std::hash::Hash;
use tracing::{debug, warn};

/// What `replace` does with a key the cache has never seen.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnknownKey {
    /// Log and ignore; the key stays unknown, so it is never evaluated (the old behaviour).
    Warn,
    /// Store it as the baseline and skip this evaluation; the next update compares against it.
    InsertAndSkip,
}

#[derive(Debug)]
pub struct StateCache<K, V> {
    map: Mutex<HashMap<K, V>>,
    on_unknown: UnknownKey,
}

impl<K: Hash + Eq + Display, V: Clone> Default for StateCache<K, V> {
    fn default() -> Self {
        Self::new(UnknownKey::InsertAndSkip)
    }
}

impl<K: Hash + Eq + Display, V: Clone> StateCache<K, V> {
    pub fn new(on_unknown: UnknownKey) -> Self {
        Self {
            map: Mutex::new(HashMap::new()),
            on_unknown,
        }
    }

    pub fn seed<I: IntoIterator<Item = (K, V)>>(&self, entries: I) {
        self.map.lock().extend(entries);
    }

    /// Store `value` under `key` and return what was there. `None` means the key
    /// was unknown and the policy applied; the caller skips this evaluation.
    pub fn replace(&self, rule: RuleId, key: K, value: V) -> Option<V> {
        let mut map = self.map.lock();
        if let Some(slot) = map.get_mut(&key) {
            return Some(std::mem::replace(slot, value));
        }
        match self.on_unknown {
            UnknownKey::Warn => {
                warn!(rule = %rule, key = %key, "update for key not in cache; ignoring")
            }
            UnknownKey::InsertAndSkip => {
                debug!(rule = %rule, key = %key, "update for key not in cache; storing as baseline");
                map.insert(key, value);
            }
        }
        None
    }

    pub fn get(&self, key: &K) -> Option<V> {
        self.map.lock().get(key).cloned()
    }

    pub fn contains(&self, key: &K) -> bool {
        self.map.lock().contains_key(key)
    }

    pub fn len(&self) -> usize {
        self.map.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.lock().is_empty()
    }

    /// Run `f` over every entry under the lock; keep it short.
    pub fn for_each(&self, mut f: impl FnMut(&K, &V)) {
        for (k, v) in self.map.lock().iter() {
            f(k, v);
        }
    }
}
