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
struct Entry<V> {
    value: V,
    /// RPC slot the value was read at, until the stream first replaces it.
    seeded_at: Option<u64>,
}

#[derive(Debug)]
pub struct StateCache<K, V> {
    map: Mutex<HashMap<K, Entry<V>>>,
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

    /// Seed with state read over RPC at `slot` (the response context slot).
    pub fn seed<I: IntoIterator<Item = (K, V)>>(&self, slot: u64, entries: I) {
        self.map
            .lock()
            .extend(entries.into_iter().map(|(k, value)| {
                let seeded_at = Some(slot);
                (k, Entry { value, seeded_at })
            }));
    }

    /// Store `value` (seen at `slot`) under `key` and return what was there. `None` means
    /// there is nothing valid to compare against; the caller skips this evaluation.
    ///
    /// A replay from the slot checkpoint delivers updates older than the RPC seed. Comparing
    /// them to newer state raises false alerts, so the first such update becomes the baseline.
    pub fn replace(&self, rule: RuleId, key: K, value: V, slot: u64) -> Option<V> {
        let mut map = self.map.lock();
        if let Some(entry) = map.get_mut(&key) {
            let seeded_at = entry.seeded_at.take();
            if seeded_at.is_some_and(|seeded| slot <= seeded) {
                debug!(rule = %rule, key = %key, slot, "update predates the seed; using it as baseline");
                entry.value = value;
                return None;
            }
            return Some(std::mem::replace(&mut entry.value, value));
        }
        match self.on_unknown {
            UnknownKey::Warn => {
                warn!(rule = %rule, key = %key, "update for key not in cache; ignoring")
            }
            UnknownKey::InsertAndSkip => {
                debug!(rule = %rule, key = %key, "update for key not in cache; storing as baseline");
                let seeded_at = None;
                map.insert(key, Entry { value, seeded_at });
            }
        }
        None
    }

    pub fn get(&self, key: &K) -> Option<V> {
        self.map.lock().get(key).map(|e| e.value.clone())
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
        for (k, e) in self.map.lock().iter() {
            f(k, &e.value);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use domain::programs::Program;

    const RULE: RuleId = RuleId::new(Program::new("test_program"), "cache");

    /// Seeded at slot 100 with the post-downtime value; the replay from an older
    /// checkpoint must not compare pre-seed updates against it.
    #[test]
    fn replayed_updates_before_seed_become_baseline() {
        let cache: StateCache<u8, &str> = StateCache::default();
        cache.seed(100, [(1, "B")]);

        assert_eq!(
            cache.replace(RULE, 1, "A", 90),
            None,
            "older than seed: baseline"
        );
        assert_eq!(
            cache.replace(RULE, 1, "B", 95),
            Some("A"),
            "change during downtime is seen"
        );
        assert_eq!(cache.replace(RULE, 1, "C", 120), Some("B"));

        cache.seed(100, [(2, "X")]);
        assert_eq!(
            cache.replace(RULE, 2, "Y", 101),
            Some("X"),
            "newer than seed compares"
        );
    }
}
