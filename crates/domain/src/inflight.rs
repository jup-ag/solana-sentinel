//! Slots of stream alerts not yet persisted to the alert outbox (or delivered, without one).
//! The slot checkpoint must not pass them: replay from it is the only way to re-raise them.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

static PENDING: Mutex<BTreeMap<u64, usize>> = Mutex::new(BTreeMap::new());

/// Holds its slot pending until the last clone drops (delivered, suppressed or given up).
#[derive(Debug, Clone)]
pub struct SlotHold(Arc<Hold>);

#[derive(Debug)]
struct Hold(u64);

impl SlotHold {
    pub fn new(slot: u64) -> Self {
        *pending().entry(slot).or_default() += 1;
        Self(Arc::new(Hold(slot)))
    }

    /// The slot this hold keeps pending: the stream slot that raised the alert.
    pub fn slot(&self) -> u64 {
        self.0 .0
    }
}

impl Drop for Hold {
    fn drop(&mut self) {
        let mut pending = pending();
        if let Some(n) = pending.get_mut(&self.0) {
            *n -= 1;
            if *n == 0 {
                pending.remove(&self.0);
            }
        }
    }
}

/// The oldest slot with an undelivered alert.
pub fn oldest_pending_slot() -> Option<u64> {
    pending().keys().next().copied()
}

fn pending() -> std::sync::MutexGuard<'static, BTreeMap<u64, usize>> {
    PENDING
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
