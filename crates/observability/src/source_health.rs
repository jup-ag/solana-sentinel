//! Per-child health of a `multi` data source: one entry per child source,
//! written by the multi source as it merges their streams, read by the
//! `health/source_divergence` rule and exported as Prometheus metrics.
//!
//! The handle is cheap to clone; every clone sees the same state. With a
//! single-source config nothing registers and [`SourceHealth::snapshot`] is
//! empty, so rules reading it stay quiet.

use crate::prometheus::metrics::{
    record_source_count, record_source_delay, record_source_restart, record_source_slot,
    record_source_up, SourceCount,
};
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Width of one rolling-window bucket.
const BUCKET: Duration = Duration::from_secs(10);
/// Buckets kept: one hour, the longest window a rule can ask for.
const MAX_BUCKETS: usize = 360;
/// Delivery delays kept per child for percentiles.
const DELAY_SAMPLES: usize = 4096;

/// What one child delivered, compared with the others.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DeliveryCounts {
    /// Transactions and account updates the child delivered, duplicates included.
    pub delivered: u64,
    /// Of those, items no other child had delivered yet.
    pub first: u64,
    /// Items another child delivered while this one was connected and past them.
    pub expected: u64,
    /// Of `expected`, items this child had not delivered by the end of the grace window.
    pub missing: u64,
    /// Items this child delivered only after the grace window had closed.
    pub late: u64,
}

impl DeliveryCounts {
    fn add(&mut self, other: &Self) {
        self.delivered += other.delivered;
        self.first += other.first;
        self.expected += other.expected;
        self.missing += other.missing;
        self.late += other.late;
    }

    /// Share of the other children's items this child never delivered, in percent.
    pub fn missing_pct(&self) -> Option<f64> {
        (self.expected > 0).then(|| self.missing as f64 * 100.0 / self.expected as f64)
    }
}

#[derive(Debug)]
struct Child {
    name: String,
    up: bool,
    /// Last (re)connect, or registration before the first one.
    since: Instant,
    last_update_at: Option<Instant>,
    max_slot: u64,
    restarts: u64,
    last_error: Option<String>,
    totals: DeliveryCounts,
    buckets: VecDeque<(Instant, DeliveryCounts)>,
    delays_ms: VecDeque<u32>,
}

impl Child {
    fn new(name: String, now: Instant) -> Self {
        Self {
            name,
            up: false,
            since: now,
            last_update_at: None,
            max_slot: 0,
            restarts: 0,
            last_error: None,
            totals: DeliveryCounts::default(),
            buckets: VecDeque::new(),
            delays_ms: VecDeque::new(),
        }
    }

    fn bucket(&mut self, now: Instant) -> &mut DeliveryCounts {
        let fresh = self
            .buckets
            .back()
            .is_none_or(|(start, _)| now.saturating_duration_since(*start) >= BUCKET);
        if fresh {
            if self.buckets.len() == MAX_BUCKETS {
                self.buckets.pop_front();
            }
            self.buckets.push_back((now, DeliveryCounts::default()));
        }
        &mut self.buckets.back_mut().expect("bucket just ensured").1
    }

    fn window(&self, now: Instant, window: Duration) -> DeliveryCounts {
        let mut sum = DeliveryCounts::default();
        for (start, counts) in self.buckets.iter().rev() {
            if now.saturating_duration_since(*start) > window {
                break;
            }
            sum.add(counts);
        }
        sum
    }
}

/// One child's state at snapshot time.
#[derive(Debug, Clone)]
pub struct ChildSnapshot {
    pub name: String,
    /// Connected (its `connect` is running).
    pub up: bool,
    pub restarts: u64,
    pub last_error: Option<String>,
    /// Time since the child last delivered anything, slots included (or since it connected).
    pub idle_for: Duration,
    /// Highest slot the child delivered.
    pub max_slot: u64,
    /// Slots behind the most advanced child.
    pub slot_lag: u64,
    /// Counts over the requested window.
    pub window: DeliveryCounts,
    /// Counts since start.
    pub totals: DeliveryCounts,
    /// Median and 95th percentile of how long after the first child this one delivered
    /// the same item, over the last few thousand items it did not deliver first.
    pub delay_p50: Option<Duration>,
    pub delay_p95: Option<Duration>,
}

/// Shared per-child health; see the module docs.
#[derive(Debug, Clone, Default)]
pub struct SourceHealth {
    inner: Arc<Mutex<Vec<Child>>>,
}

impl SourceHealth {
    pub fn new() -> Self {
        Self::default()
    }

    /// Start tracking `names` (in child order), dropping anything tracked before.
    pub fn register(&self, names: &[String]) {
        let now = Instant::now();
        *self.inner.lock() = names.iter().map(|n| Child::new(n.clone(), now)).collect();
        for name in names {
            record_source_up(name, false);
        }
    }

    pub fn is_empty(&self) -> bool {
        self.inner.lock().is_empty()
    }

    /// Child `i`'s `connect` started; `restart` when it is not the first start.
    pub fn connected(&self, i: usize, restart: bool, now: Instant) {
        let mut children = self.inner.lock();
        let Some(c) = children.get_mut(i) else { return };
        c.up = true;
        c.since = now;
        record_source_up(&c.name, true);
        if restart {
            c.restarts += 1;
            record_source_restart(&c.name);
        }
    }

    /// Child `i`'s `connect` returned or failed.
    pub fn disconnected(&self, i: usize, reason: &str) {
        let mut children = self.inner.lock();
        let Some(c) = children.get_mut(i) else { return };
        c.up = false;
        c.last_error = Some(reason.to_string());
        record_source_up(&c.name, false);
    }

    /// Child `i` delivered something (any update, slots included) at `slot` (0 = unknown).
    pub fn delivered_any(&self, i: usize, slot: u64, now: Instant) {
        let mut children = self.inner.lock();
        let Some(c) = children.get_mut(i) else { return };
        c.last_update_at = Some(now);
        if slot > c.max_slot {
            c.max_slot = slot;
        }
    }

    /// Add `delta` to child `i`'s counters.
    pub fn count(&self, i: usize, delta: DeliveryCounts, now: Instant) {
        let mut children = self.inner.lock();
        let Some(c) = children.get_mut(i) else { return };
        c.totals.add(&delta);
        c.bucket(now).add(&delta);
        for (kind, n) in [
            (SourceCount::Delivered, delta.delivered),
            (SourceCount::First, delta.first),
            (SourceCount::Expected, delta.expected),
            (SourceCount::Missing, delta.missing),
            (SourceCount::Late, delta.late),
        ] {
            if n > 0 {
                record_source_count(&c.name, kind, n);
            }
        }
    }

    /// Child `i` delivered an item `delay` after the first child did.
    pub fn delay(&self, i: usize, delay: Duration) {
        let mut children = self.inner.lock();
        let Some(c) = children.get_mut(i) else { return };
        if c.delays_ms.len() == DELAY_SAMPLES {
            c.delays_ms.pop_front();
        }
        c.delays_ms
            .push_back(u32::try_from(delay.as_millis()).unwrap_or(u32::MAX));
        record_source_delay(&c.name, delay.as_secs_f64());
    }

    /// Export slot and slot-lag gauges for every child.
    pub fn publish_slots(&self) {
        let children = self.inner.lock();
        let top = children.iter().map(|c| c.max_slot).max().unwrap_or(0);
        for c in children.iter() {
            record_source_slot(&c.name, c.max_slot, top.saturating_sub(c.max_slot));
        }
    }

    /// Every child's state, counts summed over the last `window`.
    pub fn snapshot(&self, now: Instant, window: Duration) -> Vec<ChildSnapshot> {
        let children = self.inner.lock();
        let top = children.iter().map(|c| c.max_slot).max().unwrap_or(0);
        children
            .iter()
            .map(|c| {
                let mut delays: Vec<u32> = c.delays_ms.iter().copied().collect();
                delays.sort_unstable();
                let pct = |p: usize| {
                    (!delays.is_empty()).then(|| {
                        let idx = (delays.len() - 1) * p / 100;
                        Duration::from_millis(u64::from(delays[idx]))
                    })
                };
                let last = c.last_update_at.map_or(c.since, |t| t.max(c.since));
                ChildSnapshot {
                    name: c.name.clone(),
                    up: c.up,
                    restarts: c.restarts,
                    last_error: c.last_error.clone(),
                    idle_for: now.saturating_duration_since(last),
                    max_slot: c.max_slot,
                    slot_lag: top.saturating_sub(c.max_slot),
                    window: c.window(now, window),
                    totals: c.totals,
                    delay_p50: pct(50),
                    delay_p95: pct(95),
                }
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn window_sums_recent_buckets_only() {
        let h = SourceHealth::new();
        h.register(&["a".into(), "b".into()]);
        let t0 = Instant::now();
        let one = DeliveryCounts {
            expected: 1,
            missing: 1,
            ..Default::default()
        };
        h.count(0, one, t0);
        h.count(0, one, t0 + Duration::from_secs(120));
        let snap = h.snapshot(t0 + Duration::from_secs(125), Duration::from_secs(60));
        assert_eq!(snap[0].window.missing, 1);
        assert_eq!(snap[0].totals.missing, 2);
        assert_eq!(snap[0].window.missing_pct(), Some(100.0));
        assert_eq!(snap[1].window.missing_pct(), None);
    }

    #[test]
    fn slot_lag_is_relative_to_the_most_advanced_child() {
        let h = SourceHealth::new();
        h.register(&["a".into(), "b".into()]);
        let now = Instant::now();
        h.delivered_any(0, 1_000, now);
        h.delivered_any(1, 940, now);
        let snap = h.snapshot(now, Duration::from_secs(60));
        assert_eq!(snap[0].slot_lag, 0);
        assert_eq!(snap[1].slot_lag, 60);
    }

    #[test]
    fn delay_percentiles() {
        let h = SourceHealth::new();
        h.register(&["a".into()]);
        for ms in 1..=100 {
            h.delay(0, Duration::from_millis(ms));
        }
        let snap = h.snapshot(Instant::now(), Duration::from_secs(60));
        assert_eq!(snap[0].delay_p50, Some(Duration::from_millis(50)));
        assert_eq!(snap[0].delay_p95, Some(Duration::from_millis(95)));
    }
}
