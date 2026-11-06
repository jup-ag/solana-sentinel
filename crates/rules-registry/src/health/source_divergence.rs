//! Poll rule over a `multi` data source's children: a child that is silent
//! (nothing, not even a slot, for `silence_seconds`), lagging (more than
//! `max_lag_slots` behind the most advanced child), or missing more than
//! `max_missing_pct` of the items the other children delivered over the last
//! `window_seconds`. One alert lists every unhealthy child; its key is the set
//! of (child, problem) pairs, so a new problem alerts again within the cooldown.
//!
//! With a single-source config there are no children and the rule never fires.

use crate::health::{HealthDeps, PROGRAM};
use serde::Deserialize;
use solana_pubkey::Pubkey;
use solana_sentinel::observability::source_health::ChildSnapshot;
use solana_sentinel::{
    Alert, DedupScope, Deps, PollCtx, PollRule, RuleId, Severity, SourceHealth, Tick,
};
use std::collections::HashSet;
use std::fmt::Write as _;
use std::time::{Duration, Instant};
use tracing::{info, warn};

const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(30);

fn default_max_lag_slots() -> u64 {
    50
}
fn default_silence_seconds() -> u64 {
    60
}
fn default_max_missing_pct() -> f64 {
    5.0
}
fn default_min_expected() -> u64 {
    100
}
fn default_window_seconds() -> u64 {
    300
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceDivergenceConfig {
    /// A child this many slots behind the most advanced one is lagging.
    #[serde(default = "default_max_lag_slots")]
    pub max_lag_slots: u64,
    /// A child that delivered nothing for this long is silent (down children included).
    #[serde(default = "default_silence_seconds")]
    pub silence_seconds: u64,
    /// A child missing more than this share of the others' items is diverging.
    #[serde(default = "default_max_missing_pct")]
    pub max_missing_pct: f64,
    /// Fewer expected items than this in the window is too small a sample to judge.
    #[serde(default = "default_min_expected")]
    pub min_expected: u64,
    /// Window the missing share is computed over (at most one hour).
    #[serde(default = "default_window_seconds")]
    pub window_seconds: u64,
    pub poll_interval_seconds: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Problem {
    Silent,
    Lagging,
    Missing,
}

impl Problem {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Silent => "silent",
            Self::Lagging => "lagging",
            Self::Missing => "missing",
        }
    }
}

#[derive(Debug)]
pub struct SourceDivergence {
    cfg: SourceDivergenceConfig,
    health: SourceHealth,
    pod_id: String,
}

impl SourceDivergence {
    fn problems(&self, c: &ChildSnapshot, children: usize) -> Vec<Problem> {
        let mut out = Vec::new();
        if c.idle_for >= Duration::from_secs(self.cfg.silence_seconds) {
            out.push(Problem::Silent);
        }
        if children > 1 && c.slot_lag > self.cfg.max_lag_slots {
            out.push(Problem::Lagging);
        }
        let pct = c.window.missing_pct().unwrap_or(0.0);
        if children > 1
            && c.window.expected >= self.cfg.min_expected
            && pct > self.cfg.max_missing_pct
        {
            out.push(Problem::Missing);
        }
        out
    }

    /// The alert for `snapshot`, if any child is unhealthy.
    fn check(&self, snapshot: &[ChildSnapshot]) -> Option<Alert> {
        let mut key: Vec<String> = Vec::new();
        let mut lines = String::new();
        let mut alert = Alert::for_rule(Self::ID).severity(Severity::Critical);
        for c in snapshot {
            let problems = self.problems(c, snapshot.len());
            let pct = c.window.missing_pct().unwrap_or(0.0);
            info!(
                source = %c.name,
                up = c.up,
                idle_secs = c.idle_for.as_secs(),
                slot = c.max_slot,
                slot_lag = c.slot_lag,
                delivered = c.window.delivered,
                first = c.window.first,
                expected = c.window.expected,
                missing = c.window.missing,
                missing_pct = format!("{pct:.2}"),
                "data source child health"
            );
            if problems.is_empty() {
                continue;
            }
            let names: Vec<&str> = problems.iter().map(|p| p.as_str()).collect();
            warn!(source = %c.name, problems = ?names, "data source child unhealthy");
            key.extend(
                problems
                    .iter()
                    .map(|p| format!("{}:{}", c.name, p.as_str())),
            );
            let _ = write!(
                lines,
                "\n- {}: {} (up={}, idle {}s, {} slots behind, missing {}/{} = {pct:.1}% over \
                 {}s{})",
                c.name,
                names.join(", "),
                c.up,
                c.idle_for.as_secs(),
                c.slot_lag,
                c.window.missing,
                c.window.expected,
                self.cfg.window_seconds,
                c.last_error
                    .as_deref()
                    .filter(|_| !c.up)
                    .map(|e| format!(", last error: {e}"))
                    .unwrap_or_default(),
            );
            alert = alert
                .meta(format!("{}_problems", c.name), names.join(","))
                .meta(format!("{}_slot_lag", c.name), c.slot_lag)
                .meta(format!("{}_idle_seconds", c.name), c.idle_for.as_secs())
                .meta(format!("{}_missing_pct", c.name), format!("{pct:.2}"));
        }
        if key.is_empty() {
            return None;
        }
        key.sort();
        Some(
            alert
                .message(format!("Data source divergence on {}:{lines}", self.pod_id))
                .meta("node", &self.pod_id)
                .key(std::iter::once(self.pod_id.clone()).chain(key))
                .scope(DedupScope::Monitor)
                .build(),
        )
    }
}

impl PollRule for SourceDivergence {
    const ID: RuleId = RuleId::new(PROGRAM, "source_divergence");
    type Config = SourceDivergenceConfig;
    type Item = Tick;

    fn accounts(_cfg: &Self::Config) -> HashSet<Pubkey> {
        HashSet::new()
    }

    fn poll_interval(cfg: &Self::Config) -> Option<Duration> {
        Some(
            cfg.poll_interval_seconds
                .map_or(DEFAULT_POLL_INTERVAL, Duration::from_secs),
        )
    }

    fn build(cfg: Self::Config, deps: &Deps) -> anyhow::Result<Self> {
        anyhow::ensure!(
            cfg.window_seconds > 0 && cfg.window_seconds <= 3600,
            "window_seconds must be in 1..=3600"
        );
        anyhow::ensure!(
            (0.0..=100.0).contains(&cfg.max_missing_pct),
            "max_missing_pct must be a percentage"
        );
        let health = deps.get::<HealthDeps>()?;
        Ok(Self {
            cfg,
            health: health.source_health.clone(),
            pod_id: health.pod_id.clone(),
        })
    }

    fn evaluate(&self, _tick: &Tick, _ctx: &PollCtx) -> Option<Alert> {
        let snapshot = self
            .health
            .snapshot(Instant::now(), Duration::from_secs(self.cfg.window_seconds));
        self.check(&snapshot)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sentinel::observability::source_health::DeliveryCounts;

    fn rule() -> SourceDivergence {
        SourceDivergence {
            cfg: SourceDivergenceConfig {
                max_lag_slots: 50,
                silence_seconds: 60,
                max_missing_pct: 5.0,
                min_expected: 100,
                window_seconds: 300,
                poll_interval_seconds: None,
            },
            health: SourceHealth::new(),
            pod_id: "pod-1".into(),
        }
    }

    fn child(name: &str) -> ChildSnapshot {
        ChildSnapshot {
            name: name.into(),
            up: true,
            restarts: 0,
            last_error: None,
            idle_for: Duration::from_secs(1),
            max_slot: 1_000,
            slot_lag: 0,
            window: DeliveryCounts {
                delivered: 500,
                first: 250,
                expected: 500,
                missing: 0,
                late: 0,
            },
            totals: DeliveryCounts::default(),
            delay_p50: None,
            delay_p95: None,
        }
    }

    #[test]
    fn healthy_children_do_not_alert() {
        assert!(rule().check(&[child("a"), child("b")]).is_none());
        assert!(rule().check(&[]).is_none());
    }

    #[test]
    fn lagging_silent_and_missing_children_alert_together() {
        let mut lagging = child("lag");
        lagging.slot_lag = 51;
        let mut silent = child("quiet");
        silent.idle_for = Duration::from_secs(61);
        silent.up = false;
        silent.last_error = Some("stream failed: refused".into());
        let mut missing = child("holey");
        missing.window.missing = 30; // 6%
        let alert = rule()
            .check(&[child("ok"), lagging, silent, missing])
            .expect("alert");
        let msg = &alert.message;
        assert!(msg.contains("lag: lagging"), "{msg}");
        assert!(msg.contains("quiet: silent"), "{msg}");
        assert!(msg.contains("last error: stream failed"), "{msg}");
        assert!(msg.contains("holey: missing"), "{msg}");
        assert!(!msg.contains("- ok"), "{msg}");

        // The key follows the set of problems: same set, same key; a new problem, a new key.
        let mut lagging = child("lag");
        lagging.slot_lag = 51;
        let again = rule().check(&[child("ok"), lagging.clone()]).unwrap();
        let same = rule().check(&[lagging.clone(), child("ok")]).unwrap();
        assert_eq!(again.local_gate_key(), same.local_gate_key());
        lagging.window.missing = 30;
        let more = rule().check(&[child("ok"), lagging]).unwrap();
        assert_ne!(again.local_gate_key(), more.local_gate_key());
    }

    #[test]
    fn small_samples_and_single_children_are_not_judged_on_divergence() {
        let mut few = child("few");
        few.window.expected = 50;
        few.window.missing = 50;
        assert!(rule().check(&[child("a"), few]).is_none());

        let mut alone = child("alone");
        alone.slot_lag = 10_000;
        alone.window.missing = 500;
        assert!(rule().check(std::slice::from_ref(&alone)).is_none());
        alone.idle_for = Duration::from_secs(600);
        assert!(
            rule().check(&[alone]).is_some(),
            "silence still counts alone"
        );
    }
}
