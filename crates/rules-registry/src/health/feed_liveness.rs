//! In-process rule: the monitor went blind somewhere.
//!
//! Every tick (no RPC) it reads the liveness registry the framework fills as pipelines,
//! rule inputs and poll loops receive data, and raises a Critical `action = alert` for:
//!
//! - `stream_stalled`: the gRPC stream delivered no slot for `stream_stall_seconds` (or no
//!   first slot within `first_slot_grace_seconds`). Every feed is silent with it, so no
//!   per-feed alert is raised on that tick.
//! - `poll_failing`: a poll source failed `poll_failure_threshold` fetches in a row.
//! - `feed_silent`: a feed that normally sees traffic has been silent longer than its bound:
//!   a configured `max_silence_seconds`, else `p99_factor` × its p99 inter-arrival gap (at
//!   least `min_silence_seconds`) once `min_samples` gaps were seen. A feed that never showed
//!   steady traffic has no learned bound and is only checked against a configured one.
//!   A rule feed whose pipeline is already silent is not alerted again.

use crate::health::{HealthDeps, DEFAULT_CHECK_INTERVAL, PROGRAM};
use serde::Deserialize;
use solana_pubkey::Pubkey;
use solana_sentinel::observability::liveness::{FeedId, FeedSnapshot};
use solana_sentinel::observability::prometheus::metrics::record_stream_slot_age;
use solana_sentinel::{
    Alert, DedupScope, Deps, FeedKind, GrpcSlotReader, Liveness, PollCtx, PollRule, RuleId,
    Severity, Tick,
};
use std::collections::HashSet;
use std::time::Duration;
use tokio::time::Instant;
use tracing::warn;

mod defaults {
    pub fn check_interval_seconds() -> u64 {
        super::DEFAULT_CHECK_INTERVAL.as_secs()
    }
    pub fn min_silence_seconds() -> u64 {
        60
    }
    pub fn p99_factor() -> f64 {
        3.0
    }
    pub fn min_samples() -> usize {
        30
    }
    pub fn stream_stall_seconds() -> u64 {
        30
    }
    pub fn first_slot_grace_seconds() -> u64 {
        600
    }
    pub fn poll_failure_threshold() -> u32 {
        3
    }
    pub fn per_rule() -> bool {
        true
    }
}

/// A fixed silence bound for the feeds matching `feed` (exact id, or a prefix ending in `*`).
/// It applies from startup, also to feeds with no learned traffic yet.
#[derive(Debug, Clone, Deserialize)]
pub struct FeedBound {
    pub feed: String,
    pub max_silence_seconds: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FeedLivenessConfig {
    #[serde(default = "defaults::check_interval_seconds")]
    pub check_interval_seconds: u64,
    /// Floor of a learned bound.
    #[serde(default = "defaults::min_silence_seconds")]
    pub min_silence_seconds: u64,
    /// Learned bound = this × the feed's p99 inter-arrival gap.
    #[serde(default = "defaults::p99_factor")]
    pub p99_factor: f64,
    /// Gaps a feed must have shown before its bound is learned.
    #[serde(default = "defaults::min_samples")]
    pub min_samples: usize,
    #[serde(default = "defaults::stream_stall_seconds")]
    pub stream_stall_seconds: u64,
    /// Before the first slot, how long the stream may take (bootstrap runs first).
    #[serde(default = "defaults::first_slot_grace_seconds")]
    pub first_slot_grace_seconds: u64,
    #[serde(default = "defaults::poll_failure_threshold")]
    pub poll_failure_threshold: u32,
    /// Also check each rule's share of its pipeline.
    #[serde(default = "defaults::per_rule")]
    pub per_rule: bool,
    #[serde(default)]
    pub bounds: Vec<FeedBound>,
    /// Feeds never checked (exact id, or a prefix ending in `*`).
    #[serde(default)]
    pub ignore: Vec<String>,
}

fn matches(pattern: &str, id: &FeedId) -> bool {
    match pattern.strip_suffix('*') {
        Some(prefix) => id.as_str().starts_with(prefix),
        None => id.as_str() == pattern,
    }
}

#[derive(Debug)]
pub struct FeedLiveness {
    config: FeedLivenessConfig,
    liveness: Liveness,
    grpc_slot: GrpcSlotReader,
    pod_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BoundSource {
    Configured,
    Learned,
}

impl BoundSource {
    fn as_str(self) -> &'static str {
        match self {
            Self::Configured => "configured",
            Self::Learned => "learned",
        }
    }
}

impl FeedLiveness {
    pub fn new(
        config: FeedLivenessConfig,
        liveness: Liveness,
        grpc_slot: GrpcSlotReader,
        pod_id: String,
    ) -> Self {
        Self {
            config,
            liveness,
            grpc_slot,
            pod_id,
        }
    }

    fn bound(&self, feed: &FeedSnapshot) -> Option<(Duration, BoundSource)> {
        if let Some(b) = self
            .config
            .bounds
            .iter()
            .find(|b| matches(&b.feed, &feed.id))
        {
            return Some((
                Duration::from_secs(b.max_silence_seconds),
                BoundSource::Configured,
            ));
        }
        feed.learned_bound(
            self.config.p99_factor,
            Duration::from_secs(self.config.min_silence_seconds),
            self.config.min_samples,
        )
        .map(|b| (b, BoundSource::Learned))
    }

    fn base(&self, check: &str) -> solana_sentinel::domain::alert::AlertBuilder {
        Alert::for_rule(Self::ID)
            .severity(Severity::Critical)
            .meta("action", "alert")
            .meta("check", check)
            .meta("node", &self.pod_id)
            .scope(DedupScope::Monitor)
    }

    /// The stream check; `Some` when the stream is stalled (and feed checks are moot).
    fn check_stream(&self) -> Option<Alert> {
        let age = self.grpc_slot.last_slot_age()?;
        record_stream_slot_age("grpc", age.as_secs_f64());
        let seen = self.grpc_slot.first_slot_seen();
        let budget = Duration::from_secs(if seen {
            self.config.stream_stall_seconds
        } else {
            self.config.first_slot_grace_seconds
        });
        if age <= budget {
            return None;
        }
        let last_slot = self.grpc_slot.load();
        let message = match last_slot {
            Some(slot) => format!(
                "Monitor blind: the gRPC stream delivered no slot for {}s (last slot {slot}); \
                 every pipeline is silent",
                age.as_secs()
            ),
            None => format!(
                "Monitor blind: the gRPC stream delivered no slot {}s after startup",
                age.as_secs()
            ),
        };
        warn!(age_s = age.as_secs(), ?last_slot, "gRPC stream stalled");
        Some(
            self.base("stream_stalled")
                .message(message)
                .meta("slot_age_s", age.as_secs())
                .meta_opt("last_slot", last_slot)
                .key([self.pod_id.as_str(), "stream_stalled"])
                .build(),
        )
    }

    /// Alerts for one tick at `now`.
    pub fn check(&self, now: Instant) -> Vec<Alert> {
        if let Some(stalled) = self.check_stream() {
            return vec![stalled];
        }
        let snaps = self.liveness.snapshot(now);
        let mut silent: HashSet<FeedId> = HashSet::new();
        let mut out = Vec::new();
        // Pipelines and poll feeds first, so a rule feed knows whether its parent is silent.
        let (rules, feeds): (Vec<_>, Vec<_>) = snaps.iter().partition(|f| f.kind == FeedKind::Rule);
        for feed in feeds.into_iter().chain(rules) {
            if self.config.ignore.iter().any(|p| matches(p, &feed.id))
                || (feed.kind == FeedKind::Rule && !self.config.per_rule)
            {
                continue;
            }
            let handle = self.liveness.handle(&feed.id);
            let verdict = |bound, is_silent| {
                if let Some(h) = &handle {
                    h.record_verdict(bound, is_silent);
                }
            };
            if feed.parent.as_ref().is_some_and(|p| silent.contains(p)) {
                silent.insert(feed.id.clone());
                verdict(None, true);
                continue;
            }
            if feed.kind == FeedKind::Poll
                && feed.consecutive_failures >= self.config.poll_failure_threshold.max(1)
            {
                silent.insert(feed.id.clone());
                verdict(None, true);
                out.push(self.poll_failing(feed));
                continue;
            }
            let bound = self.bound(feed);
            let silent_for = feed.silent_for(now);
            let is_silent = bound.is_some_and(|(b, _)| silent_for > b);
            verdict(bound.map(|(b, _)| b), is_silent);
            if let (true, Some((bound, source))) = (is_silent, bound) {
                silent.insert(feed.id.clone());
                out.push(self.feed_silent(feed, silent_for, bound, source));
            }
        }
        out
    }

    fn poll_failing(&self, feed: &FeedSnapshot) -> Alert {
        let error = feed.last_error.as_deref().unwrap_or("unknown error");
        warn!(feed = %feed.id, failures = feed.consecutive_failures, error, "Poll source failing");
        self.base("poll_failing")
            .message(format!(
                "Monitor blind spot: poll feed `{}` failed {} fetches in a row (last error: \
                 {error}); its rules see no data",
                feed.id, feed.consecutive_failures
            ))
            .meta("feed", feed.id.as_str())
            .meta("kind", feed.kind.as_str())
            .meta("feed_program", feed.id.program())
            .meta("consecutive_failures", feed.consecutive_failures)
            .meta("last_error", error)
            .key([self.pod_id.as_str(), feed.id.as_str(), "poll_failing"])
            .build()
    }

    fn feed_silent(
        &self,
        feed: &FeedSnapshot,
        silent_for: Duration,
        bound: Duration,
        source: BoundSource,
    ) -> Alert {
        warn!(feed = %feed.id, silent_s = silent_for.as_secs(), bound_s = bound.as_secs(), "Feed silent");
        let last = if feed.events == 0 {
            "nothing received since startup".to_string()
        } else {
            format!("last event at slot {}", feed.last_slot)
        };
        self.base("feed_silent")
            .message(format!(
                "Monitor blind spot: feed `{}` received nothing for {}s (bound {}s, {}); {last}",
                feed.id,
                silent_for.as_secs(),
                bound.as_secs(),
                source.as_str()
            ))
            .meta("feed", feed.id.as_str())
            .meta("kind", feed.kind.as_str())
            .meta("feed_program", feed.id.program())
            .meta("silent_for_s", silent_for.as_secs())
            .meta("bound_s", bound.as_secs())
            .meta("bound_source", source.as_str())
            .meta_opt("p99_gap_ms", feed.p99_gap.map(|g| g.as_millis()))
            .meta("samples", feed.samples)
            .meta("events", feed.events)
            .meta("last_slot", feed.last_slot)
            .key([self.pod_id.as_str(), feed.id.as_str(), "feed_silent"])
            .build()
    }
}

impl PollRule for FeedLiveness {
    const ID: RuleId = RuleId::new(PROGRAM, "feed_liveness");
    type Config = FeedLivenessConfig;
    type Item = Tick;

    fn accounts(_cfg: &Self::Config) -> HashSet<Pubkey> {
        HashSet::new()
    }

    fn poll_interval(cfg: &Self::Config) -> Option<Duration> {
        Some(Duration::from_secs(cfg.check_interval_seconds.max(1)))
    }

    fn build(cfg: Self::Config, deps: &Deps) -> anyhow::Result<Self> {
        let health = deps.get::<HealthDeps>()?;
        Ok(Self::new(
            cfg,
            health.liveness.clone(),
            health.grpc_slot.clone(),
            health.pod_id.clone(),
        ))
    }

    fn evaluate(&self, _tick: &Tick, _ctx: &PollCtx) -> Option<Alert> {
        None // see evaluate_all: one alert per blind feed
    }

    fn evaluate_all(&self, _tick: &Tick, _ctx: &PollCtx) -> Vec<Alert> {
        self.check(Instant::now())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sentinel::testing::config_from_toml;

    fn make(toml: &str) -> (FeedLiveness, Liveness) {
        let liveness = Liveness::new();
        let cfg: FeedLivenessConfig = config_from_toml(toml);
        (
            FeedLiveness::new(
                cfg,
                liveness.clone(),
                GrpcSlotReader::detached(),
                "node-a".into(),
            ),
            liveness,
        )
    }

    fn checks(alerts: &[Alert]) -> Vec<(String, String)> {
        alerts
            .iter()
            .map(|a| {
                (
                    a.metadata["check"].clone(),
                    a.metadata.get("feed").cloned().unwrap_or_default(),
                )
            })
            .collect()
    }

    /// A busy feed learns its bound; past it the rule alerts once per feed, Critical, with
    /// action = alert. Its rule feed is silent with it and is not alerted twice.
    #[tokio::test(start_paused = true)]
    async fn busy_feed_going_silent_alerts() {
        let (rule, liveness) = make("min_silence_seconds = 60\nmin_samples = 30");
        let pipe = liveness.feed("lend", FeedKind::Account, "vault", None);
        let input = liveness.feed("lend", FeedKind::Rule, "solvency", Some(&pipe));
        let quiet = liveness.feed("lend", FeedKind::Instruction, "admin", None);
        quiet.record_event(1); // one event: never learned, never alerted
        for slot in 0..100 {
            pipe.record_event(slot);
            input.record_event(slot);
            tokio::time::advance(Duration::from_secs(2)).await;
        }
        assert!(rule.check(Instant::now()).is_empty(), "traffic flowing");

        tokio::time::advance(Duration::from_secs(57)).await;
        assert!(
            rule.check(Instant::now()).is_empty(),
            "59 s silent: under the 60 s floor"
        );
        tokio::time::advance(Duration::from_secs(5)).await;
        let alerts = rule.check(Instant::now());
        assert_eq!(
            checks(&alerts),
            [("feed_silent".to_string(), "lend:account:vault".to_string())]
        );
        let alert = &alerts[0];
        assert_eq!(alert.severity, Severity::Critical);
        assert_eq!(alert.metadata["action"], "alert");
        assert_eq!(alert.metadata["bound_source"], "learned");
        assert_eq!(alert.metadata["bound_s"], "60");
        assert_eq!(alert.metadata["last_slot"], "99");
        assert_eq!(alert.metadata["feed_program"], "lend");
    }

    /// A rule's own input can go silent while its pipeline stays busy.
    #[tokio::test(start_paused = true)]
    async fn rule_input_silent_inside_a_busy_pipeline() {
        let (rule, liveness) = make("min_silence_seconds = 10\nmin_samples = 5");
        let pipe = liveness.feed("lend", FeedKind::Account, "vault", None);
        let input = liveness.feed("lend", FeedKind::Rule, "solvency", Some(&pipe));
        for slot in 0..20 {
            pipe.record_event(slot);
            input.record_event(slot);
            tokio::time::advance(Duration::from_secs(1)).await;
        }
        for slot in 20..60 {
            pipe.record_event(slot);
            tokio::time::advance(Duration::from_secs(1)).await;
        }
        assert_eq!(
            checks(&rule.check(Instant::now())),
            [("feed_silent".to_string(), "lend:rule:solvency".to_string())]
        );
        let (no_rules, liveness) =
            make("per_rule = false\nmin_silence_seconds = 10\nmin_samples = 5");
        let pipe = liveness.feed("lend", FeedKind::Account, "vault", None);
        let input = liveness.feed("lend", FeedKind::Rule, "solvency", Some(&pipe));
        for slot in 0..20 {
            input.record_event(slot);
            tokio::time::advance(Duration::from_secs(1)).await;
        }
        tokio::time::advance(Duration::from_secs(30)).await;
        assert!(no_rules.check(Instant::now()).is_empty());
    }

    /// Configured bounds apply from startup, also to a feed that never received anything;
    /// ignored feeds are never checked.
    #[tokio::test(start_paused = true)]
    async fn configured_bounds_and_ignores() {
        let (rule, liveness) = make(
            r#"
            ignore = ["lend:transaction:*"]
            [[bounds]]
            feed = "lend:instruction:*"
            max_silence_seconds = 300
            "#,
        );
        liveness.feed("lend", FeedKind::Instruction, "admin", None);
        liveness.feed("lend", FeedKind::Transaction, "raw", None);
        tokio::time::advance(Duration::from_secs(301)).await;
        let alerts = rule.check(Instant::now());
        assert_eq!(
            checks(&alerts),
            [(
                "feed_silent".to_string(),
                "lend:instruction:admin".to_string()
            )]
        );
        assert_eq!(alerts[0].metadata["bound_source"], "configured");
        assert!(alerts[0].message.contains("nothing received since startup"));
    }

    /// A poll source failing N times in a row alerts with its last error; a success clears it.
    #[tokio::test(start_paused = true)]
    async fn poll_source_failing() {
        let (rule, liveness) = make("poll_failure_threshold = 3");
        let poll = liveness.feed("lend", FeedKind::Poll, "VaultSource:60s", None);
        let input = liveness.feed("lend", FeedKind::Rule, "utilization", Some(&poll));
        poll.record_event(1);
        input.record_event(1);
        poll.record_failure("429 Too Many Requests");
        poll.record_failure("429 Too Many Requests");
        assert!(rule.check(Instant::now()).is_empty());
        poll.record_failure("429 Too Many Requests");
        let alerts = rule.check(Instant::now());
        assert_eq!(
            checks(&alerts),
            [(
                "poll_failing".to_string(),
                "lend:poll:VaultSource:60s".to_string()
            )]
        );
        assert_eq!(alerts[0].metadata["last_error"], "429 Too Many Requests");
        poll.record_event(2);
        assert!(rule.check(Instant::now()).is_empty());
    }

    /// A stalled stream raises one alert and no per-feed noise; no first slot gets a grace.
    #[tokio::test(start_paused = true)]
    async fn stream_stall_is_one_alert() {
        let liveness = Liveness::new();
        let (reader, slots) = GrpcSlotReader::manual();
        let cfg: FeedLivenessConfig = config_from_toml(
            "stream_stall_seconds = 30\nfirst_slot_grace_seconds = 600\n[[bounds]]\nfeed = \
             \"*\"\nmax_silence_seconds = 1",
        );
        let rule = FeedLiveness::new(cfg, liveness.clone(), reader, "node-a".into());
        liveness.feed("lend", FeedKind::Account, "vault", None);

        tokio::time::advance(Duration::from_secs(100)).await;
        // No first slot yet, inside the grace: only the feed (bound 1 s) is silent.
        assert_eq!(checks(&rule.check(Instant::now()))[0].0, "feed_silent");
        tokio::time::advance(Duration::from_secs(600)).await;
        assert_eq!(
            checks(&rule.check(Instant::now())),
            [("stream_stalled".to_string(), String::new())]
        );

        slots.record(500);
        assert_eq!(checks(&rule.check(Instant::now()))[0].0, "feed_silent");
        tokio::time::advance(Duration::from_secs(31)).await;
        let alerts = rule.check(Instant::now());
        assert_eq!(alerts.len(), 1);
        assert_eq!(alerts[0].metadata["check"], "stream_stalled");
        assert_eq!(alerts[0].metadata["last_slot"], "500");
        assert_eq!(alerts[0].severity, Severity::Critical);
    }
}
