//! Is the monitor still seeing anything? Per-feed liveness and canary delivery state.
//!
//! A *feed* is anything that delivers input to rules: a stream pipeline (account,
//! instruction, transaction), one rule's share of a pipeline, or a poll bucket. The
//! framework registers every feed when it builds it and records each event (and, for poll
//! feeds, each failed fetch) on its [`FeedHandle`]. Each feed keeps its last event time and
//! slot plus a window of recent inter-arrival gaps, from which the liveness rule learns how
//! long a silence is abnormal ([`FeedSnapshot::learned_bound`]).
//!
//! The [`CanaryTracker`] holds the other half: which canary routes are expected, when each
//! canary was emitted and when each sink accepted it. The canary watchdog and the Betterstack
//! heartbeat both read it.
//!
//! Time is `tokio::time::Instant`, so tests can run hours of silence on a paused clock.

use crate::prometheus::metrics::{record_canary_delivered, record_canary_sent, FeedMetrics};
use domain::canary::{CanaryRoute, CanaryScope};
use domain::rules::RuleId;
use domain::sinks::SinkId;
use parking_lot::Mutex;
use std::collections::{HashMap, VecDeque};
use std::fmt::{self, Display};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

/// Inter-arrival gaps kept per feed; the learned bound is a high percentile of these.
pub const GAP_WINDOW: usize = 512;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FeedKind {
    Account,
    Instruction,
    Transaction,
    Poll,
    /// One rule's input: the part of its pipeline (or poll bucket) that matches the rule.
    Rule,
}

impl FeedKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Account => "account",
            Self::Instruction => "instruction",
            Self::Transaction => "transaction",
            Self::Poll => "poll",
            Self::Rule => "rule",
        }
    }
}

/// `{program}:{kind}:{name}`, e.g. `jup_lend:account:vault_config` or `jup_lend:rule:solvency`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct FeedId(String);

impl FeedId {
    pub fn new(program: &str, kind: FeedKind, name: &str) -> Self {
        Self(format!("{program}:{}:{name}", kind.as_str()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn program(&self) -> &str {
        self.0.split(':').next().unwrap_or_default()
    }
}

impl Display for FeedId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Default)]
struct FeedState {
    last_event: Option<Instant>,
    last_slot: u64,
    events: u64,
    gaps: VecDeque<Duration>,
    consecutive_failures: u32,
    failures: u64,
    last_error: Option<String>,
    last_failure: Option<Instant>,
}

#[derive(Debug)]
struct Feed {
    id: FeedId,
    kind: FeedKind,
    /// The feed this one is a part of (a rule's pipeline); silent with it, so not alerted twice.
    parent: Option<FeedId>,
    registered_at: Instant,
    state: Mutex<FeedState>,
    metrics: FeedMetrics,
}

/// Where a pipeline, rule or poll loop records what it received. Cheap to clone.
#[derive(Debug, Clone)]
pub struct FeedHandle(Arc<Feed>);

impl FeedHandle {
    pub fn id(&self) -> &FeedId {
        &self.0.id
    }

    pub fn record_event(&self, slot: u64) {
        self.record_event_at(Instant::now(), slot);
    }

    pub fn record_event_at(&self, now: Instant, slot: u64) {
        let mut s = self.0.state.lock();
        if let Some(last) = s.last_event {
            if s.gaps.len() == GAP_WINDOW {
                s.gaps.pop_front();
            }
            s.gaps.push_back(now.saturating_duration_since(last));
        }
        s.last_event = Some(now);
        s.last_slot = s.last_slot.max(slot);
        s.events += 1;
        if s.consecutive_failures > 0 {
            s.consecutive_failures = 0;
            self.0.metrics.poll_consecutive_failures.set(0);
        }
        drop(s);
        self.0.metrics.events.inc();
    }

    /// A poll fetch that failed; consecutive failures reset on the next event.
    pub fn record_failure(&self, error: &str) {
        self.record_failure_at(Instant::now(), error);
    }

    pub fn record_failure_at(&self, now: Instant, error: &str) {
        let mut s = self.0.state.lock();
        s.consecutive_failures += 1;
        s.failures += 1;
        s.last_error = Some(error.chars().take(300).collect());
        s.last_failure = Some(now);
        let consecutive = s.consecutive_failures;
        drop(s);
        self.0.metrics.poll_failures.inc();
        self.0
            .metrics
            .poll_consecutive_failures
            .set(i64::from(consecutive));
    }

    /// Publish what the liveness rule decided for this feed.
    pub fn record_verdict(&self, bound: Option<Duration>, silent: bool) {
        if let Some(bound) = bound {
            self.0.metrics.bound.set(bound.as_secs_f64());
        }
        self.0.metrics.silent.set(i64::from(silent));
    }

    fn snapshot(&self, now: Instant) -> FeedSnapshot {
        let s = self.0.state.lock();
        let snap = FeedSnapshot {
            id: self.0.id.clone(),
            kind: self.0.kind,
            parent: self.0.parent.clone(),
            registered_at: self.0.registered_at,
            last_event: s.last_event,
            last_slot: s.last_slot,
            events: s.events,
            samples: s.gaps.len(),
            p99_gap: percentile(&s.gaps, 0.99),
            consecutive_failures: s.consecutive_failures,
            failures: s.failures,
            last_error: s.last_error.clone(),
            last_failure: s.last_failure,
        };
        drop(s);
        self.0.metrics.age.set(snap.silent_for(now).as_secs_f64());
        snap
    }
}

fn percentile(gaps: &VecDeque<Duration>, q: f64) -> Option<Duration> {
    if gaps.is_empty() {
        return None;
    }
    let mut sorted: Vec<Duration> = gaps.iter().copied().collect();
    sorted.sort_unstable();
    let rank = (q * sorted.len() as f64).ceil() as usize;
    Some(sorted[rank.clamp(1, sorted.len()) - 1])
}

/// One feed as the liveness rule sees it on a tick.
#[derive(Debug, Clone)]
pub struct FeedSnapshot {
    pub id: FeedId,
    pub kind: FeedKind,
    pub parent: Option<FeedId>,
    pub registered_at: Instant,
    pub last_event: Option<Instant>,
    pub last_slot: u64,
    pub events: u64,
    /// Inter-arrival gaps in the window (at most [`GAP_WINDOW`]).
    pub samples: usize,
    pub p99_gap: Option<Duration>,
    pub consecutive_failures: u32,
    pub failures: u64,
    pub last_error: Option<String>,
    pub last_failure: Option<Instant>,
}

impl FeedSnapshot {
    /// Time since the last event, or since registration when there was none.
    pub fn silent_for(&self, now: Instant) -> Duration {
        now.saturating_duration_since(self.last_event.unwrap_or(self.registered_at))
    }

    /// `factor × p99 gap`, at least `min`, once the window holds `min_samples` gaps.
    /// `None` while the feed has not shown it normally sees traffic.
    pub fn learned_bound(
        &self,
        factor: f64,
        min: Duration,
        min_samples: usize,
    ) -> Option<Duration> {
        if self.samples < min_samples.max(1) {
            return None;
        }
        let p99 = self.p99_gap?;
        Some(p99.mul_f64(factor.max(1.0)).max(min))
    }
}

/// The registry of feeds plus the canary tracker; one per service, cloned everywhere.
#[derive(Debug, Clone)]
pub struct Liveness(Arc<Inner>);

#[derive(Debug)]
struct Inner {
    started: Instant,
    feeds: Mutex<Vec<FeedHandle>>,
    canary: CanaryTracker,
}

impl Default for Liveness {
    fn default() -> Self {
        Self::new()
    }
}

impl Liveness {
    pub fn new() -> Self {
        Self(Arc::new(Inner {
            started: Instant::now(),
            feeds: Mutex::new(Vec::new()),
            canary: CanaryTracker::default(),
        }))
    }

    pub fn started(&self) -> Instant {
        self.0.started
    }

    /// Register a feed, or get the one already registered under this id.
    pub fn feed(
        &self,
        program: &str,
        kind: FeedKind,
        name: &str,
        parent: Option<&FeedHandle>,
    ) -> FeedHandle {
        let id = FeedId::new(program, kind, name);
        let mut feeds = self.0.feeds.lock();
        if let Some(existing) = feeds.iter().find(|f| f.0.id == id) {
            return existing.clone();
        }
        let handle = FeedHandle(Arc::new(Feed {
            metrics: FeedMetrics::new(program, kind.as_str(), id.as_str()),
            id,
            kind,
            parent: parent.map(|p| p.id().clone()),
            registered_at: Instant::now(),
            state: Mutex::new(FeedState::default()),
        }));
        feeds.push(handle.clone());
        handle
    }

    /// The handle of a registered feed.
    pub fn handle(&self, id: &FeedId) -> Option<FeedHandle> {
        self.0.feeds.lock().iter().find(|f| f.id() == id).cloned()
    }

    /// Every feed, in registration order; refreshes the age gauges.
    pub fn snapshot(&self, now: Instant) -> Vec<FeedSnapshot> {
        let feeds = self.0.feeds.lock().clone();
        feeds.iter().map(|f| f.snapshot(now)).collect()
    }

    pub fn canary(&self) -> &CanaryTracker {
        &self.0.canary
    }
}

// ---- canaries -----------------------------------------------------------------

/// What the canary rule armed: the routes it emits on, and how fresh deliveries must be.
#[derive(Debug, Clone)]
pub struct CanaryPlan {
    pub rule: RuleId,
    pub interval: Duration,
    pub deadline: Duration,
    pub routes: Vec<CanaryRoute>,
    pub armed_at: Instant,
}

impl CanaryPlan {
    /// A route/sink pair is missed when its freshest delivered canary is older than this.
    pub fn budget(&self) -> Duration {
        self.interval + self.deadline
    }
}

#[derive(Debug, Clone, Copy)]
struct Delivered {
    emitted_at: Instant,
}

#[derive(Debug, Default)]
struct CanaryState {
    known: Vec<CanaryRoute>,
    plan: Option<CanaryPlan>,
    seq: u64,
    /// seq → (route, emitted at), pruned past a few budgets.
    emitted: HashMap<u64, (String, Instant)>,
    /// (route, sink) → freshest delivered canary.
    delivered: HashMap<(String, SinkId), Delivered>,
}

/// One route/sink pair with no fresh canary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanaryMiss {
    pub route: String,
    pub sink: SinkId,
    /// Age of the freshest canary this sink accepted on this route; `None` when none ever did.
    pub freshest_age: Option<Duration>,
    pub budget: Duration,
}

/// Expected canary routes, emissions and per-sink deliveries.
#[derive(Debug, Default)]
pub struct CanaryTracker(Mutex<CanaryState>);

impl CanaryTracker {
    /// Every route the configured rules imply (both scopes); set once by the service wiring.
    pub fn set_routes(&self, routes: Vec<CanaryRoute>) {
        self.0.lock().known = routes;
    }

    pub fn known_routes(&self) -> Vec<CanaryRoute> {
        self.0.lock().known.clone()
    }

    /// Start expecting canaries: the routes of `scope` (narrowed to `programs` when given,
    /// minus `exclude_sinks`). Returns the routes the rule emits on.
    pub fn arm(
        &self,
        rule: RuleId,
        interval: Duration,
        deadline: Duration,
        scope: CanaryScope,
        programs: &[String],
        exclude_sinks: &[SinkId],
    ) -> Vec<CanaryRoute> {
        let mut s = self.0.lock();
        let routes: Vec<CanaryRoute> = s
            .known
            .iter()
            .filter(|r| r.id.starts_with(scope.prefix()))
            .filter(|r| programs.is_empty() || r.programs.iter().any(|p| programs.contains(p)))
            .filter_map(|r| {
                let sinks: Vec<SinkId> = r
                    .sinks
                    .iter()
                    .filter(|s| !exclude_sinks.contains(s))
                    .cloned()
                    .collect();
                (!sinks.is_empty()).then(|| CanaryRoute { sinks, ..r.clone() })
            })
            .collect();
        s.plan = Some(CanaryPlan {
            rule,
            interval,
            deadline,
            routes: routes.clone(),
            armed_at: Instant::now(),
        });
        routes
    }

    pub fn plan(&self) -> Option<CanaryPlan> {
        self.0.lock().plan.clone()
    }

    /// Sinks a canary on `route` goes to; `None` when the route is not armed.
    pub fn route_sinks(&self, route: &str) -> Option<Vec<SinkId>> {
        let s = self.0.lock();
        let plan = s.plan.as_ref()?;
        plan.routes
            .iter()
            .find(|r| r.id == route)
            .map(|r| r.sinks.clone())
    }

    /// Allocate the next canary on `route`, emitted now.
    pub fn emit(&self, route: &str, now: Instant) -> u64 {
        let mut s = self.0.lock();
        s.seq += 1;
        let seq = s.seq;
        let keep = s
            .plan
            .as_ref()
            .map_or(Duration::from_secs(3600), |p| p.budget() * 4);
        s.emitted
            .retain(|_, (_, at)| now.saturating_duration_since(*at) <= keep);
        s.emitted.insert(seq, (route.to_string(), now));
        drop(s);
        record_canary_sent(route);
        seq
    }

    /// `sink` accepted canary `seq`. Unknown or pruned canaries (a previous run's, replayed
    /// from the outbox) are ignored.
    pub fn delivered(&self, seq: u64, sink: &SinkId, now: Instant) {
        let mut s = self.0.lock();
        let Some((route, emitted_at)) = s.emitted.get(&seq).cloned() else {
            return;
        };
        let key = (route.clone(), sink.clone());
        let fresher = s
            .delivered
            .get(&key)
            .is_none_or(|d| d.emitted_at < emitted_at);
        if fresher {
            s.delivered.insert(key, Delivered { emitted_at });
        }
        drop(s);
        record_canary_delivered(
            &route,
            sink.as_str(),
            now.saturating_duration_since(emitted_at).as_secs_f64(),
        );
    }

    /// Route/sink pairs whose freshest delivered canary is older than the plan's budget.
    /// Nothing is missed before one budget has passed since arming.
    pub fn check(&self, now: Instant) -> Vec<CanaryMiss> {
        let s = self.0.lock();
        let Some(plan) = &s.plan else {
            return Vec::new();
        };
        let budget = plan.budget();
        let in_grace = now.saturating_duration_since(plan.armed_at) <= budget;
        let mut misses = Vec::new();
        for route in &plan.routes {
            for sink in &route.sinks {
                let freshest_age = s
                    .delivered
                    .get(&(route.id.clone(), sink.clone()))
                    .map(|d| now.saturating_duration_since(d.emitted_at));
                let missed = !in_grace && freshest_age.is_none_or(|age| age > budget);
                crate::prometheus::metrics::record_canary_state(
                    &route.id,
                    sink.as_str(),
                    freshest_age.map(|a| a.as_secs_f64()),
                    missed,
                );
                if missed {
                    misses.push(CanaryMiss {
                        route: route.id.clone(),
                        sink: sink.clone(),
                        freshest_age,
                        budget,
                    });
                }
            }
        }
        misses
    }

    /// No armed route/sink pair is missing its canary (also true when canaries are off).
    pub fn healthy(&self, now: Instant) -> bool {
        self.check(now).is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use domain::canary::canary_routes;
    use domain::programs::Program;

    const RULE: RuleId = RuleId::new(Program::new("health"), "canary");

    #[test]
    fn learns_a_bound_from_inter_arrival_gaps() {
        let liveness = Liveness::new();
        let feed = liveness.feed("p", FeedKind::Account, "parser", None);
        let t0 = Instant::now();
        for i in 0..300u32 {
            // 2 s apart, with one 30 s outlier the p99 must not be fooled by.
            let at = t0
                + Duration::from_secs(u64::from(i) * 2)
                + Duration::from_secs(if i > 50 { 28 } else { 0 });
            feed.record_event_at(at, u64::from(i));
        }
        let snap = &liveness.snapshot(t0)[0];
        assert_eq!(snap.events, 300);
        assert_eq!(snap.samples, 299);
        assert_eq!(snap.last_slot, 299);
        assert_eq!(snap.p99_gap, Some(Duration::from_secs(2)));
        assert_eq!(
            snap.learned_bound(3.0, Duration::from_secs(60), 30),
            Some(Duration::from_secs(60)),
            "3 × 2 s is under the floor"
        );
        assert_eq!(
            snap.learned_bound(3.0, Duration::from_secs(1), 30),
            Some(Duration::from_secs(6))
        );
        assert_eq!(snap.learned_bound(3.0, Duration::from_secs(1), 400), None);
    }

    #[test]
    fn same_id_is_one_feed_and_failures_reset_on_success() {
        let liveness = Liveness::new();
        let a = liveness.feed("p", FeedKind::Poll, "60s", None);
        let b = liveness.feed("p", FeedKind::Poll, "60s", None);
        a.record_failure("rpc down");
        b.record_failure("rpc down");
        let snap = &liveness.snapshot(Instant::now())[0];
        assert_eq!(liveness.snapshot(Instant::now()).len(), 1);
        assert_eq!(snap.consecutive_failures, 2);
        assert_eq!(snap.last_error.as_deref(), Some("rpc down"));
        a.record_event(5);
        assert_eq!(liveness.snapshot(Instant::now())[0].consecutive_failures, 0);
        assert_eq!(liveness.snapshot(Instant::now())[0].failures, 2);
    }

    #[test]
    fn rule_feed_names_its_pipeline() {
        let liveness = Liveness::new();
        let pipe = liveness.feed("p", FeedKind::Instruction, "ix", None);
        let rule = liveness.feed("p", FeedKind::Rule, "my_rule", Some(&pipe));
        assert_eq!(rule.id().as_str(), "p:rule:my_rule");
        assert_eq!(rule.id().program(), "p");
        let snaps = liveness.snapshot(Instant::now());
        assert_eq!(snaps[1].parent.as_ref(), Some(pipe.id()));
    }

    #[tokio::test(start_paused = true)]
    async fn canary_freshness_per_route_and_sink() {
        let tracker = CanaryTracker::default();
        let chat = SinkId::new("chat");
        let store = SinkId::new("store");
        tracker.set_routes(canary_routes([(
            "lend",
            &[chat.clone(), store.clone()][..],
        )]));
        let routes = tracker.arm(
            RULE,
            Duration::from_secs(60),
            Duration::from_secs(30),
            CanaryScope::Route,
            &[],
            &[],
        );
        assert_eq!(routes.len(), 1);
        let route = routes[0].id.clone();
        assert_eq!(
            tracker.route_sinks(&route).unwrap(),
            [chat.clone(), store.clone()]
        );
        assert!(
            tracker.route_sinks("program:lend").is_none(),
            "other scope not armed"
        );

        // In grace: nothing missed yet.
        assert!(tracker.healthy(Instant::now()));
        let seq = tracker.emit(&route, Instant::now());
        tracker.delivered(seq, &chat, Instant::now());
        tokio::time::advance(Duration::from_secs(91)).await;
        let misses = tracker.check(Instant::now());
        // chat's canary is 91 s old (> 90 s budget); store never got one.
        assert_eq!(misses.len(), 2);
        assert!(misses
            .iter()
            .any(|m| m.sink == store && m.freshest_age.is_none()));

        let seq = tracker.emit(&route, Instant::now());
        tracker.delivered(seq, &chat, Instant::now());
        tracker.delivered(seq, &store, Instant::now());
        assert!(tracker.healthy(Instant::now()));

        // An unknown seq (a previous run's canary) changes nothing.
        tracker.delivered(9_999, &store, Instant::now());
        assert!(tracker.healthy(Instant::now()));
    }

    #[test]
    fn arm_narrows_programs_and_drops_excluded_sinks() {
        let tracker = CanaryTracker::default();
        let chat = SinkId::new("chat");
        let pager = SinkId::new("pager");
        tracker.set_routes(canary_routes([
            ("lend", &[chat.clone()][..]),
            ("health", &[pager.clone()][..]),
        ]));
        let program = tracker.arm(
            RULE,
            Duration::from_secs(1),
            Duration::from_secs(1),
            CanaryScope::Program,
            &["lend".to_string()],
            &[],
        );
        assert_eq!(program.len(), 1);
        assert_eq!(program[0].id, "program:lend");
        let routes = tracker.arm(
            RULE,
            Duration::from_secs(1),
            Duration::from_secs(1),
            CanaryScope::Route,
            &[],
            &[pager],
        );
        assert_eq!(routes.len(), 1, "the pager-only route has no sink left");
        assert_eq!(routes[0].sinks, [chat]);
    }
}
