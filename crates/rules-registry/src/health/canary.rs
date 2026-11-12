//! Canary: every `interval_seconds`, one synthetic alert per canary route through the real
//! alert path (tap → gates → batching → sinks), proving that alerts still reach every sink.
//!
//! A route is a distinct sink set among the configured rules (`scope = "route"`, default) or
//! one program's sinks (`scope = "program"`). The canaries are tagged (`canary = "true"`,
//! `action = "canary"`): reactors skip them, Slack and Betterstack incident sinks acknowledge
//! them without posting (Slack posts them to `canary_channel` when one is set), ClickHouse
//! stores them (filter `metadata['canary'] = 'true'` out of incident queries).
//!
//! This rule only emits. Whether each canary reached each sink within
//! `interval_seconds + deadline_seconds` is checked by the service's canary watchdog, which
//! raises a Critical alert routed by this rule's own `[[rules]]` sinks, and by the Betterstack
//! heartbeat, withheld while any canary is missing.

use crate::health::{HealthDeps, PROGRAM};
use serde::Deserialize;
use solana_pubkey::Pubkey;
use solana_sentinel::domain::canary::{
    CanaryRoute, CanaryScope, CANARY, CANARY_NODE, CANARY_ROUTE, CANARY_SEQ,
};
use solana_sentinel::domain::sinks::SinkId;
use solana_sentinel::{
    Alert, DedupScope, Deps, Liveness, PollCtx, PollRule, RuleId, Severity, Tick,
};
use std::collections::HashSet;
use std::time::Duration;
use tokio::time::Instant;
use tracing::{info, warn};

mod defaults {
    pub fn interval_seconds() -> u64 {
        300
    }
    pub fn deadline_seconds() -> u64 {
        120
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct CanaryConfig {
    /// One canary per route every this many seconds.
    #[serde(default = "defaults::interval_seconds")]
    pub interval_seconds: u64,
    /// How late a canary may reach its sink; a route/sink pair whose freshest canary is older
    /// than `interval + deadline` is missed.
    #[serde(default = "defaults::deadline_seconds")]
    pub deadline_seconds: u64,
    #[serde(default)]
    pub scope: CanaryScope,
    /// Only routes of these programs; empty is every program.
    #[serde(default)]
    pub programs: Vec<String>,
    /// Sinks never sent canaries (and never expected to accept one).
    #[serde(default)]
    pub exclude_sinks: Vec<SinkId>,
}

#[derive(Debug)]
pub struct Canary {
    liveness: Liveness,
    routes: Vec<CanaryRoute>,
    pod_id: String,
}

impl Canary {
    /// Arm the tracker and keep the routes to emit on.
    pub fn new(cfg: &CanaryConfig, liveness: Liveness, pod_id: String) -> Self {
        let routes = liveness.canary().arm(
            Self::ID,
            Duration::from_secs(cfg.interval_seconds.max(1)),
            Duration::from_secs(cfg.deadline_seconds),
            cfg.scope,
            &cfg.programs,
            &cfg.exclude_sinks,
        );
        if routes.is_empty() {
            warn!(scope = ?cfg.scope, "Canary armed with no route: no configured rule routes to a sink");
        } else {
            info!(
                routes = ?routes.iter().map(|r| r.id.as_str()).collect::<Vec<_>>(),
                interval_s = cfg.interval_seconds,
                deadline_s = cfg.deadline_seconds,
                "Canary armed"
            );
        }
        Self {
            liveness,
            routes,
            pod_id,
        }
    }

    pub fn routes(&self) -> &[CanaryRoute] {
        &self.routes
    }

    /// One canary per route, emitted now.
    pub fn emit(&self) -> Vec<Alert> {
        let now = Instant::now();
        self.routes
            .iter()
            .map(|route| {
                let seq = self.liveness.canary().emit(&route.id, now);
                let sinks: Vec<&str> = route.sinks.iter().map(SinkId::as_str).collect();
                Alert::for_rule(Self::ID)
                    .severity(Severity::Info)
                    .message(format!(
                        "[canary] synthetic delivery probe #{seq} for route `{}` from {}; not an \
                         incident",
                        route.id, self.pod_id
                    ))
                    .meta(CANARY, "true")
                    .meta(CANARY_ROUTE, &route.id)
                    .meta(CANARY_SEQ, seq)
                    .meta(CANARY_NODE, &self.pod_id)
                    .meta("action", "canary")
                    .meta("sinks", sinks.join(","))
                    // Unique per probe: the gates never suppress a canary.
                    .key([self.pod_id.as_str(), route.id.as_str(), &seq.to_string()])
                    .scope(DedupScope::Monitor)
                    .build()
            })
            .collect()
    }
}

impl PollRule for Canary {
    const ID: RuleId = RuleId::new(PROGRAM, "canary");
    type Config = CanaryConfig;
    type Item = Tick;

    fn accounts(_cfg: &Self::Config) -> HashSet<Pubkey> {
        HashSet::new()
    }

    fn poll_interval(cfg: &Self::Config) -> Option<Duration> {
        Some(Duration::from_secs(cfg.interval_seconds.max(1)))
    }

    fn build(cfg: Self::Config, deps: &Deps) -> anyhow::Result<Self> {
        let health = deps.get::<HealthDeps>()?;
        Ok(Self::new(
            &cfg,
            health.liveness.clone(),
            health.pod_id.clone(),
        ))
    }

    fn evaluate(&self, _tick: &Tick, _ctx: &PollCtx) -> Option<Alert> {
        None // see evaluate_all: one canary per route
    }

    fn evaluate_all(&self, _tick: &Tick, _ctx: &PollCtx) -> Vec<Alert> {
        self.emit()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sentinel::domain::canary::canary_routes;
    use solana_sentinel::testing::config_from_toml;

    #[tokio::test(start_paused = true)]
    async fn one_tagged_canary_per_route() {
        let liveness = Liveness::new();
        let chat = [SinkId::new("chat"), SinkId::new("store")];
        let pager = [SinkId::new("pager")];
        liveness.canary().set_routes(canary_routes([
            ("lend", &chat[..]),
            ("usd", &chat[..]),
            ("health", &pager[..]),
        ]));
        let cfg: CanaryConfig = config_from_toml("interval_seconds = 60\ndeadline_seconds = 30");
        let canary = Canary::new(&cfg, liveness.clone(), "node-a".into());
        let alerts = canary.emit();
        assert_eq!(alerts.len(), 2, "two distinct sink sets");
        for alert in &alerts {
            assert!(alert.is_canary());
            assert_eq!(alert.metadata["action"], "canary");
            assert_eq!(alert.severity, Severity::Info);
            assert!(alert.message.starts_with("[canary]"));
        }
        // Distinct probes never share a dedup key.
        let again = canary.emit();
        assert_ne!(alerts[0].fleet_gate_key, again[0].fleet_gate_key);
        assert_eq!(
            liveness.canary().plan().unwrap().budget(),
            Duration::from_secs(90)
        );

        let per_program: CanaryConfig =
            config_from_toml("scope = \"program\"\nexclude_sinks = [\"store\"]");
        let canary = Canary::new(&per_program, liveness, "node-a".into());
        let ids: Vec<&str> = canary.routes().iter().map(|r| r.id.as_str()).collect();
        assert_eq!(ids, ["program:health", "program:lend", "program:usd"]);
        assert_eq!(canary.routes()[1].sinks, [SinkId::new("chat")]);
    }
}
