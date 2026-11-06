//! Synthetic canary alerts: probes the monitor sends through its own alert path
//! (tap → gates → batching → sinks) to prove that alerts still reach every sink.
//!
//! A canary is an ordinary [`Alert`] tagged with [`CANARY`] metadata. It is routed by
//! its [`CANARY_ROUTE`] (a set of sinks) instead of by its rule, skipped by reactors,
//! and sinks that would page a human (Slack, Betterstack incidents) acknowledge it
//! without posting, unless they are given a dedicated canary destination.

use crate::alert::Alert;
use crate::sinks::SinkId;
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};

/// Metadata key marking an alert as a canary; its value is `"true"`.
pub const CANARY: &str = "canary";
/// Metadata key naming the canary route (a sink set) the canary exercises.
pub const CANARY_ROUTE: &str = "canary_route";
/// Metadata key with the canary's sequence number on this node.
pub const CANARY_SEQ: &str = "canary_seq";
/// Metadata key with the node that emitted the canary.
pub const CANARY_NODE: &str = "canary_node";

impl Alert {
    /// Whether this alert is a synthetic canary, not an incident.
    pub fn is_canary(&self) -> bool {
        self.metadata.get(CANARY).is_some_and(|v| v == "true")
    }

    /// The route a canary exercises; `None` for real alerts.
    pub fn canary_route(&self) -> Option<&str> {
        if !self.is_canary() {
            return None;
        }
        self.metadata.get(CANARY_ROUTE).map(String::as_str)
    }

    /// The canary's sequence number; `None` for real alerts.
    pub fn canary_seq(&self) -> Option<u64> {
        if !self.is_canary() {
            return None;
        }
        self.metadata.get(CANARY_SEQ).and_then(|s| s.parse().ok())
    }
}

/// What one canary exercises: a set of sinks, named by the sinks or by the program whose
/// rules route there.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanaryRoute {
    /// `sinks:<a>+<b>` for a distinct sink set, `program:<name>` for a program's sinks.
    pub id: String,
    pub sinks: Vec<SinkId>,
    /// Programs with at least one rule routed to these sinks.
    pub programs: Vec<String>,
}

/// How canaries are cut: one per distinct sink set, or one per program.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CanaryScope {
    /// One canary per distinct sink set among the configured rules.
    #[default]
    Route,
    /// One canary per program, to the union of its rules' sinks.
    Program,
}

impl CanaryScope {
    pub fn prefix(self) -> &'static str {
        match self {
            Self::Route => "sinks:",
            Self::Program => "program:",
        }
    }
}

/// Every canary route the configured rules imply, both scopes: one per distinct sink set
/// (`sinks:…`) and one per program (`program:…`). Rules routed nowhere add none.
pub fn canary_routes<'a>(
    rules: impl IntoIterator<Item = (&'a str, &'a [SinkId])>,
) -> Vec<CanaryRoute> {
    let mut by_set: BTreeMap<BTreeSet<String>, BTreeSet<String>> = BTreeMap::new();
    let mut by_program: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for (program, sinks) in rules {
        if sinks.is_empty() {
            continue;
        }
        let set: BTreeSet<String> = sinks.iter().map(|s| s.as_str().to_string()).collect();
        by_program
            .entry(program.to_string())
            .or_default()
            .extend(set.iter().cloned());
        by_set.entry(set).or_default().insert(program.to_string());
    }
    let to_sinks = |set: &BTreeSet<String>| set.iter().map(|s| SinkId::new(s.as_str())).collect();
    let mut out: Vec<CanaryRoute> = by_set
        .iter()
        .map(|(set, programs)| CanaryRoute {
            id: format!(
                "{}{}",
                CanaryScope::Route.prefix(),
                set.iter().cloned().collect::<Vec<_>>().join("+")
            ),
            sinks: to_sinks(set),
            programs: programs.iter().cloned().collect(),
        })
        .collect();
    out.extend(by_program.iter().map(|(program, set)| CanaryRoute {
        id: format!("{}{program}", CanaryScope::Program.prefix()),
        sinks: to_sinks(set),
        programs: vec![program.clone()],
    }));
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::programs::Program;
    use crate::rules::RuleId;

    const RULE: RuleId = RuleId::new(Program::new("health"), "canary");

    #[test]
    fn canary_tags_round_trip() {
        let canary = Alert::for_rule(RULE)
            .meta(CANARY, "true")
            .meta(CANARY_ROUTE, "chat")
            .meta(CANARY_SEQ, 7)
            .build();
        assert!(canary.is_canary());
        assert_eq!(canary.canary_route(), Some("chat"));
        assert_eq!(canary.canary_seq(), Some(7));

        // A real alert with a stray route key is not a canary.
        let real = Alert::for_rule(RULE).meta(CANARY_ROUTE, "chat").build();
        assert!(!real.is_canary());
        assert_eq!(real.canary_route(), None);
    }

    #[test]
    fn routes_cover_sink_sets_and_programs() {
        let chat = [SinkId::new("chat"), SinkId::new("store")];
        let pager = [SinkId::new("pager")];
        let routes = canary_routes([
            ("lend", &chat[..]),
            ("usd", &chat[..]),
            ("health", &pager[..]),
            ("lend", &pager[..]),
            ("quiet", &[][..]),
        ]);
        let ids: Vec<&str> = routes.iter().map(|r| r.id.as_str()).collect();
        assert_eq!(
            ids,
            [
                "sinks:chat+store",
                "sinks:pager",
                "program:health",
                "program:lend",
                "program:usd"
            ]
        );
        assert_eq!(routes[0].programs, ["lend", "usd"]);
        assert_eq!(
            routes[3].sinks.len(),
            3,
            "a program gets the union of its sinks"
        );
    }
}
