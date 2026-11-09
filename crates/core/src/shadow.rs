//! Shadow mode: a rule that runs and alerts as usual but can never trigger a pause.
//!
//! A rule's `[[rules]]` block may say `shadow = true`. Its alerts keep their severity,
//! message, routes and cooldown; only an `action` of `L1` / `L2` is rewritten to `alert`, with
//! `shadow = "true"` and `would_action = "L1" | "L2"` added (the `vaults` stay), so reactors
//! that act on `action` (the pause simulator) ignore it while dashboards can still count the
//! pauses the rule would have asked for. The alert tap applies it before any reactor sees the
//! alert, so no rule code changes.

use domain::alert::Alert;
use domain::rules::RuleId;
use std::collections::HashSet;
use std::sync::Arc;

/// Metadata key every rule uses for its response (`alert`, `L1`, `L2`).
pub const ACTION: &str = "action";
/// Set to `"true"` on an alert whose pause action was withheld by shadow mode.
pub const SHADOW: &str = "shadow";
/// The action a shadowed rule asked for (`L1` / `L2`).
pub const WOULD_ACTION: &str = "would_action";

/// Actions shadow mode withholds: the ones a reactor may turn into a pause.
const PAUSE_ACTIONS: [&str; 2] = ["L1", "L2"];

/// The rules in shadow, by `(program, rule name)` as written in the rules files.
#[derive(Debug, Clone, Default)]
pub struct ShadowRules(Arc<HashSet<(String, String)>>);

impl ShadowRules {
    pub fn new<P: Into<String>, N: Into<String>>(rules: impl IntoIterator<Item = (P, N)>) -> Self {
        Self(Arc::new(
            rules
                .into_iter()
                .map(|(p, n)| (p.into(), n.into()))
                .collect(),
        ))
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn contains(&self, rule: RuleId) -> bool {
        // Compared in place: a shadow set is a few dozen entries, checked only on alerts.
        self.0
            .iter()
            .any(|(p, n)| p == rule.program.as_str() && n == rule.name)
    }

    /// Rewrite `alert` if its rule is in shadow; see [`shadow_alert`].
    pub fn apply(&self, alert: &mut Alert) -> bool {
        !self.is_empty() && self.contains(alert.rule) && shadow_alert(alert)
    }
}

/// Withhold a pause action: `action = L1|L2` becomes `action = alert`, `shadow = true`,
/// `would_action = L1|L2`. Everything else (severity, message, `vaults`, dedup keys) is kept.
/// Returns whether the alert carried a pause action.
pub fn shadow_alert(alert: &mut Alert) -> bool {
    let Some(action) = alert.metadata.get(ACTION) else {
        return false;
    };
    if !PAUSE_ACTIONS.contains(&action.as_str()) {
        return false;
    }
    let would = action.clone();
    alert
        .metadata
        .insert(ACTION.to_string(), "alert".to_string());
    alert
        .metadata
        .insert(SHADOW.to_string(), "true".to_string());
    alert.metadata.insert(WOULD_ACTION.to_string(), would);
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use domain::alert::Severity;
    use domain::programs::Program;

    const PROGRAM: Program = Program::new("p");
    const RULE: RuleId = RuleId::new(PROGRAM, "r");
    const OTHER: RuleId = RuleId::new(PROGRAM, "other");

    fn alert(rule: RuleId, action: &str) -> Alert {
        Alert::for_rule(rule)
            .severity(Severity::Critical)
            .message("drain")
            .meta(ACTION, action)
            .meta("vaults", "3,7")
            .build()
    }

    #[test]
    fn pause_actions_become_alerts_with_the_would_be_action() {
        for level in ["L1", "L2"] {
            let mut a = alert(RULE, level);
            let before = a.clone();
            assert!(shadow_alert(&mut a));
            assert_eq!(a.metadata[ACTION], "alert");
            assert_eq!(a.metadata[SHADOW], "true");
            assert_eq!(a.metadata[WOULD_ACTION], level);
            assert_eq!(a.metadata["vaults"], "3,7");
            assert_eq!(a.severity, before.severity);
            assert_eq!(a.message, before.message);
            assert_eq!(a.local_gate_key, before.local_gate_key);
            assert_eq!(a.fleet_gate_key, before.fleet_gate_key);
        }
    }

    #[test]
    fn non_pause_alerts_are_untouched() {
        let mut a = alert(RULE, "alert");
        let before = a.metadata.clone();
        assert!(!shadow_alert(&mut a));
        assert_eq!(a.metadata, before);

        let mut none = Alert::for_rule(RULE).message("x").build();
        assert!(!shadow_alert(&mut none));
        assert!(!none.metadata.contains_key(SHADOW));
    }

    #[test]
    fn only_listed_rules_are_shadowed() {
        let shadow = ShadowRules::new([("p", "r")]);
        let mut mine = alert(RULE, "L1");
        let mut other = alert(OTHER, "L1");
        assert!(shadow.apply(&mut mine));
        assert!(!shadow.apply(&mut other));
        assert_eq!(mine.metadata[ACTION], "alert");
        assert_eq!(other.metadata[ACTION], "L1");
        assert!(!other.metadata.contains_key(SHADOW));
        assert!(!ShadowRules::default().apply(&mut alert(RULE, "L1")));
    }
}
