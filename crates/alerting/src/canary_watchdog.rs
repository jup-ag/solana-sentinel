//! The canary watchdog: a small task, independent of the rules and their loops, that checks
//! every armed canary route reached each of its sinks recently, and raises a Critical alert
//! (routed by the canary rule's own `[[rules]]` entry) for each route/sink pair that did not.
//!
//! It is deliberately not the canary emitter: if the emitter's loop dies, the deliveries stop
//! and the watchdog sees it. If the alert path itself is broken the watchdog's alert may not
//! get out either; the Betterstack heartbeat (withheld while a canary is missing) covers that.

use domain::alert::{Alert, DedupScope, Severity};
use observability::liveness::{CanaryMiss, Liveness};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{error, info, warn};

/// How often the watchdog looks; well under any sensible canary deadline.
pub const DEFAULT_WATCHDOG_INTERVAL: Duration = Duration::from_secs(5);

/// The alert for one missed route/sink pair, as the canary rule's own alert.
pub fn missed_alert(liveness: &Liveness, miss: &CanaryMiss, node: &str) -> Option<Alert> {
    let plan = liveness.canary().plan()?;
    let age = miss.freshest_age.map_or_else(
        || "none ever arrived".to_string(),
        |a| format!("freshest is {}s old", a.as_secs()),
    );
    Some(
        Alert::for_rule(plan.rule)
            .severity(Severity::Critical)
            .message(format!(
                "Monitor blind spot: no canary on route `{}` reached sink `{}` within {}s \
                 ({age}). Alerts on this route may not be delivered.",
                miss.route,
                miss.sink,
                miss.budget.as_secs()
            ))
            .meta("action", "alert")
            .meta("check", "canary_missed")
            .meta("route", &miss.route)
            .meta("sink", miss.sink.as_str())
            .meta("budget_s", miss.budget.as_secs())
            .meta_opt("freshest_age_s", miss.freshest_age.map(|a| a.as_secs()))
            .meta("node", node)
            .key([node, miss.route.as_str(), miss.sink.as_str()])
            .scope(DedupScope::Monitor)
            .build(),
    )
}

/// Check the canary tracker every `every` and send an alert per missed pair to `alerts_tx`
/// (the alerting service's input, past the tap: a broken tap must not hide it).
pub fn spawn_canary_watchdog(
    liveness: Liveness,
    alerts_tx: mpsc::Sender<Alert>,
    node: String,
    every: Duration,
    cancel_token: CancellationToken,
    tracker: &TaskTracker,
) {
    let mut tick = tokio::time::interval(every);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    tracker.spawn(async move {
        info!(every_s = every.as_secs(), "Canary watchdog started");
        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => break,
                _ = tick.tick() => {
                    for miss in liveness.canary().check(tokio::time::Instant::now()) {
                        error!(route = %miss.route, sink = %miss.sink, "Canary missed its sink");
                        let Some(alert) = missed_alert(&liveness, &miss, &node) else {
                            continue;
                        };
                        if alerts_tx.send(alert).await.is_err() {
                            warn!("Alerting service gone; canary watchdog stopping");
                            return;
                        }
                    }
                }
            }
        }
    });
}
