//! Periodic re-evaluation for stream rules that ask for it (`sweep_interval`).

use crate::handler::forward_alert;
use crate::rule::BuiltStreamRule;
use domain::alert::Alert;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::info;

pub fn spawn_sweep_loops<Ev>(
    rules: &[BuiltStreamRule<Ev>],
    alerts_tx: &mpsc::Sender<Alert>,
    cancel_token: &CancellationToken,
    tracker: &TaskTracker,
) {
    for rule in rules {
        let Some((interval, sweep)) = rule.sweep.clone() else {
            continue;
        };
        let id = rule.id;
        let counteract = rule.counteract.clone();
        let alerts_tx = alerts_tx.clone();
        let cancel_token = cancel_token.clone();
        info!(rule = %id, interval_secs = interval.as_secs(), "Starting periodic sweep");

        let mut tick = tokio::time::interval(interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        tracker.spawn(async move {
            loop {
                tokio::select! {
                    _ = cancel_token.cancelled() => {
                        info!(rule = %id, "Cancellation requested, stopping sweep");
                        break;
                    }
                    _ = tick.tick() => {
                        for alert in sweep() {
                            let failed = counteract.as_ref().and_then(|c| c(&alert, None));
                            for alert in std::iter::once(alert).chain(failed) {
                                forward_alert(alert, None, None, &alerts_tx).await;
                            }
                        }
                    }
                }
            }
        });
    }
}
