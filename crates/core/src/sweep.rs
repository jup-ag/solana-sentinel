//! Periodic re-evaluation for stream rules that ask for it (`sweep_interval`).

use crate::handler::forward_alert;
use crate::rule::SharedGrpcRule;
use domain::alert::Alert;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::info;

pub fn spawn_sweep_loops<Ev: Send + Sync + 'static>(
    rules: &[SharedGrpcRule<Ev>],
    alerts_tx: &mpsc::Sender<Alert>,
    cancel_token: &CancellationToken,
    tracker: &TaskTracker,
) {
    for rule in rules {
        let Some(interval) = rule.sweep_interval() else {
            continue;
        };
        let rule = rule.clone();
        let alerts_tx = alerts_tx.clone();
        let cancel_token = cancel_token.clone();
        info!(rule = %rule.id(), interval_secs = interval.as_secs(), "Starting periodic sweep");

        let mut tick = tokio::time::interval(interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        tracker.spawn(async move {
            loop {
                tokio::select! {
                    _ = cancel_token.cancelled() => {
                        info!(rule = %rule.id(), "Cancellation requested, stopping sweep");
                        break;
                    }
                    _ = tick.tick() => {
                        for alert in rule.sweep() {
                            forward_alert(alert, None, None, &alerts_tx).await;
                        }
                    }
                }
            }
        });
    }
}
