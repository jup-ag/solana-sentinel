use crate::cooldown_gate::CooldownGate;
use crate::fleet_gate::FleetGate;
use crate::router::Router;
use domain::alert::Alert;
use domain::sinks::SinkId;
use observability::prometheus::metrics::{record_alert, record_alert_suppressed};
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, error, info, warn};

const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_secs(5);
const DEFAULT_BUFFER_CAPACITY: usize = 1024;
const SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_secs(10);
const SINK_DELIVERY_TIMEOUT: Duration = Duration::from_secs(5);

pub struct AlertingService {
    router: Router,
    /// `None` disables that layer of dedup; see `should_send_alert`.
    cooldown_gate: Option<CooldownGate>,
    alerts_rx: mpsc::Receiver<Alert>,
    fleet_gate: Option<FleetGate>,
    cancel_token: CancellationToken,
    buffer_capacity: usize,
    flush_interval: Duration,
}

impl AlertingService {
    pub fn new(
        alerts_rx: mpsc::Receiver<Alert>,
        router: Router,
        cooldown_gate: Option<CooldownGate>,
        fleet_gate: Option<FleetGate>,
        cancel_token: CancellationToken,
    ) -> Self {
        Self {
            router,
            alerts_rx,
            cooldown_gate,
            fleet_gate,
            cancel_token,
            buffer_capacity: DEFAULT_BUFFER_CAPACITY,
            flush_interval: DEFAULT_FLUSH_INTERVAL,
        }
    }

    pub fn with_buffer_capacity(mut self, n: usize) -> Self {
        self.buffer_capacity = n.max(1);
        self
    }

    pub fn with_flush_interval(mut self, interval: Duration) -> Self {
        self.flush_interval = interval;
        self
    }

    pub fn start(self, tracker: &TaskTracker) {
        let Self {
            router,
            mut cooldown_gate,
            mut alerts_rx,
            mut fleet_gate,
            cancel_token,
            buffer_capacity,
            flush_interval,
        } = self;

        tracker.spawn(async move {
            info!(
                sink_count = router.sink_count(),
                flush_interval = ?flush_interval,
                buffer_capacity,
                cooldown_gate = cooldown_gate.is_some(),
                fleet_gate = fleet_gate.is_some(),
                "Starting alerting service",
            );

            let mut buffer: Vec<Alert> = Vec::with_capacity(buffer_capacity);
            let mut flush_tick = tokio::time::interval(flush_interval);
            flush_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            flush_tick.tick().await; // consume the immediate first tick

            loop {
                tokio::select! {
                    _ = cancel_token.cancelled() => {
                        info!("Cancellation requested, stopping alerting service");
                        break;
                    }

                    maybe_alert = alerts_rx.recv() => {
                        match maybe_alert {
                            Some(alert) => {
                                if !Self::should_send_alert(&alert, &mut cooldown_gate, &mut fleet_gate).await {
                                    continue;
                                }
                                record_alert(alert.severity);
                                buffer.push(alert);
                                if buffer.len() >= buffer_capacity {
                                    Self::flush(&router, &mut buffer).await;
                                }
                            }
                            None => {
                                info!("Alerts channel closed; draining buffer");
                                break;
                            }
                        }
                    }

                    _ = flush_tick.tick() => {
                        if !buffer.is_empty() {
                            Self::flush(&router, &mut buffer).await;
                        }
                    }
                }
            }

            // Drain remaining channel contents, then flush once with a hard cap.
            while let Ok(alert) = alerts_rx.try_recv() {
                if !Self::should_send_alert(&alert, &mut cooldown_gate, &mut fleet_gate).await {
                    continue;
                }
                record_alert(alert.severity);
                buffer.push(alert);
            }

            let timed_out =
                tokio::time::timeout(SHUTDOWN_DRAIN_TIMEOUT, Self::flush(&router, &mut buffer))
                    .await
                    .is_err();

            if !buffer.is_empty() && timed_out {
                warn!("Shutdown drain exceeded timeout; alerts may be lost");
            }

            info!("Alerting service stopped");
        });
    }

    async fn flush(router: &Router, buffer: &mut Vec<Alert>) {
        if buffer.is_empty() {
            return;
        }

        // Bucket alerts by destination sink.
        let mut per_sink: HashMap<SinkId, Vec<Alert>> = HashMap::new();
        let mut unrouted = 0usize;

        for alert in buffer.drain(..) {
            let targets = router.route(alert.rule);
            if targets.is_empty() {
                unrouted += 1;
                continue;
            }
            // Move alert to the last target, clone for the rest.
            let Some((last, rest)) = targets.split_last() else {
                continue;
            };

            for sink_id in rest {
                per_sink
                    .entry(sink_id.clone())
                    .or_default()
                    .push(alert.clone());
            }

            per_sink.entry(last.clone()).or_default().push(alert);
        }

        if unrouted > 0 {
            warn!(count = unrouted, "Alerts dropped: no matching route");
        }

        // Dispatch per-sink batches concurrently.
        let futures = per_sink.into_iter().map(|(sink_id, batch)| async move {
            let Some(sink) = router.sink(&sink_id) else {
                warn!(sink = %sink_id, "Route references unconfigured sink");
                return;
            };
            let count = batch.len();

            tokio::time::timeout(SINK_DELIVERY_TIMEOUT, async {
                if let Err(e) = sink.send_batch(&batch).await {
                    warn!(
                        sink = %sink_id,
                        error = %e,
                        batch_len = count,
                        "Failed to deliver alerts to sink"
                    );
                }
            })
            .await
            .unwrap_or_else(|_| {
                warn!(sink = %sink_id, "Alert delivery to sink timed out");
            })
        });

        futures::future::join_all(futures).await;
    }

    /// Cooldown gate first (repeat of the same alert within its window), then
    /// the fleet gate (already claimed by another node). A disabled gate passes.
    async fn should_send_alert(
        alert: &Alert,
        cooldown_gate: &mut Option<CooldownGate>,
        fleet_gate: &mut Option<FleetGate>,
    ) -> bool {
        if let Some(gate) = cooldown_gate {
            match gate.check(alert).await {
                Ok(true) => {}
                Ok(false) => {
                    debug!(rule = alert.rule.name, "Alert suppressed by cooldown");
                    record_alert_suppressed(alert.rule.name);
                    return false;
                }
                // fail-open: a Redis error must not lose alerts
                Err(e) => error!("Failed to check cooldown gate: {e}"),
            }
        }

        if let Some(gate) = fleet_gate {
            match gate.check(alert).await {
                Ok(true) => {}
                Ok(false) => {
                    debug!(rule = alert.rule.name, "Alert suppressed by fleet dedup");
                    record_alert_suppressed(alert.rule.name);
                    return false;
                }
                Err(e) => error!("Failed to check fleet gate: {e}"),
            }
        }

        true
    }
}
