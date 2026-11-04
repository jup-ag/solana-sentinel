use crate::cooldown_gate::CooldownGate;
use crate::router::Router;
use domain::alert::Alert;
use domain::sinks::SinkId;
use observability::prometheus::metrics::record_alert;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{error, info, warn};

const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_secs(5);
const DEFAULT_BUFFER_CAPACITY: usize = 1024;
const SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_secs(10);
// Per alert, not per batch: Slack posts one at a time with its own 10s timeout and retries,
// so a flat 5s cut every burst after the first alert. Capped so a dead sink can't stall the loop.
const SINK_DELIVERY_TIMEOUT_PER_ALERT: Duration = Duration::from_secs(15);
const SINK_DELIVERY_TIMEOUT_MAX: Duration = Duration::from_secs(60);
// Failed deliveries are retried on the next flush ticks, then given up.
const MAX_DELIVERY_ATTEMPTS: u32 = 3;

pub struct AlertingService {
    router: Router,
    cooldown_gate: CooldownGate,
    alerts_rx: mpsc::Receiver<Alert>,
    cancel_token: CancellationToken,
    buffer_capacity: usize,
    flush_interval: Duration,
}

impl AlertingService {
    pub fn new(
        alerts_rx: mpsc::Receiver<Alert>,
        router: Router,
        cooldown_gate: CooldownGate,
        cancel_token: CancellationToken,
    ) -> Self {
        Self {
            router,
            alerts_rx,
            cooldown_gate,
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
            cancel_token,
            buffer_capacity,
            flush_interval,
        } = self;

        tracker.spawn(async move {
            info!(
                sink_count = router.sink_count(),
                flush_interval = ?flush_interval,
                buffer_capacity,
                local_gate = cooldown_gate.local_gate_enabled(),
                fleet_gate = cooldown_gate.fleet_gate_enabled(),
                "Starting alerting service",
            );

            let mut buffer: Vec<Pending> = Vec::with_capacity(buffer_capacity);
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
                                Self::admit(&router, &mut cooldown_gate, &mut buffer, alert).await;
                                if buffer.len() >= buffer_capacity {
                                    Self::flush(&router, &mut cooldown_gate, &mut buffer, MAX_DELIVERY_ATTEMPTS).await;
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
                            Self::flush(&router, &mut cooldown_gate, &mut buffer, MAX_DELIVERY_ATTEMPTS).await;
                        }
                    }
                }
            }

            // Drain the channel and make one last attempt; the gate calls hit Redis, so the
            // whole drain shares the cap. A claim left open is taken over by the next run.
            let drain = async {
                while let Ok(alert) = alerts_rx.try_recv() {
                    Self::admit(&router, &mut cooldown_gate, &mut buffer, alert).await;
                }
                Self::flush(&router, &mut cooldown_gate, &mut buffer, 1).await;
            };
            if tokio::time::timeout(SHUTDOWN_DRAIN_TIMEOUT, drain).await.is_err() {
                warn!("Shutdown drain exceeded timeout; alerts may be lost");
            }

            info!("Alerting service stopped");
        });
    }

    /// Gate the alert and queue it for every sink its rule routes to.
    async fn admit(
        router: &Router,
        gate: &mut CooldownGate,
        buffer: &mut Vec<Pending>,
        alert: Alert,
    ) {
        if !gate.should_send(&alert).await {
            return;
        }
        let sinks = router.route(alert.rule).to_vec();
        if sinks.is_empty() {
            warn!(rule = alert.rule.name, "Alert dropped: no matching route");
            gate.release(&alert).await;
            return;
        }
        record_alert(alert.severity);
        buffer.push(Pending {
            alert,
            sinks,
            attempts: 0,
            delivered: false,
        });
    }

    /// One delivery attempt for everything buffered. Alerts still owed to a sink stay
    /// buffered for the next flush until `max_attempts`; then their claims are settled.
    async fn flush(
        router: &Router,
        gate: &mut CooldownGate,
        buffer: &mut Vec<Pending>,
        max_attempts: u32,
    ) {
        if buffer.is_empty() {
            return;
        }

        let mut per_sink: HashMap<&SinkId, Vec<usize>> = HashMap::new();
        for (i, pending) in buffer.iter().enumerate() {
            for sink in &pending.sinks {
                per_sink.entry(sink).or_default().push(i);
            }
        }

        // Dispatch per-sink batches concurrently.
        let attempts = per_sink.into_iter().map(|(sink_id, idxs)| {
            let batch: Vec<Alert> = idxs.iter().map(|&i| buffer[i].alert.clone()).collect();
            async move {
                let Some(sink) = router.sink(sink_id) else {
                    warn!(sink = %sink_id, "Route references unconfigured sink");
                    return (sink_id.clone(), idxs, None);
                };
                let count = batch.len();
                let budget = (SINK_DELIVERY_TIMEOUT_PER_ALERT * count as u32)
                    .min(SINK_DELIVERY_TIMEOUT_MAX);
                let delivered = tokio::time::timeout(budget, sink.send_each(&batch))
                    .await
                    .unwrap_or_else(|_| {
                        warn!(sink = %sink_id, batch_len = count, "Alert delivery to sink timed out");
                        vec![false; count]
                    });
                (sink_id.clone(), idxs, Some(delivered))
            }
        });
        let results = futures::future::join_all(attempts).await;

        for (sink_id, idxs, delivered) in results {
            // An unconfigured sink can never succeed; stop owing it.
            let delivered = delivered.unwrap_or_else(|| vec![true; idxs.len()]);
            for (i, ok) in idxs.into_iter().zip(delivered) {
                if ok {
                    buffer[i].delivered = true;
                    buffer[i].sinks.retain(|s| s != &sink_id);
                }
            }
        }

        let mut retry = Vec::new();
        for mut pending in buffer.drain(..) {
            pending.attempts += 1;
            if pending.sinks.is_empty() {
                gate.confirm(&pending.alert).await;
                continue;
            }
            if pending.attempts < max_attempts {
                retry.push(pending);
                continue;
            }
            error!(
                rule = pending.alert.rule.name,
                sinks = ?pending.sinks,
                attempts = pending.attempts,
                "Giving up on alert delivery",
            );
            // Delivered somewhere: keep the claim so those sinks don't get it twice.
            if pending.delivered {
                gate.confirm(&pending.alert).await;
            } else {
                gate.release(&pending.alert).await;
            }
        }
        *buffer = retry;
    }
}

/// An admitted alert and the sinks it is still owed to.
struct Pending {
    alert: Alert,
    sinks: Vec<SinkId>,
    attempts: u32,
    delivered: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fleet_gate::FleetGate;
    use crate::sinks::{AlertSink, AlertSinkError};
    use async_trait::async_trait;
    use domain::programs::Program;
    use domain::rules::RuleId;
    use parking_lot::Mutex;
    use std::sync::Arc;

    const RULE: RuleId = RuleId::new(Program::new("test_program"), "rule");

    /// Fails the first `fail_first` posts, then delivers; records delivered messages.
    struct FlakySink {
        id: SinkId,
        fail_first: usize,
        posts: Mutex<usize>,
        delivered: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait]
    impl AlertSink for FlakySink {
        fn id(&self) -> &SinkId {
            &self.id
        }
        async fn send(&self, alert: &Alert) -> Result<(), AlertSinkError> {
            let mut posts = self.posts.lock();
            *posts += 1;
            if *posts <= self.fail_first {
                return Err(AlertSinkError::BetterStackSinkError("down".into()));
            }
            self.delivered.lock().push(alert.message.clone());
            Ok(())
        }
        async fn send_batch(&self, alerts: &[Alert]) -> Result<(), AlertSinkError> {
            for a in alerts {
                self.send(a).await?;
            }
            Ok(())
        }
    }

    fn alert(msg: &str) -> Alert {
        Alert::for_rule(RULE).message(msg).key([msg]).build()
    }

    async fn run(fail_first: usize, alerts: Vec<Alert>) -> Vec<String> {
        let id: SinkId = "flaky".into();
        let delivered = Arc::new(Mutex::new(Vec::new()));
        let sink = FlakySink {
            id: id.clone(),
            fail_first,
            posts: Mutex::new(0),
            delivered: Arc::clone(&delivered),
        };
        let router = Router::new(
            HashMap::from([(RULE, vec![id.clone()])]),
            HashMap::from([(id, Box::new(sink) as Box<dyn AlertSink>)]),
        );
        let gate = CooldownGate::new(None, Some(FleetGate::in_memory(HashMap::new())));
        let (tx, rx) = mpsc::channel(8);
        let cancel = CancellationToken::new();
        let tracker = TaskTracker::new();
        AlertingService::new(rx, router, gate, cancel.clone())
            .with_flush_interval(Duration::from_millis(10))
            .start(&tracker);

        for a in alerts {
            tx.send(a).await.unwrap();
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        cancel.cancel();
        tracker.close();
        tracker.wait().await;
        let out = delivered.lock().clone();
        out
    }

    /// A transient sink failure is retried on the next flush instead of lost.
    #[tokio::test]
    async fn failed_delivery_is_retried() {
        assert_eq!(run(2, vec![alert("a")]).await, vec!["a"]);
    }

    /// Given up after every attempt failed, the fleet claim is released, so the
    /// next occurrence of the same alert is not suppressed by it.
    #[tokio::test]
    async fn undelivered_alert_releases_its_claim() {
        let delivered = run(MAX_DELIVERY_ATTEMPTS as usize, vec![alert("a"), alert("a")]).await;
        assert_eq!(delivered, vec!["a"]);
    }
}
