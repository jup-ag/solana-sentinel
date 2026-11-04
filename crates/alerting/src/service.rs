use crate::cooldown_gate::CooldownGate;
use crate::outbox::Outbox;
use crate::router::Router;
use crate::sinks::Delivery;
use chrono::Utc;
use domain::alert::Alert;
use domain::sinks::SinkId;
use observability::prometheus::metrics::record_alert;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{error, info, warn};
use uuid::Uuid;

const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_secs(5);
const DEFAULT_BUFFER_CAPACITY: usize = 1024;
const SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_secs(10);
// Per alert, not per batch: Slack posts one at a time with its own 10s timeout and retries,
// so a flat 5s cut every burst after the first alert. Capped so a dead sink can't stall the loop.
const SINK_DELIVERY_TIMEOUT_PER_ALERT: Duration = Duration::from_secs(15);
const SINK_DELIVERY_TIMEOUT_MAX: Duration = Duration::from_secs(60);
// Failed deliveries are retried on the next flush ticks, then parked with a growing backoff
// (an outage outlasts a few ticks) until the alert is older than its max age.
const MAX_DELIVERY_ATTEMPTS: u32 = 3;
const PARK_BASE: Duration = Duration::from_secs(60);
const PARK_MAX: Duration = Duration::from_secs(15 * 60);
const DEFAULT_MAX_ALERT_AGE: Duration = Duration::from_secs(24 * 60 * 60);

pub struct AlertingService {
    router: Router,
    cooldown_gate: CooldownGate,
    outbox: Option<Outbox>,
    alerts_rx: mpsc::Receiver<Alert>,
    cancel_token: CancellationToken,
    buffer_capacity: usize,
    flush_interval: Duration,
    max_alert_age: Duration,
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
            outbox: None,
            cancel_token,
            buffer_capacity: DEFAULT_BUFFER_CAPACITY,
            flush_interval: DEFAULT_FLUSH_INTERVAL,
            max_alert_age: DEFAULT_MAX_ALERT_AGE,
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

    /// Persist undelivered alerts, and re-admit what a previous run left on start.
    pub fn with_outbox(mut self, outbox: Outbox) -> Self {
        self.outbox = Some(outbox);
        self
    }

    /// How long an undeliverable alert is retried before it is given up.
    pub fn with_max_alert_age(mut self, age: Duration) -> Self {
        self.max_alert_age = age;
        self
    }

    pub fn start(self, tracker: &TaskTracker) {
        let Self {
            router,
            cooldown_gate,
            outbox,
            mut alerts_rx,
            cancel_token,
            buffer_capacity,
            flush_interval,
            max_alert_age,
        } = self;

        tracker.spawn(async move {
            info!(
                sink_count = router.sink_count(),
                flush_interval = ?flush_interval,
                buffer_capacity,
                local_gate = cooldown_gate.local_gate_enabled(),
                fleet_gate = cooldown_gate.fleet_gate_enabled(),
                outbox = outbox.is_some(),
                "Starting alerting service",
            );

            // Fires SHUTDOWN_DRAIN_TIMEOUT after cancellation, also cutting a flush already running.
            let hard_stop = CancellationToken::new();
            tokio::spawn({
                let (cancel, hard_stop) = (cancel_token.clone(), hard_stop.clone());
                async move {
                    cancel.cancelled().await;
                    tokio::time::sleep(SHUTDOWN_DRAIN_TIMEOUT).await;
                    hard_stop.cancel();
                }
            });

            let mut d = Dispatcher {
                router,
                gate: cooldown_gate,
                outbox,
                buffer: Vec::with_capacity(buffer_capacity),
                max_alert_age,
            };
            d.recover().await;

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
                                d.admit(alert).await;
                                if d.buffer.len() >= buffer_capacity {
                                    d.flush(&hard_stop).await;
                                }
                            }
                            None => {
                                info!("Alerts channel closed; draining buffer");
                                break;
                            }
                        }
                    }

                    _ = flush_tick.tick() => d.flush(&hard_stop).await,
                }
            }

            // Drain the channel and make one last attempt, within the deadline counted from
            // cancellation (or from here if the channel closed). Open claims pass to the next run.
            let drain = async {
                while let Ok(alert) = alerts_rx.try_recv() {
                    d.admit(alert).await;
                }
                d.flush(&hard_stop).await;
            };
            tokio::select! {
                _ = drain => {}
                _ = hard_stop.cancelled() => warn!("Shutdown drain exceeded timeout"),
                _ = tokio::time::sleep(SHUTDOWN_DRAIN_TIMEOUT) => warn!("Shutdown drain exceeded timeout"),
            }
            if !d.buffer.is_empty() {
                if d.outbox.is_some() {
                    info!(undelivered = d.buffer.len(), "Undelivered alerts kept in the outbox for the next run");
                } else {
                    warn!(undelivered = d.buffer.len(), "Stopping with alerts still owed to sinks; they are lost");
                }
            }

            info!("Alerting service stopped");
        });
    }
}

/// The alerting loop's state: what is owed to which sink, and where it is persisted.
struct Dispatcher {
    router: Router,
    gate: CooldownGate,
    outbox: Option<Outbox>,
    buffer: Vec<Pending>,
    max_alert_age: Duration,
}

impl Dispatcher {
    /// Re-admit what the previous run left unsettled. Its claims are ours and taken over;
    /// one it confirmed before dying is suppressed by the gate and dropped here.
    async fn recover(&mut self) {
        let Some(outbox) = &mut self.outbox else {
            return;
        };
        let restored = match outbox.load(&self.router).await {
            Ok(restored) => restored,
            Err(e) => {
                error!("Failed to load the alert outbox: {e}");
                return;
            }
        };
        if !restored.is_empty() {
            info!(
                count = restored.len(),
                "Re-admitting alerts left undelivered by the previous run"
            );
        }
        for pending in restored {
            if self.gate.should_send(&pending.alert).await {
                self.buffer.push(pending);
            } else {
                self.forget(pending.alert.id).await;
            }
        }
    }

    /// Persist the alert, gate it, and queue it for every sink its rule routes to.
    async fn admit(&mut self, alert: Alert) {
        let sinks = self.router.route(alert.rule).to_vec();
        if sinks.is_empty() {
            warn!(rule = alert.rule.name, "Alert dropped: no matching route");
            return;
        }
        let mut pending = Pending {
            alert,
            sinks,
            attempts: 0,
            delivered: false,
            next_try: None,
        };
        // Stored before the gate claims it, so a crash in between still re-raises it.
        let stored = self.store(&pending).await;
        if !self.gate.should_send(&pending.alert).await {
            if stored {
                self.forget(pending.alert.id).await;
            }
            return;
        }
        // Durable now: the slot checkpoint no longer has to wait for it.
        if stored {
            pending.alert.slot_hold = None;
        }
        record_alert(pending.alert.severity);
        self.buffer.push(pending);
    }

    /// One delivery attempt for every alert that is due. Failed ones are retried, then parked,
    /// and settled once delivered everywhere or older than `max_alert_age`.
    async fn flush(&mut self, hard_stop: &CancellationToken) {
        let now = Instant::now();
        let mut per_sink: HashMap<SinkId, Vec<usize>> = HashMap::new();
        for (i, pending) in self.buffer.iter().enumerate() {
            if pending.next_try.is_some_and(|t| t > now) {
                continue;
            }
            for sink in &pending.sinks {
                per_sink.entry(sink.clone()).or_default().push(i);
            }
        }
        if per_sink.is_empty() {
            return;
        }
        // Outcomes live outside the send futures, so a timeout or hard stop keeps what completed.
        let mut jobs: Vec<(SinkId, Vec<usize>, Vec<Delivery>)> = per_sink
            .into_iter()
            .map(|(sink, idxs)| {
                let n = idxs.len();
                (sink, idxs, vec![Delivery::Pending; n])
            })
            .collect();

        // Dispatch per-sink batches concurrently.
        let (router, buffer) = (&self.router, &self.buffer);
        let sends = jobs.iter_mut().map(|(sink_id, idxs, out)| {
            let batch: Vec<Alert> = idxs.iter().map(|&i| buffer[i].alert.clone()).collect();
            async move {
                let Some(sink) = router.sink(sink_id) else {
                    // An unconfigured sink can never succeed; stop owing it.
                    warn!(sink = %sink_id, "Route references unconfigured sink");
                    out.fill(Delivery::Delivered);
                    return;
                };
                let budget = (SINK_DELIVERY_TIMEOUT_PER_ALERT * batch.len() as u32)
                    .min(SINK_DELIVERY_TIMEOUT_MAX);
                if tokio::time::timeout(budget, sink.send_each(&batch, out))
                    .await
                    .is_err()
                {
                    warn!(sink = %sink_id, batch_len = batch.len(), "Alert delivery to sink timed out");
                    // In flight or never reached: counts as a failed attempt.
                    for d in out.iter_mut().filter(|d| **d == Delivery::Pending) {
                        *d = Delivery::Failed;
                    }
                }
            }
        });
        tokio::select! {
            _ = futures::future::join_all(sends) => {}
            _ = hard_stop.cancelled() => warn!("Shutdown deadline reached mid-delivery"),
        }

        let mut changed = vec![false; self.buffer.len()];
        let mut failed = vec![false; self.buffer.len()];
        for (sink_id, idxs, out) in jobs {
            for (i, outcome) in idxs.into_iter().zip(out) {
                match outcome {
                    Delivery::Delivered => {
                        let pending = &mut self.buffer[i];
                        pending.delivered = true;
                        pending.sinks.retain(|s| s != &sink_id);
                        changed[i] = true;
                    }
                    Delivery::Failed => failed[i] = true,
                    Delivery::Pending | Delivery::Deferred => {}
                }
            }
        }

        let buffer = std::mem::take(&mut self.buffer);
        for ((mut pending, changed), failed) in buffer.into_iter().zip(changed).zip(failed) {
            if pending.sinks.is_empty() {
                self.settle(pending, true).await;
                continue;
            }
            if failed {
                pending.attempts += 1;
                if pending.attempts >= MAX_DELIVERY_ATTEMPTS {
                    let age = (Utc::now() - pending.alert.fired_at)
                        .to_std()
                        .unwrap_or_default();
                    if age >= self.max_alert_age {
                        error!(
                            rule = pending.alert.rule.name,
                            sinks = ?pending.sinks,
                            attempts = pending.attempts,
                            "Giving up on alert delivery",
                        );
                        // Delivered somewhere: keep the claim so those sinks don't get it twice.
                        let delivered = pending.delivered;
                        self.settle(pending, delivered).await;
                        continue;
                    }
                    let backoff = park_backoff(pending.attempts);
                    warn!(
                        rule = pending.alert.rule.name,
                        sinks = ?pending.sinks,
                        attempts = pending.attempts,
                        retry_in_s = backoff.as_secs(),
                        "Alert still undelivered; parking it",
                    );
                    pending.next_try = Some(now + backoff);
                }
            }
            if changed || failed {
                self.store(&pending).await;
            }
            self.buffer.push(pending);
        }
    }

    /// Done with an alert: its claims stand if it reached any sink, otherwise they are freed.
    async fn settle(&mut self, pending: Pending, delivered: bool) {
        if delivered {
            self.gate.confirm(&pending.alert).await;
        } else {
            self.gate.release(&pending.alert).await;
        }
        self.forget(pending.alert.id).await;
    }

    async fn store(&mut self, pending: &Pending) -> bool {
        let Some(outbox) = &mut self.outbox else {
            return false;
        };
        match outbox.put(pending).await {
            Ok(()) => true,
            Err(e) => {
                error!(
                    rule = pending.alert.rule.name,
                    "Failed to persist alert to the outbox: {e}"
                );
                false
            }
        }
    }

    async fn forget(&mut self, id: Uuid) {
        if let Some(outbox) = &mut self.outbox {
            if let Err(e) = outbox.remove(id).await {
                error!(%id, "Failed to remove alert from the outbox: {e}");
            }
        }
    }
}

/// Doubles per parked attempt, from PARK_BASE up to PARK_MAX.
fn park_backoff(attempts: u32) -> Duration {
    let doublings = attempts.saturating_sub(MAX_DELIVERY_ATTEMPTS).min(8);
    (PARK_BASE * (1 << doublings)).min(PARK_MAX)
}

/// An admitted alert and the sinks it is still owed to.
pub(crate) struct Pending {
    pub(crate) alert: Alert,
    pub(crate) sinks: Vec<SinkId>,
    pub(crate) attempts: u32,
    pub(crate) delivered: bool,
    /// Parked until then; `None` is due now.
    pub(crate) next_try: Option<Instant>,
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

    fn router(fail_first: usize, delivered: &Arc<Mutex<Vec<String>>>) -> Router {
        let id: SinkId = "flaky".into();
        let sink = FlakySink {
            id: id.clone(),
            fail_first,
            posts: Mutex::new(0),
            delivered: Arc::clone(delivered),
        };
        Router::new(
            HashMap::from([(RULE, vec![id.clone()])]),
            HashMap::from([(id, Box::new(sink) as Box<dyn AlertSink>)]),
        )
    }

    /// Runs one service over `alerts` and returns what the sink delivered.
    async fn run(
        fail_first: usize,
        alerts: Vec<Alert>,
        configure: impl FnOnce(AlertingService) -> AlertingService,
    ) -> Vec<String> {
        let delivered = Arc::new(Mutex::new(Vec::new()));
        let gate = CooldownGate::new(None, Some(FleetGate::in_memory(HashMap::new())));
        let (tx, rx) = mpsc::channel(8);
        let cancel = CancellationToken::new();
        let tracker = TaskTracker::new();
        let service =
            AlertingService::new(rx, router(fail_first, &delivered), gate, cancel.clone())
                .with_flush_interval(Duration::from_millis(10));
        configure(service).start(&tracker);

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
        assert_eq!(run(2, vec![alert("a")], |s| s).await, vec!["a"]);
    }

    /// Given up after every attempt failed, the fleet claim is released, so the
    /// next occurrence of the same alert is not suppressed by it.
    #[tokio::test]
    async fn undelivered_alert_releases_its_claim() {
        let alerts = vec![alert("a"), alert("a")];
        let delivered = run(MAX_DELIVERY_ATTEMPTS as usize, alerts, |s| {
            s.with_max_alert_age(Duration::ZERO)
        })
        .await;
        assert_eq!(delivered, vec!["a"]);
    }

    /// An alert one run could not deliver is persisted, and the next run delivers it.
    #[tokio::test]
    #[ignore = "needs Docker (testcontainers); run with --ignored"]
    async fn undelivered_alert_survives_a_restart() {
        use testcontainers::runners::AsyncRunner;
        use testcontainers::ImageExt;
        use testcontainers_modules::redis::Redis;

        let container = Redis::default().with_tag("7-alpine").start().await.unwrap();
        let port = container.get_host_port_ipv4(6379).await.unwrap();
        let client = redis::Client::open(format!("redis://127.0.0.1:{port}")).unwrap();
        let conn = redis::aio::ConnectionManager::new(client).await.unwrap();
        let outbox = Outbox::new(conn.clone(), "TEST_NODE", "test");

        let first = run(usize::MAX, vec![alert("a")], |s| {
            s.with_outbox(outbox.clone())
        })
        .await;
        assert!(first.is_empty());

        let second = run(0, vec![], |s| s.with_outbox(outbox.clone())).await;
        assert_eq!(second, vec!["a"]);

        let mut conn = conn;
        let left: usize = redis::AsyncCommands::hlen(&mut conn, "TEST_NODE:test:alerts:outbox")
            .await
            .unwrap();
        assert_eq!(left, 0, "a delivered alert leaves the outbox");
    }
}
