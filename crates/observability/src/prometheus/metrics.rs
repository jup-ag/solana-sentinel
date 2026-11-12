//! Prometheus metrics for the monitoring service.
//!
//! Metrics are global statics so they can be incremented from anywhere without
//! threading a handle through call sites. They register themselves to a private
//! global `REGISTRY` on first access; `gather()` exposes that registry to the
//! `/metrics` HTTP handler. Rates (instruction, account-update, transaction)
//! are exposed as monotonic counters of totals; the per-second rate is derived
//! in PromQL via `rate()` rather than stored.

use crate::prometheus::registry::REGISTRY;
use prometheus::{
    GaugeVec, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Opts,
};
use std::fmt::Display;
use std::sync::LazyLock;

/// Metric name prefix; set once through [`init`] before any metric is touched.
static NAMESPACE: std::sync::OnceLock<String> = std::sync::OnceLock::new();

fn namespace() -> &'static str {
    NAMESPACE.get_or_init(|| "solana_sentinel".to_string())
}

/// Latency buckets (seconds) for RPC calls, tuned for sub-ms to multi-second.
const RPC_LATENCY_BUCKETS: &[f64] = &[
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

/// Alerts fired, labeled by severity (e.g. "info", "warning", "critical").
static ALERTS_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    let m = IntCounterVec::new(
        Opts::new("alerts_total", "Alerts fired, by severity").namespace(namespace()),
        &["severity"],
    )
    .expect("invalid alerts_total metric");
    REGISTRY
        .register(Box::new(m.clone()))
        .expect("register alerts_total");
    m
});

/// Alerts suppressed by per-rule cooldown, labeled by rule name.
static ALERTS_SUPPRESSED_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    let m = IntCounterVec::new(
        Opts::new(
            "alerts_suppressed_total",
            "Alerts suppressed by per-rule cooldown, by rule",
        )
        .namespace(namespace()),
        &["rule"],
    )
    .expect("invalid alerts_suppressed_total metric");
    REGISTRY
        .register(Box::new(m.clone()))
        .expect("register alerts_suppressed_total");
    m
});

/// Total Grpc events processed, labeled by event type (e.g. "instruction_update", "account_update", "transaction_update", "slot_update").
static GRPC_EVENTS_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    let m = IntCounterVec::new(
        Opts::new("grpc_events_total", "Total Grpc events processed").namespace(namespace()),
        &["event_type"],
    )
    .expect("invalid grpc_events_total metric");
    REGISTRY
        .register(Box::new(m.clone()))
        .expect("register grpc_events_total");
    m
});

/// RPC fetch status
static RPC_FETCH_STATUS_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    let m = IntCounterVec::new(
        Opts::new("rpc_fetch_status_total", "RPC fetch status").namespace(namespace()),
        &["method", "status"],
    )
    .expect("invalid rpc_fetch_status_total metric");
    REGISTRY
        .register(Box::new(m.clone()))
        .expect("register rpc_fetch_status_total");
    m
});

/// RPC call latency in seconds, labeled by method.
static RPC_LATENCY_SECONDS: LazyLock<HistogramVec> = LazyLock::new(|| {
    let m = HistogramVec::new(
        HistogramOpts::new("rpc_latency_seconds", "RPC call latency in seconds")
            .namespace(namespace())
            .buckets(RPC_LATENCY_BUCKETS.to_vec()),
        &["method"],
    )
    .expect("invalid rpc_latency_seconds metric");
    REGISTRY
        .register(Box::new(m.clone()))
        .expect("register rpc_latency_seconds");
    m
});

/// Tracked slot for health monitoring, set by both rpc and gRPC updates, to detect lag.
static TRACKED_SLOT: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    let m = IntGaugeVec::new(
        Opts::new("tracked_slot", "Current tracked slot for health monitoring")
            .namespace(namespace()),
        &["source"],
    )
    .expect("invalid tracked_slot metric");
    REGISTRY
        .register(Box::new(m.clone()))
        .expect("register tracked_slot");
    m
});
fn counter_vec(name: &str, help: &str, labels: &[&str]) -> IntCounterVec {
    let m = IntCounterVec::new(Opts::new(name, help).namespace(namespace()), labels)
        .unwrap_or_else(|e| panic!("invalid {name} metric: {e}"));
    REGISTRY
        .register(Box::new(m.clone()))
        .unwrap_or_else(|e| panic!("register {name}: {e}"));
    m
}

fn int_gauge_vec(name: &str, help: &str, labels: &[&str]) -> IntGaugeVec {
    let m = IntGaugeVec::new(Opts::new(name, help).namespace(namespace()), labels)
        .unwrap_or_else(|e| panic!("invalid {name} metric: {e}"));
    REGISTRY
        .register(Box::new(m.clone()))
        .unwrap_or_else(|e| panic!("register {name}: {e}"));
    m
}

fn gauge_vec(name: &str, help: &str, labels: &[&str]) -> GaugeVec {
    let m = GaugeVec::new(Opts::new(name, help).namespace(namespace()), labels)
        .unwrap_or_else(|e| panic!("invalid {name} metric: {e}"));
    REGISTRY
        .register(Box::new(m.clone()))
        .unwrap_or_else(|e| panic!("register {name}: {e}"));
    m
}

// ---- liveness: is every feed still receiving data, do alerts still reach the sinks ----

const FEED_LABELS: &[&str] = &["program", "kind", "feed"];

/// Events received per feed (pipeline, rule input, poll bucket).
static FEED_EVENTS_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    counter_vec(
        "feed_events_total",
        "Events received per feed (pipeline, rule input, poll tick)",
        FEED_LABELS,
    )
});

/// Seconds since the feed's last event (since registration when it never had one).
static FEED_LAST_EVENT_AGE_SECONDS: LazyLock<GaugeVec> = LazyLock::new(|| {
    gauge_vec(
        "feed_last_event_age_seconds",
        "Seconds since the feed's last event, refreshed on every liveness check",
        FEED_LABELS,
    )
});

/// The silence bound the liveness rule applies to the feed (learned or configured).
static FEED_SILENCE_BOUND_SECONDS: LazyLock<GaugeVec> = LazyLock::new(|| {
    gauge_vec(
        "feed_silence_bound_seconds",
        "Silence the liveness rule tolerates on the feed before alerting; absent until learned",
        FEED_LABELS,
    )
});

/// 1 while the liveness rule considers the feed silent or failing.
static FEED_SILENT: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    int_gauge_vec(
        "feed_silent",
        "1 while the feed is silent past its bound or its poll source keeps failing",
        FEED_LABELS,
    )
});

/// Failed poll ticks per poll feed.
static FEED_POLL_FAILURES_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    counter_vec(
        "feed_poll_failures_total",
        "Failed fetches per poll feed",
        FEED_LABELS,
    )
});

/// Failed poll ticks in a row per poll feed; 0 after a success.
static FEED_POLL_CONSECUTIVE_FAILURES: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    int_gauge_vec(
        "feed_poll_consecutive_failures",
        "Failed fetches in a row per poll feed",
        FEED_LABELS,
    )
});

/// Seconds since the gRPC stream delivered a slot.
static STREAM_SLOT_AGE_SECONDS: LazyLock<GaugeVec> = LazyLock::new(|| {
    gauge_vec(
        "stream_slot_age_seconds",
        "Seconds since the stream delivered a slot",
        &["source"],
    )
});

static CANARY_SENT_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    counter_vec(
        "canary_sent_total",
        "Canary alerts emitted, per route",
        &["route"],
    )
});

static CANARY_DELIVERED_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    counter_vec(
        "canary_delivered_total",
        "Canary alerts a sink accepted, per route and sink",
        &["route", "sink"],
    )
});

/// Canary emit → sink accepted, in seconds.
static CANARY_LATENCY_SECONDS: LazyLock<HistogramVec> = LazyLock::new(|| {
    let m = HistogramVec::new(
        HistogramOpts::new(
            "canary_latency_seconds",
            "Seconds from canary emission to a sink accepting it",
        )
        .namespace(namespace())
        .buckets(vec![
            0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0,
        ]),
        &["sink"],
    )
    .expect("invalid canary_latency_seconds metric");
    REGISTRY
        .register(Box::new(m.clone()))
        .expect("register canary_latency_seconds");
    m
});

/// Age of the freshest canary a sink accepted, per route and sink.
static CANARY_DELIVERY_AGE_SECONDS: LazyLock<GaugeVec> = LazyLock::new(|| {
    gauge_vec(
        "canary_delivery_age_seconds",
        "Seconds since the freshest delivered canary was emitted, per route and sink",
        &["route", "sink"],
    )
});

/// 1 while a route/sink pair has no fresh canary.
static CANARY_MISSED: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    int_gauge_vec(
        "canary_missed",
        "1 while no fresh canary reached the sink on this route",
        &["route", "sink"],
    )
});

/// Betterstack heartbeats withheld because the alert path is unhealthy.
static HEARTBEAT_WITHHELD_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    let m = IntCounter::with_opts(
        Opts::new(
            "heartbeat_withheld_total",
            "Betterstack heartbeats withheld because a canary missed its sink",
        )
        .namespace(namespace()),
    )
    .expect("invalid heartbeat_withheld_total metric");
    REGISTRY
        .register(Box::new(m.clone()))
        .expect("register heartbeat_withheld_total");
    m
});

/// Per-feed metric handles, resolved once at registration so the hot path is one atomic add.
#[derive(Debug, Clone)]
pub struct FeedMetrics {
    pub events: IntCounter,
    pub age: prometheus::Gauge,
    pub bound: prometheus::Gauge,
    pub silent: IntGauge,
    pub poll_failures: IntCounter,
    pub poll_consecutive_failures: IntGauge,
}

impl FeedMetrics {
    pub fn new(program: &str, kind: &str, feed: &str) -> Self {
        let labels = [program, kind, feed];
        Self {
            events: FEED_EVENTS_TOTAL.with_label_values(&labels),
            age: FEED_LAST_EVENT_AGE_SECONDS.with_label_values(&labels),
            bound: FEED_SILENCE_BOUND_SECONDS.with_label_values(&labels),
            silent: FEED_SILENT.with_label_values(&labels),
            poll_failures: FEED_POLL_FAILURES_TOTAL.with_label_values(&labels),
            poll_consecutive_failures: FEED_POLL_CONSECUTIVE_FAILURES.with_label_values(&labels),
        }
    }
}

pub fn record_stream_slot_age(source: &str, age_secs: f64) {
    STREAM_SLOT_AGE_SECONDS
        .with_label_values(&[source])
        .set(age_secs);
}

pub fn record_canary_sent(route: &str) {
    CANARY_SENT_TOTAL.with_label_values(&[route]).inc();
}

pub fn record_canary_delivered(route: &str, sink: &str, latency_secs: f64) {
    CANARY_DELIVERED_TOTAL
        .with_label_values(&[route, sink])
        .inc();
    CANARY_LATENCY_SECONDS
        .with_label_values(&[sink])
        .observe(latency_secs);
}

pub fn record_canary_state(route: &str, sink: &str, age_secs: Option<f64>, missed: bool) {
    if let Some(age) = age_secs {
        CANARY_DELIVERY_AGE_SECONDS
            .with_label_values(&[route, sink])
            .set(age);
    }
    CANARY_MISSED
        .with_label_values(&[route, sink])
        .set(i64::from(missed));
}

pub fn record_heartbeat_withheld() {
    HEARTBEAT_WITHHELD_TOTAL.inc();
}

/// Force initialization of all metrics so they appear in scrapes before their
/// first use. Call once at startup with the deployment's metric prefix;
/// otherwise a metric stays absent until the first time its static is touched.
pub fn init(namespace: &str) {
    let _ = NAMESPACE.set(namespace.to_string());
    LazyLock::force(&ALERTS_TOTAL);
    LazyLock::force(&ALERTS_SUPPRESSED_TOTAL);
    LazyLock::force(&GRPC_EVENTS_TOTAL);
    LazyLock::force(&RPC_FETCH_STATUS_TOTAL);
    LazyLock::force(&RPC_LATENCY_SECONDS);
    LazyLock::force(&TRACKED_SLOT);
    LazyLock::force(&FEED_EVENTS_TOTAL);
    LazyLock::force(&FEED_LAST_EVENT_AGE_SECONDS);
    LazyLock::force(&FEED_SILENCE_BOUND_SECONDS);
    LazyLock::force(&FEED_SILENT);
    LazyLock::force(&FEED_POLL_FAILURES_TOTAL);
    LazyLock::force(&FEED_POLL_CONSECUTIVE_FAILURES);
    LazyLock::force(&STREAM_SLOT_AGE_SECONDS);
    LazyLock::force(&CANARY_SENT_TOTAL);
    LazyLock::force(&CANARY_DELIVERED_TOTAL);
    LazyLock::force(&CANARY_LATENCY_SECONDS);
    LazyLock::force(&CANARY_DELIVERY_AGE_SECONDS);
    LazyLock::force(&CANARY_MISSED);
    LazyLock::force(&HEARTBEAT_WITHHELD_TOTAL);
}

/// Increment the alert counter for a given severity
pub fn record_alert<S: Into<String>>(severity: S) {
    ALERTS_TOTAL.with_label_values(&[&severity.into()]).inc();
}

/// Increment the suppressed-alert counter for a given rule.
pub fn record_alert_suppressed(rule: &str) {
    ALERTS_SUPPRESSED_TOTAL.with_label_values(&[rule]).inc();
}

#[derive(Debug, Clone, Copy)]
pub enum RpcFetchStatus {
    Success,
    Failure,
}

impl Display for RpcFetchStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Success => write!(f, "success"),
            Self::Failure => write!(f, "failure"),
        }
    }
}

/// Increment the RPC fetch status counter for a given method and status.
pub fn record_rpc_fetch_status(method: &str, status: RpcFetchStatus) {
    RPC_FETCH_STATUS_TOTAL
        .with_label_values(&[method, &status.to_string()])
        .inc();
}

/// Get a latency histogram handle for a given RPC method; observe seconds on it.
pub fn record_rpc_latency(method: &str, latency: f64) {
    RPC_LATENCY_SECONDS
        .with_label_values(&[method])
        .observe(latency);
}

#[derive(Debug, Clone, Copy)]
pub enum GrpcEvent {
    InstructionUpdate,
    AccountUpdate,
    TransactionUpdate,
    SlotUpdate,
}

/// Increment the appropriate counter for a given gRPC event.
pub fn record_grpc_event(event: GrpcEvent) {
    match event {
        GrpcEvent::InstructionUpdate => {
            GRPC_EVENTS_TOTAL.with_label_values(&["instruction"]).inc();
        }
        GrpcEvent::AccountUpdate => {
            GRPC_EVENTS_TOTAL.with_label_values(&["account"]).inc();
        }
        GrpcEvent::TransactionUpdate => {
            GRPC_EVENTS_TOTAL.with_label_values(&["transaction"]).inc();
        }
        GrpcEvent::SlotUpdate => {
            GRPC_EVENTS_TOTAL.with_label_values(&["slot"]).inc();
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum SlotSource {
    Rpc,
    Grpc,
}

impl Display for SlotSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Rpc => write!(f, "rpc"),
            Self::Grpc => write!(f, "grpc"),
        }
    }
}

/// Record the tracked slot gauge for health monitoring.
pub fn record_tracked_slot(rpc_slot: u64, grpc_slot: u64) {
    TRACKED_SLOT
        .with_label_values(&[&SlotSource::Rpc.to_string()])
        .set(rpc_slot as i64);
    TRACKED_SLOT
        .with_label_values(&[&SlotSource::Grpc.to_string()])
        .set(grpc_slot as i64);
}
