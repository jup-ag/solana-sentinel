//! Prometheus metrics for the monitoring service.
//!
//! Metrics are global statics so they can be incremented from anywhere without
//! threading a handle through call sites. They register themselves to a private
//! global `REGISTRY` on first access; `gather()` exposes that registry to the
//! `/metrics` HTTP handler. Rates (instruction, account-update, transaction)
//! are exposed as monotonic counters of totals; the per-second rate is derived
//! in PromQL via `rate()` rather than stored.

use crate::prometheus::registry::REGISTRY;
use prometheus::{HistogramOpts, HistogramVec, IntCounterVec, IntGaugeVec, Opts};
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
