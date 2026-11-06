use crate::betterstack::heartbeat::Heartbeat;
use crate::http_server::run_observability_http_server;
use crate::liveness::Liveness;
use crate::slot_tracker::SlotTracker;
use std::net::SocketAddr;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

pub mod betterstack;
pub mod http_server;
pub mod liveness;
pub mod prometheus;
pub mod slot_tracker;
pub mod source_health;

/// Betterstack heartbeat settings; `None` disables the heartbeat task (local runs).
pub struct HeartbeatSettings<'a> {
    pub url: &'a str,
    pub interval: Duration,
    /// Withhold the heartbeat while a canary misses its sink, so Betterstack pages when the
    /// alert path breaks even if nothing in the process can report it.
    pub gate_on_canary: bool,
}

pub fn run_observability_stack(
    slot_tracker: &SlotTracker,
    liveness: &Liveness,
    bind_addr: SocketAddr,
    heartbeat: Option<HeartbeatSettings<'_>>,
    cancel_token: CancellationToken,
    tracker: &TaskTracker,
) {
    // Start the observability HTTP server for liveness and metrics endpoints
    run_observability_http_server(
        slot_tracker.clone(),
        liveness.clone(),
        bind_addr,
        cancel_token.clone(),
        tracker,
    );

    // Slot health is a rule now (program `system`), so only the heartbeat remains here.
    if let Some(hb) = heartbeat {
        let mut heartbeat = Heartbeat::new(hb.url, hb.interval, cancel_token);
        if hb.gate_on_canary {
            heartbeat = heartbeat.gated_on(liveness.clone());
        }
        heartbeat.start(tracker);
    }
}
