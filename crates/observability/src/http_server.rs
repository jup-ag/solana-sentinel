use crate::liveness::Liveness;
use crate::slot_tracker::SlotTracker;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use serde_json::json;
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{error, info};

/// Shared state for the HTTP server.
#[derive(Clone)]
struct HttpState {
    /// slot_tracker is used to track the last seen slot from the gRPC source and is used in the liveness probe.
    slot_tracker: SlotTracker,
    liveness: Liveness,
}

impl HttpState {
    pub fn new(slot_tracker: SlotTracker, liveness: Liveness) -> Self {
        Self {
            slot_tracker,
            liveness,
        }
    }
}

/// Liveness probe: returns `200 OK` as long as the process can serve requests.
async fn liveness(State(state): State<HttpState>) -> impl IntoResponse {
    // Reaching this handler means the process is up and serving.
    (
        StatusCode::OK,
        Json(json!({ "last_seen_grpc_slot": &state.slot_tracker.load_last_seen_grpc_slot() })),
    )
}

/// Feed and canary state: what each pipeline last received, what each sink last accepted.
async fn liveness_report(State(state): State<HttpState>) -> impl IntoResponse {
    Json(liveness_json(&state.liveness, tokio::time::Instant::now()))
}

fn liveness_json(liveness: &Liveness, now: tokio::time::Instant) -> serde_json::Value {
    let secs = |d: std::time::Duration| (d.as_secs_f64() * 10.0).round() / 10.0;
    let feeds: Vec<_> = liveness
        .snapshot(now)
        .into_iter()
        .map(|f| {
            json!({
                "feed": f.id.as_str(),
                "kind": f.kind.as_str(),
                "parent": f.parent.as_ref().map(|p| p.as_str().to_string()),
                "events": f.events,
                "last_slot": f.last_slot,
                "silent_for_s": secs(f.silent_for(now)),
                "p99_gap_s": f.p99_gap.map(secs),
                "samples": f.samples,
                "consecutive_failures": f.consecutive_failures,
                "last_error": f.last_error,
            })
        })
        .collect();
    let canary = liveness.canary();
    let misses: Vec<_> = canary
        .check(now)
        .into_iter()
        .map(|m| {
            json!({
                "route": m.route,
                "sink": m.sink.as_str(),
                "freshest_age_s": m.freshest_age.map(secs),
                "budget_s": secs(m.budget),
            })
        })
        .collect();
    let plan = canary.plan().map(|p| {
        json!({
            "interval_s": secs(p.interval),
            "deadline_s": secs(p.deadline),
            "routes": p.routes.iter().map(|r| json!({
                "route": r.id,
                "sinks": r.sinks.iter().map(|s| s.as_str().to_string()).collect::<Vec<_>>(),
            })).collect::<Vec<_>>(),
        })
    });
    json!({ "feeds": feeds, "canary": { "plan": plan, "missed": misses } })
}

/// Router.
fn router(state: HttpState) -> Router {
    Router::new()
        .route("/health", get(liveness)) // liveness: is the process alive?
        .route("/metrics", get(metrics)) // Prometheus metrics endpoint
        .route("/liveness", get(liveness_report)) // per-feed and canary state, JSON
        .with_state(state) // inject HttpState into the readiness handler
}

async fn shutdown_signal(cancel_token: CancellationToken) {
    // Resolves when someone else cancels the token — this is the signal to drain.
    cancel_token.cancelled().await;
}

/// Prometheus metrics handler: returns the current snapshot of all metrics in the Prometheus text exposition format.
async fn metrics() -> impl IntoResponse {
    match crate::prometheus::registry::gather() {
        Ok((content_type, body)) => {
            ([(axum::http::header::CONTENT_TYPE, content_type)], body).into_response()
        }
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "encode error").into_response(),
    }
}

/// Bind and serve the HTTP server on `addr` until the process exits.
async fn serve(
    addr: SocketAddr,
    state: HttpState,
    cancel_token: CancellationToken,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind(addr)
        .await
        .map_err(|e| anyhow::anyhow!("failed to bind on {addr}: {e}"))?;

    axum::serve(listener, router(state))
        .with_graceful_shutdown(shutdown_signal(cancel_token))
        .await
        .map_err(|e| anyhow::anyhow!("HTTP server error: {e}"))?;

    Ok(())
}

pub fn run_observability_http_server(
    slot_tracker: SlotTracker,
    liveness: Liveness,
    bind_addr: SocketAddr,
    cancel_token: CancellationToken,
    tracker: &TaskTracker,
) {
    info!("Starting Observability HTTP server on {bind_addr}");
    let state = HttpState::new(slot_tracker, liveness);
    tracker.spawn(async move {
        if let Err(e) = serve(bind_addr, state, cancel_token.clone()).await {
            error!("HTTP server error: {e}");
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::liveness::FeedKind;

    #[test]
    fn liveness_report_lists_feeds() {
        let liveness = Liveness::new();
        liveness
            .feed("p", FeedKind::Account, "parser", None)
            .record_event(42);
        let report = liveness_json(&liveness, tokio::time::Instant::now());
        assert_eq!(report["feeds"][0]["feed"], "p:account:parser");
        assert_eq!(report["feeds"][0]["last_slot"], 42);
        assert!(report["canary"]["plan"].is_null());
    }
}
