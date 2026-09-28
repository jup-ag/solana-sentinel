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
}

impl HttpState {
    pub fn new(slot_tracker: SlotTracker) -> Self {
        Self { slot_tracker }
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

/// Router.
fn router(state: HttpState) -> Router {
    Router::new()
        .route("/health", get(liveness)) // liveness: is the process alive?
        .route("/metrics", get(metrics)) // Prometheus metrics endpoint
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
    bind_addr: SocketAddr,
    cancel_token: CancellationToken,
    tracker: &TaskTracker,
) {
    info!("Starting Observability HTTP server on {bind_addr}");
    let state = HttpState::new(slot_tracker);
    tracker.spawn(async move {
        if let Err(e) = serve(bind_addr, state, cancel_token.clone()).await {
            error!("HTTP server error: {e}");
        }
    });
}
