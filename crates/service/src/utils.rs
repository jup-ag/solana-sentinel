use anyhow::Context;
use redis::aio::{ConnectionManager, ConnectionManagerConfig};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::signal;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

pub enum ShutdownReason {
    Signal,
    Internal,
}

/// Install the aws-lc-rs rustls provider unless the host already installed one.
/// Returns `true` when this call installed it.
pub fn setup_crypto_provider() -> bool {
    if rustls::crypto::CryptoProvider::get_default().is_some() {
        return false;
    }
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("Failed to install default crypto provider");
    true
}

/// Install the default fmt subscriber (`RUST_LOG` filter, `LOG_FORMAT=json` for
/// JSON lines) unless the host already set a global subscriber. Returns `true`
/// when this call installed it.
pub fn setup_tracing_subscriber() -> bool {
    if tracing::dispatcher::has_been_set() {
        return false;
    }
    let env_filter = tracing_subscriber::EnvFilter::builder()
        .with_default_directive(tracing_subscriber::filter::LevelFilter::INFO.into())
        .from_env_lossy();

    let builder = tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_thread_ids(true)
        .with_line_number(true);

    if std::env::var("LOG_FORMAT").as_deref() == Ok("json") {
        builder.json().flatten_event(true).try_init().is_ok()
    } else {
        builder.try_init().is_ok()
    }
}

/// Handles Ctrl-C and SIGTERM from now on by cancelling `cancel_token`, so a signal during
/// startup also takes the drain path. Call before startup; `wait_for_shutdown` reads the result.
pub fn listen_for_shutdown_signal(cancel_token: &CancellationToken) -> Arc<AtomicBool> {
    let signalled = Arc::new(AtomicBool::new(false));
    // Registered here, not on first poll, so no SIGTERM slips through before the task runs.
    let mut sigterm = sigterm_stream();
    let (cancel, flag) = (cancel_token.clone(), Arc::clone(&signalled));
    tokio::spawn(async move {
        tokio::select! {
            result = signal::ctrl_c() => match result {
                Ok(()) => info!("Shutdown signal received; shutting down"),
                Err(err) => {
                    error!(error = %err, "Failed to listen for shutdown signal");
                    return;
                }
            },
            // Kubernetes stops pods with SIGTERM; unhandled, it kills the process before the alert drain.
            () = recv_sigterm(&mut sigterm) => info!("SIGTERM received; shutting down"),
            () = cancel.cancelled() => return,
        }
        flag.store(true, Ordering::Relaxed);
        cancel.cancel();
    });
    signalled
}

/// Waits for shutdown and says whether a signal or an internal failure caused it.
pub async fn wait_for_shutdown(
    cancel_token: &CancellationToken,
    signalled: &AtomicBool,
) -> ShutdownReason {
    cancel_token.cancelled().await;
    if signalled.load(Ordering::Relaxed) {
        ShutdownReason::Signal
    } else {
        info!("Consumer processing completed; shutting down");
        ShutdownReason::Internal
    }
}

#[cfg(unix)]
type SigTerm = Option<signal::unix::Signal>;
#[cfg(not(unix))]
type SigTerm = ();

#[cfg(unix)]
fn sigterm_stream() -> SigTerm {
    signal::unix::signal(signal::unix::SignalKind::terminate())
        .inspect_err(|err| error!(error = %err, "Failed to listen for SIGTERM"))
        .ok()
}

#[cfg(not(unix))]
fn sigterm_stream() -> SigTerm {}

#[cfg(unix)]
async fn recv_sigterm(sigterm: &mut SigTerm) {
    match sigterm {
        Some(s) => {
            s.recv().await;
        }
        None => std::future::pending().await,
    }
}

#[cfg(not(unix))]
async fn recv_sigterm(_: &mut SigTerm) {
    std::future::pending().await
}

/// Connects to Redis with a timeout and retry configuration.
pub async fn connect_to_redis(redis_url: &str) -> anyhow::Result<ConnectionManager> {
    let client = redis::Client::open(redis_url)?;
    let cfg = ConnectionManagerConfig::new()
        .set_connection_timeout(Some(Duration::from_secs(2)))
        .set_response_timeout(Some(Duration::from_secs(1)))
        .set_number_of_retries(3);
    let conn = ConnectionManager::new_with_config(client, cfg).await?;
    Ok(conn)
}

pub fn extract_pod_id() -> anyhow::Result<String> {
    let id = hostname::get()
        .context("failed to read hostname")?
        .to_string_lossy()
        .trim()
        .to_owned();
    anyhow::ensure!(
        !id.is_empty(),
        "hostname is empty; cannot derive slot checkpoint key"
    );
    Ok(id)
}
