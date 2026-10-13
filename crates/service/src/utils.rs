use anyhow::Context;
use redis::aio::{ConnectionManager, ConnectionManagerConfig};
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

pub async fn wait_for_shutdown_signal(cancel_token: CancellationToken) -> ShutdownReason {
    tokio::select! {
        result = signal::ctrl_c() => {
            match result {
                Ok(()) => info!("Shutdown signal received; shutting down"),
                Err(err) => error!(error = %err, "Failed to listen for shutdown signal"),
            }
            ShutdownReason::Signal
        }
        _ = cancel_token.cancelled() => {
            info!("Consumer processing completed; shutting down");
            ShutdownReason::Internal
        }
    }
}

/// Connects to Redis with a timeout and retry configuration.
pub async fn connect_to_redis(redis_url: &str) -> anyhow::Result<ConnectionManager> {
    let client = redis::Client::open(redis_url)?;
    let cfg = ConnectionManagerConfig::new()
        .set_connection_timeout(Duration::from_secs(2))
        .set_response_timeout(Duration::from_secs(1))
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
