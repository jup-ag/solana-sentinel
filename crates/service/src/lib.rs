//! The service as a library: a deployment binary calls [`run`] with its
//! programs. Everything else (config, validation, wiring, consumer) is here.

pub mod config;
pub mod consumer;
pub mod validate;
pub mod wiring;

use crate::config::SentinelConfig;
use crate::consumer::ConsumerService;
use crate::validate::validate_programs;
use alerting::cooldown_gate::CooldownGate;
use alerting::fleet_gate::FleetGate;
use alerting::service::AlertingService;
use anyhow::Context;
use observability::slot_tracker::SlotTracker;
use observability::{prometheus, run_observability_stack, HeartbeatSettings};
use redis::aio::{ConnectionManager, ConnectionManagerConfig};
use redis::cluster::ClusterClient;
use redis::cluster_async::ClusterConnection;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sentinel::ProgramSpec;
use std::sync::Arc;
use std::time::Duration;
use tokio::signal;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{error, info};

/// What a deployment binary passes in: its name and the programs it runs.
pub struct RunOptions {
    /// Prefix for metric names and Redis keys; also what logs call the service.
    pub service_name: &'static str,
    /// The programs this deployment can run; `rules.toml` picks among them.
    pub programs: &'static [fn() -> ProgramSpec],
}

/// Run the service until a shutdown signal or an internal failure. With `check`
/// as the first CLI argument it validates config and exits after printing the
/// resolved subscriptions. Exits the process non-zero on an internal stop so a
/// supervisor restarts it.
pub async fn run(opts: RunOptions) -> anyhow::Result<()> {
    setup_crypto_provider();
    setup_tracing_subscriber();
    prometheus::metrics::init(opts.service_name);

    info!(service = opts.service_name, "Starting service");

    let config = Arc::new(SentinelConfig::load()?);
    info!("Configuration loaded");

    // Typed rule config is checked before anything connects; `check` stops here.
    let programs = validate_programs(&config, opts.programs)?;
    if std::env::args().nth(1).as_deref() == Some("check") {
        for program in &programs {
            print!("{}", program.report());
        }
        println!("ok: {} program(s) validated", programs.len());
        return Ok(());
    }

    let cancel_token = CancellationToken::new();
    let tracker = TaskTracker::new();
    let rpc = Arc::new(RpcClient::new(config.rpc().url.to_string()));

    // Extract the pod ID from the environment (hostname) for use in Redis keys as a namespace
    let pod_id = extract_pod_id()?;
    info!(pod_id = %pod_id, "Pod ID extracted from environment");

    let (router, cooldowns) = wiring::build_alerting_router(&config, &programs)?;

    // Sidecar Redis holds per-pod state (slot checkpoint, cooldown gate); fleet Redis the shared
    // gate. Without `[redis]` both gates are in-memory, which is fine for a single local process.
    let (cooldown_gate, fleet_gate, redis_sidecar_conn) = match config.redis() {
        Some(redis) => {
            let sidecar = connect_to_redis(&redis.redis_sidecar_url).await?;
            let fleet = connect_to_redis_cluster(&redis.redis_fleet_url).await?;
            (
                CooldownGate::new(
                    sidecar.clone(),
                    cooldowns.clone(),
                    &pod_id,
                    opts.service_name,
                ),
                FleetGate::new(fleet, cooldowns, opts.service_name),
                Some(sidecar),
            )
        }
        None => {
            info!("No [redis] configured; using in-memory cooldown and fleet gates");
            (
                CooldownGate::in_memory(cooldowns.clone()),
                FleetGate::in_memory(cooldowns),
                None,
            )
        }
    };

    // Tracks the last seen slot and persists it to Redis for recovery across restarts.
    let slot_tracker = SlotTracker::try_init(
        &rpc,
        redis_sidecar_conn,
        &pod_id,
        cancel_token.clone(),
        &tracker,
    )
    .await?;

    // alerts channel to send alerts from the consumer service to the alerting service(clickhouse, slack and betterstack sinks)
    let (alerts_tx, alerts_rx) = mpsc::channel(1024);

    // start the consumer service which starts processing messages from grpc + bootstraps any cached state from the rpc
    let consumer_service = ConsumerService::new(
        &Arc::clone(&config),
        programs,
        alerts_tx.clone(),
        cancel_token.clone(),
        Arc::clone(&rpc),
        slot_tracker.clone(),
    );
    consumer_service.start(&tracker).await?;

    // Start the observability stack: metrics / health HTTP server and the optional heartbeat.
    let heartbeat = config
        .observability()
        .betterstack
        .as_ref()
        .map(|hb| HeartbeatSettings {
            url: hb.heartbeat_url.as_str(),
            interval: Duration::from_secs(hb.heartbeat_interval_seconds),
        });
    run_observability_stack(
        &slot_tracker,
        config.observability().server.bind_addr,
        heartbeat,
        cancel_token.clone(),
        &tracker,
    );

    // Start the alerting service and send alerts to the clickhouse and slack sinks
    let alerting_service = AlertingService::new(
        alerts_rx,
        router,
        cooldown_gate,
        fleet_gate,
        cancel_token.clone(),
    );
    alerting_service.start(&tracker);

    // Wait for shutdown signal (Ctrl+C) or cancellation token to be triggered
    let reason = wait_for_shutdown_signal(cancel_token.clone()).await;

    cancel_token.cancel();
    tracker.close();
    tracker.wait().await;

    info!("Shutdown complete");

    if matches!(reason, ShutdownReason::Internal) {
        error!("Service stopped unexpectedly; exiting non-zero to trigger restart");
        std::process::exit(1);
    }

    Ok(())
}

enum ShutdownReason {
    Signal,
    Internal,
}

fn setup_crypto_provider() {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("failed to install rustls crypto provider");
}

fn setup_tracing_subscriber() {
    let env_filter = tracing_subscriber::EnvFilter::builder()
        .with_default_directive(tracing_subscriber::filter::LevelFilter::INFO.into())
        .from_env_lossy();

    let builder = tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_thread_ids(true)
        .with_line_number(true);

    if std::env::var("LOG_FORMAT").as_deref() == Ok("json") {
        builder.json().flatten_event(true).init();
    } else {
        builder.init();
    }
}

async fn wait_for_shutdown_signal(cancel_token: CancellationToken) -> ShutdownReason {
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

// Connects to Redis with a timeout and retry configuration.
async fn connect_to_redis(redis_url: &str) -> anyhow::Result<ConnectionManager> {
    let client = redis::Client::open(redis_url)?;
    let cfg = ConnectionManagerConfig::new()
        .set_connection_timeout(Duration::from_secs(2))
        .set_response_timeout(Duration::from_secs(1))
        .set_number_of_retries(3);
    let conn = ConnectionManager::new_with_config(client, cfg).await?;
    Ok(conn)
}

// Connects to a cluster-mode Redis (ElastiCache CME) with timeouts and retries.
async fn connect_to_redis_cluster(redis_url: &str) -> anyhow::Result<ClusterConnection> {
    let client = ClusterClient::builder(vec![redis_url])
        .connection_timeout(Duration::from_secs(2))
        .response_timeout(Duration::from_secs(1)) // per-attempt
        .retries(1)
        .build()?;
    Ok(client.get_async_connection().await?)
}

fn extract_pod_id() -> anyhow::Result<String> {
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
