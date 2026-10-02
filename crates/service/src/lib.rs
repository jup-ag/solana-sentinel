//! The service as a library: a deployment binary calls [`run`] with its
//! programs. Everything else (config, validation, wiring, consumer) is here.

pub mod config;
pub mod consumer;
mod utils;
pub mod validate;
pub mod wiring;

use crate::config::SentinelConfig;
use crate::consumer::ConsumerService;
use crate::utils::{
    connect_to_redis, extract_pod_id, setup_crypto_provider, setup_tracing_subscriber,
    wait_for_shutdown_signal, ShutdownReason,
};
use crate::validate::validate_programs;
use alerting::service::AlertingService;
use observability::slot_tracker::SlotTracker;
use observability::{prometheus, run_observability_stack};
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sentinel::{ProgramSpec, ValidatedProgram};
use std::sync::Arc;
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
    print_program_reports(&programs);

    let cancel_token = CancellationToken::new();
    let tracker = TaskTracker::new();
    let rpc = Arc::new(RpcClient::new(config.rpc().url.to_string()));

    // Extract the pod ID from the environment (hostname) for use in Redis keys as a namespace
    let pod_id = extract_pod_id()?;
    info!(pod_id = %pod_id, "Pod ID extracted from environment");

    let (router, cooldowns) = wiring::build_alerting_router(&config, &programs)?;

    // Sidecar Redis holds per-pod state (slot checkpoint, local gate);
    let redis_sidecar_conn = connect_to_redis(&config.redis().redis_sidecar_url).await?;

    let cooldown_gate = wiring::build_cooldown_gate(
        &config,
        cooldowns,
        &redis_sidecar_conn,
        &pod_id,
        opts.service_name,
    )
    .await?;

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
        &config,
        programs,
        alerts_tx.clone(),
        cancel_token.clone(),
        Arc::clone(&rpc),
        slot_tracker.clone(),
    );
    consumer_service.start(&tracker).await?;

    // Start the observability stack: metrics / health HTTP server and the optional heartbeat.
    let observability = config.observability();
    run_observability_stack(
        &slot_tracker,
        observability.server.bind_addr,
        observability.heartbeat(),
        cancel_token.clone(),
        &tracker,
    );

    // Start the alerting service and send alerts to the clickhouse and slack sinks
    let alerting_service =
        AlertingService::new(alerts_rx, router, cooldown_gate, cancel_token.clone());
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

fn print_program_reports(programs: &[ValidatedProgram]) {
    for program in programs {
        print!("{}", program.report());
    }
    println!("ok: {} program(s) validated", programs.len());
}
