//! The service as a library: a deployment binary calls [`run`] with its
//! programs. Everything else (config, validation, wiring, consumer) is here.

pub mod config;
pub mod consumer;
#[cfg(feature = "testing")]
pub mod testing;
mod utils;
pub mod validate;
pub mod wiring;

use crate::config::SentinelConfig;
use crate::consumer::ConsumerService;
use crate::utils::{
    connect_to_redis, extract_pod_id, listen_for_shutdown_signal, wait_for_shutdown, ShutdownReason,
};
pub use crate::utils::{setup_crypto_provider, setup_tracing_subscriber};
use crate::validate::validate_programs;
use alerting::canary_watchdog::{spawn_canary_watchdog, DEFAULT_WATCHDOG_INTERVAL};
use alerting::outbox::Outbox;
use alerting::service::AlertingService;
use anyhow::Context;
use data_source::rpc::build_rpc_client;
use observability::slot_tracker::SlotTracker;
use observability::{prometheus, run_observability_stack};
use solana_sentinel::reactor::spawn_alert_tap;
use solana_sentinel::{Liveness, ProgramSpec, ValidatedProgram};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{error, info, warn};

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
///
/// The host owns the tokio runtime, the rustls crypto provider, and the tracing
/// subscriber. Anything installed before this call is respected; otherwise
/// [`setup_crypto_provider`] and [`setup_tracing_subscriber`] supply defaults.
pub async fn run(opts: RunOptions) -> anyhow::Result<()> {
    setup_crypto_provider();
    setup_tracing_subscriber();
    prometheus::metrics::init(opts.service_name);

    info!(service = opts.service_name, "Starting service");

    let config = Arc::new(SentinelConfig::load()?);
    config.data_source().validate()?;
    info!("Configuration loaded");

    // Typed rule config is checked before anything connects; `check` stops here.
    let programs = validate_programs(&config, opts.programs)?;
    print_program_reports(&programs);

    let cancel_token = CancellationToken::new();
    let signalled = listen_for_shutdown_signal(&cancel_token);
    let tracker = TaskTracker::new();
    let rpc = Arc::new(build_rpc_client(
        config.rpc().url.as_str(),
        &config.rpc().headers,
    )?);

    // Extract the pod ID from the environment (hostname) for use in Redis keys as a namespace
    let pod_id = extract_pod_id()?;
    info!(pod_id = %pod_id, "Pod ID extracted from environment");

    let (router, cooldowns) = wiring::build_alerting_router(&config, &programs)?;

    // Feed liveness and canary state: pipelines record into it, the health rules read it,
    // the alerting service reports canary deliveries, the heartbeat and watchdog check it.
    let liveness = Liveness::new();
    liveness
        .canary()
        .set_routes(wiring::canary_routes(config.programs_with_rules()));

    // Sidecar Redis holds per-pod state (slot checkpoint, local gate);
    let redis_sidecar_conn = connect_to_redis(&config.redis().redis_sidecar_url)
        .await
        .context("Sidecar Redis unavailable")?;

    let cooldown_gate = wiring::build_cooldown_gate(
        &config,
        cooldowns,
        &redis_sidecar_conn,
        &pod_id,
        opts.service_name,
    )
    .await
    .context("Failed to build cooldown gate")?;

    let outbox = Outbox::new(redis_sidecar_conn.clone(), &pod_id, opts.service_name);

    // Tracks the last seen slot and persists it to Redis for recovery across restarts.
    let slot_tracker = SlotTracker::try_init(
        &rpc,
        redis_sidecar_conn,
        &pod_id,
        cancel_token.clone(),
        &tracker,
        config.data_source().has_yellowstone(),
    )
    .await
    .context("Failed to initialize slot tracker")?;

    // Up before bootstrap, so a slow RPC warm-up doesn't fail the liveness probe;
    // a stream that never delivers is caught by the slot tracker's stall watchdog.
    let observability = config.observability();
    run_observability_stack(
        &slot_tracker,
        &liveness,
        observability.server.bind_addr,
        observability.heartbeat(),
        cancel_token.clone(),
        &tracker,
    );

    // Rules send to `rules_tx`; the alert tap hands each alert to the reactors, then forwards it
    // to the alerting service (cooldown / fleet gates, sinks) on `alerts_tx`.
    let (rules_tx, rules_rx) = mpsc::channel(1024);
    let (alerts_tx, alerts_rx) = mpsc::channel(1024);

    // start the consumer service which starts processing messages from grpc + bootstraps any cached state from the rpc
    let consumer_service = ConsumerService::new(
        &config,
        programs,
        // Kept here too, so the channel stays open for the whole run as before.
        rules_tx.clone(),
        cancel_token.clone(),
        Arc::clone(&rpc),
        slot_tracker.clone(),
        pod_id.clone(),
        liveness.clone(),
    );
    // Bootstrap can take minutes of RPC; a shutdown during it skips straight to the drain.
    let reactors = tokio::select! {
        started = consumer_service.start(&tracker) => started.context("Failed to start consumer")?,
        () = cancel_token.cancelled() => {
            warn!("Shutdown requested during startup");
            Vec::new()
        }
    };
    // Past the tap, so a stuck tap or reactor cannot hide a missed canary.
    spawn_canary_watchdog(
        liveness.clone(),
        alerts_tx.clone(),
        pod_id.clone(),
        DEFAULT_WATCHDOG_INTERVAL,
        cancel_token.clone(),
        &tracker,
    );
    spawn_alert_tap(reactors, rules_rx, alerts_tx, &tracker);

    // Start the alerting service and send alerts to the clickhouse and slack sinks
    let alerting_service =
        AlertingService::new(alerts_rx, router, cooldown_gate, cancel_token.clone())
            .with_outbox(outbox)
            .with_liveness(liveness);
    alerting_service.start(&tracker);

    // Wait for shutdown signal (Ctrl+C, SIGTERM) or cancellation token to be triggered
    let reason = wait_for_shutdown(&cancel_token, &signalled).await;

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
