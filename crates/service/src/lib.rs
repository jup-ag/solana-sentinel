//! The service as a library: a deployment binary calls [`run`] with its
//! programs. Everything else (config, validation, wiring, consumer) is here.

pub mod config;
pub mod consumer;
mod utils;
pub mod validate;
pub mod wiring;

use crate::config::service::DataSourceConfig;
use crate::config::SentinelConfig;
use crate::consumer::ConsumerService;
use crate::utils::{
    connect_to_redis, extract_pod_id, listen_for_shutdown_signal, wait_for_shutdown, ShutdownReason,
};
pub use crate::utils::{setup_crypto_provider, setup_tracing_subscriber};
use crate::validate::validate_programs;
use crate::wiring::alerting::{build_alerting_router, build_cooldown_gate};
use crate::wiring::riposte::RiposteService;
use alerting::outbox::Outbox;
use alerting::service::AlertingService;
use anyhow::Context;
use data_source::rpc::build_rpc_client;
use observability::slot_tracker::SlotTracker;
use observability::{prometheus, run_observability_stack};
use solana_sentinel::{ProgramSpec, ValidatedProgram};
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
    info!("Configuration loaded");

    // Everything that needs no network is checked before anything connects; `check` stops here.
    let programs = validate_programs(&config, opts.programs)?;
    let (router, cooldowns) = build_alerting_router(&config, &programs)?;
    let riposte_plan = RiposteService::validate(&config, &programs)?;
    print_program_reports(&programs);
    if std::env::args().nth(1).as_deref() == Some("check") {
        return Ok(());
    }

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

    // Sidecar Redis holds per-pod state (slot checkpoint, local gate);
    let redis_sidecar_conn = connect_to_redis(&config.redis().redis_sidecar_url)
        .await
        .context("Sidecar Redis unavailable")?;

    // alerts channel to send alerts from the consumer service to the alerting service(clickhouse, slack and betterstack sinks)
    let (alerts_tx, alerts_rx) = mpsc::channel(1024);
    // Riposte's countermeasure tasks, and alerting's own token: on shutdown alerting stops only
    // after those tasks have reported, so an in-flight outcome ("unknown") still reaches sinks.
    let riposte_tracker = TaskTracker::new();
    let alerting_cancel = CancellationToken::new();

    // Before the stream connects, so a bad signer fails fast. Rules feed its queue directly,
    // so alerting's flush never delays a countermeasure; its outcomes come back as alerts.
    let riposte_ctx = match riposte_plan {
        Some(plan) => {
            let riposte = RiposteService::build(
                plan,
                &config,
                &rpc,
                alerts_tx.clone(),
                &pod_id,
                opts.service_name,
                &cancel_token,
                &tracker,
            )
            .await?;
            Some(riposte.start(&riposte_tracker))
        }
        None => None,
    };

    let cooldown_gate = build_cooldown_gate(
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
        matches!(config.data_source(), DataSourceConfig::YellowstoneGrpc(_)),
    )
    .await
    .context("Failed to initialize slot tracker")?;

    // Up before bootstrap, so a slow RPC warm-up doesn't fail the liveness probe;
    // a stream that never delivers is caught by the slot tracker's stall watchdog.
    let observability = config.observability();
    run_observability_stack(
        &slot_tracker,
        observability.server.bind_addr,
        observability.heartbeat(),
        cancel_token.clone(),
        &tracker,
    );

    // Before bootstrap: poll and sweep loops (and Riposte's outcomes) alert as soon as they spawn,
    // and a full channel would time out Riposte's outcome reports.
    let alerting_service =
        AlertingService::new(alerts_rx, router, cooldown_gate, alerting_cancel.clone())
            .with_outbox(outbox);
    alerting_service.start(&tracker);

    // start the consumer service which starts processing messages from grpc + bootstraps any cached state from the rpc
    let consumer_service = ConsumerService::new(
        &config,
        programs,
        alerts_tx.clone(),
        cancel_token.clone(),
        Arc::clone(&rpc),
        slot_tracker.clone(),
        pod_id.clone(),
        riposte_ctx,
    );
    // Bootstrap can take minutes of RPC; a shutdown during it skips straight to the drain.
    tokio::select! {
        started = consumer_service.start(&tracker) => started.context("Failed to start consumer")?,
        () = cancel_token.cancelled() => warn!("Shutdown requested during startup"),
    }

    // Wait for shutdown signal (Ctrl+C, SIGTERM) or cancellation token to be triggered
    let reason = wait_for_shutdown(&cancel_token, &signalled).await;

    cancel_token.cancel();
    riposte_tracker.close();
    riposte_tracker.wait().await;
    alerting_cancel.cancel();
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
