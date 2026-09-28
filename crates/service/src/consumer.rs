//! Steps 4–6 of startup: bootstrap every validated program concurrently, install
//! their pipelines into one shipstern runtime plus poll / sweep loops, run it.

use crate::config::service::{GeyserSourceConfig, YellowstoneConfig};
use crate::config::SentinelConfig;
use anyhow::Result;
use domain::alert::Alert;
use futures::future::try_join_all;
use ingest::{GrpcSource, WebsocketSource};
use observability::slot_tracker::SlotTracker;
use shipstern::config::{BufferConfig, ShipsternConfig};
use shipstern::filter_pipeline::FilterPipeline;
use shipstern::{CommitmentLevel, Runtime};
use shipstern_core::Prefilter;
use shipstern_slot_parser::SlotParser;
use shipstern_yellowstone_grpc_source::{YellowstoneGrpcConfig, YellowstoneGrpcSource};
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sentinel::refresh::spawn_refresh_loops;
use solana_sentinel::{
    BootstrapCtx, Dispatcher, FilterHandle, InstallTarget, PipelineSink, StreamSource,
    ValidatedProgram,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{error, info, warn};

pub struct ConsumerService {
    config: Arc<SentinelConfig>,
    programs: Vec<ValidatedProgram>,
    alerts_tx: mpsc::Sender<Alert>,
    cancel_token: CancellationToken,
    rpc: Arc<RpcClient>,
    slot_tracker: SlotTracker,
}

impl ConsumerService {
    pub fn new(
        config: &Arc<SentinelConfig>,
        programs: Vec<ValidatedProgram>,
        alerts_tx: mpsc::Sender<Alert>,
        cancel_token: CancellationToken,
        rpc: Arc<RpcClient>,
        slot_tracker: SlotTracker,
    ) -> Self {
        Self {
            config: Arc::clone(config),
            programs,
            alerts_tx,
            cancel_token,
            rpc,
            slot_tracker,
        }
    }

    pub async fn start(self, tracker: &TaskTracker) -> Result<()> {
        info!("Starting consumer service");

        let Self {
            config,
            programs,
            alerts_tx,
            cancel_token,
            rpc,
            slot_tracker,
        } = self;

        if programs.is_empty() {
            warn!("No programs enabled; service will only keep the source connection alive");
        } else {
            info!(
                programs = ?programs.iter().map(|p| p.program().as_str()).collect::<Vec<_>>(),
                "Program listeners enabled"
            );
        }

        let ctx = BootstrapCtx {
            rpc: Arc::clone(&rpc),
            grpc_slot: slot_tracker.reader(),
        };

        // Programs are independent; their RPC warm-ups overlap.
        let start = Instant::now();
        let bootstrapped = try_join_all(programs.into_iter().map(|p| p.bootstrap(&ctx))).await?;
        info!("Finished bootstrapping programs in {:.2?}", start.elapsed());

        // Shipstern runs its own dispatch; every other source feeds the framework dispatcher.
        let mut sink = match config.geyser_source() {
            GeyserSourceConfig::Yellowstone(_) => {
                PipelineSink::Shipstern(Runtime::<YellowstoneGrpcSource>::builder())
            }
            GeyserSourceConfig::Grpc(_) | GeyserSourceConfig::Websocket(_) => {
                PipelineSink::Dispatcher(Dispatcher::new())
            }
        };
        // Slot pipeline first: tracks the latest slot for observability and slot-lag rules.
        sink.slot(FilterPipeline::new(
            SlotParser,
            [slot_tracker.clone()],
            Prefilter::builder().slots(),
        ));

        let mut target = InstallTarget::new(
            sink,
            Arc::clone(&rpc),
            alerts_tx,
            cancel_token.clone(),
            tracker,
        );
        let installed: Vec<_> = bootstrapped
            .into_iter()
            .map(|p| p.install(&mut target))
            .collect();
        let (sink, grpc_pipelines) = target.finish();
        info!(grpc_pipelines, "Stream pipelines installed");

        let from_slot = Some(slot_tracker.load_last_seen_grpc_slot()).filter(|s| *s > 0);
        let handle = match (sink, config.geyser_source()) {
            (PipelineSink::Shipstern(builder), GeyserSourceConfig::Yellowstone(yc)) => {
                let runtime = builder.build(yellowstone_runtime_config(yc.clone(), from_slot));
                // Keep the handle so programs can push refreshed prefilters while the stream runs.
                let handle = FilterHandle::Shipstern(runtime.handle());
                spawn_runtime(runtime, cancel_token.clone(), tracker);
                handle
            }
            (PipelineSink::Dispatcher(dispatcher), source_cfg) => {
                let (source, channel_size): (Box<dyn StreamSource>, usize) = match source_cfg {
                    GeyserSourceConfig::Grpc(g) => (
                        Box::new(GrpcSource {
                            endpoint: g.endpoint.clone(),
                            x_token: g.token.clone(),
                            timeout: Duration::from_secs(g.timeout_seconds),
                            from_slot,
                        }),
                        g.sources_channel_size,
                    ),
                    GeyserSourceConfig::Websocket(w) => (
                        Box::new(WebsocketSource::new(
                            w.url.clone(),
                            Arc::clone(&rpc),
                            &w.commitment,
                        )?),
                        w.sources_channel_size,
                    ),
                    GeyserSourceConfig::Yellowstone(_) => unreachable!("sink chosen from source"),
                };
                spawn_dispatched_source(source, dispatcher, channel_size, &cancel_token, tracker)
            }
            (PipelineSink::Shipstern(_), _) => unreachable!("sink chosen from source"),
        };

        spawn_refresh_loops(installed, &handle, &ctx, &cancel_token, tracker);

        Ok(())
    }
}

/// Run a [`StreamSource`] into the dispatcher; either side stopping shuts the service down.
fn spawn_dispatched_source(
    source: Box<dyn StreamSource>,
    dispatcher: Dispatcher,
    channel_size: usize,
    cancel_token: &CancellationToken,
    tracker: &TaskTracker,
) -> FilterHandle {
    let filters = dispatcher.filters();
    info!(filters = ?filters.parser_ids().collect::<Vec<_>>(), "Subscribing through the framework dispatcher");
    let (filters_tx, filters_rx) = watch::channel(filters.clone());
    let (updates_tx, updates_rx) = mpsc::channel(channel_size);

    Arc::new(dispatcher).spawn(updates_rx, cancel_token.clone(), tracker);

    let cancel = cancel_token.clone();
    tracker.spawn(async move {
        match source
            .run(filters, filters_rx, updates_tx, cancel.clone())
            .await
        {
            Ok(()) => info!("Stream source stopped; shutting down service"),
            Err(err) => error!(error = %err, "Stream source failed; shutting down service"),
        }
        cancel.cancel();
    });

    FilterHandle::Watch(filters_tx)
}

fn yellowstone_runtime_config(
    config: YellowstoneConfig,
    from_slot: Option<u64>,
) -> ShipsternConfig<YellowstoneGrpcConfig> {
    ShipsternConfig {
        source: YellowstoneGrpcConfig {
            endpoint: config.endpoint,
            x_token: config.token,
            timeout: config.timeout_seconds,
            // Specs request fastest path short of shred-level monitoring.
            commitment_level: Some(CommitmentLevel::Processed),
            from_slot,
            accounts_data_slice: Vec::new(),
            max_decoding_message_size: None,
            accept_compression: None,
            auto_reconnect: false,
            reconnect_max_retries: None,
            reconnect_slot_retention: None,
        },
        buffer: BufferConfig {
            jobs: Some(config.jobs),
            sources_channel_size: config.sources_channel_size,
        },
    }
}

/// Run the shipstern runtime on the tracker; when it stops for any reason the service shuts down.
fn spawn_runtime(
    runtime: Runtime<YellowstoneGrpcSource>,
    cancel_token: CancellationToken,
    tracker: &TaskTracker,
) {
    tracker.spawn(async move {
        tokio::select! {
            _ = cancel_token.cancelled() => info!("Cancellation requested; stopping consumer runtime"),
            result = runtime.try_run_async() => match result {
                Ok(()) => {
                    info!("Consumer runtime stopped; shutting down service");
                    cancel_token.cancel();
                }
                Err(err) => {
                    error!(error = %err, "Consumer runtime failed; shutting down service");
                    cancel_token.cancel();
                }
            }
        }
    });
}
