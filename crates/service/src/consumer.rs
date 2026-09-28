//! Steps 4–6 of startup: bootstrap every validated program concurrently, install
//! their pipelines into one shipstern runtime plus poll / sweep loops, run it.

use crate::config::service::{BufferSettings, DataSourceConfig};
use crate::config::SentinelConfig;
use anyhow::Result;
use data_source::{
    KafkaConfig, KafkaSource, WebsocketConfig, WebsocketSource, YellowstoneGrpcConfig,
    YellowstoneGrpcSource,
};
use domain::alert::Alert;
use futures::future::try_join_all;
use observability::slot_tracker::SlotTracker;
use shipstern::config::{BufferConfig, ShipsternConfig};
use shipstern::filter_pipeline::FilterPipeline;
use shipstern::sources::FilterUpdateSource;
use shipstern::{CommitmentLevel, Runtime};
use shipstern_core::Prefilter;
use shipstern_slot_parser::SlotParser;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sentinel::refresh::spawn_refresh_loops;
use solana_sentinel::{
    BootstrapCtx, BootstrappedProgram, InstallTarget, PipelineSink, ValidatedProgram,
};
use std::sync::Arc;
use tokio::sync::mpsc;
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

        let launch = Launch {
            bootstrapped,
            ctx,
            rpc,
            alerts_tx,
            slot_tracker,
            cancel_token,
            tracker,
        };
        let from_slot = Some(launch.slot_tracker.load_last_seen_grpc_slot()).filter(|s| *s > 0);

        // The runtime is generic over its source, so the config variant picks the type here.
        match config.data_source() {
            DataSourceConfig::YellowstoneGrpc(c) => launch.run::<YellowstoneGrpcSource>(
                YellowstoneGrpcConfig {
                    endpoint: c.endpoint.clone(),
                    x_token: c.token.clone(),
                    timeout: c.timeout_seconds,
                    // Fastest path short of shred-level monitoring.
                    commitment_level: Some(CommitmentLevel::Processed),
                    from_slot,
                    accounts_data_slice: Vec::new(),
                    max_decoding_message_size: None,
                    accept_compression: None,
                    auto_reconnect: c.auto_reconnect,
                    reconnect_max_retries: None,
                    reconnect_slot_retention: None,
                },
                &c.buffer,
            ),
            DataSourceConfig::Websocket(c) => launch.run::<WebsocketSource>(
                WebsocketConfig {
                    url: c.url.clone(),
                    rpc_url: config.rpc().url.to_string(),
                    commitment: c.commitment.clone(),
                },
                &c.buffer,
            ),
            DataSourceConfig::Kafka(c) => launch.run::<KafkaSource>(
                KafkaConfig {
                    brokers: c.brokers.clone(),
                    group_id: c.group_id.clone(),
                    topics: c.topics.clone(),
                    auto_offset_reset: c.auto_offset_reset.clone(),
                    properties: c.properties.clone(),
                },
                &c.buffer,
            ),
        }
        Ok(())
    }
}

/// Everything needed to build and run the runtime once the source type is known.
struct Launch<'a> {
    bootstrapped: Vec<BootstrappedProgram>,
    ctx: BootstrapCtx,
    rpc: Arc<RpcClient>,
    alerts_tx: mpsc::Sender<Alert>,
    slot_tracker: SlotTracker,
    cancel_token: CancellationToken,
    tracker: &'a TaskTracker,
}

impl Launch<'_> {
    fn run<S: FilterUpdateSource>(self, source: S::Config, buffer: &BufferSettings)
    where
        S::Config: Send,
    {
        let mut builder = Runtime::<S>::builder();
        // Slot pipeline first: tracks the latest slot for observability and slot-lag rules.
        builder.add_slot(Box::new(FilterPipeline::new(
            SlotParser,
            [self.slot_tracker.clone()],
            Prefilter::builder().slots(),
        )));

        let mut target = InstallTarget::new(
            &mut builder,
            Arc::clone(&self.rpc),
            self.alerts_tx,
            self.cancel_token.clone(),
            self.tracker,
        );
        let installed: Vec<_> = self
            .bootstrapped
            .into_iter()
            .map(|p| p.install(&mut target))
            .collect();
        let grpc_pipelines = target.finish();
        info!(grpc_pipelines, "Stream pipelines installed");

        let runtime = builder.build(ShipsternConfig {
            source,
            buffer: BufferConfig {
                // One worker: rules that compare against last-seen state need
                // updates for an account in stream order, which more workers break.
                jobs: Some(1),
                sources_channel_size: buffer.sources_channel_size,
            },
        });
        // Keep the handle so programs can push refreshed prefilters while the stream runs.
        let handle = runtime.handle();
        spawn_runtime(runtime, self.cancel_token.clone(), self.tracker);
        spawn_refresh_loops(
            installed,
            &handle,
            &self.ctx,
            &self.cancel_token,
            self.tracker,
        );
    }
}

/// Run the shipstern runtime on the tracker; when it stops for any reason the service shuts down.
fn spawn_runtime<S: FilterUpdateSource>(
    runtime: Runtime<S>,
    cancel_token: CancellationToken,
    tracker: &TaskTracker,
) where
    S::Config: Send,
{
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
