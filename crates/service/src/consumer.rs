//! Steps 4–6 of startup: start every validated program concurrently (bootstrap,
//! build pipelines, spawn poll / sweep loops), load the stream pipelines into
//! one shipstern runtime, run it.

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
use shipstern::sources::{FilterUpdateSource, FromConfig};
use shipstern::{CommitmentLevel, Runtime};
use shipstern_core::Prefilter;
use shipstern_slot_parser::SlotParser;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sentinel::refresh::spawn_refresh_loops;
use solana_sentinel::{BootstrapCtx, StartCtx, StartedProgram, StreamPipeline, ValidatedProgram};
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

        let ctx = StartCtx {
            bootstrap: BootstrapCtx {
                rpc,
                grpc_slot: slot_tracker.reader(),
            },
            alerts_tx,
            cancel_token: cancel_token.clone(),
            tracker,
        };

        // Programs are independent; their RPC warm-ups overlap.
        let start = Instant::now();
        let started = try_join_all(programs.into_iter().map(|p| p.start(&ctx))).await?;
        info!("Finished starting programs in {:.2?}", start.elapsed());

        let launch = Launch {
            started,
            ctx: ctx.bootstrap,
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
    started: Vec<StartedProgram>,
    ctx: BootstrapCtx,
    slot_tracker: SlotTracker,
    cancel_token: CancellationToken,
    tracker: &'a TaskTracker,
}

impl Launch<'_> {
    fn run<S: FilterUpdateSource + FromConfig>(
        mut self,
        source: <S as FromConfig>::Config,
        buffer: &BufferSettings,
    ) {
        // Slot pipeline first: tracks the latest slot for observability and slot-lag rules.
        let mut builder = Runtime::builder().slot(FilterPipeline::new(
            SlotParser,
            [self.slot_tracker.clone()],
            Prefilter::builder().slots(),
        ));

        let mut grpc_pipelines = 0;
        for program in &mut self.started {
            for pipeline in std::mem::take(&mut program.pipelines) {
                builder = match pipeline {
                    StreamPipeline::Account(p) => builder.account(p),
                    StreamPipeline::Instruction(p) => builder.instruction(p),
                    StreamPipeline::Transaction(p) => builder.transaction(p),
                };
                grpc_pipelines += 1;
            }
        }
        info!(grpc_pipelines, "Stream pipelines installed");

        let runtime = builder.build::<S>(ShipsternConfig {
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
            self.started,
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
