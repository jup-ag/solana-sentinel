//! End-to-end harness: the real program start, pipelines, alert tap, cooldown gate,
//! batching and canary watchdog, fed by a scripted in-process source and delivering to
//! in-memory sinks. Enable with the `testing` feature.
//!
//! ```rust,ignore
//! let mut h = Harness::start(rules, &[my_program::spec, health::spec]).await?;
//! h.send(FakeUpdate::account(pubkey, owner, data, slot)).await;
//! h.stall();                                  // the source stays connected, sends nothing
//! tokio::time::sleep(Duration::from_secs(90)).await;
//! assert!(h.sink("pager").incidents().iter().any(|a| a.metadata["check"] == "feed_silent"));
//! ```
//!
//! No Redis, RPC or network: the fleet gate is in-memory, the local gate is off, the RPC
//! client points nowhere (programs that need RPC at bootstrap will fail to start), and the
//! gRPC slot reader is detached (no stream-stall check). Time is tokio's, so a test with
//! `#[tokio::test(start_paused = true)]` runs minutes of silence instantly.

use crate::config::map_programs_with_rules;
use crate::config::rules::{RuleConfig, RulesConfig};
use crate::validate::validate_program_rules;
use crate::wiring::{build_routes, canary_routes};
use alerting::canary_watchdog::spawn_canary_watchdog;
use alerting::cooldown_gate::CooldownGate;
use alerting::fleet_gate::FleetGate;
use alerting::router::Router;
use alerting::service::AlertingService;
use alerting::sinks::{AlertSink, AlertSinkError};
use anyhow::Context;
use async_trait::async_trait;
use domain::alert::Alert;
use domain::sinks::SinkId;
use futures::future::try_join_all;
use parking_lot::Mutex;
use shipstern::config::BufferConfig;
use shipstern::sources::{SourceContext, SourceExitStatus, SourceTrait};
use shipstern::Runtime;
use solana_pubkey::Pubkey;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sentinel::reactor::spawn_alert_tap;
use solana_sentinel::{
    BootstrapCtx, BuiltReactor, FeedKind, GrpcSlotReader, Liveness, ProgramSpec, SourceHealth,
    StartCtx, StreamPipeline,
};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::geyser::{
    SubscribeUpdate, SubscribeUpdateAccount, SubscribeUpdateAccountInfo,
};

/// The node id the harness runs as.
pub const HARNESS_NODE: &str = "harness-node";

// ---- sinks ------------------------------------------------------------------

#[derive(Debug, Default)]
struct MemoryState {
    alerts: Mutex<Vec<Alert>>,
    failing: AtomicBool,
}

/// What an in-memory sink accepted; switch it to failing to simulate a dead sink.
#[derive(Debug, Clone, Default)]
pub struct MemorySinkHandle(Arc<MemoryState>);

impl MemorySinkHandle {
    /// Every alert accepted, canaries included, in delivery order.
    pub fn alerts(&self) -> Vec<Alert> {
        self.0.alerts.lock().clone()
    }

    pub fn canaries(&self) -> Vec<Alert> {
        self.alerts().into_iter().filter(Alert::is_canary).collect()
    }

    /// Accepted alerts that are not canaries.
    pub fn incidents(&self) -> Vec<Alert> {
        self.alerts()
            .into_iter()
            .filter(|a| !a.is_canary())
            .collect()
    }

    /// While failing, every delivery errors (and is retried by the alerting service).
    pub fn set_failing(&self, failing: bool) {
        self.0.failing.store(failing, Ordering::SeqCst);
    }
}

/// An [`AlertSink`] that keeps what it accepts.
pub struct MemorySink {
    id: SinkId,
    state: MemorySinkHandle,
}

impl MemorySink {
    pub fn new(id: SinkId) -> (Self, MemorySinkHandle) {
        let state = MemorySinkHandle::default();
        (
            Self {
                id,
                state: state.clone(),
            },
            state,
        )
    }
}

#[async_trait]
impl AlertSink for MemorySink {
    fn id(&self) -> &SinkId {
        &self.id
    }

    async fn send(&self, alert: &Alert) -> Result<(), AlertSinkError> {
        if self.state.0.failing.load(Ordering::SeqCst) {
            return Err(AlertSinkError::BetterStackSinkError(format!(
                "memory sink `{}` is failing",
                self.id
            )));
        }
        self.state.0.alerts.lock().push(alert.clone());
        Ok(())
    }

    async fn send_batch(&self, alerts: &[Alert]) -> Result<(), AlertSinkError> {
        for alert in alerts {
            self.send(alert).await?;
        }
        Ok(())
    }
}

// ---- source -----------------------------------------------------------------

/// One update the scripted source delivers.
#[derive(Debug, Clone)]
pub enum FakeUpdate {
    Account {
        pubkey: [u8; 32],
        owner: [u8; 32],
        data: Vec<u8>,
        slot: u64,
    },
}

impl FakeUpdate {
    pub fn account(pubkey: Pubkey, owner: Pubkey, data: Vec<u8>, slot: u64) -> Self {
        Self::Account {
            pubkey: pubkey.to_bytes(),
            owner: owner.to_bytes(),
            data,
            slot,
        }
    }
}

/// A shipstern source fed by a channel. Account updates are tagged with every account
/// filter (the parsers' local gates still apply). When the channel closes the source stays
/// connected and silent, like a stalled stream, until the runtime stops.
#[derive(Debug)]
pub struct FakeSource {
    rx: Mutex<Option<mpsc::Receiver<FakeUpdate>>>,
}

impl FakeSource {
    pub fn new() -> (Self, mpsc::Sender<FakeUpdate>) {
        let (tx, rx) = mpsc::channel(1024);
        (
            Self {
                rx: Mutex::new(Some(rx)),
            },
            tx,
        )
    }
}

#[async_trait]
impl SourceTrait for FakeSource {
    async fn connect(&self, ctx: SourceContext) -> Result<SourceExitStatus, shipstern::Error> {
        let SourceContext { filters, tx, .. } = ctx;
        let account_filters: Vec<String> = filters
            .parsers_filters
            .iter()
            .filter(|(_, p)| p.account.is_some())
            .map(|(id, _)| id.clone())
            .collect();
        let Some(mut rx) = self.rx.lock().take() else {
            return Ok(SourceExitStatus::Error(
                "fake source connected twice".into(),
            ));
        };
        while let Some(update) = rx.recv().await {
            let FakeUpdate::Account {
                pubkey,
                owner,
                data,
                slot,
            } = update;
            let update = SubscribeUpdate {
                filters: account_filters.clone(),
                update_oneof: Some(UpdateOneof::Account(SubscribeUpdateAccount {
                    account: Some(SubscribeUpdateAccountInfo {
                        pubkey: pubkey.to_vec(),
                        lamports: 1,
                        owner: owner.to_vec(),
                        executable: false,
                        rent_epoch: 0,
                        data,
                        write_version: slot,
                        txn_signature: Some(vec![slot as u8; 64]),
                    }),
                    slot,
                    is_startup: false,
                })),
                created_at: None,
            };
            if tx.send(Ok(update)).await.is_err() {
                return Ok(SourceExitStatus::ReceiverDropped);
            }
        }
        // Stalled: connected, silent.
        tx.closed().await;
        Ok(SourceExitStatus::ReceiverDropped)
    }
}

// ---- harness ----------------------------------------------------------------

/// Timings of the harness's own loops.
#[derive(Debug, Clone, Copy)]
pub struct HarnessOptions {
    pub flush_interval: Duration,
    pub watchdog_interval: Duration,
}

impl Default for HarnessOptions {
    fn default() -> Self {
        Self {
            flush_interval: Duration::from_millis(500),
            watchdog_interval: Duration::from_secs(5),
        }
    }
}

/// A running service minus the network: see the module docs.
pub struct Harness {
    pub liveness: Liveness,
    sinks: HashMap<SinkId, MemorySinkHandle>,
    source: Option<mpsc::Sender<FakeUpdate>>,
    // Held so the rule → tap channel stays open, as the service does.
    _rules_tx: mpsc::Sender<Alert>,
    cancel: CancellationToken,
    tracker: TaskTracker,
}

/// Parse a rules file body (`[[rules]]` blocks).
pub fn rules_from_toml(toml: &str) -> anyhow::Result<Vec<RuleConfig>> {
    Ok(RulesConfig::parse_str(toml, "inline")?.0)
}

impl Harness {
    pub async fn start(
        rules: Vec<RuleConfig>,
        registry: &[fn() -> ProgramSpec],
    ) -> anyhow::Result<Self> {
        Self::start_with(rules, registry, HarnessOptions::default()).await
    }

    pub async fn start_with(
        rules: Vec<RuleConfig>,
        registry: &[fn() -> ProgramSpec],
        options: HarnessOptions,
    ) -> anyhow::Result<Self> {
        let programs_rules = map_programs_with_rules(rules);
        let validated = validate_program_rules(&programs_rules, registry)?;

        let mut handles = HashMap::new();
        let mut sinks: HashMap<SinkId, Box<dyn AlertSink>> = HashMap::new();
        for sink in programs_rules
            .iter()
            .flat_map(|p| p.rules.iter().flat_map(|r| r.sinks.iter()))
        {
            if sinks.contains_key(sink) {
                continue;
            }
            let (memory, handle) = MemorySink::new(sink.clone());
            sinks.insert(sink.clone(), Box::new(memory));
            handles.insert(sink.clone(), handle);
        }
        let (routes, cooldowns) = build_routes(&programs_rules, &validated, &sinks)?;
        let router = Router::new(routes, sinks);

        let liveness = Liveness::new();
        liveness.canary().set_routes(canary_routes(&programs_rules));
        let gate = CooldownGate::new(None, Some(FleetGate::in_memory(cooldowns)));

        let cancel = CancellationToken::new();
        let tracker = TaskTracker::new();
        let (rules_tx, rules_rx) = mpsc::channel(1024);
        let (alerts_tx, alerts_rx) = mpsc::channel(1024);

        let ctx = StartCtx {
            bootstrap: BootstrapCtx {
                rpc: Arc::new(RpcClient::new("http://127.0.0.1:9".to_string())),
                grpc_slot: GrpcSlotReader::detached(),
                pod_id: HARNESS_NODE.to_string(),
                liveness: liveness.clone(),
                source_health: SourceHealth::new(),
            },
            alerts_tx: rules_tx.clone(),
            cancel_token: cancel.clone(),
            tracker: &tracker,
        };
        let mut started = try_join_all(validated.into_iter().map(|p| p.start(&ctx)))
            .await
            .context("programs failed to start")?;
        let reactors: Vec<BuiltReactor> = started
            .iter_mut()
            .flat_map(|p| std::mem::take(&mut p.reactors))
            .collect();

        let mut builder = Runtime::builder();
        let mut pipelines = 0;
        for program in &mut started {
            for pipeline in std::mem::take(&mut program.pipelines) {
                builder = match pipeline {
                    StreamPipeline::Account(p) => builder.account(p),
                    StreamPipeline::Instruction(p) => builder.instruction(p),
                    StreamPipeline::Transaction(p) => builder.transaction(p),
                };
                pipelines += 1;
            }
        }
        let (source, source_tx) = FakeSource::new();
        if pipelines > 0 {
            let runtime = builder
                .try_build_with(
                    source,
                    BufferConfig {
                        jobs: Some(1),
                        sources_channel_size: 1024,
                    },
                )
                .map_err(|e| anyhow::anyhow!("runtime build failed: {e:?}"))?;
            let cancel = cancel.clone();
            tracker.spawn(async move {
                tokio::select! {
                    () = cancel.cancelled() => {}
                    result = runtime.try_run_async() => {
                        if let Err(e) = result {
                            tracing::error!(error = %e, "harness runtime stopped");
                        }
                    }
                }
            });
        }

        spawn_canary_watchdog(
            liveness.clone(),
            alerts_tx.clone(),
            HARNESS_NODE.to_string(),
            options.watchdog_interval,
            cancel.clone(),
            &tracker,
        );
        spawn_alert_tap(reactors, rules_rx, alerts_tx, &tracker);
        AlertingService::new(alerts_rx, router, gate, cancel.clone())
            .with_flush_interval(options.flush_interval)
            .with_liveness(liveness.clone())
            .start(&tracker);

        Ok(Self {
            liveness,
            sinks: handles,
            source: Some(source_tx),
            _rules_tx: rules_tx,
            cancel,
            tracker,
        })
    }

    /// The in-memory sink every rule naming `id` routes to.
    pub fn sink(&self, id: &str) -> MemorySinkHandle {
        self.sinks
            .get(&SinkId::new(id))
            .cloned()
            .unwrap_or_else(|| panic!("no sink `{id}` is named by the rules"))
    }

    /// Deliver one update through the source and wait until a stream pipeline recorded it
    /// (so a paused clock cannot run ahead of the runtime); panics after [`Self::stall`].
    pub async fn send(&self, update: FakeUpdate) {
        let before = self.stream_events();
        self.source
            .as_ref()
            .expect("source stalled")
            .send(update)
            .await
            .expect("runtime gone");
        for _ in 0..1_000_000 {
            if self.stream_events() > before {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("the update never reached a pipeline");
    }

    /// Events recorded on every stream pipeline feed.
    fn stream_events(&self) -> u64 {
        self.liveness
            .snapshot(tokio::time::Instant::now())
            .iter()
            .filter(|f| {
                matches!(
                    f.kind,
                    FeedKind::Account | FeedKind::Instruction | FeedKind::Transaction
                )
            })
            .map(|f| f.events)
            .sum()
    }

    /// From now on the source sends nothing, but stays connected.
    pub fn stall(&mut self) {
        self.source = None;
    }

    /// Cancel everything and wait (bounded) for the tasks to drain.
    pub async fn shutdown(self) {
        let Self {
            cancel,
            tracker,
            source,
            _rules_tx,
            ..
        } = self;
        // The tap runs until every rule sender is gone; the harness holds one.
        drop(_rules_tx);
        drop(source);
        cancel.cancel();
        tracker.close();
        if tokio::time::timeout(Duration::from_secs(60), tracker.wait())
            .await
            .is_err()
        {
            tracing::warn!("harness tasks still running after shutdown");
        }
    }
}
