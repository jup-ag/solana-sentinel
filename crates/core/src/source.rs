//! Source-agnostic ingestion. A [`StreamSource`] is any transport that turns a
//! filter set into raw updates (Yellowstone gRPC, a websocket, a replay file);
//! the [`Dispatcher`] routes each update to the pipelines whose filter matched.
//! Shipstern remains the default path and does its own dispatch; everything
//! else goes through here.

use async_trait::async_trait;
use shipstern::handler::{BoxPipeline, DynPipeline};
use shipstern::instruction::InstructionPipeline;
use shipstern_core::instruction::InstructionUpdate;
use shipstern_core::{
    AccountUpdate, Filters, GetPrefilter, Prefilter, SlotUpdate, TransactionUpdate,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, error, info, warn};

/// Filter name under which every instruction pipeline's prefilter is unioned.
pub const INSTRUCTIONS_FILTER: &str = "instructions";
/// Filter name of the slot subscription.
pub const SLOTS_FILTER: &str = "slots";

#[derive(Debug)]
pub enum RawUpdate {
    Account(AccountUpdate),
    /// Boxed: a transaction is several times the size of the other variants.
    Transaction(Box<TransactionUpdate>),
    Slot(SlotUpdate),
}

/// One raw update plus the names of the filters it matched, as the server (or
/// the source itself) reports them.
#[derive(Debug)]
pub struct SourceUpdate {
    pub filters: Vec<String>,
    pub update: RawUpdate,
}

/// A transport that delivers raw updates for a named filter set.
///
/// `filters` keys are the names the [`Dispatcher`] routes on; a source must
/// tag every update with the names it matched. `filter_updates` carries later
/// sets (a program discovered new accounts); honouring it is optional.
#[async_trait]
pub trait StreamSource: Send + 'static {
    async fn run(
        self: Box<Self>,
        filters: Filters,
        filter_updates: watch::Receiver<Filters>,
        sink: mpsc::Sender<SourceUpdate>,
        cancel: CancellationToken,
    ) -> anyhow::Result<()>;
}

/// Routes raw updates to pipelines by filter name. Transactions also feed the
/// bundled instruction pipelines, building the CPI tree once per transaction.
#[derive(Default)]
pub struct Dispatcher {
    account: HashMap<String, BoxPipeline<'static, AccountUpdate>>,
    transaction: HashMap<String, BoxPipeline<'static, TransactionUpdate>>,
    instruction: Vec<BoxPipeline<'static, InstructionUpdate>>,
    slot: Vec<BoxPipeline<'static, SlotUpdate>>,
}

impl std::fmt::Debug for Dispatcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Dispatcher")
            .field("account", &self.account.keys().collect::<Vec<_>>())
            .field("transaction", &self.transaction.keys().collect::<Vec<_>>())
            .field("instruction", &self.instruction.len())
            .field("slot", &self.slot.len())
            .finish()
    }
}

impl Dispatcher {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_account(
        &mut self,
        pipeline: impl DynPipeline<AccountUpdate> + Send + Sync + 'static,
    ) {
        let id = pipeline.id().into_owned();
        if self
            .account
            .insert(id.clone(), Box::new(pipeline))
            .is_some()
        {
            warn!(parser = %id, "Duplicate account pipeline id; the later one wins");
        }
    }

    pub fn add_transaction(
        &mut self,
        pipeline: impl DynPipeline<TransactionUpdate> + Send + Sync + 'static,
    ) {
        let id = pipeline.id().into_owned();
        if self
            .transaction
            .insert(id.clone(), Box::new(pipeline))
            .is_some()
        {
            warn!(parser = %id, "Duplicate transaction pipeline id; the later one wins");
        }
    }

    pub fn add_instruction(
        &mut self,
        pipeline: impl DynPipeline<InstructionUpdate> + Send + Sync + 'static,
    ) {
        self.instruction.push(Box::new(pipeline));
    }

    pub fn add_slot(&mut self, pipeline: impl DynPipeline<SlotUpdate> + Send + Sync + 'static) {
        self.slot.push(Box::new(pipeline));
    }

    /// The named filter set a source subscribes with: one entry per account /
    /// transaction pipeline, the instruction union, and the slot feed.
    pub fn filters(&self) -> Filters {
        let mut map: HashMap<String, Prefilter> = HashMap::new();
        for (id, p) in &self.account {
            map.insert(id.clone(), p.prefilter());
        }
        for (id, p) in &self.transaction {
            map.insert(id.clone(), p.prefilter());
        }
        if !self.instruction.is_empty() {
            map.insert(
                INSTRUCTIONS_FILTER.to_string(),
                self.instruction
                    .iter()
                    .map(GetPrefilter::prefilter)
                    .collect(),
            );
        }
        if !self.slot.is_empty() {
            map.insert(
                SLOTS_FILTER.to_string(),
                Prefilter::builder()
                    .slots()
                    .build()
                    .expect("static slot prefilter"),
            );
        }
        Filters::new(map)
    }

    pub fn is_empty(&self) -> bool {
        self.account.is_empty() && self.transaction.is_empty() && self.instruction.is_empty()
    }

    pub async fn dispatch(&self, update: SourceUpdate) {
        let SourceUpdate { filters, update } = update;
        match update {
            RawUpdate::Account(acc) => {
                for name in &filters {
                    if let Some(p) = self.account.get(name) {
                        if let Err(e) = p.handle(&acc).await {
                            error!(pipeline = %name, error = ?e, "Account pipeline failed");
                        }
                    }
                }
            }
            RawUpdate::Transaction(tx) => {
                for name in &filters {
                    if let Some(p) = self.transaction.get(name) {
                        if let Err(e) = p.handle(&tx).await {
                            error!(pipeline = %name, error = ?e, "Transaction pipeline failed");
                        }
                    }
                }
                if !self.instruction.is_empty() && filters.iter().any(|f| f == INSTRUCTIONS_FILTER)
                {
                    self.dispatch_instructions(&tx).await;
                }
            }
            RawUpdate::Slot(slot) => {
                for p in &self.slot {
                    if let Err(e) = p.handle(&slot).await {
                        error!(error = ?e, "Slot pipeline failed");
                    }
                }
            }
        }
    }

    /// Same fan-out shipstern's bundled [`InstructionPipeline`] does: every
    /// instruction of the tree to every instruction pipeline, which self-filter.
    async fn dispatch_instructions(&self, tx: &TransactionUpdate) {
        let ixs = match InstructionUpdate::build_from_txn(tx) {
            Ok(ixs) => ixs,
            Err(e) => {
                debug!(error = %e, "Could not build instruction tree; skipping transaction");
                return;
            }
        };
        for ix in ixs.iter().flat_map(InstructionUpdate::visit_all) {
            for p in &self.instruction {
                if let Err(e) = p.handle(ix).await {
                    error!(pipeline = %p.id(), error = ?e, "Instruction pipeline failed");
                }
            }
        }
    }

    /// Drain `rx` until the source closes it or `cancel` fires.
    pub fn spawn(
        self: Arc<Self>,
        mut rx: mpsc::Receiver<SourceUpdate>,
        cancel: CancellationToken,
        tracker: &TaskTracker,
    ) {
        let _ = InstructionPipeline::ID; // the shipstern path keys instructions here; ours under INSTRUCTIONS_FILTER
        tracker.spawn(async move {
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    next = rx.recv() => match next {
                        Some(update) => self.dispatch(update).await,
                        None => {
                            info!("Source closed; dispatcher stopping");
                            break;
                        }
                    }
                }
            }
        });
    }
}
