//! Parser wrappers the framework installs around a program's parser: a
//! program-prefixed id (no cross-program collisions) and, for instruction
//! pipelines, a local transaction-accounts gate. Shipstern bundles every
//! instruction parser behind one pipeline, so without the gate a parser sees
//! instructions from transactions another program subscribed to.

use crate::handler::{RawEvent, RuleHandler};
use crate::prefilter::Prefilter;
use crate::source::Dispatcher;
use domain::programs::Program;
use shipstern::builder::RuntimeBuilder;
use shipstern::filter_pipeline::FilterPipeline;
use shipstern::handler::DynPipeline;
use shipstern_core::instruction::{AccountKeys, InstructionUpdate};
use shipstern_core::{
    AccountUpdate, ParseError, ParseResult, Parser, PrefilterBuilder, SlotUpdate, TransactionUpdate,
};
use shipstern_yellowstone_grpc_source::YellowstoneGrpcSource;
use solana_pubkey::Pubkey;
use std::borrow::Cow;
use std::collections::HashSet;
use std::fmt::Debug;

pub type GrpcBuilder = RuntimeBuilder<YellowstoneGrpcSource>;

/// Where installed pipelines go: shipstern's own runtime, or the framework
/// dispatcher fed by any [`crate::source::StreamSource`].
pub enum PipelineSink {
    Shipstern(GrpcBuilder),
    Dispatcher(Dispatcher),
}

impl PipelineSink {
    /// The slot feed, always present: the tracker needs it for health and checkpoints.
    pub fn slot(&mut self, pipeline: impl DynPipeline<SlotUpdate> + Send + Sync + 'static) {
        match self {
            Self::Shipstern(builder) => {
                let b = std::mem::take(builder);
                *builder = b.slot(pipeline);
            }
            Self::Dispatcher(d) => d.add_slot(pipeline),
        }
    }
}

/// Which pipeline kind a raw update type belongs to, and how to install it.
pub trait StreamInput: RawEvent + Sized + Send + Sync + 'static {
    const KIND: PipelineKind;

    /// The yellowstone filters this prefilter expands to for this kind.
    fn subscriptions(prefilter: &Prefilter) -> Vec<PrefilterBuilder>;

    /// Whether the kind shares one bundled pipeline and so needs the local gate.
    fn needs_gate() -> bool {
        false
    }

    fn passes_gate(&self, _gate: &TxGate) -> bool {
        true
    }

    fn add_pipeline<P, Ev>(
        sink: &mut PipelineSink,
        parser: FrameworkParser<P>,
        handler: RuleHandler<Ev>,
        sub: PrefilterBuilder,
    ) where
        P: Parser<Input = Self, Output = Ev> + Debug + Send + Sync + 'static,
        Ev: Send + Sync + 'static;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PipelineKind {
    Account,
    Instruction,
    Transaction,
    Poll,
}

impl PipelineKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Account => "account",
            Self::Instruction => "instruction",
            Self::Transaction => "transaction",
            Self::Poll => "poll",
        }
    }
}

impl StreamInput for AccountUpdate {
    const KIND: PipelineKind = PipelineKind::Account;

    fn subscriptions(prefilter: &Prefilter) -> Vec<PrefilterBuilder> {
        prefilter.account_subscriptions()
    }

    fn add_pipeline<P, Ev>(
        sink: &mut PipelineSink,
        parser: FrameworkParser<P>,
        handler: RuleHandler<Ev>,
        sub: PrefilterBuilder,
    ) where
        P: Parser<Input = Self, Output = Ev> + Debug + Send + Sync + 'static,
        Ev: Send + Sync + 'static,
    {
        let pipeline = FilterPipeline::new(parser, [handler], sub);
        match sink {
            PipelineSink::Shipstern(builder) => {
                let b = std::mem::take(builder);
                *builder = b.account(pipeline);
            }
            PipelineSink::Dispatcher(d) => d.add_account(pipeline),
        }
    }
}

impl StreamInput for InstructionUpdate {
    const KIND: PipelineKind = PipelineKind::Instruction;

    fn subscriptions(prefilter: &Prefilter) -> Vec<PrefilterBuilder> {
        if prefilter.has_tx_subscription() {
            vec![prefilter.tx_subscription()]
        } else {
            Vec::new()
        }
    }

    fn needs_gate() -> bool {
        true
    }

    fn passes_gate(&self, gate: &TxGate) -> bool {
        gate.matches(&self.shared.accounts)
    }

    fn add_pipeline<P, Ev>(
        sink: &mut PipelineSink,
        parser: FrameworkParser<P>,
        handler: RuleHandler<Ev>,
        sub: PrefilterBuilder,
    ) where
        P: Parser<Input = Self, Output = Ev> + Debug + Send + Sync + 'static,
        Ev: Send + Sync + 'static,
    {
        let pipeline = FilterPipeline::new(parser, [handler], sub);
        match sink {
            PipelineSink::Shipstern(builder) => {
                let b = std::mem::take(builder);
                *builder = b.instruction(pipeline);
            }
            PipelineSink::Dispatcher(d) => d.add_instruction(pipeline),
        }
    }
}

impl StreamInput for TransactionUpdate {
    const KIND: PipelineKind = PipelineKind::Transaction;

    fn subscriptions(prefilter: &Prefilter) -> Vec<PrefilterBuilder> {
        if prefilter.has_tx_subscription() {
            vec![prefilter.tx_subscription()]
        } else {
            Vec::new()
        }
    }

    fn add_pipeline<P, Ev>(
        sink: &mut PipelineSink,
        parser: FrameworkParser<P>,
        handler: RuleHandler<Ev>,
        sub: PrefilterBuilder,
    ) where
        P: Parser<Input = Self, Output = Ev> + Debug + Send + Sync + 'static,
        Ev: Send + Sync + 'static,
    {
        let pipeline = FilterPipeline::new(parser, [handler], sub);
        match sink {
            PipelineSink::Shipstern(builder) => {
                let b = std::mem::take(builder);
                *builder = b.transaction(pipeline);
            }
            PipelineSink::Dispatcher(d) => d.add_transaction(pipeline),
        }
    }
}

/// The transaction-level part of a pipeline's prefilter, re-checked locally.
#[derive(Debug, Clone, Default)]
pub struct TxGate {
    include: HashSet<Pubkey>,
    required: HashSet<Pubkey>,
}

impl TxGate {
    pub fn from_prefilter(prefilter: &Prefilter) -> Self {
        Self {
            include: prefilter.tx_accounts_include.clone(),
            required: prefilter.tx_accounts_required.clone(),
        }
    }

    pub fn matches(&self, keys: &AccountKeys) -> bool {
        if self.include.is_empty() && self.required.is_empty() {
            return true;
        }
        let present: HashSet<Pubkey> = keys
            .static_keys
            .iter()
            .chain(&keys.dynamic_rw)
            .chain(&keys.dynamic_ro)
            .filter_map(|k| Pubkey::try_from(k.as_slice()).ok())
            .collect();
        let any_included =
            self.include.is_empty() || present.iter().any(|k| self.include.contains(k));
        let all_required = self.required.iter().all(|k| present.contains(k));
        any_included && all_required
    }
}

/// Program's parser with a prefixed id and an optional transaction gate.
#[derive(Debug, Clone)]
pub struct FrameworkParser<P> {
    inner: P,
    id: String,
    gate: Option<TxGate>,
}

impl<P> FrameworkParser<P> {
    pub fn new(inner: P, id: String, gate: Option<TxGate>) -> Self {
        Self { inner, id, gate }
    }
}

/// `"{program}:{parser_id}"`, with `:{n}` for the n-th extra subscription of one pipeline.
pub fn scoped_parser_id(program: Program, parser_id: &str, subscription: usize) -> String {
    if subscription == 0 {
        format!("{program}:{parser_id}")
    } else {
        format!("{program}:{parser_id}:{subscription}")
    }
}

impl<P> Parser for FrameworkParser<P>
where
    P: Parser + Send + Sync,
    P::Input: StreamInput,
    P::Output: Send,
{
    type Input = P::Input;
    type Output = P::Output;

    fn id(&self) -> Cow<'static, str> {
        Cow::Owned(self.id.clone())
    }

    fn prefilter(&self) -> shipstern_core::Prefilter {
        self.inner.prefilter()
    }

    async fn parse(&self, input: &Self::Input) -> ParseResult<Self::Output> {
        if let Some(gate) = &self.gate {
            if !input.passes_gate(gate) {
                return Err(ParseError::Filtered);
            }
        }
        self.inner.parse(input).await
    }
}
