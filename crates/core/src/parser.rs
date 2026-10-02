//! Parser wrappers the framework installs around a program's parser: a
//! program-prefixed id (no cross-program collisions) and, for instruction
//! pipelines, a local transaction-accounts gate. Instruction parsers share one
//! bundled subscription, so without the gate a parser sees instructions from
//! transactions another program subscribed to.

use crate::rule::StreamCtx;
use crate::subscription::Subscription;
use domain::programs::Program;
use observability::prometheus::metrics::GrpcEvent;
use shipstern::handler::BoxPipeline;
use shipstern_core::instruction::{AccountKeys, InstructionUpdate};
use shipstern_core::{
    AccountUpdate, ParseError, ParseResult, Parser, PrefilterBuilder, TransactionUpdate,
};
use solana_pubkey::Pubkey;
use std::borrow::Cow;
use std::collections::HashSet;
use utility::signature::bytes_to_signature_str;

/// A raw shipstern update the framework can build a [`StreamCtx`] from.
pub trait StreamInput: Send + Sync + 'static {
    const KIND: PipelineKind;

    fn stream_ctx(&self) -> StreamCtx;

    /// Instruction updates re-check the transaction accounts locally; the others pass.
    fn passes_gate(&self, _gate: &TxGate) -> bool {
        true
    }
}

impl StreamInput for AccountUpdate {
    const KIND: PipelineKind = PipelineKind::Account;

    fn stream_ctx(&self) -> StreamCtx {
        let account = self.account.as_ref();
        StreamCtx {
            slot: self.slot,
            tx_sig: account
                .and_then(|a| a.txn_signature.as_deref())
                .map(bytes_to_signature_str),
            account: account.and_then(|a| Pubkey::try_from(a.pubkey.as_slice()).ok()),
        }
    }
}

impl StreamInput for InstructionUpdate {
    const KIND: PipelineKind = PipelineKind::Instruction;

    fn stream_ctx(&self) -> StreamCtx {
        StreamCtx {
            slot: self.shared.slot,
            tx_sig: Some(bytes_to_signature_str(&self.shared.signature)),
            account: None,
        }
    }

    fn passes_gate(&self, gate: &TxGate) -> bool {
        gate.matches(&self.shared.accounts)
    }
}

impl StreamInput for TransactionUpdate {
    const KIND: PipelineKind = PipelineKind::Transaction;

    fn stream_ctx(&self) -> StreamCtx {
        StreamCtx {
            slot: self.slot,
            tx_sig: self
                .transaction
                .as_ref()
                .map(|t| bytes_to_signature_str(&t.signature)),
            account: None,
        }
    }
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

    /// The shipstern prefilters a subscription expands to for this kind.
    pub fn prefilters(self, subscription: &Subscription) -> Vec<PrefilterBuilder> {
        match self {
            Self::Account => subscription.account_prefilters(),
            Self::Instruction | Self::Transaction if subscription.watches_transactions() => {
                vec![subscription.tx_prefilter()]
            }
            Self::Instruction | Self::Transaction | Self::Poll => Vec::new(),
        }
    }

    pub(crate) fn metric(self) -> GrpcEvent {
        match self {
            Self::Account => GrpcEvent::AccountUpdate,
            Self::Instruction => GrpcEvent::InstructionUpdate,
            Self::Transaction => GrpcEvent::TransactionUpdate,
            // Poll feeds have no stream handler, so this is never asked.
            Self::Poll => unreachable!("poll pipelines record no stream metric"),
        }
    }
}

/// A stream pipeline ready for the runtime builder; the consumer adds it to
/// whichever source it configured.
pub enum StreamPipeline {
    Account(BoxPipeline<'static, AccountUpdate>),
    Instruction(BoxPipeline<'static, InstructionUpdate>),
    Transaction(BoxPipeline<'static, TransactionUpdate>),
}

/// The transaction-level part of a pipeline's subscription, re-checked locally.
#[derive(Debug, Clone, Default)]
pub struct TxGate {
    include: HashSet<Pubkey>,
    required: HashSet<Pubkey>,
}

impl TxGate {
    pub fn from_subscription(subscription: &Subscription) -> Self {
        Self {
            include: subscription.tx_accounts_include.clone(),
            required: subscription.tx_accounts_required.clone(),
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
