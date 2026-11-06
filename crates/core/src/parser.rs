//! Parser wrappers the framework installs around a program's parser: a
//! program-prefixed id (no cross-program collisions) and, for instruction
//! pipelines, a local transaction-accounts gate. Instruction parsers share one
//! bundled subscription, so without the gate a parser sees instructions from
//! transactions another program subscribed to.

use crate::rule::StreamCtx;
use crate::subscription::{OwnedAccounts, Subscription};
use domain::programs::Program;
use observability::prometheus::metrics::GrpcEvent;
use shipstern::handler::BoxPipeline;
use shipstern_core::instruction::{AccountKeys, InstructionUpdate};
use shipstern_core::{
    AccountFilter, AccountUpdate, MemcmpData, ParseError, ParseResult, Parser, PrefilterBuilder,
    TransactionUpdate,
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

    /// Whether this update is part of a rule's own input (its static subscription), for
    /// per-rule liveness. Kinds without a local check count every update.
    fn matches_input(&self, matcher: &InputMatcher) -> bool {
        matcher.all
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

    fn matches_input(&self, matcher: &InputMatcher) -> bool {
        if matcher.all {
            return true;
        }
        let Some(account) = self.account.as_ref() else {
            return false;
        };
        if let Ok(pubkey) = Pubkey::try_from(account.pubkey.as_slice()) {
            if matcher.accounts.contains(&pubkey) {
                return true;
            }
        }
        let Ok(owner) = Pubkey::try_from(account.owner.as_slice()) else {
            return false;
        };
        matcher.owned.iter().any(|o| {
            o.owners.contains(&owner) && o.filters.iter().all(|f| filter_matches(f, account))
        })
    }
}

/// Best-effort local check of one server-side account filter; filters it cannot evaluate pass.
fn filter_matches(filter: &AccountFilter, account: &shipstern_core::AccountUpdateInfo) -> bool {
    match filter {
        AccountFilter::Memcmp { offset, data } => {
            let bytes = match data {
                MemcmpData::Bytes(b) => b.clone(),
                MemcmpData::Base58(s) => match shipstern_core::bs58::decode(s).into_vec() {
                    Ok(b) => b,
                    Err(_) => return true,
                },
                MemcmpData::Base64(_) => return true,
            };
            let start = usize::try_from(*offset).unwrap_or(usize::MAX);
            account
                .data
                .get(start..start.saturating_add(bytes.len()))
                .is_some_and(|window| window == bytes.as_slice())
        }
        AccountFilter::DataSize(size) => account.data.len() as u64 == *size,
        AccountFilter::TokenAccountState(_) | AccountFilter::Lamports(_) => true,
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

    fn matches_input(&self, matcher: &InputMatcher) -> bool {
        matcher.all || (matcher.watches_tx && matcher.tx.matches(&self.shared.accounts))
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
            Self::Instruction if subscription.watches_transactions() => {
                vec![subscription.shared_tx_prefilter()]
            }
            Self::Transaction if subscription.watches_transactions() => {
                vec![subscription.tx_prefilter()]
            }
            Self::Instruction | Self::Transaction | Self::Poll => Vec::new(),
        }
    }

    pub fn feed_kind(self) -> observability::liveness::FeedKind {
        use observability::liveness::FeedKind;
        match self {
            Self::Account => FeedKind::Account,
            Self::Instruction => FeedKind::Instruction,
            Self::Transaction => FeedKind::Transaction,
            Self::Poll => FeedKind::Poll,
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

/// One rule's own input inside its pipeline, from the rule's static subscription. A rule
/// with no static subscription (fed only by the program's dynamic one) counts every update.
#[derive(Debug, Clone, Default)]
pub struct InputMatcher {
    /// Count every update of the pipeline.
    pub all: bool,
    pub accounts: HashSet<Pubkey>,
    pub owned: Vec<OwnedAccounts>,
    watches_tx: bool,
    tx: TxGate,
}

impl InputMatcher {
    pub fn all() -> Self {
        Self {
            all: true,
            ..Self::default()
        }
    }

    pub fn from_subscription(subscription: &Subscription) -> Self {
        if subscription == &Subscription::default() {
            return Self::all();
        }
        Self {
            all: false,
            accounts: subscription.accounts.clone(),
            owned: subscription.owned.clone(),
            watches_tx: subscription.watches_transactions(),
            tx: TxGate::from_subscription(subscription),
        }
    }
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
        let mut prefilter = self.inner.prefilter();
        // Instruction prefilters are unioned into one wire filter, where a required set would
        // bind every other parser too. Codegen parsers require their program id; they self-filter.
        if P::Input::KIND == PipelineKind::Instruction {
            if let Some(tx) = prefilter.transaction.as_mut() {
                tx.accounts_required.clear();
            }
        }
        prefilter
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raw::RawInstructionParser;
    use shipstern_core::Prefilter;

    const SQUADS: Pubkey = Pubkey::new_from_array([1; 32]);
    const MULTISIG: Pubkey = Pubkey::new_from_array([2; 32]);
    const WALLET: Pubkey = Pubkey::new_from_array([3; 32]);

    /// Stands in for a codegen parser, which requires its own program id.
    #[derive(Debug, Clone)]
    struct RequiresProgram;

    impl Parser for RequiresProgram {
        type Input = InstructionUpdate;
        type Output = ();

        fn id(&self) -> Cow<'static, str> {
            Cow::Borrowed("requires_program")
        }

        fn prefilter(&self) -> shipstern_core::Prefilter {
            Prefilter::builder()
                .transaction_accounts([SQUADS])
                .build()
                .unwrap()
        }

        async fn parse(&self, _: &InstructionUpdate) -> ParseResult<()> {
            Ok(())
        }
    }

    // Mirrors FilterPipeline::prefilter for one instruction pipeline.
    fn pipeline_prefilter<P>(p: P, sub: &Subscription) -> Prefilter
    where
        P: Parser<Input = InstructionUpdate> + Send + Sync,
        P::Output: Send,
    {
        let mut prefilter = FrameworkParser::new(p, "id".into(), None).prefilter();
        for b in PipelineKind::Instruction.prefilters(sub) {
            prefilter.merge(b.build().unwrap());
        }
        prefilter
    }

    /// Shipstern unions every instruction prefilter into one wire filter; a required key
    /// from one program must not narrow what another program's instruction rules receive.
    #[test]
    fn bundled_instruction_filter_carries_no_required_keys() {
        let mut bundled = pipeline_prefilter(
            RequiresProgram,
            &Subscription::new().tx_accounts_include([MULTISIG]),
        );
        bundled.merge(pipeline_prefilter(
            RawInstructionParser,
            &Subscription::new().tx_accounts_required([WALLET]),
        ));

        let tx = bundled.transaction.unwrap();
        assert!(tx.accounts_required.is_empty());
        let include: HashSet<Pubkey> = tx
            .accounts_include
            .iter()
            .map(|k| Pubkey::new_from_array(k.0))
            .collect();
        assert_eq!(include, HashSet::from([MULTISIG, WALLET]));
    }
}
