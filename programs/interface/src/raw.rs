//! Program-agnostic parsers for rules that inspect raw instruction bytes or a
//! whole transaction (member-wallet sentinels, Anchor IDL admin calls, …).

use shipstern_core::instruction::{InstructionShared, InstructionUpdate};
use shipstern_core::{ParseError, ParseResult, Parser, Prefilter, TransactionUpdate};
use solana_pubkey::Pubkey;
use std::borrow::Cow;
use std::sync::Arc;

/// One instruction, any program, undecoded.
#[derive(Debug, Clone)]
pub struct RawInstruction {
    pub program_id: Pubkey,
    pub accounts: Vec<Pubkey>,
    pub data: Vec<u8>,
    /// Per-transaction context (account keys, header, signers, logs) shared by every instruction.
    pub shared: Arc<InstructionShared>,
}

#[derive(Debug, Clone, Default)]
pub struct RawInstructionParser;

impl Parser for RawInstructionParser {
    type Input = InstructionUpdate;
    type Output = RawInstruction;

    fn id(&self) -> Cow<'static, str> {
        Cow::Borrowed("raw_instruction")
    }

    fn prefilter(&self) -> Prefilter {
        Prefilter::default()
    }

    async fn parse(&self, input: &Self::Input) -> ParseResult<Self::Output> {
        Ok(RawInstruction {
            program_id: Pubkey::new_from_array(input.program.0),
            accounts: input
                .accounts
                .iter()
                .map(|pk| Pubkey::new_from_array(pk.0))
                .collect(),
            data: input.data.clone(),
            shared: Arc::clone(&input.shared),
        })
    }
}

/// One instruction in a transaction's flattened CPI tree.
#[derive(Debug, Clone)]
pub struct RawIxPayload {
    pub program_id: Pubkey,
    pub data: Vec<u8>,
    /// The program that invoked this instruction via CPI; `None` for top-level instructions.
    pub invoked_by: Option<Pubkey>,
}

/// A whole transaction: shared context plus its instructions in tree-walk order.
#[derive(Debug, Clone)]
pub struct RawTransaction {
    pub shared: Arc<InstructionShared>,
    pub ixs: Vec<RawIxPayload>,
}

#[derive(Debug, Clone, Default)]
pub struct RawTransactionParser;

impl Parser for RawTransactionParser {
    type Input = TransactionUpdate;
    type Output = RawTransaction;

    fn id(&self) -> Cow<'static, str> {
        Cow::Borrowed("raw_transaction")
    }

    fn prefilter(&self) -> Prefilter {
        Prefilter::default()
    }

    async fn parse(&self, input: &Self::Input) -> ParseResult<Self::Output> {
        let outer_ixs = InstructionUpdate::build_from_txn(input)
            .map_err(|e| ParseError::Other(format!("build_from_txn: {e}").into()))?;

        let shared = outer_ixs
            .first()
            .map(|ix| Arc::clone(&ix.shared))
            .ok_or_else(|| ParseError::Other("tx has no instructions".into()))?;

        let mut ixs = Vec::new();
        for ix in &outer_ixs {
            collect_ixs(ix, None, &mut ixs);
        }

        Ok(RawTransaction { shared, ixs })
    }
}

/// Depth-first walk of an instruction and all its CPI descendants.
fn collect_ixs(ix: &InstructionUpdate, invoked_by: Option<Pubkey>, out: &mut Vec<RawIxPayload>) {
    let program_id = Pubkey::new_from_array(ix.program.0);
    out.push(RawIxPayload {
        program_id,
        data: ix.data.clone(),
        invoked_by,
    });
    for inner in &ix.inner {
        collect_ixs(inner, Some(program_id), out);
    }
}

/// One account update, any owner, undecoded.
#[derive(Debug, Clone)]
pub struct RawAccount {
    pub pubkey: Pubkey,
    pub owner: Pubkey,
    pub lamports: u64,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, Default)]
pub struct RawAccountParser;

impl Parser for RawAccountParser {
    type Input = shipstern_core::AccountUpdate;
    type Output = RawAccount;

    fn id(&self) -> Cow<'static, str> {
        Cow::Borrowed("raw_account")
    }

    fn prefilter(&self) -> Prefilter {
        Prefilter::default()
    }

    async fn parse(&self, input: &Self::Input) -> ParseResult<Self::Output> {
        let acc = input.account.as_ref().ok_or(ParseError::Filtered)?;
        Ok(RawAccount {
            pubkey: Pubkey::try_from(acc.pubkey.as_slice()).map_err(|_| ParseError::Filtered)?,
            owner: Pubkey::try_from(acc.owner.as_slice()).map_err(|_| ParseError::Filtered)?,
            lamports: acc.lamports,
            data: acc.data.clone(),
        })
    }
}
