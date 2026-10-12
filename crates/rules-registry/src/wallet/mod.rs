//! Not an on-chain program: watches wallets (multisig members, deployer keys)
//! and program-agnostic instruction shapes over the raw feeds.

pub mod anchor_idl_instruction;
pub mod unapproved_program_invoke;

use solana_sentinel::raw::{RawInstructionParser, RawTransactionParser};
use solana_sentinel::{Program, ProgramSpec, StreamRules};

pub const PROGRAM: Program = Program::new("wallet");

pub fn spec() -> ProgramSpec {
    ProgramSpec::new(PROGRAM)
        .instruction(
            RawInstructionParser,
            StreamRules::new().register::<anchor_idl_instruction::AnchorIdlInstruction>(),
        )
        .transaction(
            RawTransactionParser,
            StreamRules::new().register::<unapproved_program_invoke::UnapprovedProgramInvoke>(),
        )
}
