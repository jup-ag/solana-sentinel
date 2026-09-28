//! Sentinel is not an on-chain program: it watches multisig member wallets and
//! program-agnostic instruction shapes over the raw instruction / transaction feeds.

pub mod rules;

use interface::raw::{RawInstructionParser, RawTransactionParser};
use interface::{Program, ProgramSpec, StreamRules};
use rules::anchor_idl_instruction::AnchorIdlInstruction;
use rules::multisig_member_nonce_account_interaction::MultisigMemberNonceAccInteraction;
use rules::multisig_member_unapproved_program_invoke::MultisigMemberUnapprovedProgramInvoke;

/// Snake_case identifier this program is known by in config and logs.
pub const PROGRAM: Program = Program::new("sentinel");

pub fn spec() -> ProgramSpec {
    ProgramSpec::builder(PROGRAM)
        .instruction(
            RawInstructionParser,
            StreamRules::new()
                .register::<MultisigMemberNonceAccInteraction>()
                .register::<AnchorIdlInstruction>(),
        )
        .transaction(
            RawTransactionParser,
            StreamRules::new().register::<MultisigMemberUnapprovedProgramInvoke>(),
        )
        .finish()
}
