//! System program: SOL balances, SOL transfers and durable-nonce activity,
//! all over the framework's raw parsers so no IDL is needed.

pub mod instruction;
pub mod lamports_below;
pub mod large_sol_transfer;
pub mod nonce_activity;

use solana_pubkey::Pubkey;
use solana_sentinel::raw::{RawAccountParser, RawInstructionParser};
use solana_sentinel::{Program, ProgramSpec, StreamRules};

pub const PROGRAM: Program = Program::new("system");
pub const SYSTEM_PROGRAM_ID: Pubkey = Pubkey::from_str_const("11111111111111111111111111111111");

pub fn spec() -> ProgramSpec {
    ProgramSpec::new(PROGRAM)
        .account(
            RawAccountParser,
            StreamRules::new().register::<lamports_below::LamportsBelow>(),
        )
        .instruction(
            RawInstructionParser,
            StreamRules::new()
                .register::<large_sol_transfer::LargeSolTransfer>()
                .register::<nonce_activity::NonceActivity>(),
        )
}
