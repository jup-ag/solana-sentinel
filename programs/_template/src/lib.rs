//! Template program: one account rule and one instruction rule on the raw
//! parsers. Replace the parsers with your program's codama parser
//! (`include_shipstern_parser!`) and the rules with real ones.

pub mod rules;

use rules::account_lamports_below::AccountLamportsBelow;
use rules::program_invoked::ProgramInvoked;
use solana_sentinel::raw::{RawAccountParser, RawInstructionParser};
use solana_sentinel::{Program, ProgramSpec, StreamRules};

/// Snake_case identifier this program is known by in `rules.toml` and logs.
pub const PROGRAM: Program = Program::new("template");

pub fn spec() -> ProgramSpec {
    ProgramSpec::builder(PROGRAM)
        // One pipeline per feed; each `.register` line is one rule.
        .account(
            RawAccountParser,
            StreamRules::new().register::<AccountLamportsBelow>(),
        )
        .instruction(
            RawInstructionParser,
            StreamRules::new().register::<ProgramInvoked>(),
        )
        .finish()
}
