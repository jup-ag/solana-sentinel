pub mod parser;
pub mod rules;

use crate::parser::TokenAccParser;
use interface::{Program, ProgramSpec, StreamRules};
use rules::token_supply_increase::TokenSupplyIncrease;
use solana_pubkey::{pubkey, Pubkey};

/// Snake_case identifier this program is known by in config and logs.
pub const PROGRAM: Program = Program::new("spl_token");

/// Mainnet pubkey for the SPL Token program.
pub const PUBKEY: Pubkey = pubkey!("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA");

pub fn spec() -> ProgramSpec {
    ProgramSpec::builder(PROGRAM)
        .account(
            TokenAccParser {},
            StreamRules::new().register::<TokenSupplyIncrease>(),
        )
        .finish()
}
