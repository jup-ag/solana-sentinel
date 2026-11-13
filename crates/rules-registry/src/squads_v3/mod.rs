//! Squads v3 (squads-mpl) multisig, over an instruction parser generated from the
//! program's IDL.

pub mod parser;

use solana_pubkey::Pubkey;
use solana_sentinel::{Program, ProgramSpec};

pub const PROGRAM: Program = Program::new("squads_v3");
pub const SQUADS_V3_PROGRAM_ID: Pubkey =
    Pubkey::from_str_const("SMPLecH534NA9acpos4G6x7uf3LWbCAwZQE9e8ZekMu");

pub fn spec() -> ProgramSpec {
    ProgramSpec::new(PROGRAM)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn idl_address_matches_program_id() {
        assert_eq!(
            Pubkey::new_from_array(parser::squads_mpl::PROGRAM_ID),
            SQUADS_V3_PROGRAM_ID
        );
    }
}
