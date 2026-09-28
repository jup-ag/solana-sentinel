pub mod parser;
pub mod rules;

use crate::parser::SquadsV4IxParser;
use interface::{Program, ProgramSpec, StreamRules};
use rules::new_member_addition::NewMemberAddition;
use rules::new_proposal::NewProposal;
use rules::threshold_change::ThresholdChange;
use rules::timelock_change::TimelockChange;
use solana_pubkey::{pubkey, Pubkey};

/// Snake_case identifier this program is known by in config and logs.
pub const PROGRAM: Program = Program::new("squads_v4");

/// Mainnet pubkey for the Squads v4 multisig program.
pub const PUBKEY: Pubkey = pubkey!("SQDS4ep65T869zMMBKyuUq6aD6EgTu8psMjkvj52pCf");

pub fn spec() -> ProgramSpec {
    ProgramSpec::builder(PROGRAM)
        .instruction(
            SquadsV4IxParser {},
            StreamRules::new()
                .register::<NewMemberAddition>()
                .register::<NewProposal>()
                .register::<ThresholdChange>()
                .register::<TimelockChange>(),
        )
        .finish()
}
