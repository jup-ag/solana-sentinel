pub mod programs;
pub mod rules;

use crate::programs::lending::LendingProgramIxParser;
use crate::programs::liquidity::LiquidityProgramStateParser;
use crate::rules::cache::JupLendCache;
use interface::{Program, ProgramSpec, StreamRules};
use rules::lending_whale_movement::LendWhaleMovement;
use rules::liquidity_auth_list_change::LiquidityAuthListChange;
use rules::liquidity_reserve_change::LiquidityReserveChange;
use rules::liquidity_reserve_solvency::LiquidityReserveSolvency;
use rules::liquidity_reserve_utilization_rate::LiquidityUtilizationRate;

/// Snake_case identifier this program is known by in config and logs.
pub const PROGRAM: Program = Program::new("jup_lend");

/// JupLend spans two on-chain programs: liquidity (account state) and lending (instructions).
/// Both share one reserve cache, fetched once before any rule is built.
pub fn spec() -> ProgramSpec {
    ProgramSpec::builder(PROGRAM)
        .deps::<JupLendCache>()
        .account(
            LiquidityProgramStateParser::new(),
            StreamRules::new()
                .register::<LiquidityUtilizationRate>()
                .register::<LiquidityReserveSolvency>()
                .register::<LiquidityReserveChange>()
                .register::<LiquidityAuthListChange>(),
        )
        .instruction(
            LendingProgramIxParser::new(),
            StreamRules::new().register::<LendWhaleMovement>(),
        )
        .finish()
}
