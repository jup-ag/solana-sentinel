pub mod parser;
pub mod rules;

use crate::parser::CollateralSource;
use interface::{PollRules, Program, ProgramSpec};
use rules::jup_usd_collaterization::JupUsdCollaterization;
use std::time::Duration;

pub const TOKEN_RESERVES_REFETCH_INTERVAL: Duration = Duration::from_secs(60);

/// Snake_case identifier this program is known by in config and logs.
pub const PROGRAM: Program = Program::new("jup_usd");

pub fn spec() -> ProgramSpec {
    ProgramSpec::builder(PROGRAM)
        .poll(
            CollateralSource,
            TOKEN_RESERVES_REFETCH_INTERVAL,
            PollRules::new().register::<JupUsdCollaterization>(),
        )
        .finish()
}
