//! Stablecoin peg checks. Not an on-chain program: prices are read from DEX
//! pool accounts (Meteora DLMM, Orca Whirlpool, Raydium CLMM) on a poll.

pub mod depeg_monitor;
pub mod parser;

use crate::stable_coin::parser::PoolSource;
use solana_sentinel::{PollRules, Program, ProgramSpec};
use std::time::Duration;

pub const PROGRAM: Program = Program::new("stable_coin");
pub const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(60);

pub fn spec() -> ProgramSpec {
    ProgramSpec::new(PROGRAM).poll(
        PoolSource,
        DEFAULT_POLL_INTERVAL,
        PollRules::new().register::<depeg_monitor::DepegMonitor>(),
    )
}
