//! Pyth oracle: legacy price accounts and pull-oracle `PriceUpdateV2` accounts.
//! Drift is stream-fed (every publish); staleness is poll-fed (silence is the signal).

pub mod oracle_staleness;
pub mod parser;
pub mod reference_price_drift;

use crate::pyth::parser::{PriceFeedSource, PythPriceFeedParser};
use solana_pubkey::Pubkey;
use solana_sentinel::{PollRules, Program, ProgramSpec, StreamRules};
use std::time::Duration;

pub const PROGRAM: Program = Program::new("pyth");
pub const PYTH_LEGACY_PROGRAM_ID: Pubkey =
    Pubkey::from_str_const("FsJ3A3u2vn5cTVofAjvy6y5kwABJAqYWpe4975bi2epH");
/// Receiver program; owns every `PriceUpdateV2`, sponsored push feeds included.
pub const PYTH_RECEIVER_PROGRAM_ID: Pubkey =
    Pubkey::from_str_const("rec5EKMGg6MxZYaMdyBfgwp4d5rB9T1VQH5pJv5LtFJ");
pub const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(5);

pub fn spec() -> ProgramSpec {
    ProgramSpec::new(PROGRAM)
        .account(
            PythPriceFeedParser,
            StreamRules::new().register::<reference_price_drift::ReferencePriceDrift>(),
        )
        .poll(
            PriceFeedSource,
            DEFAULT_POLL_INTERVAL,
            PollRules::new().register::<oracle_staleness::OracleStaleness>(),
        )
}
