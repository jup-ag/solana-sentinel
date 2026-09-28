pub mod parser;
pub mod rules;

use crate::parser::{PriceFeedSource, PythPriceFeedParser};
use rules::oracle_staleness::OracleStaleness;
use rules::reference_price_drift::ReferencePriceDrift;
use solana_pubkey::{pubkey, Pubkey};
use solana_sentinel::{PollRules, Program, ProgramSpec, StreamRules};
use std::time::Duration;

/// Cadence of the price-feed poll used by staleness checks.
pub const ORACLE_PRICE_FEED_REFETCH_INTERVAL: Duration = Duration::from_secs(5);

/// Snake_case identifier this program is known by in config and logs.
pub const PROGRAM: Program = Program::new("pyth");

/// Mainnet pubkeys for the Pyth oracle programs.
pub const PUBKEY_LEGACY: Pubkey = pubkey!("FsJ3A3u2vn5cTVofAjvy6y5kwABJAqYWpe4975bi2epH");
pub const PUBKEY_NEW: Pubkey = pubkey!("rec5EKMGg6MxZYaMdyBfgwp4d5rB9T1VQH5pJv5LtFJ");

/// Drift is stream-fed (every publish); staleness is poll-fed (silence is the signal).
pub fn spec() -> ProgramSpec {
    ProgramSpec::builder(PROGRAM)
        .account(
            PythPriceFeedParser,
            StreamRules::new().register::<ReferencePriceDrift>(),
        )
        .poll(
            PriceFeedSource,
            ORACLE_PRICE_FEED_REFETCH_INTERVAL,
            PollRules::new().register::<OracleStaleness>(),
        )
        .finish()
}
