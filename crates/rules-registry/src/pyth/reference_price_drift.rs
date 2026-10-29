//! Account rule: a feed's published price moves more than `drift_bps` away from a
//! configured reference price (e.g. a stablecoin feed against 1.00).

use crate::pyth::parser::PriceFeedUpdate;
use crate::pyth::PROGRAM;
use serde::Deserialize;
use solana_pubkey::Pubkey;
use solana_sentinel::utility::math::to_ui_price_compact;
use solana_sentinel::utility::pubkey::deserialize_pubkey_map;
use solana_sentinel::{Alert, Deps, RuleId, Severity, StreamCtx, StreamRule, Subscription};
use std::collections::HashMap;
use tracing::{debug, warn};

#[derive(Debug, Deserialize)]
pub struct ReferencePriceWithDrift {
    /// Asset ticker for alert metadata only.
    pub ticker: String,
    /// Raw integer on the feed's own scale (1.23 at expo -6 is 1_230_000); a wrong
    /// scale silently produces wrong drifts, so confirm each feed's expo.
    pub price: i64,
    /// The feed's exponent; only used to render prices in alerts.
    pub exponent: i32,
    /// Allowed drift; 1 bps = 0.01%.
    pub drift_bps: u32,
}

#[derive(Debug, Deserialize)]
pub struct ReferencePriceDriftConfig {
    /// Reference price and drift band per feed account.
    #[serde(deserialize_with = "deserialize_pubkey_map")]
    pub reference_prices: HashMap<Pubkey, ReferencePriceWithDrift>,
}

#[derive(Debug)]
pub struct ReferencePriceDrift {
    reference_prices: HashMap<Pubkey, ReferencePriceWithDrift>,
}

impl StreamRule for ReferencePriceDrift {
    const ID: RuleId = RuleId::new(PROGRAM, "reference_price_drift");
    type Config = ReferencePriceDriftConfig;
    type Event = PriceFeedUpdate;

    fn subscription(cfg: &Self::Config) -> Subscription {
        Subscription::new().accounts(cfg.reference_prices.keys().copied())
    }

    fn build(cfg: Self::Config, _deps: &Deps) -> anyhow::Result<Self> {
        anyhow::ensure!(
            !cfg.reference_prices.is_empty(),
            "reference_prices must not be empty"
        );
        for (pubkey, entry) in &cfg.reference_prices {
            anyhow::ensure!(
                entry.price > 0,
                "feed {pubkey} ({}) has non-positive reference price {}",
                entry.ticker,
                entry.price
            );
            if entry.drift_bps == 0 {
                warn!(%pubkey, ticker = %entry.ticker, "drift_bps is 0; any move will alert");
            }
        }
        Ok(Self {
            reference_prices: cfg.reference_prices,
        })
    }

    fn evaluate(&self, parsed: &PriceFeedUpdate, _ctx: &StreamCtx) -> Option<Alert> {
        let reference = self.reference_prices.get(&parsed.pubkey)?;
        let current_price = parsed.feed.price();
        debug!(
            oracle = %parsed.pubkey, current_price, reference = reference.price,
            drift_bps = reference.drift_bps, "oracle price vs reference"
        );
        let (exceeded, drift_pct) =
            is_drift_exceeded(current_price, reference.price, reference.drift_bps)?;
        if !exceeded {
            return None;
        }
        Some(
            Alert::for_rule(Self::ID)
                .severity(Severity::Critical)
                .message(format!(
                    "Oracle feed {} price drifted from reference price by {drift_pct:.4}%",
                    parsed.pubkey,
                ))
                .meta("oracle", parsed.pubkey)
                .meta("ticker", &reference.ticker)
                .meta(
                    "current_price_ui",
                    to_ui_price_compact(current_price, parsed.feed.exponent()),
                )
                .meta(
                    "reference_price_ui",
                    to_ui_price_compact(reference.price, reference.exponent),
                )
                .meta(
                    "allowed_drift_percent",
                    format!("{:.4}%", f64::from(reference.drift_bps) / 100.0),
                )
                .meta("drift_percent", format!("{drift_pct:.4}%"))
                .key([parsed.pubkey])
                .build(),
        )
    }
}

/// `(drift > band, signed drift %)`; both prices must be on the same scale.
pub fn is_drift_exceeded(current: i64, reference: i64, drift_bps: u32) -> Option<(bool, f64)> {
    if reference <= 0 {
        return None;
    }
    let drift_pct = (current - reference) as f64 / reference as f64 * 100.0;
    Some(((drift_pct * 100.0).abs() > f64::from(drift_bps), drift_pct))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pyth::parser::fixtures::price_update_v2;
    use crate::pyth::parser::PythPriceFeedParser;
    use crate::pyth::PYTH_RECEIVER_PROGRAM_ID;
    use solana_sentinel::assert_fires;
    use solana_sentinel::testing::{account_update, config_from_toml, parse, stream_ctx};

    #[test]
    fn drift_band_is_symmetric_and_exclusive() {
        let reference = 100_000_000; // 1.00 at expo -8
        assert_eq!(
            is_drift_exceeded(100_500_000, reference, 25),
            Some((true, 0.50))
        );
        assert_eq!(
            is_drift_exceeded(99_500_000, reference, 25),
            Some((true, -0.50))
        );
        assert_eq!(
            is_drift_exceeded(100_100_000, reference, 25),
            Some((false, 0.10))
        );
        // Exactly on the band is not a breach.
        assert_eq!(
            is_drift_exceeded(100_250_000, reference, 25),
            Some((false, 0.25))
        );
    }

    #[tokio::test]
    async fn streamed_feed_outside_band_fires() {
        let feed = Pubkey::new_unique();
        let cfg = config_from_toml(&format!(
            "[reference_prices]\n\"{feed}\" = {{ ticker = \"JUPUSD/USD\", price = 100000000, \
             exponent = -8, drift_bps = 25 }}"
        ));
        let rule = ReferencePriceDrift::build(cfg, &Deps::default()).unwrap();
        let update = |price| {
            account_update(
                feed,
                PYTH_RECEIVER_PROGRAM_ID,
                price_update_v2(price, -8, 1),
            )
        };

        let inside = parse(&PythPriceFeedParser, &update(100_100_000)).await;
        assert!(rule.evaluate(&inside, &stream_ctx(1, None)).is_none());
        let alert = assert_fires!(
            rule,
            parse(&PythPriceFeedParser, &update(99_000_000)).await,
            stream_ctx(1, None)
        );
        assert_eq!(alert.metadata["drift_percent"], "-1.0000%");
        assert_eq!(alert.metadata["current_price_ui"], "0.99");
    }
}
