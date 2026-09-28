use crate::parser::PriceFeedUpdate;
use interface::{Alert, GrpcRule, Prefilter, RuleDef, RuleId, Severity, StreamCtx};
use serde::Deserialize;
use solana_pubkey::Pubkey;
use std::collections::HashMap;
use tracing::{debug, warn};
use utility::math::to_ui_price_compact;
use utility::pubkey::deserialize_pubkey_map;

#[derive(Debug)]
pub struct ReferencePriceDrift {
    config: ReferencePriceDriftConfig,
}
#[derive(Debug, Deserialize)]
pub struct ReferencePriceWithDrift {
    /// Asset ticker — for human-readable alert metadata; not used in logic.
    pub ticker: String,
    /// Reference price as a raw integer on the *feed's own* scale (value = price * 10^expo).
    /// Must match the feed's expo: e.g. 1.23 at expo -6 is 1_230_000. A mismatch
    /// silently produces wrong drifts, so confirm each feed's expo.
    pub price: i64,
    /// Exponent of the feed's price (negative for decimal scale). Used only for human-readable alert metadata; drift logic is on raw price.
    pub exponent: i32,
    /// Allowed drift before alerting. 1 bps = 0.01% = 0.0001.
    pub drift_bps: u32,
}

#[derive(Debug, Deserialize)]
pub struct ReferencePriceDriftConfig {
    /// Per-feed reference price + drift band, keyed by base58 price-feed pubkey.
    #[serde(deserialize_with = "deserialize_pubkey_map")]
    pub reference_prices: HashMap<Pubkey, ReferencePriceWithDrift>,
}

impl RuleDef for ReferencePriceDrift {
    const ID: RuleId = RuleId::new(crate::PROGRAM, "reference_price_drift");
    type Config = ReferencePriceDriftConfig;
    type Deps = ();

    fn build(config: Self::Config, _deps: &()) -> anyhow::Result<Self> {
        // Validate at startup so misconfiguration fails loudly rather than
        // silently disabling the check at evaluation time.
        for (pubkey, entry) in &config.reference_prices {
            if entry.price <= 0 {
                anyhow::bail!(
                    "reference_price_drift: feed {pubkey} ({}) has non-positive reference price \
                     {}; a reference price must be > 0",
                    entry.ticker,
                    entry.price
                );
            }
            if entry.drift_bps == 0 {
                warn!(
                    %pubkey,
                    ticker = %entry.ticker,
                    "reference_price_drift: drift_bps is 0; any price not exactly equal to the \
                     reference will alert"
                );
            }
        }

        Ok(Self { config })
    }
}

impl GrpcRule for ReferencePriceDrift {
    type Event = PriceFeedUpdate;

    fn static_prefilter(cfg: &Self::Config) -> Prefilter {
        Prefilter::new().accounts(cfg.reference_prices.keys().copied())
    }

    fn evaluate(&self, parsed: &PriceFeedUpdate, _ctx: &StreamCtx) -> Option<Alert> {
        let oracle_pubkey = parsed.pubkey;
        let reference = self.config.reference_prices.get(&oracle_pubkey)?;

        let current_price = parsed.feed.price();

        debug!(
            "oracle feed {} current price: {}, reference price: {}, allowed drift: {} bps",
            oracle_pubkey, current_price, reference.price, reference.drift_bps
        );

        let (is_exceeded, drift_pct) =
            is_drift_exceeded(current_price, reference.price, reference.drift_bps)?;

        if !is_exceeded {
            return None;
        }

        Some(
            Alert::for_rule(Self::ID)
                .severity(Severity::Critical)
                .message(format!(
                    "Oracle feed {} price drifted from reference price by {:.4}%",
                    oracle_pubkey, drift_pct,
                ))
                .meta("oracle", oracle_pubkey)
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
                    format!("{:.4}%", (reference.drift_bps as f64) / 100.0),
                )
                .meta("drift_percent", format!("{:.4}%", drift_pct))
                .key([oracle_pubkey])
                .build(),
        )
    }
}

/// Both prices MUST be on the same scale
pub fn is_drift_exceeded(
    current_price: i64,
    reference_price: i64,
    configured_drift_bps: u32,
) -> Option<(bool, f64)> {
    // A non-positive reference can't define a meaningful drift band.
    if reference_price <= 0 {
        return None;
    }

    let drift = current_price - reference_price;

    let drift_pct = (drift as f64) / (reference_price as f64) * 100.0; // convert to percentage

    let drift_bps = drift_pct * 100.0; // convert to bps

    Some((drift_bps.abs() > configured_drift_bps as f64, drift_pct))
}

#[cfg(test)]
mod tests {
    use super::*;
    use utility::math::to_ui_price;

    #[test]
    fn test_is_drift_exceeded() {
        // 25 bps = 0.25%. Bps is a ratio; price decimal scale is irrelevant.
        let drift_bps = 25;

        // Reference price 1.00 (8-decimal scale)
        let reference_price = 100_000_000;

        // 1.005 -> 50 bps above, exceeds 25
        assert_eq!(
            is_drift_exceeded(100_500_000, reference_price, drift_bps),
            Some((true, 0.50))
        );

        // 1.001 -> 10 bps above, within 25
        assert_eq!(
            is_drift_exceeded(100_100_000, reference_price, drift_bps),
            Some((false, 0.10))
        );

        // 0.995 -> 50 bps below, exceeds 25 (symmetric via abs)
        assert_eq!(
            is_drift_exceeded(99_500_000, reference_price, drift_bps),
            Some((true, -0.50))
        );

        // 0.999 -> 10 bps below, within 25
        assert_eq!(
            is_drift_exceeded(99_900_000, reference_price, drift_bps),
            Some((false, -0.10))
        );

        // Exactly 25 bps -> boundary. `>` means NOT exceeded.
        // 1.0025 == 25 bps exactly
        assert_eq!(
            is_drift_exceeded(100_250_000, reference_price, drift_bps),
            Some((false, 0.25))
        );

        // Non-positive reference -> None
        assert_eq!(is_drift_exceeded(90_000_000, -100_000_000, drift_bps), None);

        // Zero reference -> None (division-by-zero guard)
        assert_eq!(is_drift_exceeded(100_000_000, 0, drift_bps), None);
    }

    #[test]
    fn test_to_ui_price() {
        fn approx(got: f64, want: f64) {
            assert!(
                (got - want).abs() <= want.abs() * 1e-12,
                "got {got}, want {want}"
            );
        }

        // 8-decimal scale: 123.45678901 -> 12345678901
        approx(to_ui_price(12_345_678_901, -8), 123.45678901);

        // 6-decimal scale: 1.23 -> 1_230_000
        approx(to_ui_price(1_230_000, -6), 1.23);

        // small scale: 123 @ -2 -> 1.23
        approx(to_ui_price(123, -2), 1.23);
    }
}
