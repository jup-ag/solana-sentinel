use crate::programs::liquidity::{LiquidityStateUpdate, TokenReserve};
use crate::rules::cache::JupLendCache;
use interface::{Alert, GrpcRule, Prefilter, RuleDef, RuleId, Severity, StreamCtx};
use serde::Deserialize;
use solana_pubkey::Pubkey;
use std::collections::HashMap;
use tracing::debug;
use utility::pubkey::deserialize_pubkey_map;

/// Reserves not listed in `thresholds` alert above this utilization (bps).
const DEFAULT_THRESHOLD_BPS: u16 = 9500;

#[derive(Debug, Deserialize)]
pub struct LiquidityUtilizationRateConfig {
    /// Token reserve pubkey -> utilization threshold in basis points (8000 = 80%).
    #[serde(deserialize_with = "deserialize_pubkey_map")]
    pub thresholds: HashMap<Pubkey, u16>,
}

#[derive(Debug)]
pub struct LiquidityUtilizationRate {
    config: LiquidityUtilizationRateConfig,
}

impl RuleDef for LiquidityUtilizationRate {
    const ID: RuleId = RuleId::new(
        crate::PROGRAM,
        "jup_lend_liquidity_reserve_utilization_rate",
    );
    type Config = LiquidityUtilizationRateConfig;
    type Deps = JupLendCache;

    fn build(config: Self::Config, _deps: &JupLendCache) -> anyhow::Result<Self> {
        Ok(Self { config })
    }
}

impl GrpcRule for LiquidityUtilizationRate {
    type Event = LiquidityStateUpdate;

    /// Every reserve, configured or not, so unlisted reserves get the default threshold.
    fn static_prefilter(_cfg: &Self::Config) -> Prefilter {
        TokenReserve::subscription()
    }

    fn evaluate(&self, parsed: &LiquidityStateUpdate, _ctx: &StreamCtx) -> Option<Alert> {
        let LiquidityStateUpdate::TokenReserve {
            reserve_pubkey,
            reserve,
            ..
        } = parsed
        else {
            return None;
        };

        let threshold_bps = self
            .config
            .thresholds
            .get(reserve_pubkey)
            .copied()
            .unwrap_or(DEFAULT_THRESHOLD_BPS);
        let utilization_bps = reserve.last_utilization;

        if utilization_bps <= threshold_bps {
            return None;
        }

        let detail = format!(
            "utilization rate {}% exceeds threshold {}%",
            utilization_bps as f64 / 100.0,
            threshold_bps as f64 / 100.0
        );
        debug!(reserve = %reserve_pubkey, %detail, "jup_lend: token reserve above utilization threshold");

        Some(
            Alert::for_rule(Self::ID)
                .severity(Severity::Warn)
                .message("JUP Lend: utilization rate exceeds threshold")
                .meta(format!("reserve :\n{reserve_pubkey}"), detail)
                .key([reserve_pubkey])
                // High utilization is sustained, not per-tx: a Tx scope would make every pod re-alert on a new signature after cooldown.
                .build(),
        )
    }
}
