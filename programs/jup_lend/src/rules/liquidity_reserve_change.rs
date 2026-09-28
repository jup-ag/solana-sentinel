use crate::programs::liquidity::{LiquidityStateUpdate, TokenReserve};
use crate::rules::cache::{normalize, JupLendCache};
use interface::{Alert, DedupScope, GrpcRule, Prefilter, RuleDef, RuleId, Severity, StreamCtx};
use parking_lot::Mutex;
use serde::Deserialize;
use solana_pubkey::Pubkey;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tracing::debug;
use utility::math::token_amount_to_ui_amount_compact_u128;

fn default_window_secs() -> u64 {
    900
}

#[derive(Debug, Deserialize)]
pub struct LiquidityReserveChangeConfig {
    /// Alert when net liquidity moves at least this many bps (1000 = 10%), in one update or cumulatively over the window.
    pub threshold_bps: u32,
    /// Drift window in seconds; each reserve's baseline rolls forward once this old, so gradual drains still alert.
    #[serde(default = "default_window_secs")]
    pub window_secs: u64,
}

/// Per-reserve state: the last observation plus a window baseline for drift.
#[derive(Debug, Clone, Copy)]
struct ObservedTokenReserve {
    /// Net liquidity at the last observation.
    liquidity: i128,
    /// Net liquidity when the current drift window opened.
    baseline_liquidity: i128,
    baseline_at: Instant,
}

/// One update's fresh net liquidity next to the two references it is judged against.
#[derive(Debug, Clone, Copy)]
struct Observation {
    curr_liquidity: i128,
    prev_liquidity: i128,
    baseline_liquidity: i128,
}

#[derive(Debug)]
pub struct LiquidityReserveChange {
    config: LiquidityReserveChangeConfig,
    /// Last-seen state per reserve, kept privately since the solvency rule refreshes the shared cache on the same updates.
    observed: Mutex<HashMap<Pubkey, ObservedTokenReserve>>,
    cache: JupLendCache,
}

impl RuleDef for LiquidityReserveChange {
    const ID: RuleId = RuleId::new(crate::PROGRAM, "jup_lend_liquidity_reserve_change");
    type Config = LiquidityReserveChangeConfig;
    type Deps = JupLendCache;

    /// Seeds the baseline from the shared cache; a reserve first seen on the stream starts its window then.
    fn build(config: Self::Config, deps: &JupLendCache) -> anyhow::Result<Self> {
        let observed = deps
            .reserves()
            .iter()
            .map(|(pubkey, reserve)| {
                let liquidity = Self::net_liquidity(reserve);
                (
                    *pubkey,
                    ObservedTokenReserve {
                        liquidity,
                        baseline_liquidity: liquidity,
                        baseline_at: Instant::now(),
                    },
                )
            })
            .collect();
        Ok(Self {
            config,
            observed: Mutex::new(observed),
            cache: deps.clone(),
        })
    }
}

impl LiquidityReserveChange {
    /// Net liquidity in token units (supplied minus borrowed); negative means bad debt.
    fn net_liquidity(reserve: &TokenReserve) -> i128 {
        let (supplied, borrowed) = normalize(reserve);
        supplied as i128 - borrowed as i128
    }

    fn fmt_liquidity(value: i128, decimals: Option<u8>) -> String {
        let sign = if value < 0 { "-" } else { "" };
        match decimals {
            Some(decimals) => format!(
                "{sign}{}",
                token_amount_to_ui_amount_compact_u128(value.unsigned_abs(), decimals)
            ),
            // Mint fetch failed at bootstrap; report base units rather than nothing.
            None => format!("{sign}{} base units", value.unsigned_abs()),
        }
    }

    fn observe(&self, reserve_pubkey: &Pubkey, current: &TokenReserve) -> Option<Observation> {
        let curr_liquidity = Self::net_liquidity(current);
        let window = Duration::from_secs(self.config.window_secs);

        let mut observed = self.observed.lock();
        match observed.entry(*reserve_pubkey) {
            Entry::Vacant(slot) => {
                slot.insert(ObservedTokenReserve {
                    liquidity: curr_liquidity,
                    baseline_liquidity: curr_liquidity,
                    baseline_at: Instant::now(),
                });
                None
            }
            Entry::Occupied(mut slot) => {
                let state = slot.get_mut();
                let prev_liquidity = state.liquidity;

                // An expired window restarts at the current level, keeping the baseline at most `window` old.
                if state.baseline_at.elapsed() >= window {
                    state.baseline_liquidity = curr_liquidity;
                    state.baseline_at = Instant::now();
                }

                let baseline_liquidity = state.baseline_liquidity;
                state.liquidity = curr_liquidity;

                Some(Observation {
                    curr_liquidity,
                    prev_liquidity,
                    baseline_liquidity,
                })
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn check_liquidity_change(
        &self,
        reserve_pubkey: &Pubkey,
        current: &TokenReserve,
        curr_liquidity: i128,
        prev_liquidity: i128,
        span: &str,
        tx_signature: &str,
        scope: DedupScope,
    ) -> Option<Alert> {
        // No meaningful baseline to express a percentage against.
        if prev_liquidity <= 0 {
            return None;
        }

        let change_bps = ((curr_liquidity - prev_liquidity) * 10_000) / prev_liquidity;
        if change_bps.unsigned_abs() < u128::from(self.config.threshold_bps) {
            return None;
        }

        let reserve_mint = Pubkey::new_from_array(current.mint.0);
        let direction = if change_bps < 0 { "dropped" } else { "rose" };
        let change_percent = (change_bps as f64 / 100.0).abs();

        debug!(
            reserve = %reserve_pubkey,
            mint = %reserve_mint,
            tx_signature,
            change_bps,
            curr_liquidity,
            prev_liquidity,
            span,
            "jup_lend: reserve liquidity changed beyond threshold",
        );

        let decimals = self.cache.reserve_mint_decimals(current);
        let prev_ui = Self::fmt_liquidity(prev_liquidity, decimals);
        let curr_ui = Self::fmt_liquidity(curr_liquidity, decimals);

        // A per-tx alert is keyed to its transaction; a drift alert shares one key across the window so re-alerts collapse.
        let key = match &scope {
            DedupScope::Tx(_) => [reserve_pubkey.to_string(), tx_signature.to_string()],
            _ => [reserve_pubkey.to_string(), span.to_string()],
        };

        Some(
            Alert::for_rule(Self::ID)
                .severity(Severity::Warn)
                .message(format!(
                    "JUP Lend: reserve liquidity {direction} {change_percent:.2}% {span} (from \
                     {prev_ui} to {curr_ui})"
                ))
                .meta("reserve", reserve_pubkey)
                .meta("reserve_mint", reserve_mint)
                .meta("previous_liquidity", prev_ui)
                .meta("current_liquidity", curr_ui)
                .meta("change_bps", change_bps)
                .meta("change_span", span)
                .meta("tx_sig", tx_signature)
                .key(key)
                .scope(scope)
                .build(),
        )
    }
}

impl GrpcRule for LiquidityReserveChange {
    type Event = LiquidityStateUpdate;

    fn static_prefilter(_cfg: &Self::Config) -> Prefilter {
        TokenReserve::subscription()
    }

    fn evaluate(&self, parsed: &LiquidityStateUpdate, _ctx: &StreamCtx) -> Option<Alert> {
        let LiquidityStateUpdate::TokenReserve {
            reserve_pubkey,
            reserve,
            tx_signature,
        } = parsed
        else {
            return None;
        };

        let observation = self.observe(reserve_pubkey, reserve)?;

        // Single-update jumps dedup per tx; drift is sustained and dedups fleet-wide, so the per-update check runs first.
        self.check_liquidity_change(
            reserve_pubkey,
            reserve,
            observation.curr_liquidity,
            observation.prev_liquidity,
            "in a single update",
            tx_signature,
            DedupScope::Tx(tx_signature.clone()),
        )
        .or_else(|| {
            self.check_liquidity_change(
                reserve_pubkey,
                reserve,
                observation.curr_liquidity,
                observation.baseline_liquidity,
                &format!("over the last {}s", self.config.window_secs),
                tx_signature,
                DedupScope::Monitor,
            )
        })
    }
}
