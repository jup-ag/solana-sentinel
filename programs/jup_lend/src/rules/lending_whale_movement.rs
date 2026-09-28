use crate::programs::lending::{
    LendingMovementIx, MovementDirection, VaultAmount, JUP_LENDING_PROGRAM_ID,
};
use crate::programs::liquidity::shares_to_tokens;
use crate::rules::cache::JupLendCache;
use interface::{Alert, GrpcRule, Prefilter, RuleDef, RuleId, Severity, StreamCtx};
use serde::{Deserialize, Serialize};
use tracing::warn;
use utility::math::token_amount_to_ui_amount_compact_u128;

const WHALE_SUPPLY_SHARE_BPS_FALLBACK: u64 = 100;

#[derive(Debug, Serialize, Deserialize)]
pub struct LendWhaleMovementConfig {
    /// A movement is a whale movement when it is at least this share of the
    /// reserve's normalized total supply, in basis points (100 = 1%). Comparing
    /// against the reserve's own supply keeps both sides in the same token
    /// denomination, so no oracle is needed to price the movement in USD.
    pub whale_supply_share_bps: Option<u64>,
}

#[derive(Debug)]
pub struct LendWhaleMovement {
    config: LendWhaleMovementConfig,
    cache: JupLendCache,
}

impl RuleDef for LendWhaleMovement {
    const ID: RuleId = RuleId::new(crate::PROGRAM, "jup_lend_whale_movement");
    type Config = LendWhaleMovementConfig;
    type Deps = JupLendCache;

    fn build(config: Self::Config, deps: &JupLendCache) -> anyhow::Result<Self> {
        Ok(Self {
            config,
            cache: deps.clone(),
        })
    }
}

impl LendWhaleMovement {
    pub fn whale_supply_share_bps(&self) -> u128 {
        self.config
            .whale_supply_share_bps
            .unwrap_or(WHALE_SUPPLY_SHARE_BPS_FALLBACK) as u128
    }
}

impl GrpcRule for LendWhaleMovement {
    type Event = LendingMovementIx;

    fn static_prefilter(_cfg: &Self::Config) -> Prefilter {
        Prefilter::new().tx_accounts_include([JUP_LENDING_PROGRAM_ID])
    }

    fn evaluate(&self, parsed: &LendingMovementIx, ctx: &StreamCtx) -> Option<Alert> {
        // A reverted transaction moved nothing.
        if !parsed.is_success()? {
            return None;
        }

        let amount = parsed.extract_vault_amount()?;
        let token_reserve_pubkey = parsed.extract_token_reserve_pubkey()?;

        let token_reserve = self.cache.reserve(&token_reserve_pubkey)?;

        // `redeem` withdrawals are in fTokens and convert via the exchange price; `withdraw` withdrawals are in tokens.
        let tokens = match amount {
            VaultAmount::Assets(tokens) => u128::from(tokens),
            VaultAmount::Shares(shares) => {
                shares_to_tokens(shares, token_reserve.supply_exchange_price)
            }
        };

        let total_supply = shares_to_tokens(
            token_reserve.total_supply_with_interest,
            token_reserve.supply_exchange_price,
        ) + u128::from(token_reserve.total_supply_interest_free);

        // A single position cannot exceed the reserve's supply. Anything larger means the
        // instruction argument was not a literal quantity (e.g. an unhandled sentinel), and
        // alerting on it would report nonsense.
        if tokens > total_supply {
            warn!(
                tx_sig = parsed.extract_tx_sig().unwrap_or_default(),
                reserve = %token_reserve_pubkey,
                tokens,
                total_supply,
                "movement exceeds reserve total supply; skipping whale check"
            );
            return None;
        }

        let threshold = total_supply * self.whale_supply_share_bps() / 10_000;

        if threshold == 0 || tokens < threshold {
            return None;
        }

        let whale_pubkey = parsed.extract_signer_pubkey()?;

        let action = match parsed.extract_direction()? {
            MovementDirection::Deposit => "deposit",
            MovementDirection::Withdraw => "withdraw",
        };

        let supply_share_pct = (tokens as f64 / total_supply as f64) * 100.0;

        let alert = Alert::for_rule(Self::ID)
            .severity(Severity::Info)
            .meta("whale", whale_pubkey)
            .meta("action", action)
            .meta("reserve", token_reserve_pubkey)
            .meta("supply_share_pct", format!("{supply_share_pct:.2}"))
            .key([
                whale_pubkey.to_string(),
                action.to_string(),
                token_reserve_pubkey.to_string(),
                tokens.to_string(),
            ])
            .scope(ctx.tx_scope());

        let alert = match self.cache.reserve_mint_decimals(&token_reserve) {
            Some(decimals) => {
                let ui_tokens = token_amount_to_ui_amount_compact_u128(tokens, decimals);
                let ui_total_supply =
                    token_amount_to_ui_amount_compact_u128(total_supply, decimals);
                alert
                    .message(format!(
                        "JUP Lend : Whale {action} detected: {ui_tokens} tokens \
                         ({supply_share_pct:.2}% of total reserve supply {ui_total_supply})"
                    ))
                    .meta("ui_amount", ui_tokens)
                    .meta("total_supply", ui_total_supply)
            }
            None => alert
                .message(format!(
                    "JUP Lend : Whale {action} detected: {tokens} (base units) tokens \
                     ({supply_share_pct:.2}% of total reserve supply {total_supply} (base units))"
                ))
                .meta("amount (base units)", tokens)
                .meta("total_supply (base_units)", total_supply),
        };

        Some(alert.build())
    }
}
