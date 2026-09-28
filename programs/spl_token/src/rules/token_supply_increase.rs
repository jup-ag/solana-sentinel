use crate::parser::TokenProgramState;
use async_trait::async_trait;
use serde::Deserialize;
use shipstern_spl_token_parser::account::Account as SplAccount;
use shipstern_spl_token_parser::Mint;
use solana_pubkey::Pubkey;
use solana_sentinel::utility::math::token_amount_to_ui_amount_compact;
use solana_sentinel::utility::pubkey::deserialize_pubkey_map;
use solana_sentinel::utility::rpc::try_fetch_mints_from_rpc;
use solana_sentinel::{
    Alert, BootstrapCtx, GrpcRule, Prefilter, RuleDef, RuleId, Severity, StateCache, StreamCtx,
};
use std::collections::{BTreeMap, HashMap};
use tracing::{debug, info};

#[derive(Debug, Deserialize)]
pub struct TokenSupplyIncreaseConfig {
    /// Per-mint threshold in basis points. 100 bps = 1%.
    #[serde(deserialize_with = "deserialize_pubkey_map")]
    pub thresholds: HashMap<Pubkey, u16>,
}

#[derive(Debug)]
pub struct TokenSupplyIncrease {
    config: TokenSupplyIncreaseConfig,
    /// Last-seen mint state; a mint missing at bootstrap is stored on its first update.
    mints: StateCache<Pubkey, Mint>,
}

impl TokenSupplyIncrease {
    fn supply_increase_bps(previous_supply: u64, current_supply: u64) -> u128 {
        let delta = current_supply - previous_supply;
        u128::from(delta) * 10_000 / u128::from(previous_supply)
    }

    fn percent_increase(previous_supply: u64, current_supply: u64) -> f64 {
        let delta = current_supply - previous_supply;
        delta as f64 / previous_supply as f64 * 100.0
    }
}

#[async_trait]
impl RuleDef for TokenSupplyIncrease {
    const ID: RuleId = RuleId::new(crate::PROGRAM, "token_supply_increase");
    type Config = TokenSupplyIncreaseConfig;
    type Deps = ();

    fn build(config: Self::Config, _deps: &()) -> anyhow::Result<Self> {
        Ok(Self {
            config,
            mints: StateCache::default(),
        })
    }

    async fn bootstrap(&mut self, ctx: &BootstrapCtx) -> anyhow::Result<()> {
        let mints: Vec<Pubkey> = self.config.thresholds.keys().copied().collect();
        let fetched = try_fetch_mints_from_rpc(&ctx.rpc, &mints, None).await?;

        let supplies: BTreeMap<String, u64> = fetched
            .iter()
            .map(|(pk, mint)| (pk.to_string(), mint.supply))
            .collect();
        info!(
            rule_id = %Self::ID,
            fetched = fetched.len(),
            requested = mints.len(),
            supplies = ?supplies,
            "Rule bootstrapped"
        );

        self.mints.seed(fetched);
        Ok(())
    }
}

impl GrpcRule for TokenSupplyIncrease {
    type Event = TokenProgramState;

    fn static_prefilter(cfg: &Self::Config) -> Prefilter {
        Prefilter::new().accounts(cfg.thresholds.keys().copied())
    }

    fn evaluate(&self, parsed: &TokenProgramState, ctx: &StreamCtx) -> Option<Alert> {
        let mint_pubkey = ctx.account?;
        let Some(SplAccount::Mint(mint_state)) = &parsed.account else {
            return None;
        };

        let threshold_bps = *self.config.thresholds.get(&mint_pubkey)?;
        let previous_supply = self
            .mints
            .replace(Self::ID, mint_pubkey, mint_state.clone())?
            .supply;
        let current_supply = mint_state.supply;

        if current_supply <= previous_supply || previous_supply == 0 {
            return None;
        }

        let percent_increase_bps = Self::supply_increase_bps(previous_supply, current_supply);
        let percent_increase = Self::percent_increase(previous_supply, current_supply);

        debug!(
            rule_id = %Self::ID,
            mint = %mint_pubkey,
            previous_supply,
            current_supply,
            percent_increase,
            percent_increase_bps,
            threshold_bps,
            "Computed supply increase"
        );

        if percent_increase_bps < u128::from(threshold_bps) {
            return None;
        }

        let decimals = mint_state.decimals as u8;
        Some(
            Alert::for_rule(Self::ID)
                .severity(Severity::Critical)
                .message(format!(
                    "Token supply increased from {previous_supply} to {current_supply} \
                     ({percent_increase:.4}% increase, threshold {threshold_bps} bps)"
                ))
                .meta("mint", mint_pubkey)
                .meta(
                    "mint_authority",
                    mint_state
                        .mint_authority
                        .as_ref()
                        .map_or_else(|| "none".to_string(), ToString::to_string),
                )
                .meta(
                    "previous_supply",
                    token_amount_to_ui_amount_compact(previous_supply, decimals),
                )
                .meta(
                    "current_supply",
                    token_amount_to_ui_amount_compact(current_supply, decimals),
                )
                .meta("percent_increase", format!("{percent_increase:.4}%"))
                .meta("threshold_bps", threshold_bps)
                .key([mint_pubkey.to_string(), current_supply.to_string()])
                .scope(ctx.tx_scope())
                .build(),
        )
    }
}
