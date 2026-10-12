//! Account rule: a mint's supply grew by at least its threshold (in basis
//! points) between two consecutive updates. Catches unexpected minting.

use crate::spl_token::{fetch_mints, PROGRAM};
use async_trait::async_trait;
use serde::Deserialize;
use shipstern_spl_token_parser::account::Account as TokenState;
use shipstern_spl_token_parser::TokenProgramState;
use solana_pubkey::Pubkey;
use solana_sentinel::utility::pubkey::deserialize_pubkey_map;
use solana_sentinel::{
    Alert, BootstrapCtx, Deps, RuleId, Severity, StateCache, StreamCtx, StreamRule, Subscription,
};
use std::collections::HashMap;

#[derive(Debug, Deserialize)]
pub struct SupplyIncreaseConfig {
    /// Mint -> threshold in basis points (100 = 1%).
    #[serde(deserialize_with = "deserialize_pubkey_map")]
    pub thresholds: HashMap<Pubkey, u16>,
}

#[derive(Debug)]
pub struct SupplyIncrease {
    thresholds: HashMap<Pubkey, u16>,
    /// Last seen supply per mint; an unknown mint becomes the baseline and is skipped once.
    supply: StateCache<Pubkey, u64>,
}

/// Growth from `previous` to `current` in basis points; `current >= previous > 0`.
fn increase_bps(previous: u64, current: u64) -> u128 {
    u128::from(current - previous) * 10_000 / u128::from(previous)
}

#[async_trait]
impl StreamRule for SupplyIncrease {
    const ID: RuleId = RuleId::new(PROGRAM, "supply_increase");
    type Config = SupplyIncreaseConfig;
    type Event = TokenProgramState;

    fn subscription(cfg: &Self::Config) -> Subscription {
        Subscription::new().accounts(cfg.thresholds.keys().copied())
    }

    fn build(cfg: Self::Config, _deps: &Deps) -> anyhow::Result<Self> {
        anyhow::ensure!(!cfg.thresholds.is_empty(), "thresholds must not be empty");
        Ok(Self {
            thresholds: cfg.thresholds,
            supply: StateCache::default(),
        })
    }

    async fn bootstrap(&mut self, ctx: &BootstrapCtx) -> anyhow::Result<()> {
        let mints: Vec<Pubkey> = self.thresholds.keys().copied().collect();
        let fetched = fetch_mints(&ctx.rpc, &mints).await?;
        self.supply
            .seed(fetched.iter().map(|(pk, mint)| (*pk, mint.supply)));
        Ok(())
    }

    fn evaluate(&self, ev: &Self::Event, ctx: &StreamCtx) -> Option<Alert> {
        let TokenState::Mint(mint) = ev.account.as_ref()? else {
            return None;
        };
        let pubkey = ctx.account?;
        let threshold = *self.thresholds.get(&pubkey)?;
        let previous = self.supply.replace(Self::ID, pubkey, mint.supply)?;
        if previous == 0 || mint.supply <= previous {
            return None;
        }
        let bps = increase_bps(previous, mint.supply);
        if bps < u128::from(threshold) {
            return None;
        }
        Some(
            Alert::for_rule(Self::ID)
                .severity(Severity::Critical)
                .message(format!(
                    "{pubkey} supply rose {}.{:02}% ({previous} -> {}), threshold {threshold} bps",
                    bps / 100,
                    bps % 100,
                    mint.supply
                ))
                .meta("mint", pubkey)
                .meta("previous_supply", previous)
                .meta("current_supply", mint.supply)
                .meta("increase_bps", bps)
                .meta("threshold_bps", threshold)
                .meta("slot", ctx.slot)
                .key([pubkey.to_string(), mint.supply.to_string()])
                .scope(ctx.tx_scope())
                .build(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use shipstern_spl_token_parser::AccountParser;
    use solana_program_option::COption;
    use solana_program_pack::Pack;
    use solana_sentinel::assert_fires;
    use solana_sentinel::testing::{account_update, config_from_toml, parse, stream_ctx};
    use spl_token_interface::state::Mint;

    fn mint_data(supply: u64) -> Vec<u8> {
        let state = Mint {
            mint_authority: COption::Some(Pubkey::new_unique()),
            supply,
            decimals: 6,
            is_initialized: true,
            freeze_authority: COption::None,
        };
        let mut data = vec![0u8; Mint::LEN];
        Mint::pack(state, &mut data).unwrap();
        data
    }

    #[tokio::test]
    async fn fires_once_supply_grows_past_threshold() {
        let mint = Pubkey::new_unique();
        let cfg = config_from_toml(&format!("[thresholds]\n\"{mint}\" = 500"));
        let rule = SupplyIncrease::build(cfg, &Deps::default()).unwrap();
        let mut ctx = stream_ctx(1, Some("sig"));
        ctx.account = Some(mint);
        let update = |supply| account_update(mint, spl_token_interface::ID, mint_data(supply));

        // First sighting is the baseline; 1% is under the 5% threshold; 10% fires.
        let baseline = parse(&AccountParser, &update(1_000_000)).await;
        assert!(rule.evaluate(&baseline, &ctx).is_none());
        let small = parse(&AccountParser, &update(1_010_000)).await;
        assert!(rule.evaluate(&small, &ctx).is_none());
        let alert = assert_fires!(rule, parse(&AccountParser, &update(1_111_000)).await, ctx);
        assert_eq!(alert.metadata["increase_bps"], "1000");
    }
}
