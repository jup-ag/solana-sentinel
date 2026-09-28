use crate::parser::CollateralSnapshot;
use interface::{Alert, PollCtx, RpcRule, RuleDef, RuleId, Severity};
use serde::Deserialize;
use solana_pubkey::Pubkey;
use std::collections::{HashMap, HashSet};
use tracing::debug;
use utility::constants::{get_decimals_for_mint, JUP_USD_DECIMALS, USDC_MINT, USDTB_MINT};
use utility::math::{compact_amount, token_amount_to_ui_amount, token_amount_to_ui_amount_compact};
use utility::pubkey::{
    derive_spl_2022_ata_address, derive_spl_ata_address, deserialize_pubkey, deserialize_pubkey_map,
};

#[derive(Debug, Deserialize)]
pub enum ReserveTickers {
    USDC,
    USDTB,
}

#[derive(Debug, Deserialize)]
pub struct JupUsdCollaterizationConfig {
    /// JUP-USD mint.
    #[serde(deserialize_with = "deserialize_pubkey")]
    pub mint: Pubkey,
    /// Custodian wallets holding USDC / USDTB reserves, mapped to which reserve tokens they hold.
    #[serde(deserialize_with = "deserialize_pubkey_map")]
    pub custodian_wallets: HashMap<Pubkey, Vec<ReserveTickers>>,
}

#[derive(Debug)]
pub struct JupUsdCollaterization {
    config: JupUsdCollaterizationConfig,
}

impl RuleDef for JupUsdCollaterization {
    const ID: RuleId = RuleId::new(crate::PROGRAM, "jup_usd_collaterization");
    type Config = JupUsdCollaterizationConfig;
    type Deps = ();

    fn build(config: Self::Config, _deps: &()) -> anyhow::Result<Self> {
        Ok(Self { config })
    }
}

impl RpcRule for JupUsdCollaterization {
    type Item = CollateralSnapshot;

    /// Every custodian's reserve token account plus the mint for total supply.
    fn accounts(cfg: &Self::Config) -> HashSet<Pubkey> {
        let mut accounts = HashSet::new();
        for (wallet, reserves) in &cfg.custodian_wallets {
            for reserve in reserves {
                accounts.insert(match reserve {
                    ReserveTickers::USDC => derive_spl_ata_address(wallet, &USDC_MINT),
                    ReserveTickers::USDTB => derive_spl_2022_ata_address(wallet, &USDTB_MINT),
                });
            }
        }
        accounts.insert(cfg.mint);
        accounts
    }

    fn evaluate(&self, parsed: &CollateralSnapshot, _ctx: &PollCtx) -> Option<Alert> {
        let _ = &self.config;
        let ratio = parsed.collateralization_ratio()?;
        if ratio >= 1.0 {
            return None;
        }

        debug!(
            "JUP-USD is undercollateralized: jupusd_supply={} reserves={:?} \
             collaterization_ratio={}",
            parsed.jupusd_supply, parsed.reserves, ratio
        );

        let custodian_total_reserves = parsed.reserves.iter().map(|r| r.amount).sum::<u64>();

        let mut by_custodian: HashMap<String, Vec<String>> = HashMap::new();
        for reserve in &parsed.reserves {
            let amount = token_amount_to_ui_amount(
                reserve.amount,
                get_decimals_for_mint(&reserve.mint).unwrap_or(0),
            );
            if amount < 10f64 {
                continue; // skip small reserves
            }
            by_custodian
                .entry(reserve.custodian.to_string())
                .or_default()
                .push(format!("{} : {}", reserve.mint, compact_amount(amount)));
        }

        Some(
            Alert::for_rule(Self::ID)
                .severity(Severity::Critical)
                .message(format!(
                    "JUP-USD is undercollateralized: collateralization ratio is {ratio:.4}"
                ))
                .meta(
                    "jupusd_total_supply",
                    token_amount_to_ui_amount_compact(parsed.jupusd_supply, JUP_USD_DECIMALS),
                )
                // All reserves share JUP-USD's decimals.
                .meta(
                    "custodian_total_reserves",
                    token_amount_to_ui_amount_compact(custodian_total_reserves, JUP_USD_DECIMALS),
                )
                .metas(by_custodian.into_iter().map(|(custodian, lines)| {
                    (format!("custodian:\n{custodian}"), lines.join("\n"))
                }))
                .meta("collateralization_ratio", format!("{ratio:.4}"))
                // Constant key: the rule is global and the ratio fluctuates every
                // snapshot, so the cooldown period alone controls re-alert frequency.
                .key(["undercollateralized"])
                .build(),
        )
    }
}
