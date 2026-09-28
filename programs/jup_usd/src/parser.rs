use anyhow::{ensure, Context};
use interface::{PollCtx, PollSource};
use solana_account::Account;
use solana_pubkey::Pubkey;
use spl_token_2022_interface::extension::StateWithExtensions;
use spl_token_2022_interface::state::{Account as TokenAccount, Mint};
use std::collections::HashMap;
use tracing::warn;
use utility::constants::{JUP_USD_MINT, SPL_TOKEN_2022_PROGRAM_ID, SPL_TOKEN_PROGRAM_ID};

#[derive(Debug)]
pub struct ReserveHolding {
    pub mint: Pubkey,
    pub amount: u64,
    pub custodian: Pubkey,
}

#[derive(Debug)]
pub struct CollateralSnapshot {
    pub jupusd_supply: u64,
    pub reserves: Vec<ReserveHolding>,
}

impl CollateralSnapshot {
    pub fn collateralization_ratio(&self) -> Option<f64> {
        if self.jupusd_supply == 0 {
            return None;
        }
        // Note: only USDC and USDTB reserves are considered collateral backing JUP-USD, so we sum them together here.
        let total_reserves: u128 = self.reserves.iter().map(|h| h.amount as u128).sum();
        Some(total_reserves as f64 / self.jupusd_supply as f64)
    }

    pub fn is_undercollateralized(&self) -> bool {
        match self.collateralization_ratio() {
            Some(ratio) => ratio < 1.0, // undercollateralized if ratio is less than 1: not enough reserves to back supply
            None => false,              // zero supply: nothing issued, so nothing to back
        }
    }
}

/// Poll feed: one cross-account snapshot per tick.
#[derive(Debug, Clone, Default)]
pub struct CollateralSource;

impl PollSource for CollateralSource {
    type Item = CollateralSnapshot;

    fn parse(&self, fetched: &HashMap<Pubkey, Account>, _ctx: &PollCtx) -> Vec<Self::Item> {
        match build_snapshot(fetched) {
            Ok(snapshot) => vec![snapshot],
            Err(e) => {
                warn!(error = %e, "jup_usd: failed to build collateral snapshot; skipping tick");
                Vec::new()
            }
        }
    }
}

/// Classify each fetched account by pubkey, parse it, and assemble the snapshot.
///
/// Classification is by identity (`pubkey == JUP_USD_MINT`), not by data length:
/// SPL Token 2022 mints and token accounts share a base size, so length can't
/// distinguish them. `StateWithExtensions::unpack` parses both classic SPL and
/// Token 2022 accounts (classic is just the no-extension case), so a single
/// parser path covers both program owners.
fn build_snapshot(accounts: &HashMap<Pubkey, Account>) -> anyhow::Result<CollateralSnapshot> {
    let mut reserves = Vec::with_capacity(accounts.len().saturating_sub(1));
    let mut supply = None;

    for (pubkey, raw) in accounts {
        if pubkey == &JUP_USD_MINT {
            supply = Some(parse_mint_supply(pubkey, raw)?);
        } else {
            reserves.push(parse_reserve(pubkey, raw)?);
        }
    }

    let supply = supply.context("JupUSD mint account not found in fetched accounts")?;
    Ok(CollateralSnapshot {
        jupusd_supply: supply,
        reserves,
    })
}

/// Parse mint supply irrespective of token program, via the Token 2022 unpacker which supports both.
fn parse_mint_supply(pubkey: &Pubkey, raw: &Account) -> anyhow::Result<u64> {
    ensure_token_program(pubkey, &raw.owner)?;
    let mint = StateWithExtensions::<Mint>::unpack(&raw.data)
        .with_context(|| format!("failed to parse JupUSD mint account {pubkey}"))?;
    Ok(mint.base.supply)
}

/// Parse a reserve holding from a classic SPL or Token 2022 token account.
fn parse_reserve(pubkey: &Pubkey, raw: &Account) -> anyhow::Result<ReserveHolding> {
    ensure_token_program(pubkey, &raw.owner)?;
    let acc = StateWithExtensions::<TokenAccount>::unpack(&raw.data)
        .with_context(|| format!("failed to parse reserve token account {pubkey}"))?;
    Ok(ReserveHolding {
        mint: acc.base.mint,
        amount: acc.base.amount,
        custodian: acc.base.owner,
    })
}

fn ensure_token_program(pubkey: &Pubkey, owner: &Pubkey) -> anyhow::Result<()> {
    ensure!(
        *owner == SPL_TOKEN_PROGRAM_ID || *owner == SPL_TOKEN_2022_PROGRAM_ID,
        "account {pubkey} has unexpected owner {owner}; expected a token program"
    );
    Ok(())
}
